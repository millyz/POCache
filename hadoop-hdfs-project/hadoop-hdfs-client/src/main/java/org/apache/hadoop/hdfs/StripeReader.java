/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunk;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

// POCache added
import org.apache.hadoop.util.Time;

// POCache added
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * The reader for reading a complete {@link StripedBlockUtil.AlignedStripe}.
 * Note that an {@link StripedBlockUtil.AlignedStripe} may cross multiple
 * stripes with cellSize width.
 */
abstract class StripeReader {

  static class ReaderRetryPolicy {
    private int fetchEncryptionKeyTimes = 1;
    private int fetchTokenTimes = 1;

    void refetchEncryptionKey() {
      fetchEncryptionKeyTimes--;
    }

    void refetchToken() {
      fetchTokenTimes--;
    }

    boolean shouldRefetchEncryptionKey() {
      return fetchEncryptionKeyTimes > 0;
    }

    boolean shouldRefetchToken() {
      return fetchTokenTimes > 0;
    }
  }

  static class BlockReaderInfo {
    final BlockReader reader;
    final DatanodeInfo datanode;
    /**
     * when initializing block readers, their starting offsets are set to the
     * same number: the smallest internal block offsets among all the readers.
     * This is because it is possible that for some internal blocks we have to
     * read "backwards" for decoding purpose. We thus use this offset array to
     * track offsets for all the block readers so that we can skip data if
     * necessary.
     */
    long blockReaderOffset;
    /**
     * We use this field to indicate whether we should use this reader. In case
     * we hit any issue with this reader, we set this field to true and avoid
     * using it for the next stripe.
     */
    boolean shouldSkip = false;

    BlockReaderInfo(BlockReader reader, DatanodeInfo dn, long offset) {
      this.reader = reader;
      this.datanode = dn;
      this.blockReaderOffset = offset;
    }

    void setOffset(long offset) {
      this.blockReaderOffset = offset;
    }

    void skip() {
      this.shouldSkip = true;
    }
  }

  protected final Map<Future<Void>, Integer> futures = new HashMap<>();
  protected final AlignedStripe alignedStripe;
  protected final CompletionService<Void> service;
  protected final LocatedBlock[] targetBlocks;
  protected final CorruptedBlocks corruptedBlocks;
  protected final BlockReaderInfo[] readerInfos;
  protected final ErasureCodingPolicy ecPolicy;
  protected final short dataBlkNum;
  protected final short parityBlkNum;
  protected final int cellSize;
  protected final RawErasureDecoder decoder;
  protected final DFSStripedInputStream dfsStripedInputStream;
  protected final Object infoLock = new Object(); // POCache added

  protected ECChunk[] decodeInputs;

  StripeReader(AlignedStripe alignedStripe,
      ErasureCodingPolicy ecPolicy, LocatedBlock[] targetBlocks,
      BlockReaderInfo[] readerInfos, CorruptedBlocks corruptedBlocks,
      RawErasureDecoder decoder,
      DFSStripedInputStream dfsStripedInputStream) {
    this.alignedStripe = alignedStripe;
    this.ecPolicy = ecPolicy;
    this.dataBlkNum = (short)ecPolicy.getNumDataUnits();
    this.parityBlkNum = (short)ecPolicy.getNumParityUnits();
    this.cellSize = ecPolicy.getCellSize();
    this.targetBlocks = targetBlocks;
    this.readerInfos = readerInfos;
    this.corruptedBlocks = corruptedBlocks;
    this.decoder = decoder;
    this.dfsStripedInputStream = dfsStripedInputStream;

    service = new ExecutorCompletionService<>(
            dfsStripedInputStream.getStripedReadsThreadPool());
  }

  /**
   * Prepare all the data chunks.
   */
  abstract void prepareDecodeInputs();

  /**
   * Prepare the parity chunk and block reader if necessary.
   */
  abstract boolean prepareParityChunk(int index);

  /**
   * Decode to get the missing data.
   * @throws IOException if the decoder is closed.
   */
  abstract void decode() throws IOException;

  /*
   * Default close do nothing.
   */
  void close() {
  }

  void updateState4SuccessRead(StripingChunkReadResult result) {
    Preconditions.checkArgument(
        result.state == StripingChunkReadResult.SUCCESSFUL);
    readerInfos[result.index].setOffset(alignedStripe.getOffsetInBlock()
        + alignedStripe.getSpanInBlock());
  }

  private void checkMissingBlocks() throws IOException {
    if (alignedStripe.missingChunksNum > parityBlkNum) {
      clearFutures();
      throw new IOException(alignedStripe.missingChunksNum
          + " missing blocks, the stripe is: " + alignedStripe
          + "; locatedBlocks is: " + dfsStripedInputStream.getLocatedBlocks());
    }
  }

  /**
   * We need decoding. Thus go through all the data chunks and make sure we
   * submit read requests for all of them.
   */
  private void readDataForDecoding() throws IOException {
    prepareDecodeInputs();
    for (int i = 0; i < dataBlkNum; i++) {
      Preconditions.checkNotNull(alignedStripe.chunks[i]);
      if (alignedStripe.chunks[i].state == StripingChunk.REQUESTED) {
        if (!readChunk(targetBlocks[i], i)) {
          alignedStripe.missingChunksNum++;
        }
      }
    }
    checkMissingBlocks();
  }

  void readParityChunks(int num) throws IOException {
    for (int i = dataBlkNum, j = 0; i < dataBlkNum + parityBlkNum && j < num;
         i++) {
      if (alignedStripe.chunks[i] == null) {
        if (prepareParityChunk(i) && readChunk(targetBlocks[i], i)) {
          j++;
        } else {
          alignedStripe.missingChunksNum++;
        }
      }
    }
    checkMissingBlocks();
  }

  private ByteBufferStrategy[] getReadStrategies(StripingChunk chunk) {
    if (chunk.useByteBuffer()) {
      ByteBufferStrategy strategy = new ByteBufferStrategy(
          chunk.getByteBuffer(), dfsStripedInputStream.getReadStatistics(),
          dfsStripedInputStream.getDFSClient());
      return new ByteBufferStrategy[]{strategy};
    }

    ByteBufferStrategy[] strategies =
        new ByteBufferStrategy[chunk.getChunkBuffer().getSlices().size()];
    for (int i = 0; i < strategies.length; i++) {
      ByteBuffer buffer = chunk.getChunkBuffer().getSlice(i);
      strategies[i] = new ByteBufferStrategy(buffer,
              dfsStripedInputStream.getReadStatistics(),
              dfsStripedInputStream.getDFSClient());
    }
    return strategies;
  }

  private int readToBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy strategy,
      ExtendedBlock currentBlock) throws IOException {
    final int targetLength = strategy.getTargetLength();
    int length = 0;
    try {
      while (length < targetLength) {
        int ret = strategy.readFromBlock(blockReader);
        if (ret < 0) {
          throw new IOException("Unexpected EOS from the reader");
        }
        length += ret;
      }
      return length;
    } catch (ChecksumException ce) {
      DFSClient.LOG.warn("Found Checksum error for "
          + currentBlock + " from " + currentNode
          + " at " + ce.getPos());
      // we want to remember which block replicas we have tried
      corruptedBlocks.addCorruptedBlock(currentBlock, currentNode);
      throw ce;
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception while reading from "
          + currentBlock + " of " + dfsStripedInputStream.getSrc() + " from "
          + currentNode, e);
      throw e;
    }
  }

  private Callable<Void> readCells(final BlockReader reader,
      final DatanodeInfo datanode, final long currentReaderOffset,
      final long targetReaderOffset, final ByteBufferStrategy[] strategies,
      final ExtendedBlock currentBlock) {
    return () -> {
      // reader can be null if getBlockReaderWithRetry failed or
      // the reader hit exception before
      if (reader == null) {
        throw new IOException("The BlockReader is null. " +
            "The BlockReader creation failed or the reader hit exception.");
      }
      Preconditions.checkState(currentReaderOffset <= targetReaderOffset);
      if (currentReaderOffset < targetReaderOffset) {
        long skipped = reader.skip(targetReaderOffset - currentReaderOffset);
        Preconditions.checkState(
            skipped == targetReaderOffset - currentReaderOffset);
      }

      for (ByteBufferStrategy strategy : strategies) {
        readToBuffer(reader, datanode, strategy, currentBlock);
      }
      return null;
    };
  }
  
  // POCache added
  private Callable<Void> readDatCellsPcache(final LocatedBlock block, int chunkIndex, final StripingChunk chunk,
      BlockReaderInfo readerInfo, long targetReaderOffset,
      ByteBufferStrategy[] strategies) {
    return () -> {
      BlockReaderInfo curReaderInfo;

      if (readerInfo == null) {
        curReaderInfo = dfsStripedInputStream.createBlockReaderInfoPcache(block,
            targetReaderOffset, targetBlocks, chunkIndex);
        if (curReaderInfo == null) {
          chunk.state = StripingChunk.MISSING;
          DFSClient.LOG.warn("zmdebug: readDatCellsPcache(): next to return false, chunkIndex {}", chunkIndex);
          return null;
        }
        synchronized (infoLock) {
          readerInfos[chunkIndex] = curReaderInfo;
        }
      } else {
        curReaderInfo = readerInfo;
      }

      chunk.state = StripingChunk.PENDING;

      final BlockReader reader = curReaderInfo.reader;
      DatanodeInfo datanode = curReaderInfo.datanode;
      long currentReaderOffset = readerInfos[chunkIndex].blockReaderOffset;
      ExtendedBlock currentBlock = block.getBlock();

      // reader can be null if getBlockReaderWithRetry failed or
      // the reader hit exception before
      if (reader == null) {
        throw new IOException("The BlockReader is null. " +
            "The BlockReader creation failed or the reader hit exception.");
      }
      synchronized (reader) { // LQ: to prevent different threads using same reader
        Preconditions.checkState(currentReaderOffset <= targetReaderOffset);
        if (currentReaderOffset < targetReaderOffset) {
          long skipped = reader.skip(targetReaderOffset - currentReaderOffset);
          Preconditions.checkState(
              skipped == targetReaderOffset - currentReaderOffset,
              "wrong skipped: %s %s %s", skipped, targetReaderOffset, currentReaderOffset);
        }
        for (ByteBufferStrategy strategy : strategies) {
          readToBuffer(reader, datanode, strategy, currentBlock);
        }
      }
      return null;
    };
  }

  // POCache added
  private Callable<Void> readParCellsPcache(String fileID, int stripeIdxCur, int substripeIdx, JedisCluster jc) {
    return () -> {
      StripingChunk chunk;
      synchronized (infoLock) {
        chunk = alignedStripe.chunks[dataBlkNum];
      }

      if (chunk == null) {
        if (!prepareParityChunk(dataBlkNum)) {
          DFSClient.LOG.warn("zmdebug readParCellsPcache(): next to return false, substripeIdx={}", substripeIdx);
          return null;
        }
        synchronized (infoLock) {
          chunk = alignedStripe.chunks[dataBlkNum];
        }
      }

      chunk.state = StripingChunk.PENDING;
      ByteBufferStrategy[] strategies = getReadStrategies(chunk);
      String parKey = fileID + "_" + stripeIdxCur + "_" + substripeIdx + "_" + dataBlkNum;
      for (ByteBufferStrategy strategy : strategies) {
        strategy.getReadBuffer().put(jc.get(parKey.getBytes()));
      }
      return null;
    };
  }

  // POCache added to read parity from redis
  boolean readParChunkPcache(String fileID, int stripeIdxCur, int substripeIdx, JedisCluster jc) {
    prepareDecodeInputs();
    Callable<Void> readCallable = readParCellsPcache(fileID, stripeIdxCur, substripeIdx, jc);
    Future<Void> request = service.submit(readCallable);
    futures.put(request, (int)dataBlkNum);
    return true;
  }

  boolean readChunk(final LocatedBlock block, int chunkIndex)
      throws IOException {
    final StripingChunk chunk = alignedStripe.chunks[chunkIndex];
    if (block == null) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    if (readerInfos[chunkIndex] == null) {
      if (!dfsStripedInputStream.createBlockReader(block,
          alignedStripe.getOffsetInBlock(), targetBlocks,
          readerInfos, chunkIndex)) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }
    } else if (readerInfos[chunkIndex].shouldSkip) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    chunk.state = StripingChunk.PENDING;
    Callable<Void> readCallable = readCells(readerInfos[chunkIndex].reader,
        readerInfos[chunkIndex].datanode,
        readerInfos[chunkIndex].blockReaderOffset,
        alignedStripe.getOffsetInBlock(), getReadStrategies(chunk),
        block.getBlock());

    Future<Void> request = service.submit(readCallable);
    futures.put(request, chunkIndex);
    return true;
  }

  // POCache added on May 23, 2018
  // This is to read chunk in parallel
  boolean readDatChunkPcache(final LocatedBlock block, int chunkIndex) {
    final StripingChunk chunk;
    BlockReaderInfo curReaderInfo;
    long targetReaderOffset;

    synchronized (infoLock) {
      chunk = alignedStripe.chunks[chunkIndex];
      curReaderInfo = readerInfos[chunkIndex];
      targetReaderOffset = alignedStripe.getOffsetInBlock();
    }
    if (block == null) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    if (curReaderInfo != null && curReaderInfo.shouldSkip) {
      DFSClient.LOG.info("zmdebug: readDatCellsPcache(): shoudleSkip, chunkIndex {}", chunkIndex);
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    Callable<Void> readCallable = readDatCellsPcache(block, chunkIndex, chunk,
        curReaderInfo, targetReaderOffset, getReadStrategies(chunk));
    Future<Void> request = service.submit(readCallable);
    futures.put(request, chunkIndex);
    return true;
  }

  // POCache added
  boolean readDatChunkPcache(final LocatedBlock block, int chunkIndex, boolean recreate) throws IOException {
    final StripingChunk chunk;
    BlockReaderInfo curReaderInfo;
    long targetReaderOffset;

    synchronized (infoLock) {
      chunk = alignedStripe.chunks[chunkIndex];
      curReaderInfo = readerInfos[chunkIndex];
      targetReaderOffset = alignedStripe.getOffsetInBlock();
    }
    if (block == null) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    if (curReaderInfo != null && curReaderInfo.shouldSkip) {
      DFSClient.LOG.info("zmdebug: readDatCellsPcache(): shoudleSkip, chunkIndex {}", chunkIndex);
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    Callable<Void> readCallable;
    if (recreate) {
      if (curReaderInfo != null)
        curReaderInfo.reader.close();
      readCallable = readDatCellsPcache(block, chunkIndex, chunk, null, targetReaderOffset,
          getReadStrategies(chunk));
    } else {
      readCallable = readDatCellsPcache(block, chunkIndex, chunk, curReaderInfo, targetReaderOffset,
          getReadStrategies(chunk));
    }
    Future<Void> request = service.submit(readCallable);
    futures.put(request, chunkIndex);
    return true;
  }

  /**
   * read the whole stripe. do decoding if necessary
   */
  void readStripe() throws IOException {
    for (int i = 0; i < dataBlkNum; i++) {
      if (alignedStripe.chunks[i] != null &&
          alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
        if (!readChunk(targetBlocks[i], i)) {
          alignedStripe.missingChunksNum++;
        }
      }
    }
    // There are missing block locations at this stage. Thus we need to read
    // the full stripe and one more parity block.
    if (alignedStripe.missingChunksNum > 0) {
      checkMissingBlocks();
      readDataForDecoding();
      // read parity chunks
      readParityChunks(alignedStripe.missingChunksNum);
    }
    // TODO: for a full stripe we can start reading (dataBlkNum + 1) chunks

    // Input buffers for potential decode operation, which remains null until
    // first read failure
    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult r = StripedBlockUtil
            .getNextCompletedStripedRead(service, futures, 0);
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Read task returned: " + r + ", for stripe "
              + alignedStripe);
        }
        StripingChunk returnedChunk = alignedStripe.chunks[r.index];
        Preconditions.checkNotNull(returnedChunk);
        Preconditions.checkState(returnedChunk.state == StripingChunk.PENDING);

        if (r.state == StripingChunkReadResult.SUCCESSFUL) {
          returnedChunk.state = StripingChunk.FETCHED;
          alignedStripe.fetchedChunksNum++;
          updateState4SuccessRead(r);
          if (alignedStripe.fetchedChunksNum == dataBlkNum) {
            clearFutures();
            break;
          }
        } else {
          returnedChunk.state = StripingChunk.MISSING;
          // close the corresponding reader
          dfsStripedInputStream.closeReader(readerInfos[r.index]);

          final int missing = alignedStripe.missingChunksNum;
          alignedStripe.missingChunksNum++;
          checkMissingBlocks();

          readDataForDecoding();
          readParityChunks(alignedStripe.missingChunksNum - missing);
        }
      } catch (InterruptedException ie) {
        String err = "Read request interrupted";
        DFSClient.LOG.error(err);
        clearFutures();
        // Don't decode if read interrupted
        throw new InterruptedIOException(err);
      }
    }

    if (alignedStripe.missingChunksNum > 0) {
      decode();
    }
  }

  // POCache added to support pcache read
  // When there is no missing block, read one parity from the cache.
  // Using the one mroe read parity to tolerate the slow block.
  void readStripePcacheProactive(String fileID, int curStripeIdx, int subStripeIdx, JedisCluster jc) 
      throws IOException {
    for (int i = 0; i < dataBlkNum; i++) {
      if (alignedStripe.chunks[i] != null && alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
        if (!readDatChunkPcache(targetBlocks[i], i)) {
          synchronized (infoLock) {
            alignedStripe.missingChunksNum++;
          }
        }
      }
    }
    // There are missing blocks at this stage. Thus we need to read
    // the full stripe and one more parity block
    boolean hasReadCachedParity = false;
    if (alignedStripe.missingChunksNum > 0) {
      checkMissingBlocks();
      readDataForDecoding();
      // read Parity chunks
      readParityChunks(alignedStripe.missingChunksNum);
    } else {
      // POCache added - read cached parity if there is no missingChunks
      readParChunkPcache(fileID, curStripeIdx, subStripeIdx, jc);
      hasReadCachedParity = true;
    }

    // Input buffers for potential decode operation, which remains null until
    // first read failure
    int recvDatNum = 0; // POCache added to record the number of received data chunk
    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult r = StripedBlockUtil.getNextCompletedStripedRead(service, futures, 0);
        StripingChunk returnedChunk;
        synchronized (infoLock) {
          returnedChunk = alignedStripe.chunks[r.index];
        }

        Preconditions.checkNotNull(returnedChunk);

        if (returnedChunk.state == StripingChunk.PENDING && r.state == StripingChunkReadResult.SUCCESSFUL) {
          returnedChunk.state = StripingChunk.FETCHED;
          int recvNum;
          synchronized(infoLock) {
            alignedStripe.fetchedChunksNum++;
            recvNum = alignedStripe.fetchedChunksNum;
          }
          if (r.index < dataBlkNum) {
            updateState4SuccessRead(r);
            recvDatNum++;
          }
          if (recvNum == dataBlkNum) {
            for (Future<Void> future : futures.keySet()) {
              int idx = futures.get(future);
              future.cancel(true);
              synchronized(infoLock) {
                if (readerInfos[idx] != null && readerInfos[idx].reader != null) {
                  readerInfos[idx].reader.close();
                  readerInfos[idx] = null;
                }
              }
            }
            futures.clear();
            break;
          }
        } else if (r.state == StripingChunkReadResult.FAILED) {
          DFSClient.LOG.info("zmdebug: readStripePcacheProactive() - Failed: subStripeIdx " + subStripeIdx +
              ", r " + r + ", returnedChunk.state " + returnedChunk.state);
          if (!readDatChunkPcache(targetBlocks[r.index], r.index, true)) {
            synchronized (infoLock) {
              alignedStripe.missingChunksNum++;
            }
          }
        } else {
          returnedChunk.state = StripingChunk.MISSING;
          // close corresponding reader
          dfsStripedInputStream.closeReader(readerInfos[r.index]);
          final int missing;
          synchronized (infoLock) {
            missing = alignedStripe.missingChunksNum;
            alignedStripe.missingChunksNum++;
          }
          checkMissingBlocks();

          if (!hasReadCachedParity || missing >= 1) {
            readDataForDecoding();
            readParityChunks(1);
          }
        }
      } catch (InterruptedException ie) {
        String err = "Read request interrupted";
        DFSClient.LOG.error(err);
        clearFutures();
        // Don't decode if read is interrupted
        throw new InterruptedIOException(err);
      }
    }

    if (recvDatNum < dataBlkNum) {
      DFSClient.LOG.info("zmdebug: missingNum {}, subStripeIdx {}", alignedStripe.missingChunksNum, subStripeIdx); 
      decode();
    }
  }

  // POCache added
  void readStripePcacheReactive(String fileID, int curStripeIdx, int subStripeIdx, JedisCluster jc) 
      throws IOException {
    for (int i = 0; i < dataBlkNum; i++) {
      if (alignedStripe.chunks[i] != null && alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
        if (!readDatChunkPcache(targetBlocks[i], i)) {
          synchronized (infoLock) {
            alignedStripe.missingChunksNum++;
          }
        }
      }
    }
    // There are missing block locations at this stage. Thus we need to read
    // the full stripe and one more parity block.
    if (alignedStripe.missingChunksNum > 0) {
      checkMissingBlocks();
      readDataForDecoding();
      // read parity chunks
      readParityChunks(alignedStripe.missingChunksNum);
    }

    // Input buffers for potential decode operation, which remains null util
    // first read failure
    boolean hasReadCachedParity = false;
    long startTS = Time.monotonicNow();
    int recvDatNum = 0; // POCache added to record the number of received data chunk
    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult r = StripedBlockUtil.getNextCompletedStripedRead(service, futures, 0);
        StripingChunk returnedChunk = alignedStripe.chunks[r.index];
        Preconditions.checkNotNull(returnedChunk);

        if (returnedChunk.state == StripingChunk.PENDING && r.state == StripingChunkReadResult.SUCCESSFUL) {
          returnedChunk.state = StripingChunk.FETCHED;
          int recvNum;
          synchronized (infoLock) {
            alignedStripe.fetchedChunksNum++;
            recvNum = alignedStripe.fetchedChunksNum;
          }
          if (r.index < dataBlkNum) {
            updateState4SuccessRead(r);
            recvDatNum++;
          }

          if (recvNum == dataBlkNum) {
            for (Future<Void> future : futures.keySet()) {
              int idx = futures.get(future);
              future.cancel(true);
              synchronized (infoLock) {
                if (readerInfos[idx] != null && readerInfos[idx].reader != null) {
                  readerInfos[idx].reader.close();
                  readerInfos[idx] = null;
                }
              }
            }
            futures.clear();
            break;
          }
        } else if (r.state == StripingChunkReadResult.FAILED) {
          DFSClient.LOG.info("zmdebug: readStripePcacheReactive() - Failed: subStripeIdx " + subStripeIdx +
              ", r " + r + ", returnedChunk.state " + returnedChunk.state);
          if (!readDatChunkPcache(targetBlocks[r.index], r.index, true)) {
            synchronized (infoLock) {
              alignedStripe.missingChunksNum++;
            }
          } else {
            returnedChunk.state = StripingChunk.MISSING;
            // close the corresponding reader
            dfsStripedInputStream.closeReader(readerInfos[r.index]);
            final int missing;
            synchronized (infoLock) {
              missing = alignedStripe.missingChunksNum;
              alignedStripe.missingChunksNum++;
            }
            checkMissingBlocks();

            if (!hasReadCachedParity || missing >= 1) {
              readDataForDecoding();
              readParityChunks(1);
            }
          }
        }
        if (!hasReadCachedParity && (Time.monotonicNow() - startTS) > 85.0) {
          // POCache added - read cached parity if there is no missingChunks
          readParChunkPcache(fileID, curStripeIdx, subStripeIdx, jc);
          hasReadCachedParity = true;
        }
      } catch (InterruptedException ie) {
        String err = "Read request interrupted";
        DFSClient.LOG.error(err);
        clearFutures();
        // Don't decode if read is interrupted.
        throw new InterruptedIOException(err);
      }
    }

    if (alignedStripe.missingChunksNum > 0 || recvDatNum < dataBlkNum) {
      decode();
    }
  }


  /**
   * Some fetched {@link StripingChunk} might be stored in original application
   * buffer instead of prepared decode input buffers. Some others are beyond
   * the range of the internal blocks and should correspond to all zero bytes.
   * When all pending requests have returned, this method should be called to
   * finalize decode input buffers.
   */

  void finalizeDecodeInputs() {
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      final StripingChunk chunk = alignedStripe.chunks[i];
      if (chunk != null && chunk.state == StripingChunk.FETCHED) {
        if (chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyTo(decodeInputs[i].getBuffer());
        } else {
          chunk.getByteBuffer().flip();
        }
      } else if (chunk != null && chunk.state == StripingChunk.ALLZERO) {
        decodeInputs[i].setAllZero(true);
      }
    }
  }

  /**
   * Decode based on the given input buffers and erasure coding policy.
   */
  void decodeAndFillBuffer(boolean fillBuffer) throws IOException {
    // Step 1: prepare indices and output buffers for missing data units
    int[] decodeIndices = prepareErasedIndices();

    final int decodeChunkNum = decodeIndices.length;
    ECChunk[] outputs = new ECChunk[decodeChunkNum];
    for (int i = 0; i < decodeChunkNum; i++) {
      outputs[i] = decodeInputs[decodeIndices[i]];
      decodeInputs[decodeIndices[i]] = null;
    }
    // Step 2: decode into prepared output buffers
    decoder.decode(decodeInputs, decodeIndices, outputs);

    // Step 3: fill original application buffer with decoded data
    if (fillBuffer) {
      for (int i = 0; i < decodeIndices.length; i++) {
        int missingBlkIdx = decodeIndices[i];
        StripingChunk chunk = alignedStripe.chunks[missingBlkIdx];
        if (chunk.state == StripingChunk.MISSING && chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyFrom(outputs[i].getBuffer());
        }
      }
    }
  }

  /**
   * Prepare erased indices.
   */
  int[] prepareErasedIndices() {
    int[] decodeIndices = new int[parityBlkNum];
    int pos = 0;
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      if (alignedStripe.chunks[i] != null &&
          alignedStripe.chunks[i].state != StripingChunk.FETCHED){
        decodeIndices[pos++] = i;
      }
    }

    int[] erasedIndices = Arrays.copyOf(decodeIndices, pos);
    return erasedIndices;
  }

  void clearFutures() {
    for (Future<Void> future : futures.keySet()) {
      future.cancel(false);
    }
    futures.clear();
  }

  boolean useDirectBuffer() {
    return decoder.preferDirectBuffer();
  }
}
