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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.*;
import java.lang.Boolean;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.BlockPCUpdateCommand.BlockPCUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.pcache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;

// POCache added
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.server.protocol.BlockLatencyReports;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

/**
 * The Parity Cache Manager handles caching parity on Memory Storage (e.g., Redis).
 *
 * This class is instantiated by the FSNamesystem.
 * It maintains the mapping of cached parity to file.
 * during file reading. Based on the read performance and
 * read/write processes, we will schedule caching and uncaching work.
 * POCache added on Dec. 11, 2017
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class ParityCacheManager {
  public static final Logger LOG = LoggerFactory.getLogger(ParityCacheManager.class);

  // The FSNamesystem that contains this ParityCacheManager;
  public final FSNamesystem namesystem;
  // The BlockManager associated with the FSN that owns this ParityCacheManager.
  private final BlockManager blockManager;
  private final Configuration conf;

  /**
   *  Cache directives, sorted by ID.
   *
   *  listCacheDirectives relies on the ordering of elements in this map
   *  to track what has already been listed by the client.
   */
  private final HashMap<Long, ParityCacheInfo> pcacheMap;
  private final HashMap<BlockPCUpdateInfo, Boolean> pcacheUpdateMap;
  private boolean hasPcacheUpdate;
  private ParityCacheAlgorithm pcCacheAlg;
  private StragglerAlgorithm stragglerAlg;
  private JedisCluster jedisc;
  private HashSet<String> masterNodes;
  private long blockSize, subblockSize;
  private int numDataBlocks, numParityBlocks;
  private StragglerEstimator stragglerEstimator = null;
  private int totalCnt = 0, missCnt = 0;

  // POCache added on Sep. 21, 2018
  // maintain a mapping between block id and file id for locating straggling files
  private long totNumStripe = 0;
  private final HashMap<Long, Long> filesUnderRead;
  private final HashSet<Long> filesToEvict;
  private int nSigmaFactor = 1;
  private boolean parallelReadOnly = false;

  private int nHits = 0;
  private int nAccesses = 0;
  private int nStragglerHits = 0;
  private int cacheAlg = 0;

  ParityCacheManager(FSNamesystem namesystem, Configuration conf,
      BlockManager blockManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.conf = conf;
    this.numDataBlocks = conf.getInt(HdfsClientConfigKeys.PcacheRead.NUM_DAT_KEY,
        HdfsClientConfigKeys.PcacheRead.NUM_DAT_DEFAULT);
    this.numParityBlocks = conf.getInt(HdfsClientConfigKeys.PcacheRead.NUM_PAR_KEY,
        HdfsClientConfigKeys.PcacheRead.NUM_PAR_DEFAULT);
    blockSize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    subblockSize = conf.getLong(HdfsClientConfigKeys.PcacheRead.SUBBLK_SIZE_KEY,
        HdfsClientConfigKeys.PcacheRead.SUBBLK_SIZE_DEFAULT);
    int cacheSize = conf.getInt(HdfsClientConfigKeys.PcacheRead.CACHE_SIZE_KEY,
        HdfsClientConfigKeys.PcacheRead.CACHE_SIZE_DEFAULT);
    String redisIp = conf.get(HdfsClientConfigKeys.DFS_PCACHE_REDIS_IP,
        HdfsClientConfigKeys.DFS_PCACHE_REDIS_IP_DEFAULT);
    cacheAlg = conf.getInt(HdfsClientConfigKeys.PcacheRead.CACHE_ALG_KEY,
        HdfsClientConfigKeys.PcacheRead.CACHE_ALG_DEFAULT);
    parallelReadOnly = (conf.get(HdfsClientConfigKeys.DFS_PARALLEL_READ_KEY) == null) ?
        false : conf.getBoolean(HdfsClientConfigKeys.DFS_PARALLEL_READ_KEY, HdfsClientConfigKeys.DFS_PARALLEL_READ_DEFAULT);
    LOG.info("qpdebug: parallel read only {}", parallelReadOnly);
    // check redis memory amount
    if (cacheSize * blockSize / (1024 * 1024 * 1024) > 16) {
      LOG.warn("zmdebug: Redis memory usage {} exceeds 16 GiB, cacheSize {}",
          cacheSize * blockSize / (1024 * 1024 * 1024), cacheSize);
    }
    LOG.info("zmdebug: Constructor blockSize {}, subblockSize {}", blockSize, subblockSize);

    pcacheMap = new HashMap<>();
    pcacheUpdateMap = new HashMap<>();

    // resolve the conflicts when the parity of file under reading need to be evicted
    filesUnderRead = new HashMap<>(); 
    filesToEvict = new HashSet<>();
    nSigmaFactor = 1;

    hasPcacheUpdate = false;
    LOG.info("qpdebug: use cache alg {} with cache size {}", cacheAlg, cacheSize);
    if (cacheAlg == 1) {
      this.pcCacheAlg = new LRUCache(cacheSize);
    } else if (cacheAlg == 2) {
      this.pcCacheAlg = new ARCCache(cacheSize);
    } else if (cacheAlg == 3) {
      this.pcCacheAlg = new LFUCache(cacheSize);
    } else if (cacheAlg == 4) {
      this.pcCacheAlg = new StragglerAwareLRU(cacheSize, this);
    } else if (cacheAlg == 5) {
      this.pcCacheAlg = new StragglerAwareCache(cacheSize, this);
    } else if (cacheAlg == 20) {
      // this is for selective replication
      this.pcCacheAlg = new LFUCache(cacheSize / this.numDataBlocks);
    } else {
      this.pcCacheAlg = new LRUCache(cacheSize);
    }
    Set<DatanodeDescriptor> datanodes = this.blockManager.getDatanodeManager().getDatanodes();
    if (datanodes != null && !datanodes.isEmpty()) {
      this.stragglerEstimator = new StragglerEstimator(this.blockManager.getDatanodeManager().getDatanodes(), this);
    } else {
      LOG.warn("zmdebug: Can NOT construct stragglerEstimator - datanodes.size() " + datanodes.size());
    }

    HashSet<HostAndPort> clusterNodes = new HashSet<>();
    clusterNodes.add(new HostAndPort(redisIp, 7000));
    jedisc = new JedisCluster(clusterNodes, new JedisPoolConfig());
    masterNodes = new HashSet<>();
    masterNodes.add(redisIp + ":7000");
    masterNodes.add(redisIp + ":7001");
    masterNodes.add(redisIp + ":7002");
    cleanRedis();
  }

  // For newly written file, reserve enough space of 1 parity for this file
  public synchronized void setFilePcache(long fileId) {
    ParityCacheInfo pcinfo = new ParityCacheInfo(this.numDataBlocks, this.numParityBlocks, 0, true, 0);

    addFilePcache(fileId, pcinfo, this.numParityBlocks);
  }

  public synchronized ParityCacheInfo getPcInfo(long fileId, boolean fromCreated, long fileLen,
                                                List<LocatedBlock> locatedblocks) {
    if (parallelReadOnly == true)
      return new ParityCacheInfo(this.numDataBlocks, 0, 0, true, 0, 0);
    nAccesses++;
    synchronized (this.filesUnderRead) {
      if (this.filesUnderRead.get(fileId) == null)
        this.filesUnderRead.put(fileId, (long)0);
      this.filesUnderRead.put(fileId,
          this.filesUnderRead.get(fileId) + 1);
      //LOG.info("qpdebug: read file {} current number of reader {}", fileId, this.filesUnderRead.get(fileId));
    }

    //LOG.info("qpdebug: get parity cache info, read a new file {}, current pcachemap size {} content {}",
        //fileId, pcacheMap.size(), pcacheMap);

    if (pcCacheAlg instanceof LFUCache) {
      ((LFUCache)pcCacheAlg).addFrequency(fileId);
    }
    if (this.pcacheMap.containsKey(fileId)) {
      nHits++;
      LOG.info("qpdebug: number of cache hits {} number of total access {}",
          nHits, nAccesses);
      ParityCacheInfo pcInfo = this.pcacheMap.get(fileId);
      if (pcInfo.getIsDone() == false) {
        if (checkIsCached(fileId, (int)getNumMemBlksByFileId(fileId)))
          pcInfo.setIsDone(true);
        pcacheMap.put(fileId, pcInfo);
      }
      pcCacheAlg.updateCachedParity(fileId, this.numParityBlocks);
      return pcInfo;
    } else {
      {
        BlockInfo[] blocks = getBlockInfo(fileId);
        for (int i = 0; i < blocks.length; i++) {
          int numNodes = blocks[i].numNodes();
          for (int j = 0; j < numNodes; j++) {
            String dataNode = blocks[i].getDatanode(j).getName();
            if (dataNode.contains("22"))
              nStragglerHits++;
          }
        }
        LOG.info("qpdebug: number of straggler hits {}, number of cache hits {}, number of total accesses {}",
            nStragglerHits, nHits, nAccesses);
      }

      int numCachedParities = pcCacheAlg.getCachedParity(fileId);
      boolean shouldCache = pcCacheAlg.shouldCacheParity(fileId);
      //LOG.info("qpdebug: should cache or not {} file {}", shouldCache, fileId);
      if (shouldCache) {
        long numStripes = getNumMemBlksByFileId(fileId);
        //LOG.info("qpdebug: file {} numOfStripes {}", fileId, numStripes);
        HashSet<Long> evicts = pcCacheAlg.updateCachedParity(fileId, (int)numStripes);
        //LOG.info("qpdebug: evict file {}", evicts);
        removeParityByFileIds(evicts);
        ParityCacheInfo pcInfo = new ParityCacheInfo(this.numDataBlocks, 0, 0, false, 0, this.numParityBlocks);
        pcacheMap.put(fileId, pcInfo);
        return pcInfo;
      }
      return new ParityCacheInfo(this.numDataBlocks, 0, 0, true, 0, 0);
    }
  }

  // For this file, use the default ParityCacheInfo
  private synchronized void addFilePcache(long fileId, ParityCacheInfo pcinfo, int numPar) {
    pcacheMap.put(fileId, pcinfo);
  }

  // remove from redis and pcacheMap
  private synchronized void removeParityByFileIds(HashSet<Long> fileIdEvict) {
    if (fileIdEvict != null) {
      for (long fileId : fileIdEvict) {
        synchronized (this.filesUnderRead) {
          if (this.filesUnderRead.containsKey(fileId)) {
            LOG.info("qpdebug: cannot evict currently under reading file {}", fileId);
            this.filesToEvict.add(fileId);
            continue;
          }
        }
        for (String key : keysOfFileId(fileId)) {
          jedisc.del(key.getBytes());
        }
        pcacheMap.remove(fileId);
      }
    }
  }

  private synchronized boolean checkIsCached(long fileId, int numBlocks) {
    int numKeysInRedis = keysOfFileId(fileId).size();
    //LOG.info("qpdebug: check is cached, file id {}, keys in redis {}",
        //fileId, numKeysInRedis);
    return numKeysInRedis == numBlocks * ((int)(this.blockSize / this.subblockSize));
  }

  private synchronized HashSet<String> keysOfFileId(long fId) {
    String pattern = String.valueOf(fId) + "*";
    HashSet<String> keys = new HashSet<>();
    Map<String, JedisPool> clusterNodes = jedisc.getClusterNodes();
    for (String k : clusterNodes.keySet()) {
      Jedis connection = clusterNodes.get(k).getResource();
      try {
        keys.addAll(connection.keys(pattern));
        //keys.addAll();
      } catch (Exception e) {
        LOG.error("zmdebug: keysOfFileId() - Getting keys error {}", e);
      } finally {
        connection.close();
      }
    }
    return keys;
  }

  // Clean Redis - only master because slaves are read-only
  private synchronized void cleanRedis() {
    Map<String, JedisPool> clusterNodes = jedisc.getClusterNodes();
    for (String k : clusterNodes.keySet()) {
      if (this.masterNodes.contains(k)) {
        Jedis jedis = clusterNodes.get(k).getResource();
        try {
          jedis.flushAll();
        } catch (Exception e) {
          LOG.error("zmdebug: cleanRedis() error: {}", e);
        } finally {
          jedis.close();
        }
      }
    }
  }

  public synchronized boolean hasPcacheUpdateTask() { return this.hasPcacheUpdate; }

  public synchronized Set<BlockPCUpdateInfo> getPendingPCUpdateList() {
    if (this.hasPcacheUpdate) {
      HashSet<BlockPCUpdateInfo> pendingPCUpdateList = new HashSet<>();
      for (BlockPCUpdateInfo each : pcacheUpdateMap.keySet()) {
        // TODO: if get or if !get?
        if (pcacheUpdateMap.get(each)) {
          pendingPCUpdateList.add(each);
          pcacheUpdateMap.put(each, false);
        }
      }
      hasPcacheUpdate = false;
      return pendingPCUpdateList;
    } else {
      return null;
    }
  }

  public BlockPCUpdateInfo genBlockPCUpdateInfo(long fileId, int newDatNum, int newParNum) {
    return new BlockPCUpdateInfo(fileId, newDatNum, newParNum);
  }

  public synchronized void addToPcacheUpdateMap() {
    long fid = 0;
    // ZMtest on Dec. 13, 2017
    if ((!pcacheMap.isEmpty()) && (pcacheMap != null)) {
      fid = ((Long) pcacheMap.keySet().toArray()[0]).longValue();
    }
    pcacheUpdateMap.put(genBlockPCUpdateInfo(fid, 2, 2), true);
    hasPcacheUpdate = true;
  }

  // POCache added on Sep. 21, 2018
  public void evict(HashSet<Long> blockIds) {
    HashSet<Long> evictedFileIds = new HashSet<>();
    for (long blockId : blockIds) {
      //if (pcacheMap.containsKey(blockId2FileId.get(blockId))) {
        //evictedFileIds.add(blockId2FileId.get(blockId));
      //}
    }
    if (!evictedFileIds.isEmpty()) {
      //for (long fileId : evictedFileIds)
        //LOG.info("qpdebug: evict file Id {}", fileId); 
      removeParityByFileIds(evictedFileIds);
    }
  }

  // POCache added
  private synchronized boolean isStragglerEstimatorNull() {
    Set<DatanodeDescriptor> datanodesLatest = this.blockManager.getDatanodeManager().getDatanodes();
    if (datanodesLatest == null) {
      return true;
    } else {
      if (this.stragglerEstimator == null || this.stragglerEstimator.getDatanodesNum() == 0) {
        this.stragglerEstimator = new StragglerEstimator(datanodesLatest, this);
      } else if (datanodesLatest.size() > this.stragglerEstimator.getDatanodesNum()) {
        this.stragglerEstimator.addDatanodes(datanodesLatest);
      }
      return (this.stragglerEstimator == null || this.stragglerEstimator.getDatanodesNum() == 0);
    }
  }

  // POCache added
  // Informed by FSNameSystem that file has been completed.
  public void reportCompleteFile(long fileId) {
    if (parallelReadOnly == true) return ;

    if (pcCacheAlg instanceof LFUCache) {
      ((LFUCache)pcCacheAlg).addFrequency(fileId);
    }

    HashSet<Long> evicts = pcCacheAlg.updateCachedParity(fileId, (int)getNumMemBlksByFileId(fileId));
    ParityCacheInfo pcInfo = new ParityCacheInfo(
        (int)(this.numDataBlocks), (int)(this.numParityBlocks),
        0, true, (int)(this.numParityBlocks));
    pcacheMap.put(fileId, pcInfo);
    removeParityByFileIds(evicts);
  }

  public void reportCompleteRead(long fileId, String slowDataNode) {
    if (pcCacheAlg instanceof StragglerAwareLRU) {
      ((StragglerAwareLRU)pcCacheAlg).addSlowDataNode(slowDataNode);
    } else if (pcCacheAlg instanceof StragglerAwareCache) {
      ((StragglerAwareCache)pcCacheAlg).addSlowDataNode(slowDataNode);
    }
    
    if (pcacheMap.containsKey(fileId)) {
      if (pcacheMap.get(fileId).getIsDone() == false) {
        if (checkIsCached(fileId, (int)getNumMemBlksByFileId(fileId))) {
          ParityCacheInfo pcInfo = pcacheMap.get(fileId);
          pcInfo.setIsDone(true);
          pcacheMap.put(fileId, pcInfo);
        }
      }
    }

    synchronized (filesUnderRead) {
      if (filesUnderRead.get(fileId) == null) return ;
      long count = filesUnderRead.get(fileId);
      count -= 1;
      //LOG.info("qpdebug: report complete read and file {} count {}", fileId, count);
      if (count == 0) {
        if (this.filesToEvict.contains(fileId)) {
          LOG.info("qpdebug: evict previous under reading file {}", fileId);
          HashSet<Long> tmp = new HashSet<>();
          tmp.add(fileId);
          removeParityByFileIds(tmp);
        }
        filesUnderRead.remove(fileId);
      } else {
        filesUnderRead.put(fileId, count);
      }
    }
  }

  // POCache added on Sep. 20, 2018
  public void reportBlockLatencyToPcm(BlockLatencyReports blocksLatency) {
    if (isStragglerEstimatorNull()) {
      LOG.warn("zmdebug: stragglerEstimator is null!");
      return ;
    }
    this.stragglerEstimator.updateBlockLatency(blocksLatency);
  }

  // POCache added on Nov. 08, 2018
  public void reportNodeServiceRateToPcm(String nodeAddr, Double nodeServiceRate) {
    if (nodeServiceRate == 0.0) return ;
    if (pcCacheAlg instanceof StragglerAwareLRU) {
      ((StragglerAwareLRU)pcCacheAlg).addNodeServiceRate(nodeAddr, nodeServiceRate);
    } else if (pcCacheAlg instanceof StragglerAwareCache) {
      ((StragglerAwareCache)pcCacheAlg).addNodeServiceRate(nodeAddr, nodeServiceRate);
    }
  }

  public BlockInfo[] getBlockInfo(long fileId) {
    return namesystem.getBlockInfo(fileId);
  }

  public long getNumMemBlksByFileId(long fileId) {
    BlockInfo[] blocks = namesystem.getBlockInfo(fileId);
    long numDataBlocks = blocks.length;

    if (this.cacheAlg == 20) return numDataBlocks;
    return (numDataBlocks + this.numDataBlocks - 1) / this.numDataBlocks;
  }

  private long getMemeryUsageByFileId(long fileId) {
    BlockInfo[] blocks = namesystem.getBlockInfo(fileId);
    long totalNumDataBlocks = blocks.length;
    long totalNumParityBlocks = numDataBlocks / this.numDataBlocks * this.numParityBlocks;
    long numUnalignedBlocks = numDataBlocks % this.numDataBlocks;

    long sizeUnalignedBlocks = 0;
    while (--numUnalignedBlocks == 0) {
      sizeUnalignedBlocks += blocks[(int)(totalNumDataBlocks - totalNumParityBlocks)].getNumBytes();
    }
    return totalNumParityBlocks * this.blockSize + sizeUnalignedBlocks;
  }

  /**
   * Resets all tracked directives and pools. Called during 2 NN checkopointing to
   * reset FSNamesystem state. See {@link FSNamesystem#clear()}.
   */
  void clear() {}
}
