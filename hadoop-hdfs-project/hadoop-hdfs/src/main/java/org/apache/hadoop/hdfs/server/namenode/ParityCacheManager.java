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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.pcache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;

// POCache added
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

/**
 * The Parity Cache Manager handles caching parity into Memory Storage (e.g., Redis).
 *
 * This class is instantiated by the FSNamesystem.
 * It maintains the mapping of cached parity to file.
 * POCache added.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class ParityCacheManager {
  public static final Logger LOG = LoggerFactory.getLogger(ParityCacheManager.class);

  // The FSNamesystem that contains this ParityCacheManager;
  public final FSNamesystem namesystem;
  public final Configuration conf;
  private HashMap<Long, String> fileId2path;

  private final HashMap<Long, ParityCacheInfo> pcacheMap;
  private ParityCacheAlgorithm pcCacheAlg;
  private JedisCluster jedisc;
  private HashSet<String> masterNodes;// for Redis
  private long blockSize, subblockSize;
  private int numDataBlocks, numParityBlocks;

  // maintain a mapping between block id and file id for locating straggling files
  private final HashMap<Long, Long> filesUnderRead;
  private final HashSet<Long> filesToEvict;
  private boolean parallelReadOnly = false;

  private int nHits = 0;
  private int nAccesses = 0;
  private int nStragglerHits = 0;
  private int cacheAlg = 0;

  private HashMap<String, Integer> dn2count;

  ParityCacheManager(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;
    this.conf = conf;
    this.fileId2path = new HashMap<>();
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
    LOG.info("qpdebug: parallelReadOnly = {}", parallelReadOnly);
    // check redis memory amount
    if (cacheSize * blockSize / (1024 * 1024 * 1024) > 16) {
      LOG.warn("zmdebug: Redis memory usage {} exceeds 16 GiB, cacheSize {}",
          cacheSize * blockSize / (1024 * 1024 * 1024), cacheSize);
    }
    LOG.info("zmdebug: Constructor blockSize {}, subblockSize {}", blockSize, subblockSize);

    pcacheMap = new HashMap<>();
    // resolve the conflicts when the parity of file under reading need to be evicted
    filesUnderRead = new HashMap<>(); 
    filesToEvict = new HashSet<>();

    if (cacheAlg == 1) {
      LOG.info("zmdebug: use LRU, cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new LRUCache(cacheSize);
    } else if (cacheAlg == 2) {
      LOG.info("zmdebug: use ARC, cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ARCCache(cacheSize);
    } else if (cacheAlg == 3) {
      LOG.info("zmdebug: use LFU, cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new LFUCache(cacheSize);
    } else if (cacheAlg == 4) {
      LOG.info("zmdebug: use SAC, cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new StragglerAwareCache(cacheSize, this);
    } else if (cacheAlg == 5000) {
      LOG.info("zmdebug: use CSAC (OSA, LRU, NONE), cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ConfigurableStragglerAwareCache(cacheSize, this,
              ConfigurableStragglerAwareCache.AdmissionPolicy.OSA, ConfigurableStragglerAwareCache.EvictionPolicy.LRU,
              ConfigurableStragglerAwareCache.PredictionPolicy.NONE);
    } else if (cacheAlg == 5010) {
      LOG.info("zmdebug: use CSAC (OSA, LFU, NONE), cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ConfigurableStragglerAwareCache(cacheSize, this,
              ConfigurableStragglerAwareCache.AdmissionPolicy.OSA, ConfigurableStragglerAwareCache.EvictionPolicy.LFU,
              ConfigurableStragglerAwareCache.PredictionPolicy.NONE);
    } else if (cacheAlg == 5020) {
      LOG.info("zmdebug: use CSAC (OSA, ARC, NONE), cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ConfigurableStragglerAwareCache(cacheSize, this,
              ConfigurableStragglerAwareCache.AdmissionPolicy.OSA, ConfigurableStragglerAwareCache.EvictionPolicy.ARC,
              ConfigurableStragglerAwareCache.PredictionPolicy.NONE);
    } else if (cacheAlg == 5001) {
      LOG.info("zmdebug: use CSAC (OSA, LRU, Recency), cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ConfigurableStragglerAwareCache(cacheSize, this,
              ConfigurableStragglerAwareCache.AdmissionPolicy.OSA, ConfigurableStragglerAwareCache.EvictionPolicy.LRU,
              ConfigurableStragglerAwareCache.PredictionPolicy.RECENCY);
    } else if (cacheAlg == 5011) {
      LOG.info("zmdebug: use CSAC (OSA, LFU, Recency), cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ConfigurableStragglerAwareCache(cacheSize, this,
              ConfigurableStragglerAwareCache.AdmissionPolicy.OSA, ConfigurableStragglerAwareCache.EvictionPolicy.LFU,
              ConfigurableStragglerAwareCache.PredictionPolicy.RECENCY);
    } else if (cacheAlg == 5021) {
      LOG.info("zmdebug: use CSAC (OSA, ARC, Recency), cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new ConfigurableStragglerAwareCache(cacheSize, this,
              ConfigurableStragglerAwareCache.AdmissionPolicy.OSA, ConfigurableStragglerAwareCache.EvictionPolicy.ARC,
              ConfigurableStragglerAwareCache.PredictionPolicy.RECENCY);
    } else if (cacheAlg == 20) {
      // this is for selective replication
      LOG.info("zmdebug: use selective replication, cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new LFUCache(cacheSize / this.numDataBlocks);
    } else {
      // LRU algorithm by default
      LOG.info("zmdebug: use LRU, cacheAlg = {} with cacheSize = {}", cacheAlg, cacheSize);
      this.pcCacheAlg = new LRUCache(cacheSize);
    }

    HashSet<HostAndPort> clusterNodes = new HashSet<>();
    clusterNodes.add(new HostAndPort(redisIp, 7000));
    LOG.info("zmdebug: redisIP = {}", redisIp);
    jedisc = new JedisCluster(clusterNodes, new JedisPoolConfig());
    masterNodes = new HashSet<>();
    masterNodes.add(redisIp + ":7000");
    masterNodes.add(redisIp + ":7001");
    masterNodes.add(redisIp + ":7002");
    cleanRedis();

    this.dn2count = new HashMap<>();
  }

  // Called by BlockManager
  public synchronized ParityCacheInfo getPcInfo(long fileId, String filePath) {
    nAccesses++;
    this.fileId2path.put(fileId, filePath);

    if (parallelReadOnly == true) {
      return new ParityCacheInfo(this.numDataBlocks, 0, 0, true, 0, 0);
    }

    // Update filesUnderRead
    synchronized (this.filesUnderRead) {
      if (this.filesUnderRead.get(fileId) == null) {
        this.filesUnderRead.put(fileId, (long) 0);
      }
      this.filesUnderRead.put(fileId, this.filesUnderRead.get(fileId) + 1);
      //LOG.info("qpdebug: read file {} current number of reader {}", fileId, this.filesUnderRead.get(fileId));
    }

    //LOG.info("qpdebug: get parity cache info, read a new file {}, current pcachemap size {} content {}",
        //fileId, pcacheMap.size(), pcacheMap);

    if (pcCacheAlg instanceof LFUCache) {
      ((LFUCache)pcCacheAlg).addFrequency(fileId);
    }


    int numCachedParities = pcCacheAlg.getCachedParity(fileId);
    boolean isCachedInPcacheMap = this.pcacheMap.containsKey(fileId);
    if ((numCachedParities == 0 && isCachedInPcacheMap) || (numCachedParities != 0 && !isCachedInPcacheMap)) {
      LOG.error("zmdebug: inconsistent result for fileId = {}, numCachedParities = {}, " +
                      "pcacheMap.containsKey = {}, pcacheMap.size() = {}",
              fileId, numCachedParities, isCachedInPcacheMap, pcacheMap.size());
    }

    if (isCachedInPcacheMap) {
      nHits++;
      LOG.info("zmdebug: hit (isCachedInPcacheMap) nStragglerHits = {}, nHits = {}, nAccesses = {}",
              nStragglerHits, nHits, nAccesses);
      ParityCacheInfo pcInfo = this.pcacheMap.get(fileId);
      if (pcInfo.getIsDone() == false) {
        if (checkIsCached(fileId, (int)getNumMemBlksByFileId(fileId)))
          pcInfo.setIsDone(true);
        pcacheMap.put(fileId, pcInfo);
      }
      return pcInfo;
    } else {
      {
        boolean hitStrag = false;
        BlockInfo[] blocks = getBlockInfo(fileId);
        for (int i = 0; i < blocks.length; i++) {
          int numNodes = blocks[i].numNodes();
          for (int j = 0; j < numNodes; j++) {
            String dataNode = blocks[i].getDatanode(j).getName();
            // straggler: node16, 192.168.10.26
            if (dataNode.contains("26")) {
              hitStrag = true;
              break;
            }
          }
        }
        if (hitStrag) {
          nStragglerHits++;
        }
        LOG.info("zmdebug: hit nStragglerHits = {}, nHits = {}, nAccesses = {}",
            nStragglerHits, nHits, nAccesses);
      }

      boolean shouldCache = pcCacheAlg.shouldCacheParity(fileId);
      if (shouldCache) {
        long numStripes = getNumMemBlksByFileId(fileId);
        HashSet<Long> evicts = pcCacheAlg.updateCachedParity(fileId, (int)numStripes);
        removeParityByFileIds(evicts);
        ParityCacheInfo pcInfo = new ParityCacheInfo(this.numDataBlocks, 0, 0,
                false, 0, this.numParityBlocks);
        pcacheMap.put(fileId, pcInfo);
        return pcInfo;
      }
      return new ParityCacheInfo(this.numDataBlocks, 0, 0, true, 0, 0);
    }
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
          LOG.info("zmdebug: cleanRedis() is done.");
          jedis.close();
        }
      }
    }
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
  // Informed by FSNameSystem that file has been completed.
  public void reportCompleteFile(long fileId, String filePath) {
    this.fileId2path.put(fileId, filePath);
//    LOG.info("zmdebug - reportCompleteFile(): fileId = {}, filePath = {}", fileId, filePath);

    if (parallelReadOnly == true) return ;

    if (pcCacheAlg instanceof LFUCache) {
      ((LFUCache)pcCacheAlg).addFrequency(fileId);
    }

    // let CSAC update info
    if (this.pcCacheAlg instanceof ConfigurableStragglerAwareCache) {
      pcCacheAlg.getCachedParity(fileId);
    }
    HashSet<Long> evicts = pcCacheAlg.updateCachedParity(fileId, (int)getNumMemBlksByFileId(fileId));
    ParityCacheInfo pcInfo = new ParityCacheInfo(
        (int)(this.numDataBlocks), (int)(this.numParityBlocks),
        0, true, (int)(this.numParityBlocks));
    pcacheMap.put(fileId, pcInfo);
//    LOG.info("zmdebug debug - preportCompleteFile(): pcaheMap.put({})", fileId);
    removeParityByFileIds(evicts);
  }

  public void reportCompleteRead(long fileId, String slowDataNode) {
    if (pcCacheAlg instanceof StragglerAwareCache) {
      ((StragglerAwareCache)pcCacheAlg).addSlowDataNode(slowDataNode);
    } else if (pcCacheAlg instanceof ConfigurableStragglerAwareCache) {
      ((ConfigurableStragglerAwareCache)pcCacheAlg).addSlowDataNode(slowDataNode);
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

  // POCache added on Nov. 08, 2018
  public void reportNodeServiceRateToPcm(String nodeAddr, Double nodeServiceRate) {
    if (nodeServiceRate == 0.0) return;
    if (pcCacheAlg instanceof StragglerAwareCache) {
      ((StragglerAwareCache)pcCacheAlg).addNodeServiceRate(nodeAddr, nodeServiceRate);
      return;
    }
    if (pcCacheAlg instanceof ConfigurableStragglerAwareCache) {
      ((ConfigurableStragglerAwareCache)pcCacheAlg).addNodeServiceRate(nodeAddr, nodeServiceRate);
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

  public String getFilePath(long fileId) {
    return this.fileId2path.get(fileId);
  }

  private void updateDn2Count(long fileId) {
    // Calculate the reading count from each datanode
    BlockInfo[] blocks = getBlockInfo(fileId);
    for (int i = 0; i < blocks.length; i++) {
      int numNodes = blocks[i].numNodes();
      for (int j = 0; j < numNodes; j++) {
        String dataNode = blocks[i].getDatanode(j).getName();
        int count = 1;
        if (this.dn2count.containsKey(dataNode)) {
          count += this.dn2count.get(dataNode);
        }
        this.dn2count.put(dataNode, count);
      }
    }
  }

}
