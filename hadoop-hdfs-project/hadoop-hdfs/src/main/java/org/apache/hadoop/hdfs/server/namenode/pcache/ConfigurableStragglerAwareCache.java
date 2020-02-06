// Implementation of Configurable Straggler Aware Cache Algorithm
// POCache added on Jan. 8. 2020
package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.server.namenode.ParityCacheManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurableStragglerAwareCache implements ParityCacheAlgorithm {
  public static final Logger LOG = LoggerFactory.getLogger(ConfigurableStragglerAwareCache.class);

  // admissionPolicy: OSA = "on single access", LFU = "least frequently used"
  public enum AdmissionPolicy { OSA, LFU }
  // evictionPolicy: LRU = "least recently used", LFU = "least frequently used", ARC = "adaptive replacement cache"
  public enum EvictionPolicy { LRU, LFU, ARC }
  // predictionPolicy: NONE = "no prefetching", RECENCY, FREQUENCY, BOTH = "recency and frequency"
  public enum PredictionPolicy { NONE, RECENCY, FREQUENCY, BOTH }

  private final int capacity;// available cache space
  private int availableCapacity;
  private double maxRatioForPrefetch;
  private int maxCapacityForPrefetch;
  private ParityCacheManager pcm;
  public HashSet<String> slowDataNodes;
  private HashSet<String> oldSlowDataNodes;// slowDataNodes detected last time
  private HashMap<String, Double> nodesServiceRate;
  private ArrayList<Long> fileRecency;// tail is more recent
  private HashMap<Long, Integer> file2frequency;
  private HashSet<Long> filesInCache;
  private int leastFrequency = 1;// the least freqency of files in cache is 1
  private ARC arcAlg = null;

  private AdmissionPolicy admitPolicy;
  private EvictionPolicy evictPolicy;
  private PredictionPolicy predictPolicy;

  public ConfigurableStragglerAwareCache(int capacity, ParityCacheManager pcm,
                                         AdmissionPolicy adpolicy, EvictionPolicy evpolicy, PredictionPolicy prepolicy) {
    this.capacity = capacity;
    this.availableCapacity = capacity;
    this.maxRatioForPrefetch = 0.4;
    this.maxCapacityForPrefetch = (int)(capacity * this.maxRatioForPrefetch);
    this.pcm = pcm;
    this.slowDataNodes = new HashSet<>();
    this.oldSlowDataNodes = new HashSet<>();
    this.nodesServiceRate = new HashMap<>();

    this.file2frequency = new HashMap<>();
    this.fileRecency = new ArrayList<>();
    this.filesInCache = new HashSet<>();

    admitPolicy = adpolicy;
    evictPolicy = evpolicy;
    predictPolicy = prepolicy;
    if (this.evictPolicy == EvictionPolicy.ARC) {
      this.arcAlg = new ARC(this.capacity);
    }


    new Thread() {
      public void run() {
        while (true) {
          synchronized (this) {
            double avg = 0, deviation = 0;
            for (Map.Entry<String, Double> entry : nodesServiceRate.entrySet()) {
              avg += entry.getValue();
            }
            if (nodesServiceRate.size() != 0 && nodesServiceRate.size() != 1) {
              avg /= nodesServiceRate.size();
            }

            for (Map.Entry<String, Double> entry : nodesServiceRate.entrySet()) {
              double value = entry.getValue();
              deviation += (value - avg) * (value - avg);
            }
            if (nodesServiceRate.size() != 1 && nodesServiceRate.size() != 0) {
              deviation /= nodesServiceRate.size() - 1;
            }
            deviation = Math.sqrt(deviation);
            oldSlowDataNodes.clear();
            oldSlowDataNodes = (HashSet) slowDataNodes.clone();
            slowDataNodes.clear();

            for (Map.Entry<String, Double> entry : nodesServiceRate.entrySet()) {
              if (entry.getValue() < avg - deviation * 3) {
                slowDataNodes.add(entry.getKey());
              }
            }
            if (!slowDataNodes.isEmpty()) {
              LOG.info("zmdebug: nodes service rate {}", nodesServiceRate);
              LOG.info("zmdebug: avg {} variance {}", avg, deviation);
              LOG.info("zmdebug: straggler nodes {}", slowDataNodes);
            }

            boolean hasSlowDNchanged = false;
            if (slowDataNodes.size() > oldSlowDataNodes.size()) {
              hasSlowDNchanged = true;
            } else if (!slowDataNodes.isEmpty()) {
              Iterator it = slowDataNodes.iterator();
              while(it.hasNext()) {
                if (!oldSlowDataNodes.contains(it.next())) {
                  hasSlowDNchanged = true;
                  break;
                }
              }
            }

            if (hasSlowDNchanged && predictPolicy != PredictionPolicy.NONE) {
              LOG.info("zmdebug - slowDataNodes changes and next to prefetch");
              // start a thread to do data prefetching
              new Thread(() -> {
                ArrayList<Long> filesPredicted = getFilesPredicted();
                // Then get the files to prefetch
                ArrayList<Long> filesToPrefetch = new ArrayList<>();
                for (int f = 0; f < filesPredicted.size() && filesToPrefetch.size() < maxCapacityForPrefetch; f++) {
                  long fileId = filesPredicted.get(f);
                  BlockInfo[] blocks = pcm.getBlockInfo(fileId);
                  for (int i = 0; i < blocks.length; i++) {
                    int numNodes = blocks[i].numNodes();
                    for (int j = 0; j < numNodes; j++) {
                      String dataNode = blocks[i].getDatanode(j).getName();
                      if (slowDataNodes.contains(dataNode) && !filesInCache.contains(fileId)) {
                        filesToPrefetch.add(fileId);
                      }
                    }
                  }
                }
                // Prefetch
                FileSystem fs = null;
                InputStream in = null;
                IOUtils.NullOutputStream out = new IOUtils.NullOutputStream();
                try {
                  fs = FileSystem.get(pcm.conf);
                } catch (IOException e) {
                  LOG.error("zmdebug - can NOT get filesystem for prefetching!");
                  e.printStackTrace();
                  return;
                }

                for (int i = 0; i < filesToPrefetch.size(); i++) {
//                  LOG.info("zmdebug - prefetch start: fileId = {}, filePath = {}",
//                          filesToPrefetch.get(i), pcm.getFilePath(filesToPrefetch.get(i)));
                  try {
                    in = fs.open(new Path(pcm.getFilePath(filesToPrefetch.get(i))));
                    IOUtils.copyBytes(in, out, pcm.conf);
                  } catch (IOException e) {
                    LOG.warn("zmdebug - fails to preftech: {}", pcm.getFilePath(filesToPrefetch.get(i)));
                    e.printStackTrace();
                  } finally {
                    IOUtils.closeStream(in);
                  }
                  LOG.info("zmdebug - prefetch end: fileId = {}, filePath = {}, isCached = {}",
                          filesToPrefetch.get(i), pcm.getFilePath(filesToPrefetch.get(i)), filesInCache.contains(filesToPrefetch.get(i)));
                }

                try {
                  fs.close();
                } catch (IOException e) {
                  LOG.error("zmdebug - can NOT close fs in preftech: {}", e);
                  e.printStackTrace();
                }

              }).start();
            }
          }
          try {
            sleep(1500);
          } catch (InterruptedException e) {
            return ;
          }
        }
      }
    }.start();
  }

  // Return files to be read
  public synchronized ArrayList<Long> getFilesPredicted() {
    // filesPredicted sorted by likelihood of reading in the future
    // The item with smaller index in filesPredicted has more likelihood of reading
    ArrayList<Long> filesPredicted = new ArrayList<>();
    if (this.predictPolicy == PredictionPolicy.RECENCY) {
      LOG.info("zmdebug - predictPolicy = RECENCY");
      for (int i = this.fileRecency.size() - 1; i >= 0; i--) {
        filesPredicted.add(fileRecency.get(i));
      }
    } else if (this.predictPolicy == PredictionPolicy.FREQUENCY) {
      LOG.info("zmdebug - predictPolicy = FREQUENCY");
      Map<Long, Integer> sortedFile2frequency = sortByValueInteger(this.file2frequency);
      for (Map.Entry<Long, Integer> en : sortedFile2frequency.entrySet()) {
        filesPredicted.add(0, en.getKey());
      }
    } else if (this.predictPolicy == PredictionPolicy.BOTH) {
      LOG.info("zmdebug - predictPolicy = BOTH");
      double maxRecency = this.fileRecency.size() - 1;
      double maxFrequency = 0;
      double weightRecency = 0.5, weightFrequency = 1 - weightRecency;
      HashMap<Long, Double> file2weight = new HashMap<>();
      for (Map.Entry<Long, Integer> en : this.file2frequency.entrySet()) {
        if (en.getValue() > maxFrequency) {
          maxFrequency = en.getValue();
        }
      }
      long fileId;
      for (int i = 0; i < this.fileRecency.size(); i++) {
        fileId = this.fileRecency.get(i);
        file2weight.put(fileId, i / maxRecency * weightRecency +
                this.file2frequency.get(fileId) / maxFrequency * weightFrequency);
      }
      Map<Long, Double> sortedFile2weight = sortByValueDouble(file2weight);
      for (Map.Entry<Long, Double> en : sortedFile2weight.entrySet()) {
        filesPredicted.add(0, en.getKey());
      }
    }

    return filesPredicted;
  }

  // Called by ParityCacheManager
  public synchronized void addNodeServiceRate(String nodeAddr, Double nodeServiceRate) {
    nodesServiceRate.put(nodeAddr, nodeServiceRate);
  }

  // Called by ParityCacheManager
  public synchronized void addSlowDataNode(String nodeAddr) {
    LOG.info("zmdeug - addSlowDataNode(): {}", nodeAddr);
    slowDataNodes.add(nodeAddr);
  }

  // Return where the file with key has parity in cache.
  // And update the information on file with key: recency, frequency, infor required by ARC
  public synchronized int getCachedParity(long key) {
    // Update recency
    if (this.fileRecency.indexOf(key) != -1) {
      this.fileRecency.remove(key);
    }
    this.fileRecency.add(key);

    // Update frequency
    if (this.file2frequency.containsKey(key)) {
      this.file2frequency.put(key, this.file2frequency.get(key) + 1);
    } else {
      this.file2frequency.put(key, 1);
    }

    // For ARC
    if (this.evictPolicy == EvictionPolicy.ARC) {
      this.arcAlg.getFromCSAC(key);
    }

    return this.filesInCache.contains(key)? 1 : 0;
  }

  // If there is no straggler, let admission policy decide to cache file with key or not;
  // Else if file with key has some blocks on the straggler nodes, then cache it;
  // Otherwise, do NOT cache file with key.
  public synchronized boolean shouldCacheParity(long key) {
    if (slowDataNodes.isEmpty()) {
      return getDecisionOfAdmission(key);
    } else {
      BlockInfo[] blocks = pcm.getBlockInfo(key);
      for (int i = 0; i < blocks.length; i++) {
        int numNodes = blocks[i].numNodes();
        for (int j = 0; j < numNodes; j++) {
          String dataNode = blocks[i].getDatanode(j).getName();
          if (slowDataNodes.contains(dataNode))
            return true;
        }
      }
    }
    return false;
  }

  // The admission policy decides whether to cache file with key
  private synchronized boolean getDecisionOfAdmission(long key) {
    if (this.admitPolicy == AdmissionPolicy.OSA) {
      if (this.fileRecency.get(this.fileRecency.size() - 1) == key) {
        return true;
      }
    } else if (this.admitPolicy == AdmissionPolicy.LFU) {
      if (this.availableCapacity > 0 || this.file2frequency.get(key) > this.leastFrequency) {
        return true;
      }
    } else {
      LOG.error("zmdebug - getDecisionOfAdmission(), invalid admitPolicy");
    }
    return false;
  }

  // Return the fileID to evict
  public synchronized HashSet<Long> updateCachedParity(long fileID, int parNum) {
    HashSet<Long> fileIdEvict = new HashSet<>();
    if (!this.filesInCache.contains(fileID) && parNum > 0) {// shouldCache = true;
      if (this.availableCapacity < parNum) {
        while (this.availableCapacity < parNum) {
          long fid = getDecisionOfEviction(fileID);
          if (this.filesInCache.contains(fid)) {
            fileIdEvict.add(fid);
            this.filesInCache.remove(fid);
            this.availableCapacity++;
          } else {
            LOG.error("zmdebug - fileToEvict = {} is not in cache", fid);
          }
        }
      } else {
        if (this.evictPolicy == EvictionPolicy.ARC) {
          this.arcAlg.updateFromCSAC(fileID);
        }
      }
      // Bring fileID to the cache
      this.availableCapacity -= parNum;
      this.filesInCache.add(fileID);
    }
    if (this.filesInCache.size() != this.capacity - this.availableCapacity) {
      LOG.error("zmdebug - inconsitent cache space: filesInCache.size = {}, capactiy = {}, availableCapacity = {}",
              this.filesInCache.size(), capacity, this.availableCapacity);
    }
    LOG.info("zmdebug - cache {}, remove {}", fileID, fileIdEvict);

    return fileIdEvict;
  }

  // The eviction policy decides which file to be evicted to cache the file, fileToCache
  private synchronized long getDecisionOfEviction(long fileToCache) {
    long fileToEvict = -1;
    if (this.evictPolicy == EvictionPolicy.LRU) {
      // Remove the least recent file
      if (fileRecency.size() - 1 - this.capacity < 0) {
        LOG.error("zmdebug - fileRecency should be larger than capacity!");// Because the one on tail may has not been cached
      }
      for (int i = fileRecency.size() - 1 - this.capacity; i < fileRecency.size(); i++) {
        if (this.filesInCache.contains(this.fileRecency.get(i))) {
          fileToEvict = this.fileRecency.get(i);
          break;
        }
      }
    } else if (this.evictPolicy == EvictionPolicy.LFU) {
      Iterator it = this.filesInCache.iterator();
      long fileId;
      int minFrequency = Integer.MAX_VALUE;
      while (it.hasNext()) {
        fileId = (long) it.next();
//        LOG.info("zmdebug - minFrequency = {}, {}.frequency = {}", minFrequency, fileId, this.file2frequency.get(fileId));
        if (this.file2frequency.get(fileId) <= minFrequency) {
            if (fileToEvict == -1 || this.fileRecency.indexOf(fileId) < this.fileRecency.indexOf(fileToEvict)) {
              fileToEvict = fileId;
              minFrequency = this.file2frequency.get(fileToEvict);
              leastFrequency = minFrequency;
            }
        }
      }
    } else if (this.evictPolicy == EvictionPolicy.ARC) {
      fileToEvict = this.arcAlg.updateFromCSAC(fileToCache);
    } else {
      LOG.error("zmdebug - getDecisionOfEviction(), invalid evictPolicy");
    }
    return fileToEvict;
  }

  // function to sort hashmap by values
  // Sort by ascending order
  private static HashMap<Long, Integer> sortByValueInteger(HashMap<Long, Integer> hm) {
    // Create a list from elements of HashMap
    List<Map.Entry<Long, Integer>> list = new LinkedList<Map.Entry<Long, Integer> >(hm.entrySet());

    // Sort the list
    Collections.sort(list, new Comparator<Map.Entry<Long, Integer>>() {
      public int compare(Map.Entry<Long, Integer> o1,
                         Map.Entry<Long, Integer> o2) {
        return (o1.getValue()).compareTo(o2.getValue());
      }
    });

    // put data from sorted list to hashmap
    HashMap<Long, Integer> temp = new LinkedHashMap<Long, Integer>();
    for (Map.Entry<Long, Integer> aa : list) {
      temp.put(aa.getKey(), aa.getValue());
    }
    return temp;
  }

  // function to sort hashmap by values
  // Sort by ascending order
  private static HashMap<Long, Double> sortByValueDouble(HashMap<Long, Double> hm) {
    // Create a list from elements of HashMap
    List<Map.Entry<Long, Double>> list = new LinkedList<Map.Entry<Long, Double> >(hm.entrySet());

    // Sort the list
    Collections.sort(list, new Comparator<Map.Entry<Long, Double>>() {
      public int compare(Map.Entry<Long, Double> o1,
                         Map.Entry<Long, Double> o2) {
        return (o1.getValue()).compareTo(o2.getValue());
      }
    });

    // put data from sorted list to hashmap
    HashMap<Long, Double> temp = new LinkedHashMap<Long, Double>();
    for (Map.Entry<Long, Double> aa : list) {
      temp.put(aa.getKey(), aa.getValue());
    }
    return temp;
  }
}
