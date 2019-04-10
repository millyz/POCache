// POCache added on Nov. 24, 2018
// This is LFU algorithm for parity cache (potato and selective replication)
package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LFUCache implements ParityCacheAlgorithm {
  public static final Logger LOG = LoggerFactory.getLogger(LFUCache.class);
  protected int capacity;

  protected HashMap<Long, Integer> frequencies;
  protected HashMap<Long, Integer> numParities;
  protected List<Long> mostFrequentKeys;
  protected int minFrequnecyInMostFrequentKeys = 0;
  protected HashSet<Long> toEvictKeys;

  public LFUCache(int capacity) {
    this.capacity = capacity;
    this.frequencies = new HashMap<>();
    this.numParities = new HashMap<>();
    this.mostFrequentKeys = new ArrayList();
  }

  public synchronized void addFrequency(long key) {
    if ( !this.frequencies.containsKey(key) ) {
      this.frequencies.put(key, 0);
    }
    int frequency = this.frequencies.get(key) + 1;
    this.frequencies.put(key, frequency);
    if (this.mostFrequentKeys.contains(key)) {
      this.mostFrequentKeys.remove(key);
      this.mostFrequentKeys.add(key);
      return ;
    }
    if (this.mostFrequentKeys.size() < capacity) {
      this.mostFrequentKeys.add(key);
    } else if (frequency >= this.minFrequnecyInMostFrequentKeys) {
      for (int i = 0; i < this.mostFrequentKeys.size(); i++) {
        long _key = this.mostFrequentKeys.get(i);
        if (frequencies.get(key) <= frequency) {
          toEvictKeys.add(_key);
          this.mostFrequentKeys.remove(_key);
          break;
        }
      }
      this.minFrequnecyInMostFrequentKeys = frequency;
      this.mostFrequentKeys.add(key);
    }
    //LOG.info("qpdebug: frequencies {} mostFrequentKeys {}", frequencies, mostFrequentKeys);
  }

  public int getCachedParity(long key) {
    if (numParities.containsKey(key))
      return numParities.get(key);
    else
      return 0;
  }

  public boolean shouldCacheParity(long key) {
    return this.mostFrequentKeys.contains(key);
  }

  public HashSet<Long> updateCachedParity(long key, int value) {
    //LOG.info("qpdebug: to evict keys {}", this.toEvictKeys);
    HashSet<Long> fileIdEvict = this.toEvictKeys;
    this.toEvictKeys = new HashSet<>();

    return fileIdEvict;
  }
}
