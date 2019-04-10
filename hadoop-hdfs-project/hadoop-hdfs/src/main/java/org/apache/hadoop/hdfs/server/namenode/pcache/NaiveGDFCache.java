package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.lang.Math;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class NaiveGDFCache implements ParityCacheAlgorithm {
  private int capacity;
  private int used;
  private final double w;
  private HashMap<Long, Integer> file2parity;
  private HashMap<Long, Double> file2frequency;
  private HashMap<Long, Long> file2ts;
  private final int countThr; // count threshold to cache in a file  
  private HashMap<Long, Integer> file2count; // read times of tiles not in cache
  final static long startTime = 0;
  private long curTime = startTime;
  private Logger LOG = Logger.getLogger(String.valueOf(getClass()));

  public NaiveGDFCache(int capacity, double w, int countThr) {
    this.capacity = capacity;
    this.w = w;
    this.countThr = countThr;
    used = 0;
    file2parity = new HashMap<>();
    file2frequency = new HashMap<>();
    file2ts = new HashMap<>();
    file2count = new HashMap<>();
    LOG.info("w = " + this.w + ", countThr=" + this.countThr);
  }

  public synchronized int getCachedParity(long fileID) {
    Integer numParity = file2parity.get(fileID);
    if (numParity == null) {
      if (file2count.get(fileID) == null) {
        file2count.put(fileID, 1);
      } else {
        int readCount = file2count.get(fileID);
        readCount++;
        file2count.put(fileID, readCount);
      }
      return 0;
    } else {
      double freq = file2frequency.get(fileID);
      long ts = file2ts.get(fileID);
      curTime += 1;
      freq = freq * Math.exp(-w * (curTime - startTime)) + 1;
      file2frequency.put(fileID, freq);
      file2ts.put(fileID, curTime);
      return numParity;
    }
  }

  public synchronized boolean shouldCacheParity(long fileID) {
    if (used == capacity) {
      return file2count.get(fileID) >= countThr;
    }
    return !file2parity.containsKey(fileID);
  }

  public synchronized HashSet<Long> updateCachedParity(long fileID, int parNum) {
    HashSet<Long> evicted = new HashSet<>();
    curTime += 1;
    if (used + parNum > capacity) {
      file2frequency.replaceAll((k, v) -> v * Math.exp(-w * (curTime - file2ts.get(k))));
      file2ts.replaceAll((k, v) -> curTime);
      while (used + parNum > capacity) {
        Map.Entry<Long, Double> minEntry = null;
        for (Map.Entry<Long, Double> entry : file2frequency.entrySet()) {
          if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0) {
            minEntry = entry;
          }
        }
        used -= file2parity.get(minEntry.getKey());
        file2parity.remove(minEntry.getKey());
        file2frequency.remove(minEntry.getKey());
        file2ts.remove(minEntry.getKey());
        evicted.add(minEntry.getKey());
      }
    }
    file2parity.put(fileID, parNum);
    file2frequency.put(fileID, 1.0);
    file2ts.put(fileID, curTime);
    used += parNum;
    if (file2count.containsKey(fileID)) {
      file2count.remove(fileID);
    }

    return evicted;
  }
}
