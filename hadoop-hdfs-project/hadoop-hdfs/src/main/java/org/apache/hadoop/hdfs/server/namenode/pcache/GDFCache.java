package org.apache.hadoop.hdfs.server.namenode.pcache;

import org.apache.hadoop.hdfs.server.namenode.pcache.ParityCacheAlgorithm;

import java.lang.Math;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GDFCache implements ParityCacheAlgorithm {

  public static final Logger LOG = LoggerFactory.getLogger(GDFCache.class);
  private int capacity;
  private int used;
  private final double w;
  private HashMap<Long, Integer> file2parity;
  private HashMap<Long, Double> file2frequency;
  private HashMap<Long, Double> newfile2frequency;
  private final double freqThr; // frequency threshold to cache in a file
  // final static long startTime = 0;
  // private long curTime = startTime;
  // private logger LOG = Logger.getLogger(String.valueOf(getClass()));

  public GDFCache(int capacity, double w, double freqThr) {
    this.capacity = capacity;
    this.w = w;
    this.freqThr = freqThr;
    used = 0;
    file2parity = new HashMap<>();
    file2frequency = new HashMap<>();
    newfile2frequency = new HashMap<>();
  }

  public synchronized int getCachedParity(long fileID) {
    file2frequency.replaceAll((k, v) -> v * Math.exp(-w));
    newfile2frequency.replaceAll((k, v) -> v * Math.exp(-w));
    Integer numParity = file2parity.get(fileID);

    if (numParity == null) {
      if (newfile2frequency.get(fileID) == null) {
        newfile2frequency.put(fileID, 1.0);
      } else {
        double freq = newfile2frequency.get(fileID);
        freq += 1;
        newfile2frequency.put(fileID, freq);
      }
      return 0;
    } else {
      double freq = file2frequency.get(fileID);
      freq++;
      file2frequency.put(fileID, freq);
      return numParity;
    }
  }

  public synchronized boolean shouldCacheParity(long fileID) {
    if (used == capacity) {
      return newfile2frequency.get(fileID) >= freqThr;
    }
    return !file2parity.containsKey(fileID);
  }

  public synchronized HashSet<Long> updateCachedParity(long fileID, int parNum) {
    LOG.info("qpdebug: current capacity {}", this.capacity);
    HashSet<Long> evicted = new HashSet<>();

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
      evicted.add(minEntry.getKey());
    }
    file2parity.put(fileID, parNum);
    used += parNum;
    if (newfile2frequency.containsKey(fileID)) {
      file2frequency.put(fileID, newfile2frequency.get(fileID));
      newfile2frequency.remove(fileID);
    } else {
      file2frequency.put(fileID, 1.0);
    }
    return evicted;
  }
}
