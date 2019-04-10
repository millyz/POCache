// POCache added on Oct. 09, 2018
package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.MutablePair;

public class StragglerAlgorithm {

  private class LongListOfLongMapping {
    private HashMap<Long, List<Long>> leftToRight;
    private HashMap<Long, Long> rightToLeft;

    LongListOfLongMapping() {
      this.leftToRight = new HashMap<>();
      this.rightToLeft = new HashMap<>();
    }

    public List<Long> getRight(long left) {
      return this.leftToRight.get(left);
    }

    public Long getLeft(long right) {
      return this.rightToLeft.get(right);
    }

    public void addMapping(long fileId, List<Long> blockIds) {
      leftToRight.put(fileId, blockIds);
      for (long blockId : blockIds) {
        rightToLeft.put(blockId, fileId);
      }
    }
  }

  private LongListOfLongMapping fileIdBlockIdsMapping;
  private LongListOfLongMapping nodeIdBlockIdsMapping;

  private HashMap<Long, Double> blockIdToStats; 
  private HashSet<Long> topkBlockIds;

  private int capacity;

  public StragglerAlgorithm(int capacity) {
    this.capacity = capacity;
    fileIdBlockIdsMapping = new LongListOfLongMapping();
  }

  public int getCachedParity(long fileId) {
    return 0;
  }

  //public HashSet<Long> updateCachedParity(long fileId) {
    //boolean shouldCache = shouldCache(fileId);

    //if (shouldCache) {
      
    //}
    //return null;
  //}

  public synchronized void updateBlockStats(HashMap<Long, Double> blockStats) {
    this.blockIdToStats.putAll(blockStats);

    this.topkBlockIds.addAll(blockStats.keySet());
    if (this.topkBlockIds.size() > this.capacity) {
      Stream<MutablePair<Long, Double>> evicted = this.topkBlockIds.stream().map(
          blockId -> new MutablePair<>(
            blockId, this.blockIdToStats.get(blockId)
          )
        ).sorted(
          (MutablePair<Long, Double> p1, MutablePair<Long, Double> p2) ->
            p1.getValue().compareTo(p2.getValue())
        );
      this.topkBlockIds.removeAll(
          evicted.limit(
            this.topkBlockIds.size() - this.capacity
          ).collect(Collectors.toSet())
        );
    }
  }

  public boolean shouldCache(long fileId) {
    List<Long> blockIds = fileIdBlockIdsMapping.getRight(fileId);

    return blockIds.stream().map(
          blockId -> blockIds.contains(blockId)
        ).reduce(
          false, (a, b) -> a && b
        );
  }

  // Add fileId to blockIds mapping
  // Add blockId to fileId mapping
  public void addFileIdBlockIdsMapping(long fileId, List<Long> blockIds) {
    fileIdBlockIdsMapping.addMapping(fileId, blockIds);
  }
}
