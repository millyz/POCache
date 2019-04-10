// POCache added on Jan. 1, 2018
// This is the algorithm for parity cache
package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;

public interface ParityCacheAlgorithm {
  int getCachedParity(long fileID); // return the number of cached parity;
  boolean shouldCacheParity(long fileID); // return whether to cache parities for fileID or not
  HashSet<Long> updateCachedParity(long fileID, int parNum); // return the fileID to evict;
}

