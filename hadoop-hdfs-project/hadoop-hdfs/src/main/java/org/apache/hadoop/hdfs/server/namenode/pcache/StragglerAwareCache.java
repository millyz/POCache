package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.ParityCacheManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StragglerAwareCache extends LRUCache {
  public static final Logger LOG = LoggerFactory.getLogger(StragglerAwareCache.class);
  private HashSet<String> slowDataNodes;
  private HashMap<String, Double> nodesServiceRate;

  private ParityCacheManager pcm;

  public StragglerAwareCache(int capacity, ParityCacheManager pcm) {
    super(capacity);
    this.pcm = pcm;
    this.slowDataNodes = new HashSet<>();
    this.nodesServiceRate = new HashMap<>();

    new Thread() {
      public void run() {
        while (true) {
          synchronized (this) {
            double avg = 0, deviation = 0;
            for (Map.Entry<String, Double> entry : nodesServiceRate.entrySet()) {
              avg += entry.getValue();
            }
            if (nodesServiceRate.size() != 0 && nodesServiceRate.size() != 1)
              avg /= nodesServiceRate.size();

            for (Map.Entry<String, Double> entry : nodesServiceRate.entrySet()) {
              double value = entry.getValue();
              deviation += (value - avg) * (value - avg);
            }
            if (nodesServiceRate.size() != 1 && nodesServiceRate.size() != 0)
              deviation /= nodesServiceRate.size() - 1;
            deviation = Math.sqrt(deviation);
            slowDataNodes.clear();
            for (Map.Entry<String, Double> entry : nodesServiceRate.entrySet()) {
              if (entry.getValue() < avg - deviation * 1) {
                slowDataNodes.add(entry.getKey());
              }
            }
            LOG.info("qpdebug: avg {} variance {}", avg, deviation);
            LOG.info("qpdebug: nodes service rate {}", nodesServiceRate);
            LOG.info("qpdebug: straggler nodes {}", slowDataNodes);
          }
          try {
            sleep(500);
          } catch (InterruptedException e) {
            return ;
          }
        }
      }
    }.start();
  }

  public void addNodeServiceRate(String nodeAddr, Double nodeServiceRate) {
    nodesServiceRate.put(nodeAddr, nodeServiceRate);
  }

  public void addSlowDataNode(String nodeAddr) {
    slowDataNodes.add(nodeAddr);
  }

  @Override
  public int getCachedParity(long key) {
    if(map.containsKey(key)) {
      Node n = map.get(key);
      remove(n);
      setHead(n);
      return n.value;
    }
    return 0;
  }

  @Override
  public boolean shouldCacheParity(long key) {
    BlockInfo[] blocks = pcm.getBlockInfo(key);
    for (int i = 0; i < blocks.length; i++) {
      int numNodes = blocks[i].numNodes();
      for (int j = 0; j < numNodes; j++) {
        String dataNode = blocks[i].getDatanode(j).getName();
        if (slowDataNodes.contains(dataNode))
          return true;
      }
    }
    return false;
  }

}
