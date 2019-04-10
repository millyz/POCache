package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.ParityCacheManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StragglerAwareLRU extends LRUCache {
  public static final Logger LOG = LoggerFactory.getLogger(StragglerAwareLRU.class);
  private HashSet<String> slowDataNodes;
  private HashMap<String, Double> nodesServiceRate;
  private HashMap<Long, Long> ttls;
  // shouldCacheParity would decide a node to remove (set nodeToRemove)
  // and in the updateCacheParity, this node would be removed (get nodeToRemove)
  // be sure to clear this after using it (clear nodeToRemove)
  private Node nodeToRemove;

  private ParityCacheManager pcm;

  public StragglerAwareLRU(int capacity, ParityCacheManager pcm) {
    super(capacity);
    this.pcm = pcm;
    this.ttls = new HashMap<>();
    this.slowDataNodes = new HashSet<>();
    this.nodesServiceRate = new HashMap<>();

    new Thread() {
      public void run() {
        while (true) {
          synchronized (nodesServiceRate) {
            // Calculate the average and std of all nodes' service rate
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
            // Figure out the slow DNs
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

  public void addNodeServiceRate(
      String nodeAddr, Double nodeServiceRate) {
    nodesServiceRate.put(nodeAddr, nodeServiceRate);
  }

  public void addSlowDataNode(String nodeAddr) {
    slowDataNodes.add(nodeAddr);
  }

  @Override
  public boolean shouldCacheParity(long key) {
    long value = pcm.getNumMemBlksByFileId(key);
    if (value <= this.capacity)
      return true;

    if (value > this.capacity) {
      // Admission policy
      BlockInfo[] blocks = pcm.getBlockInfo(key);
      for (int i = 0; i < blocks.length; i++) {
        int numNodes = blocks[i].numNodes();
        for (int j = 0; j < numNodes; j++) {
          String dataNode = blocks[i].getDatanode(j).getName();
          if (slowDataNodes.contains(dataNode)) {
            return true;
          }
        }
      }
  
      // Eviction policy
      Node node = this.end;
      while (true) {
        //LOG.info("** node {}", node.key);
        boolean shouldCache = true;
        long oldKey = node.key;
        blocks = pcm.getBlockInfo(oldKey);

        // loop over all datablocks within the file
        // to decide whether it hits the straggler
        // if it hits straggler and is still alive
        // then we cannot evict it
        // otherwise shouldCache should be true
        // and this node should be evicted
        for (int i = 0; i < blocks.length; i++) {
          int numNodes = blocks[i].numNodes();
          for (int j = 0; j < numNodes; j++) {
            String dataNode = blocks[i].getDatanode(j).getName();
            if (shouldCache == true
                && slowDataNodes.contains(dataNode)) {
              shouldCache = false;
            }
          }
        }
        if (shouldCache == true) {
          nodeToRemove = node;
          //LOG.info("******");
          return true;
        }

        if (node == this.head) break;
        node = node.pre;
      }
      //LOG.info("******");
    }
    nodeToRemove = null;
    return false;
  }

  @Override
  public synchronized HashSet<Long> updateCachedParity(long key, int value) {
    Node tmp = head;
    //if (tmp != null) {
      //LOG.info("qpdebug: ***** cached nodes:");
      //do {
        //LOG.info("node {}", tmp.key);
        //tmp = tmp.next;
      //} while (tmp != null);

      //LOG.info("qpdebug: *********");
    //}
    HashSet<Long> fileIdEvict = new HashSet<Long>();
    if(map.containsKey(key)) {
      Node old = map.get(key);
      if (old.value != value) {
        return update(key, value);
      }
      remove(old);
      setHead(old);
      //ttls.put(key, (long)20);
    } else {
      Node created = new Node(key, value);
      //ttls.put(key, (long)20);
      if (capacity < value) {
        if (this.nodeToRemove == null)
          this.nodeToRemove = end;
        capacity += this.nodeToRemove.value;
        fileIdEvict.add(this.nodeToRemove.key);
        map.remove(this.nodeToRemove.key);
        //ttls.remove(this.nodeToRemove.key);
        remove(this.nodeToRemove);
        this.nodeToRemove = null;

        setHead(created);
      } else {
        setHead(created);
      }    
      map.put(key, created);
      capacity -= value;
    }
    return fileIdEvict;
  }
}
