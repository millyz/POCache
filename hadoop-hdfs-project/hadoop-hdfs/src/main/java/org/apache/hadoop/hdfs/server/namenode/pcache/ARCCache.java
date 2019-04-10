// POCache added on Feb. 4, 2018
// This is ARC algorithm for parity cache
// Adapted from https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/adaptive/ArcPolicy.java


package org.apache.hadoop.hdfs.server.namenode.pcache;


import java.util.HashSet;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum QueueType { T1, B1, T2, B2 };

class ArcNode {
  final long key; // key is the fileID
  int value;
  ArcNode prev;
  ArcNode next;
  QueueType type;

  ArcNode() {
    this.key = Long.MIN_VALUE;
    this.value = 1;
    this.prev = this;
    this.next = this;
  }

  ArcNode(long key) {
    this.key = key;
    this.value = 1;
    this.prev = this;
    this.next = this;
  }

  ArcNode(long key, int value) {
    this.key = key;
    this.value = value;
  }

  /** Appends the node to the tail of the list. */
  public void appendToTail(ArcNode head) {
    ArcNode tail = head.prev;
    head.prev = this;
    // POCache added correct on Apr. 20, 2018
    this.next = head;

    this.prev = tail;
    tail.next = this;
  }

  /** Removes the node from the list */
  public void remove() {
    prev.next = this.next;
    next.prev = this.prev;
    prev = next = null;
    type = null;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("ArcNode(").append(key).append(",").
      append(value).append(",").append(type).append(")").toString();
  }
}

public class ARCCache implements ParityCacheAlgorithm {
  // In Cache:
  // - T1: Pages that have been accessed at least once
  // - T2: Pages that have been accessed at least twice
  // Ghost:
  // - B1: Evicted from T1
  // - B2: Evicted from T2
  // Adapt:
  // - Hit in B1 should increase size of T1, drop entry from T2 to B2
  // - Hit in B2 should increase size of T2, drop entry from T1 to B1
  public static final Logger LOG = LoggerFactory.getLogger(ARCCache.class);
  private final int capacity;
  private final HashMap<Long, ArcNode> map;
  private final ArcNode headT1;
  private final ArcNode headT2;
  private final ArcNode headB1;
  private final ArcNode headB2;
  private int sizeT1;
  private int sizeT2;
  private int sizeB1;
  private int sizeB2;
  private int p;

  public ARCCache(int capacity) {
    this.capacity = capacity;
    this.map = new HashMap<>();
    this.headT1 = new ArcNode();
    this.headT2 = new ArcNode();
    this.headB1 = new ArcNode();
    this.headB2 = new ArcNode();
  }

  // Get the parity number that are in cache for key
  public int getCachedParity(long key) {
    if (this.map.containsKey(key)) {
      ArcNode node = this.map.get(key);
      int val = node.value;
      // onHit
      if (node.type == QueueType.T1 || node.type == QueueType.T2) {
        this.onHit(node);
        return val;
      }
    }
    return 0;
  }

  // Cache all the new files that are not in cache
  public boolean shouldCacheParity(long key) {
    LOG.info("qpdebug: current capacity {}", this.capacity);
    if (this.map.containsKey(key)) {
      ArcNode node = this.map.get(key);
      if (node.type == QueueType.T1 || node.type == QueueType.T2) {
        return false;
      }
    }
    return true;
  }

  // Load file's parity into cache
  public HashSet<Long> updateCachedParity(long key, int new_value) {
    LOG.info("qpdebug: current capacity {}", this.capacity);
    ArcNode node = map.get(key);
    if (node == null) {
      return onMiss(key, new_value);
    } else if (node.type == QueueType.B1) {
      return onHitB1(node, new_value);
    } else if (node.type == QueueType.B2) {
      return onHitB2(node, new_value);
    } else {
      onHit(node);
    }
    return null;
  }

  // T1 --> T2
  private void onHit(ArcNode node) {
    // x ∈ T1 
    if (node.type == QueueType.T1) {
      sizeT1--;
      sizeT2++;
    }
    node.remove();
    node.type = QueueType.T2;
    node.appendToTail(headT2);
  }

  private HashSet<Long> onHit(ArcNode node, int new_value) {
    if (node.type == QueueType.T1) {
      sizeT1--;
      sizeT2++;
    }
    node.remove();
    node.type = QueueType.T2;
    node.appendToTail(headT2);
    return new HashSet<>();
  }

  // B1 --> T2
  private HashSet<Long> onHitB1(ArcNode node, int new_value) {
    p = Math.min(capacity, p + Math.max(sizeB2 / sizeB1, 1));
    HashSet<Long> fileIdEvict = evict(node);

    sizeT2++;
    sizeB1--;
    node.remove();
    node.type = QueueType.T2;
    node.appendToTail(headT2);

    return fileIdEvict;
  }

  // B2 --> T2
  private HashSet<Long> onHitB2(ArcNode node, int new_value) {
    p = Math.max(0, p - Math.max(sizeB1 / sizeB2, 1));
    HashSet<Long> fileIdEvict = evict(node);

    sizeT2++;
    sizeB2--;
    node.remove();
    node.type = QueueType.T2;
    node.appendToTail(headT2);
    return fileIdEvict;
  }

  // --> T1
  private HashSet<Long> onMiss(long key, int new_value) {
    ArcNode node = new ArcNode(key);
    node.type = QueueType.T1;

    HashSet<Long> fileIdEvict = new HashSet<>();

    int sizeL1 = (sizeT1 + sizeB1);
    int sizeL2 = (sizeT2 + sizeB2);

    if (sizeL1 == capacity) {
      if (sizeT1 < capacity) {
        ArcNode victim = headB1.next;
        map.remove(victim.key);
        victim.remove();
        sizeB1--;
        fileIdEvict = evict(node);
      } else {
        ArcNode victim = headT1.next;
        map.remove(victim.key);
        victim.remove();
        sizeT1--;
        fileIdEvict.add(victim.key);
      }
    } else if ((sizeL1 < capacity) && (sizeL1 + sizeL2) >= capacity) {
      if ((sizeL1 + sizeL2) >= (2 * capacity)) {
        ArcNode victim = headB2.next;
        map.remove(victim.key);
        victim.remove();
        sizeB2--;
      }
      // TODO: ??
      fileIdEvict = evict(node);
    }

    sizeT1++;
    map.put(key, node);
    node.appendToTail(headT1);

    return fileIdEvict;
  }

  private HashSet<Long> evict(ArcNode candidate) {
    HashSet<Long> fileIdEvict = new HashSet<>();
    if ((sizeT1 >= 1) && (((candidate.type == QueueType.B2) && (sizeT1 == p)) || (sizeT1 > p))) {
      ArcNode victim = headT1.next;
      victim.remove();
      victim.type = QueueType.B1;
      victim.appendToTail(headB1);
      sizeT1--;
      sizeB1++;
      fileIdEvict.add(victim.key);
    } else {
      ArcNode victim = headT2.next;
      victim.remove();
      victim.type = QueueType.B2;
      victim.appendToTail(headB2);
      sizeT2--;
      sizeB2++;
      fileIdEvict.add(victim.key);
    }
    return fileIdEvict;
  }
}
