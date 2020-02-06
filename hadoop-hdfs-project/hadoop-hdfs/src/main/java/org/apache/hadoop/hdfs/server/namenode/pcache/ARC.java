// POCache added on Jan. 8, 2020
// This is ARC algorithm used by ConfigurableStraggleAwareCache algorithm
// Adapted from https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/adaptive/ArcPolicy.java
package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ARC {
  // In Cache:
  // - T1: Pages that have been accessed at least once
  // - T2: Pages that have been accessed at least twice
  // Ghost:
  // - B1: Evicted from T1
  // - B2: Evicted from T2
  // Adapt:
  // - Hit in B1 should increase size of T1, drop entry from T2 to B2
  // - Hit in B2 should increase size of T2, drop entry from T1 to B1
  public static final Logger LOG = LoggerFactory.getLogger(ARC.class);
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

  public ARC(int capacity) {
    this.capacity = capacity;
    this.map = new HashMap<>();
    this.headT1 = new ArcNode();
    this.headT2 = new ArcNode();
    this.headB1 = new ArcNode();
    this.headB2 = new ArcNode();
  }

  // File with key is read
  public void getFromCSAC(long key) {
    if (this.map.containsKey(key)) {
      ArcNode node = this.map.get(key);
      // onHit
      if (node.type == QueueType.T1 || node.type == QueueType.T2) {
        this.onHit(node);
      }
    }
  }

  // Cache file with key From CSAC
  public long updateFromCSAC(long key) {
    ArcNode node = map.get(key);
    if (node == null) {
      return onMiss(key);
    } else if (node.type == QueueType.B1) {
      return onHitB1(node);
    } else if (node.type == QueueType.B2) {
      return onHitB2(node);
    } else {
      LOG.error("zmdebug - evictFromSAC(): {} has been cached.", key);
    }
    return -1;
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

  // B1 --> T2
  private long onHitB1(ArcNode node) {
    p = Math.min(capacity, p + Math.max(sizeB2 / sizeB1, 1));
    long fileIdEvict = evict(node);

    sizeT2++;
    sizeB1--;
    node.remove();
    node.type = QueueType.T2;
    node.appendToTail(headT2);

    return fileIdEvict;
  }

  // B2 --> T2
  private long onHitB2(ArcNode node) {
    p = Math.max(0, p - Math.max(sizeB1 / sizeB2, 1));
    long fileIdEvict = evict(node);

    sizeT2++;
    sizeB2--;
    node.remove();
    node.type = QueueType.T2;
    node.appendToTail(headT2);
    return fileIdEvict;
  }

  // --> T1
  private long onMiss(long key) {
    ArcNode node = new ArcNode(key);
    node.type = QueueType.T1;
    long fileIdEvict = -1;

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
        fileIdEvict = victim.key;
      }
    } else if ((sizeL1 < capacity) && (sizeL1 + sizeL2) >= capacity) {
      if ((sizeL1 + sizeL2) >= (2 * capacity)) {
        ArcNode victim = headB2.next;
        map.remove(victim.key);
        victim.remove();
        sizeB2--;
      }
      fileIdEvict = evict(node);
    }

    sizeT1++;
    map.put(key, node);
    node.appendToTail(headT1);

    return fileIdEvict;
  }

  private long evict(ArcNode candidate) {
    long fileIdEvict = -1;
    if ((sizeT1 >= 1) && (((candidate.type == QueueType.B2) && (sizeT1 == p)) || (sizeT1 > p))) {
      ArcNode victim = headT1.next;
      victim.remove();
      victim.type = QueueType.B1;
      victim.appendToTail(headB1);
      sizeT1--;
      sizeB1++;
      fileIdEvict = victim.key;
    } else {
      ArcNode victim = headT2.next;
      victim.remove();
      victim.type = QueueType.B2;
      victim.appendToTail(headB2);
      sizeT2--;
      sizeB2++;
      fileIdEvict = victim.key;
    }
    return fileIdEvict;
  }
}
