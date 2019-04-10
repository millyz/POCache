// POCache added on Jan. 1, 2018
// This is LRU algorithm for parity cache.
package org.apache.hadoop.hdfs.server.namenode.pcache;

import java.util.HashSet;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LRUCache implements ParityCacheAlgorithm {
  public static final Logger LOG = LoggerFactory.getLogger(LRUCache.class);
  protected int capacity;
  protected HashMap<Long, Node> map = new HashMap<>();
  protected Node head = null;
  protected Node end = null;
  protected HashSet<Long> newFiles = new HashSet<>();

  public LRUCache(int capacity) {
    this.capacity = capacity;
  }

  public int getCachedParity(long key) {
    if(map.containsKey(key)) {
      Node n = map.get(key);
      remove(n);
      setHead(n);
      return n.value;
    } else {
      this.newFiles.add(key);
    }
    return 0;
  }

  public boolean shouldCacheParity(long fileID) {
    if (this.newFiles.contains(fileID)) return true;
    return false;
  }

  public HashSet<Long> updateCachedParity(long key, int value) {
    HashSet<Long> fileIdEvict = new HashSet<Long>();
    if(map.containsKey(key)) {
      Node old = map.get(key);
      if (old.value != value) {
        return update(key, value);
      }
      remove(old);
      setHead(old);
    } else {
      Node created = new Node(key, value);
      if(capacity < value) {
        while (capacity < value) {
          capacity += end.value;
          fileIdEvict.add(end.key);
          map.remove(end.key);
          remove(end);
        }
        setHead(created);
      } else {
        setHead(created);
      }    
      map.put(key, created);
      capacity -= value;
      if (this.newFiles.contains(key))
        this.newFiles.remove(key);
    }
    return fileIdEvict;
  }

  protected HashSet<Long> update(long key, int value) {
    HashSet<Long> fileIdEvict = new HashSet<Long>();
    // TODO - incease/decrease the parity for the cached file
    return fileIdEvict;
  }

  protected void remove(Node n){
    if(n.pre != null){
      n.pre.next = n.next;
    } else {
      head = n.next;
    }

    if(n.next != null){
      n.next.pre = n.pre;
    } else {
      end = n.pre;
    }
  }

  protected void setHead(Node n){
    n.next = head;
    n.pre = null;
    if(head != null) {
      head.pre = n;
    }
    head = n;
    if(end == null) {
      end = head;
    }
  }

  public static class Node {
    long key;// fileID
    int value;// number of parity blocks for each file
    Node pre;
    Node next;

    public Node(long key, int value){
      this.key = key;
      this.value = value;
    }
  }
}
