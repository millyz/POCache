package org.apache.hadoop.hdfs.server.namenode.pcache;

import org.apache.hadoop.hdfs.protocol.DatanodeID;

import javax.xml.crypto.Data;

// This class is to keep record of the related information on each DataNode
public class StragglerInfo {
  private String dnID;
  private double estimate;
  private long timestamp;

  StragglerInfo(String dnID, long ts) {
    this.dnID = dnID;
    this.estimate = 0.;
    this.timestamp = ts;
  }

  StragglerInfo(String dnID, double estimate, long timestamp) {
    this.dnID = dnID;
    this.estimate = estimate;
    this.timestamp = timestamp;
  }

  void updateEstimate(double newEstimate) {
    this.estimate = newEstimate;
    this.timestamp = System.nanoTime();
  }

  void updateEstimate(double newEstimate, long newTimestamp) {
    this.estimate = newEstimate;
    this.timestamp = newTimestamp;
  }

  String getDnID() {
    return this.dnID;
  }

  double getEstimate() {
    return this.estimate;
  }

  long getTimestamp() { return this.timestamp; }
}

