package org.apache.hadoop.hdfs.server.namenode.pcache;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockLatencyReports;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import org.apache.hadoop.hdfs.server.namenode.ParityCacheManager;

public class StragglerEstimator {
  public static final Logger LOG = LoggerFactory.getLogger(StragglerEstimator.class);
  private ParityCacheManager parityCacheManager;
  private Set<DatanodeDescriptor> datanodes;
  private HashMap<String, StragglerInfo> estimates;
  final static double timeGranularity = 0.000000001; // original is nanoSecond;
  final static boolean useTimeForEWMA = true;
  final static double alphaEWMA = 0.8;

  public StragglerEstimator(Set<DatanodeDescriptor> datanodes, ParityCacheManager parityCacheManager) {
    // POCache added
    this.parityCacheManager = parityCacheManager;
    /* */
    this.datanodes = datanodes;
    this.estimates = new HashMap<>();
    long tsNow = Time.monotonicNow();
    this.topkBlocksLatency = new HashMap<>();
    for (DatanodeDescriptor each : datanodes) {
      this.estimates.put(each.getIpAddr(), new StragglerInfo(each.getIpAddr(), tsNow));
    }
    LOG.info("zmdebug: StragglerEstimator is constructed successfully!");
  }

  // The estimate for each node is the probability of being a straggler
  // POCache added on Apr. 3, 2018
  public void updateEstimateForDn(String dnID, boolean isSlow, long tsNow) {
    if (this.estimates.containsKey(dnID)) {
      double valOld = this.estimates.get(dnID).getEstimate();
      double valNew;
      if (useTimeForEWMA) {
        long tsOld = this.estimates.get(dnID).getTimestamp();
        valNew = calEWMAWithTime(alphaEWMA, valOld, isSlow, 0.8, tsOld, tsNow);
      } else {
        valNew = calEWMA(alphaEWMA, valOld, isSlow);
      }
      this.estimates.get(dnID).updateEstimate(valNew, tsNow);
    } else {
      LOG.warn("updateEstimateForDn() - no datanodeID: " + dnID);
    }
  }

  // Calculate the probability of being a straggler using EWMA
  public double calEWMA(double alpha, double valOld, boolean isSlow) {
    if (isSlow) {
      return alpha * 1. + (1. - alpha) * valOld;
    } else {
      return (1. - alpha) * valOld;
    }
  }

  // Calculate the probability of being a straggler using EWMAwithTime
  public double calEWMAWithTime(double alpha, double valOld, boolean isSlow,
      double lambda, long tsOld, long tsNow) {
    double timePart = Math.exp((tsOld - tsNow) * timeGranularity / lambda);
    if (isSlow) {
      return alpha * 1. + (1. - alpha) * valOld * timePart;
    } else {
      return (1. - alpha) * valOld * timePart;
    }
  }

  // This is to return the estimated the slow DataNodes for this stripe.
  // The number returned is not more than numParityCached.
  // Here numParityCached is usually is 1
  public List<String> getSlowDNsPossible(List<LocatedBlock> locatedblocks, int numParityCached) {
    List<String> slowDNs = new ArrayList<>();
    double maxProb = 0.;
    String slowDN = null;
    double tsOfSlowDN = Time.monotonicNow();

    for (LocatedBlock lb : locatedblocks) {
      DatanodeInfo[] dnInfo = lb.getLocations();
      for (DatanodeInfo each : dnInfo) {
        String dnID = each.getIpAddr();
        if (this.estimates.get(dnID).getEstimate() > maxProb) {
          maxProb = this.estimates.get(dnID).getEstimate();
          slowDN = dnID;
          tsOfSlowDN = this.estimates.get(dnID).getTimestamp();
        }
      }
    }

    if (slowDN != null) {
      // the slowDN is accessed 30s ago
      double timePast = (Time.monotonicNow() - tsOfSlowDN) * 0.000000001;
      if (timePast < 30.0) {
        slowDNs.add(slowDN);
      }
    }
    return slowDNs;
  }

  // This is to update the estimate for each datanode by taking the newest read latency
  public void updateEstimateForDn(String datanodeID, double readLatency, long tsNow) {
    if (this.estimates.containsKey(datanodeID)) {
      double valOld = this.estimates.get(datanodeID).getEstimate();
      double valNew;
      if (useTimeForEWMA) {
        long tsOld = this.estimates.get(datanodeID).getTimestamp();
        valNew = calEWMAWithTime(alphaEWMA, valOld, readLatency, 0.8, tsOld, tsNow);
      } else {
        valNew = calEWMA(alphaEWMA, valOld, readLatency);
      }
      this.estimates.get(datanodeID).updateEstimate(valNew, tsNow);
    }
  }

  public double calEWMA(double alpha, double valOld, double valUpdate) {
    return alpha * valUpdate + (1. - alpha) * valOld;
  }

  public double calEWMAWithTime(double alpha, double valOld, double valUpdate,
      double lambda, long tsOld, long tsNow) {
    double timePart = Math.exp((tsOld - tsNow) * timeGranularity / lambda);
    return alpha * valUpdate + (1. - alpha) * valOld * timePart;
  }

  public int getDatanodesNum() {
    if (this.estimates == null) {
      return 0;
    } else {
      return this.estimates.size();
    }
  }

  // When new datanodes added to the cluster
  public void addDatanodes(Set<DatanodeDescriptor> datanodes) {
    this.datanodes = datanodes;
    long tsNow = Time.monotonicNow();
    for (DatanodeDescriptor each : datanodes) {
      if (!this.estimates.containsKey(each.getIpAddr())) {
        this.estimates.put(each.getIpAddr(), new StragglerInfo(each.getIpAddr(), tsNow));
      }
    }
  }

  public void showAll() {
    for (String dnID : this.estimates.keySet()) {
      LOG.info("zmdebug: showAll() - dnID {}, estimates {}",
          dnID, this.estimates.get(dnID).getEstimate());
    }
  }

  // POCache added on Sep. 20, 2018
  private Map<Long, Double> topkBlocksLatency;
  
  public synchronized void updateBlockLatency(BlockLatencyReports blocksLatency) {
    // <BlockId, LatencyPerAccess> Map
    HashSet<Long> evicted = new HashSet<>();
    this.topkBlocksLatency.putAll(blocksLatency.getBlocksLatency());

    if (this.topkBlocksLatency.size() <= 40) return ;

    ArrayList<Double> latencies = new ArrayList<>(topkBlocksLatency.values());
    Collections.sort(latencies);
    double _min = latencies.get(latencies.size() - 40);

    for (Map.Entry<Long, Double> entry : topkBlocksLatency.entrySet()) {
      if (entry.getValue() < _min) {
        evicted.add(entry.getKey());
      }
    }

    for (long key : evicted) topkBlocksLatency.remove(key);
    // ParityCacheManager evict possible files
    parityCacheManager.evict(evicted);
  }

  public void updateBlockLatency(long fileId, double latency) {
    // <BlockId, LatencyPerAccess> Map
    HashSet<Long> evicted = new HashSet<>();
    this.topkBlocksLatency.put(fileId, latency);

    if (this.topkBlocksLatency.size() <= 40) return ;

    ArrayList<Double> latencies = new ArrayList<>(topkBlocksLatency.values());
    Collections.sort(latencies);
    double _min = latencies.get(latencies.size() - 40);

    for (Map.Entry<Long, Double> entry : topkBlocksLatency.entrySet()) {
      if (entry.getValue() < _min) {
        evicted.add(entry.getKey());
      }
    }

    for (long key : evicted)  {
      //LOG.info("qpdebug: evict key {}", key);
      topkBlocksLatency.remove(key);
    }
    // ParityCacheManager evict possible files
    parityCacheManager.evict(evicted);
  }

  public boolean checkBlockLatency(long blockId) {
    for (long _blockId : topkBlocksLatency.keySet()) {
      if (_blockId == blockId) return true; 
    }
    return false;
  }
}
