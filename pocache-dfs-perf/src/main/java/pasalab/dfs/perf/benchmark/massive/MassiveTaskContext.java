package pasalab.dfs.perf.benchmark.massive;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.benchmark.SimpleTaskContext;

public class MassiveTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(3);
    mInfoStatistics = new HashMap<String, List<String>>(1);
    List<Double> basicThroughputs = new ArrayList<Double>(threads.length);
    List<Double> readThroughputs = new ArrayList<Double>(threads.length);
    List<Double> writeThroughputs = new ArrayList<Double>(threads.length);
    List<String> latencies = new ArrayList<String>(threads.length);
    for (PerfThread thread : threads) {
      if (!((MassiveThread) thread).getSuccess()) {
        mSuccess = false;
      }
      basicThroughputs.add(((MassiveThread) thread).getBasicWriteThroughput());
      readThroughputs.add(((MassiveThread) thread).getReadThroughput());
      writeThroughputs.add(((MassiveThread) thread).getWriteThroughput());
      latencies.add(((MassiveThread) thread).getLatencies());
    }
    mAdditiveStatistics.put("BasicWriteThroughput(MB/s)", basicThroughputs);
    mAdditiveStatistics.put("ReadThroughput(MB/s)", readThroughputs);
    mAdditiveStatistics.put("WriteThroughput(MB/s)", writeThroughputs);
    mInfoStatistics.put("Latencies of accesses(ms)", latencies);
  }
}
