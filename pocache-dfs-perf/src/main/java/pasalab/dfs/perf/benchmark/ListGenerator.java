package pasalab.dfs.perf.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937a;

/**
 * Test file list generator
 */
public class ListGenerator {
  private static Random sRand = new Random(123456);
  //private static ZipfDistribution zipf;

  public static List<String> generateRandomReadFiles(int filesNum, List<String> candidates) {
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    for (int i = 0; i < filesNum; i ++) {
      ret.add(candidates.get(sRand.nextInt(range)));
    }
    return ret;
  }

  public static List<String> generateZipfReadFiles(int id, int filesNum, List<String> candidates) {
    Well19937a rand = new Well19937a(123456 + id * 100);
    ZipfDistribution zipf = new ZipfDistribution(rand, candidates.size() - 1, 0.9);
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    for (int i = 0; i < filesNum; i++) {
      ret.add(candidates.get(zipf.sample()));
    }
    
    return ret;
  }

  public static List<String> generateSequenceReadFiles(int id, int threadsNum, int filesNum,
      List<String> candidates) {
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    int index = range / threadsNum * id;
    for (int i = 0; i < filesNum; i ++) {
      ret.add(candidates.get(index));
      index = (index + 1) % range;
    }
    return ret;
  }

  public static List<String> generateWriteFiles(int id, int filesNum, String dirPrefix) {
    List<String> ret = new ArrayList<String>(filesNum);
    for (int i = 0; i < filesNum; i ++) {
      ret.add(dirPrefix + "/" + id + "-" + i);
    }
    return ret;
  }
}
