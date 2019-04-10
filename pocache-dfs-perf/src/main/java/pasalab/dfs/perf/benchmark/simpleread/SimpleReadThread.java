package pasalab.dfs.perf.benchmark.simpleread;

import java.io.IOException;
import java.util.List;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.benchmark.ListGenerator;
import pasalab.dfs.perf.benchmark.Operators;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

import org.apache.commons.io.IOUtils;
import java.io.InputStream;
import org.apache.log4j.Logger;
import java.util.ArrayList;

public class SimpleReadThread extends PerfThread {
  private int mBufferSize;
  private PerfFileSystem mFileSystem;
  private List<String> mReadFiles;

  private boolean mSuccess;
  private double mThroughput; // in MB/s

  private List<String> latenciesOfAccesses;

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getThroughput() {
    return mThroughput;
  }

  public String getLatencies() {
    return String.join("\n", latenciesOfAccesses);
  }

  public void run() {
    long timeMs = 0; 
    long readBytes = 0;
    mSuccess = true;
    int count = 0;
    for (String fileName : mReadFiles) {
      count += 1;
      LOG.info(String.valueOf(count) + " " + fileName);
      try {
        Process p = new ProcessBuilder("bash", "/home/potato/potato_benchmark/all_flush_cache.sh").start();
        InputStream is = p.getInputStream();
        p.waitFor();
        String result = IOUtils.toString(is, "UTF-8");
        LOG.info(result);
      } catch (IOException e) {
      } catch (InterruptedException e) {
      } finally {
      }

      long _readBytes = 0;
      long _readTimeMs = 0;

      long tTimeMs = System.currentTimeMillis();
      try {
        _readBytes = Operators.readSingleFile(mFileSystem, fileName, mBufferSize);
        readBytes += _readBytes;
      } catch (IOException e) {
        LOG.error("Failed to read file " + fileName, e);
        mSuccess = false;
      }

      _readTimeMs = System.currentTimeMillis() - tTimeMs;
      timeMs += _readTimeMs;
      latenciesOfAccesses.add(
          "read " + fileName + " " +
          String.valueOf(_readBytes) + " " + 
          String.valueOf(_readTimeMs) + "ms");
    }
    LOG.info("readBytes = " + String.valueOf(readBytes));
    LOG.info("timeMs = " + String.valueOf(timeMs));
    mThroughput = (readBytes / 1024.0 / 1024.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    try {
      mFileSystem = Operators.connect(PerfConf.get().DFS_ADDRESS, taskConf);
      String readDir = taskConf.getProperty("read.dir");
      List<String> candidates = mFileSystem.list(readDir);
      if (candidates == null || candidates.isEmpty()) {
        throw new IOException("No file to read");
      }
      boolean isRandom = "RANDOM".equals(taskConf.getProperty("read.mode"));
      boolean isZipf = "Zipf".equals(taskConf.getProperty("read.mode"));
      int filesNum = taskConf.getIntProperty("files.per.thread");
      LOG.info(String.valueOf(filesNum));
      if (isRandom) {
        mReadFiles = ListGenerator.generateRandomReadFiles(filesNum, candidates);
      } else if (isZipf) {
        mReadFiles = ListGenerator.generateZipfReadFiles(mTaskId, filesNum, candidates);
        for (String fileName : mReadFiles) {
          LOG.info(fileName);
        }
      } else {
        mReadFiles =
            ListGenerator.generateSequenceReadFiles(mId, PerfConf.get().THREADS_NUM, filesNum,
                candidates);
      }
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mThroughput = 0;

    latenciesOfAccesses = new ArrayList<String>();
    return true;
  }

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    try {
      Operators.close(mFileSystem);
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }
}
