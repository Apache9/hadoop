package com.xiaomi.infra.hadoop;

import com.xiaomi.common.perfcounter.PerfCounter;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class IPCServerPerfCounter {
  public interface UserInformationCallBack {
    String getUserName();
  }

  // Top metric is used to track per user's qps
  private class TopMetric {
    volatile int numOps = 0;
    volatile int timeOfZeros = 0;
  }

  private class IPCServerMetric {
    // It is not necessary very accurate for perfcounter metrics. Otherwise we
    // need heavy atomic or lock operations. Just use volatile w/o lock to avoid
    // heavy cpu load.
    volatile long numOps = 0;
    volatile long queueTime = 0;
    volatile long processingTime = 0;
    Map<String, TopMetric> topMetrics;

    IPCServerMetric() {
      topMetrics = new LinkedHashMap<String, TopMetric>();
    }
  }

  private Map<String, IPCServerMetric> metrics;
  private Thread perfCounterPushThread = null;
  private volatile boolean shouldRun = true;
  private String prefix = null;
  private UserInformationCallBack userInfoCB;
  public static final Log LOG = LogFactory.getLog(IPCServerPerfCounter.class.getName());

  public void addMetric(String name, int num, long queueTime,
      long processingTime) {
    IPCServerMetric ipcMetric = metrics.get(name);
    if (ipcMetric != null) {
      ipcMetric.numOps += num;
      ipcMetric.queueTime += queueTime;
      ipcMetric.processingTime += processingTime;
      if (userInfoCB != null) {
        String user = userInfoCB.getUserName();
        TopMetric topMetric = ipcMetric.topMetrics.get(user);
        if (topMetric == null) {
          synchronized (ipcMetric.topMetrics) {
            topMetric = ipcMetric.topMetrics.get(user);
            if (topMetric == null) {
              topMetric = new TopMetric();
              ipcMetric.topMetrics.put(user, topMetric);
            }
          }
        }
        topMetric.numOps += num;
        topMetric.timeOfZeros = 0;
      }
    }
  }

  public void stopPerfCounter() {
    shouldRun = false;
  }
  
  public IPCServerPerfCounter() {
  }
  
  private void reportQpsAvgQueueProcessTime(final long reportIntervalMs) {
    long totalOps = 0;
    for (String metricName : metrics.keySet()) {
      IPCServerMetric ipcMetric = metrics.get(metricName);
      // Get metrics value
      long numOps = ipcMetric.numOps;
      long queueTime = ipcMetric.queueTime;
      long processingTime = ipcMetric.processingTime;
      totalOps += numOps;
      // Reset metrics
      ipcMetric.numOps = 0;
      ipcMetric.queueTime = 0;
      ipcMetric.processingTime = 0;
      // Calculate avg queuetime and processing time
      long opQps = numOps * 1000 / reportIntervalMs;
      long avgQueueTime = queueTime / ((numOps == 0) ? 1 : numOps);
      long avgProcessingTime = processingTime / ((numOps == 0) ? 1 : numOps);
      // Push to perfcounter
      PerfCounter.setGaugeValue(prefix + metricName + "-Qps", opQps);
      PerfCounter.setGaugeValue(prefix + metricName + "-AvgQueueTime",
          avgQueueTime);
      PerfCounter.setGaugeValue(prefix + metricName + "-AvgProcessTime",
          avgProcessingTime);
    }
    long totalQps = totalOps * 1000 / reportIntervalMs;
    PerfCounter.setGaugeValue(prefix + "-Qps", totalQps);
  }

  private void reportTop(final long reportIntervalMs, final int topExpireNum) {
    for (String metricName : metrics.keySet()) {
      IPCServerMetric ipcMetric = metrics.get(metricName);
      Map<String, TopMetric> topMetrics = ipcMetric.topMetrics;
      for (String user : topMetrics.keySet()) {
        TopMetric topMetric = topMetrics.get(user);
        if (topMetric.numOps != 0) {
          long opQps = topMetric.numOps * 1000 / reportIntervalMs;
          topMetric.numOps = 0;
          topMetric.timeOfZeros = 0;
          PerfCounter.setGaugeValue(prefix + metricName + "-" + user + "-Qps",
              opQps);
        } else {
          topMetric.timeOfZeros++;
          if (topMetric.timeOfZeros == topExpireNum) {
            synchronized (topMetrics) {
              // in case too many memory is used
              topMetrics.remove(user);
            }
          }
        }
      }
    }
  }

  public void initIPCServerPerfCounter(final long reportIntervalMs, 
      final List<Class<?>> protocols, final String servicePrefix,
      final int topExpireNum, UserInformationCallBack uc) {
    metrics = new LinkedHashMap<String, IPCServerMetric>();
    prefix = servicePrefix;
    userInfoCB = uc;
    for (Class<?> protocol : protocols) {
      LOG.info("Add PerfCounter for protocol " + protocol);
      for (Method method : protocol.getDeclaredMethods()) {
        LOG.info("Add PerfCounter for method " + method.getName());
        IPCServerMetric metric = new IPCServerMetric();
        metrics.put(method.getName(), metric);
      }
    }
    perfCounterPushThread = new Thread() {
      @Override
      public void run() {
        while (shouldRun) {
          try {
            Thread.sleep(reportIntervalMs);
          } catch (InterruptedException e) {
            if (shouldRun == false) {
              break;
            }
          }
          reportQpsAvgQueueProcessTime(reportIntervalMs);
          reportTop(reportIntervalMs, topExpireNum);
        }
      }
    };
  }
  
  public void shutdownIPCServerPerfCounter() {
    if (perfCounterPushThread != null) {
      shouldRun = false;
      perfCounterPushThread.interrupt();
      try {
        perfCounterPushThread.join(1000);
      } catch (InterruptedException ie) {
        // do nothing
      }
    }
  }
  
  public void startIPCServerPerfCounter() {
    assert (perfCounterPushThread != null);
    perfCounterPushThread.start();
  }
}
