package com.xiaomi.clusterone.canary;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.Canary;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by xiegang1 on 17-1-17.
 */

// Simple implementation of canary sink that allows to plot on
// file or standard output timings or failures.
public class StdOutSink implements Sink {
  private boolean clusterAvailableStatus = true;
  private long lastSummaryTime = 0;
  private long lastStatusChangeTime = 0;
  private long unavailableTime = 0;
  private final Log LOG = LogFactory.getLog(StdOutSink.class);

  StdOutSink() {
    lastStatusChangeTime = System.currentTimeMillis();
    lastSummaryTime = lastStatusChangeTime;
  }

  @Override
  public void init (Object o) {}

  @Override
  public void publishNodeHealth(NodeType type, String host, NodeState state) {
    LOG.info(host + " is a " + type.name() + ", current state: " + state.name());
  }

  @Override
  public void publishTiming(OpType type, long msTime) {
    LOG.info(type.name() + " latency: " + msTime + " ms");
  }

  @Override
  public void publishAvailableStatus(boolean isAvailable){
    if (isAvailable != clusterAvailableStatus) {
      long curTime = System.currentTimeMillis();
      if (!clusterAvailableStatus) {
        unavailableTime += curTime - lastStatusChangeTime;
      }
      LOG.info("cluster become " + (isAvailable ? "available" : "unavailable"));
      clusterAvailableStatus = isAvailable;
      lastStatusChangeTime = curTime;
    } else if (!clusterAvailableStatus) {
      LOG.info("cluster is still unavailable");
    }
  }

  @Override
  public void publishCorruptBlocks(Path corruptFilePath) {
    LOG.info("Found missing blocks in file: " + corruptFilePath);
  }

  @Override
  public void publishCapacityRemaining(double percent) {
    LOG.info("Cluster capacity remaining :" + percent + " in percent");
  }

  @Override
  public void reportSummary() {
    long curTime = System.currentTimeMillis();
    if (!clusterAvailableStatus) {
      unavailableTime += curTime - lastStatusChangeTime;
    }
    double unavailableRate = unavailableTime/(double)(curTime - lastSummaryTime);
    LOG.info("Available rate : "  + (1.0 -unavailableRate));
    lastStatusChangeTime = curTime;
    lastSummaryTime = curTime;
    unavailableTime = 0;
  }

  @Override
  public void publishDataNodeLatency(String dataNode, OpType type, long msTime) {
    LOG.info("Datanode: " + dataNode + " " + type.name() + " latency: " + msTime + " ms");
  }

  @Override
  public void publishDNReadLatencyPercentitle() {
    return;
  }

  @Override
  public void publishMaxTxIdDelta(String ns, long maxTxDelta) {
    return;
  }

  @Override
  public void publishMaxJournalDelay(String ns, long maxTxDelta) {
    return;
  }

  @Override
  public void publishMaxLiveNodesDiff(String ns, long maxLiveNodesDiff) { return;}

  @Override
  public void publishDatanodeAvailability()  { return; }

  @Override
  public void publishDatanodeReadSLAAvailability() { return; }

  @Override
  public void publishDatanodeWriteSLAAvailability() { return; }

  @Override
  public void publishCorruptedFileNum(long num) { return; }

}