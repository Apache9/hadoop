package com.xiaomi.infra.hadoop;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.Canary;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FalconSink implements Canary.Sink, Configurable {
  private static final Log LOG = LogFactory.getLog(FalconSink.class);
  private static final String DEFAULT_FALCON_URI = "http://127.0.0.1:1988/v1/push";

  private Configuration conf;
  private HttpClient client = new HttpClient();

  // For metrics monitor
  final private Map<NodeType, AtomicInteger> nodeCount = new HashMap<NodeType, AtomicInteger>();
  final private Map<NodeType, List<String>> failedNodes = new HashMap<NodeType, List<String>>();
  final private List<Path> corruptPaths = new ArrayList<Path>();
  private Map<String, AtomicInteger> recentFailedTimes = new HashMap<String, AtomicInteger>();
  private long readLatency = -1;
  private long writeLatency = -1;

  // For availability calculating
  private boolean clusterAvailableStatus = true;
  private long lastSummaryTime = 0;
  private long lastStatusChangeTime = 0;
  private long unavailableTime = 0;

  @Override
  public void publishNodeHealth(NodeType type, String host, NodeState state) {
    if (!nodeCount.containsKey(type)) {
      nodeCount.put(type, new AtomicInteger(0));
    }
    nodeCount.get(type).incrementAndGet();

    // Record failed nodes
    if (state == NodeState.FAILED) {
      if (!failedNodes.containsKey(type)) {
        failedNodes.put(type, new ArrayList<String>());
      }
      failedNodes.get(type).add(host);
    }
  }

  @Override
  public void publishTiming(OpType type, long msTime) {
    if (type == OpType.READ) readLatency = msTime;
    else writeLatency = msTime;
  }

  @Override
  public void publishCorruptBlocks(Path corruptFilePath) {
    corruptPaths.add(corruptFilePath);
  }

  @Override
  public void publishAvailableStatus(boolean isAvailable) {
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

  private void updateRecentFailedTimes() {
    Map<String, AtomicInteger> newMap = new HashMap<String, AtomicInteger>();
    for (NodeType type : failedNodes.keySet()) {
      for (String host : failedNodes.get(type)) {
        if (recentFailedTimes.containsKey(host)) {
          newMap.put(host, new AtomicInteger(recentFailedTimes.get(host).get() + 1));
        } else {
          newMap.put(host, new AtomicInteger(1));
        }
      }
    }
    recentFailedTimes = newMap;
  }

  private void clearDataSets() {
    nodeCount.clear();
    failedNodes.clear();
    corruptPaths.clear();
    readLatency = -1;
    writeLatency = -1;
    unavailableTime = 0;
  }

  private JSONObject buildFalconMetric(String clusterName, String key, double value) throws Exception {
    JSONObject metric = new JSONObject();
    metric.put("endpoint", "hdfs-canary");
    metric.put("metric", key);
    metric.put("timestamp", System.currentTimeMillis() / 1000);
    metric.put("value", value);
    metric.put("step", 60);
    metric.put("counterType", "GAUGE");
    String type = conf.get("dfs.canary.cluster.type", "tst");
    metric.put("tags", "srv=hdfs,type=" + type.toLowerCase() + ",cluster=" + clusterName);
    return metric;
  }

  private void pushAvailabilityToFalcon(String clusterName, double avail) {
    String uri = conf.get("dfs.canary.sink.falcon.uri", DEFAULT_FALCON_URI);
    PostMethod post = new PostMethod(uri);
    JSONArray data = new JSONArray();
    try {
      data.put(buildFalconMetric(clusterName, "cluster-availability", avail));
    } catch (Exception e) {
      LOG.warn("Create json error.", e);
    }
    LOG.info(data.toString());
    post.setRequestBody(data.toString());
    try {
      client.executeMethod(post);
    } catch (IOException e) {
      LOG.warn("Push metrics to falcon failed", e);
    }
  }

  @Override
  public void reportSummary() {
    String clusterName = conf.get("dfs.nameservices", "unknown");
    // Report availability
    long curTime = System.currentTimeMillis();
    if (!clusterAvailableStatus) {
      unavailableTime += curTime - lastStatusChangeTime;
    }
    double availableRate = (1.0 - unavailableTime/(double)(curTime - lastSummaryTime)) * 100;
    pushAvailabilityToFalcon(clusterName, availableRate);
    lastStatusChangeTime = curTime;
    lastSummaryTime = curTime;

    // if cluster is available and sniff succeed
    if (!nodeCount.isEmpty()) {
      // Report nodes overview
      LOG.info("Node Type\tTotal");
      for (NodeType type : nodeCount.keySet()) {
        LOG.info(String.format("%s\t%d", type.name(), nodeCount.get(type).get()));
      }

      updateRecentFailedTimes();
      // Report failed nodes
      for (NodeType type : failedNodes.keySet()) {
        for (String host : failedNodes.get(type)) {
          LOG.info(String.format("%s %s failed %d times recently", type.name(), host,
                  recentFailedTimes.get(host).get()));
        }
      }

      // Report latency
      LOG.info(String.format("Read latency: %d ms, Write latency %d ms", readLatency,
              writeLatency));

      // Corrupt files
      LOG.info("File with corrupt blocks:");
      for (Path path : corruptPaths) {
        LOG.info(path);
      }
    }

    clearDataSets();
  }
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
