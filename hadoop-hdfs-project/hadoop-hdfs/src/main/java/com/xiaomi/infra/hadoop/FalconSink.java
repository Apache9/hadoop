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
import org.codehaus.jettison.json.JSONException;
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
  final private Map<String, Long> dataNodeReadLatencyMap = new HashMap<String, Long>();
  private long readLatency = -1;
  private long writeLatency = -1;
  private double capacityRemaining;
  private String tagString = null;
  private static final String DEFAULT_CANARY_ENDPOINT = "hdfs-canary";
  private long percentile99Latency = 0;
  private long percentile95Latency = 0;
  private long percentile75Latency = 0;
  private String nameService = null;
  private long maxTxDelta= 0;
  private long maxJournalDelay= 0;

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

  @Override
  public void publishCapacityRemaining(double percent) {
    capacityRemaining = percent;
  }

  @Override
  public void publishDataNodeLatency(String dataNode, OpType type, long msTime) {
    if (type == OpType.READ) {
      dataNodeReadLatencyMap.put(dataNode, msTime);
    }
  }

  @Override
  public void publishDNReadLatencyPercentitle () {
    List<Long> readLatencyList = new ArrayList<Long>(dataNodeReadLatencyMap.values());
    Collections.sort(readLatencyList);

    int percentile99 = readLatencyList.size() * 99 / 100;
    int percentile95 = readLatencyList.size() * 95 / 100;
    int percentile75 = readLatencyList.size() * 75 / 100;

    percentile99Latency = readLatencyList.get(percentile99);
    percentile95Latency = readLatencyList.get(percentile95);
    percentile75Latency = readLatencyList.get(percentile75);
    LOG.info(String.format("Datanode read latency percentile:99:%d 95:%d 75:%d", percentile99Latency, percentile95Latency, percentile75Latency));
  }

  @Override
  public void publishMaxTxIdDelta(String ns, long maxTxDelta) {
    if (nameService == null) {
      nameService = ns;
    }
    this.maxTxDelta = maxTxDelta;
  }

  @Override
  public void publishMaxJournalDelay(String ns, long maxJournalDelay) {
    if (nameService == null) {
      nameService = ns;
    }
    this.maxJournalDelay = maxJournalDelay;
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
    dataNodeReadLatencyMap.clear();
    readLatency = -1;
    writeLatency = -1;
    unavailableTime = 0;
    percentile99Latency = 0;
    percentile95Latency = 0;
    percentile75Latency = 0;
  }

  private JSONObject buildFalconMetricCommon(String endpoint, String key, double value) {
    JSONObject metric = new JSONObject();
    try {
      metric.put("endpoint", endpoint);
      metric.put("metric", key);
      metric.put("timestamp", System.currentTimeMillis() / 1000);
      metric.put("value", value);
      metric.put("step", 60);
      metric.put("counterType", "GAUGE");
    } catch (JSONException je) {
      LOG.warn("build json object error: ", je);
    }
    return metric;
  }

  private String buildFalconMetricTagsString() {
    String tagString = new String();
    Map<String, String> metricTagList = new LinkedHashMap();
    metricTagList.put("srv", "hdfs");
    metricTagList.put("type", conf.get("dfs.canary.cluster.type", "tst"));
    metricTagList.put("cluster", conf.get("dfs.nameservices", "unknown"));

    for (Map.Entry<String, String> tagEntry : metricTagList.entrySet()) {
      tagString += tagEntry.getKey();
      tagString += "=";
      tagString += tagEntry.getValue();
      tagString += ",";
    }

    tagString = tagString.substring(0,tagString.length()-1);

    return tagString;

  }

  private JSONObject buildFalconMetric(String endpoint, String key, double value) {
    JSONObject metric = buildFalconMetricCommon(endpoint, key, value);
    if (tagString == null) {
      tagString = buildFalconMetricTagsString();
    }

    try {
      metric.put("tags", tagString);
    } catch (JSONException je) {
      LOG.warn("build json Metric error: ", je);
    }
    return metric;
  }

  private void PushToFalcon(JSONArray payload) {
    String uri = conf.get("dfs.canary.sink.falcon.uri", DEFAULT_FALCON_URI);
    long startTime = System.currentTimeMillis();
    PostMethod post = new PostMethod(uri);
    LOG.info(payload.toString());
    post.setRequestBody(payload.toString());
    try {
      client.executeMethod(post);
    } catch (IOException e) {
      LOG.warn("Push metrics to falcon failed", e);
    }

    LOG.info(String.format("Pushing the metrics to falcon takes : %d", System.currentTimeMillis() - startTime));

  }

  @Override
  public void reportSummary() {
    JSONArray payload = new JSONArray();
    // Report availability
    long curTime = System.currentTimeMillis();
    if (!clusterAvailableStatus) {
      unavailableTime += curTime - lastStatusChangeTime;
    }
    double availableRate = (1.0 - unavailableTime/(double)(curTime - lastSummaryTime)) * 100;
    lastStatusChangeTime = curTime;
    lastSummaryTime = curTime;
    payload.put(buildFalconMetric(DEFAULT_CANARY_ENDPOINT, "cluster-availability", availableRate));

    // Report cluster remaining capacity
    payload.put(buildFalconMetric(DEFAULT_CANARY_ENDPOINT, "cluster-capacity-remaining", capacityRemaining));

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

      // latency of the cluster
      buildClusterReadLatencyMetrix(payload, DEFAULT_CANARY_ENDPOINT, readLatency);
      buildClusterWriteLatencyMetrix(payload, DEFAULT_CANARY_ENDPOINT, writeLatency);

      // latency of the datanodes
      buildDNsReadLatencyMetrix(payload);

      // latency percentile of datanodes
      buildDNReadLatencyPercentitleMetrix(payload);

      // TxIds
      buildMaxTxIdDeltaMetrix(payload);

      // journal delay
      buildMaxJournalDelayMetrix(payload);

    }

    PushToFalcon(payload);
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

  private void buildLatencyMetrix(JSONArray payload, String endpoint, OpType opType, long latency) {
    if (opType == OpType.READ) {
      payload.put(buildFalconMetric(endpoint, "read_latency", latency));
    } else {
      payload.put(buildFalconMetric(endpoint, "write_latency", latency));
    }
  }

  private void buildDNsReadLatencyMetrix(JSONArray payload) {
    for (Map.Entry<String, Long> entry : dataNodeReadLatencyMap.entrySet()) {
      buildLatencyMetrix(payload, entry.getKey(), OpType.READ, entry.getValue().longValue());
    }
  }

  private void buildClusterReadLatencyMetrix(JSONArray payload, String endpoint, long latency) {
      buildLatencyMetrix(payload, endpoint, OpType.READ, latency);
  }

  private void buildClusterWriteLatencyMetrix(JSONArray payload, String endpoint, long latency) {
      buildLatencyMetrix(payload, endpoint, OpType.WRITE, latency);
  }

  public void buildDNReadLatencyPercentitleMetrix(JSONArray payload)
  {
    payload.put(buildFalconMetric(DEFAULT_CANARY_ENDPOINT, "percentile99_read_latency", percentile99Latency));
    payload.put(buildFalconMetric(DEFAULT_CANARY_ENDPOINT, "percentile95_read_latency", percentile95Latency));
    payload.put(buildFalconMetric(DEFAULT_CANARY_ENDPOINT, "percentile75_read_latency", percentile75Latency));

  }

  public void buildMaxTxIdDeltaMetrix(JSONArray payload)
  {
    payload.put(buildFalconMetric(nameService, "MaxTxDelta", maxTxDelta));
  }

  public void buildMaxJournalDelayMetrix(JSONArray payload)
  {
    payload.put(buildFalconMetric(nameService, "MaxJournalDelay", maxJournalDelay));
  }
}
