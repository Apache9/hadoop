package com.xiaomi.infra.hdfs.metrics.tags;

/**
 * Created by xiegang1 on 18-1-26.
 */
public class datanode extends HDFSMetricsTag {
  public void init(String clusterName, String task, String serviceType) {
    super.init(clusterName, "DataNode", task, serviceType);
  }
}
