package com.xiaomi.clusterone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.Serializable;

public class Task implements Serializable {
  private static final Log LOG = LogFactory.getLog(Task.class);
  private String cluster;
  private int taskId;
  private String version = "unkown";
  private String propertiesStr = null;

  public String getPropertiesStr() {
    return propertiesStr;
  }

  public void setPropertiesStr(String propertiesStr) {
    this.propertiesStr = propertiesStr;
  }

  public Task(String cluster, int taskId, String propertiesStr) {
    super();
    this.cluster = cluster;
    this.taskId = taskId;
    this.propertiesStr = propertiesStr;

  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }


  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }


  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }


  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("{");
    sb.append("cluster=").append(cluster);
    sb.append(" taskId=").append(taskId);
    sb.append("}");
    return sb.toString();
  }
}
