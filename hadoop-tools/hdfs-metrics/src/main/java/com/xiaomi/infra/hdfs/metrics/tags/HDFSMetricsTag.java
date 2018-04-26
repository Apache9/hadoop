package com.xiaomi.infra.hdfs.metrics.tags;

import com.google.gson.reflect.TypeToken;
import com.xiaomi.infra.galaxy.lcs.log.core.metrics.bean.MetricsTag;

import java.util.Map;

public class HDFSMetricsTag extends MetricsTag {
  protected String clusterName;
  protected String jobName;
  protected String taskName;
  protected String serviceType;


  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public HDFSMetricsTag() {
    this.topicName = "HDFS";
  }

  public void init(String clusterName, String jobName, String task, String serviceType) {
    this.clusterName = clusterName;
    this.jobName = jobName;
    this.taskName = task;
    this.serviceType = serviceType;
  }
  public void init(String clusterName, String task, String serviceType) {

  }


  public String getKey() {
    return this.serviceType + this.clusterName + this.jobName + this.taskName;
  }

  public String getJobKey() {
    return this.serviceType + this.clusterName + this.jobName;
  }

  public String toTag() {
    Map map = (Map)gson.fromJson(this.toJson(), (new TypeToken() {
    }).getType());
    return map.toString().replaceAll("\\{", "").replaceAll("\\}", "");
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static HDFSMetricsTag fromJson(String jsonStr) {
    return (HDFSMetricsTag)gson.fromJson(jsonStr, HDFSMetricsTag.class);
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    HDFSMetricsTag other = (HDFSMetricsTag) obj;
    if (serviceType != null ? !serviceType.equals(other.serviceType) : other.serviceType != null) {
      return false;
    }

    if (topicName != null ? !topicName.equals(other.topicName) : other.topicName != null) {
      return false;
    }
    if (clusterName != null ? !clusterName.equals(other.clusterName) : other.clusterName != null) {
      return false;
    }
    if (jobName != null ? !jobName.equals(other.jobName) : other.jobName != null) {
      return false;
    }
    if (taskName != null ? !taskName.equals(other.taskName) : other.taskName != null) {
      return false;
    }
    return true;
  }

  public int hashCode() {
    int result = this.serviceType != null?this.serviceType.hashCode():0;
    result = 31 * result + (this.topicName != null?this.topicName.hashCode():0);
    result = 31 * result + (this.clusterName != null?this.clusterName.hashCode():0);
    result = 31 * result + (this.jobName != null?this.jobName.hashCode():0);
    result = 31 * result + (this.taskName != null?this.taskName.hashCode():0);
    return result;
  }
}

