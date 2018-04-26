package com.xiaomi.infra.hdfs.metrics.sink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.xiaomi.infra.galaxy.lcs.log.core.metrics.utils.MetricsConstants;
import com.xiaomi.infra.galaxy.lcs.log.log4j.metrics.registry.LCSMetricsRegistry;
import com.xiaomi.infra.hdfs.metrics.tags.HDFSMetricsTag;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;



public class LCSSink implements MetricsSink {
  public static final Log LOG = LogFactory.getLog(LCSSink.class);
  private static final String SERVICE_TYPE_KEY = "hdfs.service.type";
  private static final String SERVICE_TYPE_DEFAULT = "tst";
  private static final String LCSSINK_ENABLE_KEY = "hdfs.metrics.lcssink.enable";
  private static final boolean LCSSINK_ENABLE_DEFAULT = true;
  private LCSMetricsRegistry registry = null;
  private HDFSMetricsTag metricsTag = null;
  private String serviceType = null;

  public void init(SubsetConfiguration conf) {
    HdfsConfiguration configuration = new HdfsConfiguration();
    if (!configuration.getBoolean(LCSSINK_ENABLE_KEY, LCSSINK_ENABLE_DEFAULT)) {
      LOG.info("lcs sink is not enabled");
      return;
    }
    String nameSpace = configuration.get("dfs.nameservices");
    String hostname = null;

    // sanity check the setting
    if (configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TALOS_CLUSTER_NAME) == null ||
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TALOS_TOPIC_NAME) == null ||
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_ORG_ID) == null ||
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TEAM_ID) == null) {
      LOG.warn("the LCSSink is enabled but the config is not set");
      return;
    }

    serviceType = configuration.get(SERVICE_TYPE_KEY, SERVICE_TYPE_DEFAULT);

    Properties properties = new Properties();
    properties.setProperty(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TALOS_CLUSTER_NAME,
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TALOS_CLUSTER_NAME));
    properties.setProperty(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TALOS_TOPIC_NAME,
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TALOS_TOPIC_NAME));
    properties.setProperty(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_ORG_ID,
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_ORG_ID));
    properties.setProperty(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TEAM_ID,
        configuration.get(MetricsConstants.GALAXY_LCS_MONITOR_METRICS_TEAM_ID));
    registry= LCSMetricsRegistry.getRegistry(properties);

    try {
      hostname = (InetAddress.getLocalHost()).getHostName();
    } catch (UnknownHostException uhe) {
      LOG.error(uhe);
      hostname =  "UnknownHost";
    }

    String tagClass = conf.getString("MetricsTag");

    if (tagClass == null) {
      LOG.error("MetricsTag is not configured");
      return;
    }

    try {
      this.metricsTag = (HDFSMetricsTag) Class.forName(tagClass).newInstance();
      if (metricsTag != null) {
        metricsTag.init(nameSpace, hostname, serviceType);
      }
    } catch (Exception e) {
      metricsTag = null;
      LOG.error(e);
      LOG.error("fail to init metrics tag " + tagClass);
    }

  }

  public void putMetrics(MetricsRecord record) {
    if (metricsTag == null || registry == null) {
      return;
    }
    LOG.debug("put metrics " + record.context() + " " + metricsTag.toJson());
    for (AbstractMetric metric : record.metrics()) {
      LOG.debug(metric.name() + "=" + metric.value() + " type=" + metric.type().name());
      if (metric.type().compareTo(MetricType.COUNTER) == 0) {
        registry.counter(metricsTag, metric.name() , metric.value().longValue());
        continue;
      }

      if (metric.type().compareTo(MetricType.GAUGE) == 0) {
        registry.gauge(metricsTag, metric.name() , metric.value().longValue());
        continue;
      }

    }
  }

  public void flush() {
  }

}
