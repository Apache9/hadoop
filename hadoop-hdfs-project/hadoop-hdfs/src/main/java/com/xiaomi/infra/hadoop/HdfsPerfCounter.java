package com.xiaomi.infra.hadoop;

import com.google.common.annotations.VisibleForTesting;
import com.xiaomi.common.perfcounter.PerfCounter;
import com.xiaomi.miliao.counter.MultiCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;

public class HdfsPerfCounter {
  private static Configuration conf = new HdfsConfiguration();
  private static boolean perfcounterEnabled =
      conf.getBoolean(DFSConfigKeys.DFS_CLIENT_PERFCOUNTER_ENABLED_KEY,
          DFSConfigKeys.DFS_CLIENT_PERFCOUNTER_ENABLED_DEFAULT);
  public static final String HDFS = "hdfs";
  public static final String HDFS_PERFCOUNTER_PREFIX =
      HDFS + MultiCounter.PATH_SEPARATOR_STRING;
  public static final String HDFS_FAIL =
      HDFS_PERFCOUNTER_PREFIX + MultiCounter.FAIL_SUFFIX;

  @VisibleForTesting
  public static void setConf(Configuration config) {
    conf = config;
    perfcounterEnabled =
      conf.getBoolean(DFSConfigKeys.DFS_CLIENT_PERFCOUNTER_ENABLED_KEY,
          DFSConfigKeys.DFS_CLIENT_PERFCOUNTER_ENABLED_DEFAULT);
  }

  public static void count(String method, long count, long time) {
    if (!perfcounterEnabled) {
      return;
    }
    PerfCounter.count(HDFS, count, time);
    PerfCounter.count(HDFS_PERFCOUNTER_PREFIX + method, count, time);
  }

  public static void countFail(String method, long count) {
    if (!perfcounterEnabled) {
      return;
    }
    PerfCounter.count(HDFS_FAIL, count);
    PerfCounter
        .count(HDFS_PERFCOUNTER_PREFIX + method + MultiCounter.FAIL_SUFFIX,
            count);
  }
}
