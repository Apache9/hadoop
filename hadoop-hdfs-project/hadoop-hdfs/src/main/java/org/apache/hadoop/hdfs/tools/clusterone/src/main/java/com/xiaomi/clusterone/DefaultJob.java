package com.xiaomi.clusterone;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

/**
 * Created by xiegang1 on 17-2-3.
 */
public class DefaultJob implements Runnable, Configurable {
  public DefaultJob(HdfsConfiguration conf) {

  }
  @Override
  public void run() {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setConf(Configuration configuration) {

  }

  @Override
  public Configuration getConf() {
    return null;
  }
}
