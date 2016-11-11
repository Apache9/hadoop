package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Cli;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Parameters;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ToolRunner;

public class BalancerService extends Balancer {
  private static long balancerInterval;
  private static final Log LOG = LogFactory.getLog(BalancerService.class);
  public BalancerService(NameNodeConnector theblockpool, Parameters p,
      Configuration conf) {
    super(theblockpool, p, conf);
  }

  public static void main(String[] args) {
    // TBD: Make args as configuration param

    Configuration conf = new HdfsConfiguration();
    try {
      SecurityUtil.login(conf, DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE,
          DFSConfigKeys.DFS_BALANCER_KERBEROS_PRINCIPAL);
    } catch (IOException ioe) {
      LOG.info("Log in failed.", ioe);
      // Sleep a while in case it crashed too frequency in mis-configured
      // environment
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ie) {
        // Ignore
      }
    }

    balancerInterval =
        conf.getLong("dfs.balancer.service.interval", 5 * 60 * 1000); // default

    while (true) {
      try {
        ToolRunner.run(new HdfsConfiguration(), new Cli(), args);
      } catch (Throwable e) {
        LOG.error("Exiting balancer due an exception", e);
        System.exit(-1);
      }
      try {
        Thread.sleep(balancerInterval);
      } catch (InterruptedException ie) {
        // Ignore
      }
    }
  }
}
