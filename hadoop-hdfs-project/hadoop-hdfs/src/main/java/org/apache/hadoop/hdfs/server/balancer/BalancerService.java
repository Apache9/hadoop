package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Cli;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Parameters;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
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

  public static void startBalancerService(HdfsConfiguration conf) {
    LOG.info("starting balancer");
    try {
      UserGroupInformation.setConfiguration(conf);
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
      LOG.info("return from login");
      return;
    }

    String[] args = new String[2];
    args[0] = "-threshold";
    args[1] = conf.get("dfs.balancer.threshold", "10");


    try {

      final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
      LOG.info("namenodes " + namenodes + " size " + namenodes.size());
    } catch (Exception e) {
    }
    
    try {
      ToolRunner.run(conf, new Cli(), args);
    } catch (Throwable e) {
      LOG.error("Exiting balancer due an exception", e);
      return;
    }

  }
}
