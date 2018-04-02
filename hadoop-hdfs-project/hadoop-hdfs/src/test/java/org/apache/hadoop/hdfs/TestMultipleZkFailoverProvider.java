package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.ha.ZKFCTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.tools.DFSZKFailoverController;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.junit.Test;

import com.google.common.base.Supplier;

public class TestMultipleZkFailoverProvider {
  private static final Log LOG = LogFactory
      .getLog(TestMultipleZkFailoverProvider.class);

  /*
   * private static final int nnPort1 = 10011; private static final int nnPort2
   * = 10012; private static final int nnPort3 = 10013; private static final int
   * nnPort4 = 10014; private static final int zkfcPort1 = 10023; private static
   * final int zkfcPort2 = 10024; private static final int zkfcPort3 = 10025;
   * private static final int zkfcPort4 = 10026; private static final String
   * nameservicePrefix = "ha-nn-uri-";
   */

  private class ZkProviderTestContext extends ClientBaseWithFixes {
    Configuration conf;
    MiniDFSCluster cluster;
    TestContext ctx;
    ZKFCThread thr1 = null, thr2 = null;
    private boolean configRetryTime = false;
    private int retryTimes;
    private int durationBetweenRetryZk;
    private String nameservice;

    public ZkProviderTestContext() {
      conf = new Configuration();
      nameservice = "ha-nn-uri-" + MiniDFSCluster.nextInstanceId();
    }

    public void setup() throws Exception {
      super.setUp();
      // Specify the quorum per-nameservice, to ensure that these configs
      // can be nameservice-scoped.
      /*
       * conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY + "." + nameservice,
       * hostPort);
       */
      conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
      conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY,
          AlwaysSucceedFencer.class.getName());
      conf.setBoolean(DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY, true);

      // Turn off IPC client caching, so that the suite can handle
      // the restart of the daemons between test cases.
      conf.setInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          0);

      // Change the failover retry times and interval
      if (configRetryTime) {
        conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
            retryTimes);
        conf.setInt(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK,
            durationBetweenRetryZk);
      }

      conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + "." + nameservice
          + ".nn1", 0);
      conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + "." + nameservice
          + ".nn2", 0);

      MiniDFSNNTopology topology =
          new MiniDFSNNTopology().addNameservice(new MiniDFSNNTopology.NSConf(
              nameservice).addNN(
              new MiniDFSNNTopology.NNConf("nn1")).addNN(
              new MiniDFSNNTopology.NNConf("nn2")));
      cluster =
          new MiniDFSCluster.Builder(conf).nnTopology(topology).numDataNodes(0)
              .build();
      cluster.waitActive();

      ctx = new TestContext();
      ctx.addThread(thr1 = new ZKFCThread(cluster, ctx, 0));
      thr1.zkfc.run(new String[] { "-formatZK" });
      // assertEquals(0, thr1.zkfc.run(new String[] { "-formatZK" }));

      thr1.start();
      waitForHAState(cluster, 0, HAServiceState.ACTIVE);

      ctx.addThread(thr2 = new ZKFCThread(cluster, ctx, 1));
      thr2.start();

      // Wait for the ZKFCs to fully start up
      ZKFCTestUtil.waitForHealthState(thr1.zkfc,
          HealthMonitor.State.SERVICE_HEALTHY, ctx);
      ZKFCTestUtil.waitForHealthState(thr2.zkfc,
          HealthMonitor.State.SERVICE_HEALTHY, ctx);
      // fs = HATestUtil.configureZKBasedFailoverFs(cluster, conf);
    }

    public void setRetryTime(int times, int interval) {
      configRetryTime = true;
      retryTimes = times;
      durationBetweenRetryZk = interval;
    }

    public String getNameService() {
      return nameservice;
    }

    public String getHostPort() {
      return hostPort;
    }

    public void shutdown() throws Exception {
      cluster.shutdown();

      if (thr1 != null) {
        thr1.interrupt();
      }
      if (thr2 != null) {
        thr2.interrupt();
      }
      if (ctx != null) {
        ctx.stop();
      }
      super.tearDown();
    }
  }

  private class ZKFCThread extends TestingThread {
    private final DFSZKFailoverController zkfc;

    public ZKFCThread(MiniDFSCluster cluster, TestContext ctx, int idx) {
      super(ctx);
      this.zkfc = DFSZKFailoverController.create(cluster.getConfiguration(idx));
    }

    @Override
    public void doWork() throws Exception {
      try {
        assertEquals(0, zkfc.run(new String[0]));
      } catch (InterruptedException ie) {
        // Interrupted by main thread, that's OK.
      }
    }
  }

  private void waitForHAState(MiniDFSCluster cluster, int nnidx,
      final HAServiceState state) throws TimeoutException, InterruptedException {
    final NameNode nn = cluster.getNameNode(nnidx);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return nn.getRpcServer().getServiceStatus().getState() == state;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        }
      }
    }, 50, 15000);
  }

  private void addSecondClusterZkConf(Configuration conf,
      MiniDFSCluster cluster, String ns)
      throws Exception {
    String logicalName = HATestUtil.getLogicalHostname(cluster);
    conf.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + logicalName,
        ZkConfiguredFailoverProxyProvider.class.getName());
    String ns1 = conf.get(DFSConfigKeys.DFS_NAMESERVICES);
    assertTrue(ns1 != null);
    String newNs = ns1 + "," + ns;
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, newNs);
  }

  @Test
  public void testBasicMultipleZkProvider() throws Exception {
    ZkProviderTestContext ctx1 =
        new ZkProviderTestContext();
    ctx1.setRetryTime(2, 0);
    ctx1.setup();
    ZkProviderTestContext ctx2 =
        new ZkProviderTestContext();
    ctx2.setRetryTime(2, 0);
    ctx2.setup();
    FileSystem fs1 =
        HATestUtil.configureZKBasedFailoverFs(ctx1.cluster, ctx1.conf);
    /*
     * FileSystem fs2 = HATestUtil.configureZKBasedFailoverFs(ctx2.cluster,
     * ctx2.conf);
     */
    String ns1 = ctx1.getNameService();
    String ns2 = ctx2.getNameService();
    fs1.mkdirs(new Path("/test1"));
    // fs2.mkdirs(new Path("/test2"));
    addSecondClusterZkConf(fs1.getConf(), ctx2.cluster, ns2);
    Path t1 = new Path("hdfs://" + ns1 + "/test1");
    Path t2 = new Path("hdfs://" + ns2 + "/test2");
    FileSystem fs2 = null;
    try {
      fs2 = FileSystem.get(t2.toUri(), fs1.getConf());
      fs2.exists(t2);
      // Neither zk quorum nor namenodes in local configuration. FS can not be
      // created successfully.
      // fix: FS can be created successfully, but can't be called.
      assertTrue(false);
    } catch (Exception e) {
    }
    System.out.println("fs1 conf is " + fs1.getConf() + " ns1 "
        + ctx1.getNameService() + " ns2 "
        + ctx2.getNameService());
    fs1.getConf().set(CommonConfigurationKeys.ZK_QUORUM_KEY + "." + ns2,
        ctx2.getHostPort());
    fs1.getConf().setBoolean(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROVIDER_TOLERATE_EMPTY_NNADDR, true);
    fs1.getConf().setBoolean("fs.hdfs.impl.disable.cache", true);
    fs2 = FileSystem.get(t2.toUri(), fs1.getConf());
    assertTrue(fs1.exists(t1));
    fs2.mkdirs(t2);
    assertTrue(fs2.exists(t2));
    try {
      ctx1.shutdown();
      ctx2.shutdown();
    } catch (Exception e) {
      // Ignore so far
    }
  }
}
