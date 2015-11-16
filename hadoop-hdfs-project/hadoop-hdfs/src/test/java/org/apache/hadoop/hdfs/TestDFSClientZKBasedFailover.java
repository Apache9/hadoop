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

package org.apache.hadoop.hdfs;

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

public class TestDFSClientZKBasedFailover {
  private static final Log LOG = LogFactory
      .getLog(TestDFSClientZKBasedFailover.class);
  private static final int nnPort1 = 10011;
  private static final int nnPort2 = 10012;
  private static final int nnPort3 = 10013;
  private static final String nameservicePrefix = "ha-nn-uri-";

  private class FailoverTestContext extends ClientBaseWithFixes {
    Configuration conf;
    MiniDFSCluster cluster;
    TestContext ctx;
    ZKFCThread thr1 = null, thr2 = null;
    FileSystem fs = null;
    private boolean configRetryTime = false;
    private int retryTimes;
    private int sleepBetweenRetry;
    private final String nameservice = nameservicePrefix
        + MiniDFSCluster.nextInstanceId();

    private FailoverTest failoverTest;

    public FailoverTestContext(FailoverTest ft) {
      failoverTest = ft;
    }

    public void setup(boolean single) throws Exception {
      super.setUp();
      conf = new Configuration();

      // Specify the quorum per-nameservice, to ensure that these configs
      // can be nameservice-scoped.
      conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY + "."
          + nameservice,
          hostPort);
      conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
      conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY,
          AlwaysSucceedFencer.class.getName());
      conf.setBoolean(DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY, true);

      // Turn off IPC client caching, so that the suite can handle
      // the restart of the daemons between test cases.
      conf.setInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          0);

      //Change the failover retry times and interval
      if (configRetryTime) {
        conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_NUM_RETRIES,
                retryTimes);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_SLEEP_BETWEEN_RETRY,
                sleepBetweenRetry);
      }

      conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + "." + nameservice
          + ".nn1", 10023);
      conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + "." + nameservice
          + ".nn2", 10024);

      MiniDFSNNTopology topology =
          new MiniDFSNNTopology().addNameservice(new MiniDFSNNTopology.NSConf(
              nameservice).addNN(
              new MiniDFSNNTopology.NNConf("nn1").setIpcPort(nnPort1)).addNN(
              new MiniDFSNNTopology.NNConf("nn2").setIpcPort(nnPort2)));
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

      if (!single) {
        ctx.addThread(thr2 = new ZKFCThread(cluster, ctx, 1));
        thr2.start();
      }

      // Wait for the ZKFCs to fully start up
      ZKFCTestUtil.waitForHealthState(thr1.zkfc,
          HealthMonitor.State.SERVICE_HEALTHY, ctx);
      if (!single) {
        ZKFCTestUtil.waitForHealthState(thr2.zkfc,
            HealthMonitor.State.SERVICE_HEALTHY, ctx);
      }
      fs = HATestUtil.configureZKBasedFailoverFs(cluster, conf);
      if (single) {
        cluster.shutdownNameNode(1);
        Thread.sleep(6000);
      }
    }

    public void setupSecondNN(int port) throws Exception {
      String key =
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice
              + ".nn2";
      cluster.getConfiguration(1).set(key, "127.0.0.1:" + port);
      cluster.restartNameNode(1);
      cluster.waitActive(1);
      ctx.addThread(thr2 = new ZKFCThread(cluster, ctx, 1));
      thr2.start();
      ZKFCTestUtil.waitForHealthState(thr2.zkfc,
              HealthMonitor.State.SERVICE_HEALTHY, ctx);
    }

    public void setRetryTime(int times, int interval) {
      configRetryTime = true;
      retryTimes = times;
      sleepBetweenRetry = interval;
    }

    public void runTest() throws Exception {
      failoverTest.runTest(this);
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

  public class StartNameNodeTask extends TimerTask {
    private FailoverTestContext ftc;

    public StartNameNodeTask(FailoverTestContext ctx) {
      ftc = ctx;
    }

    @Override
    public void run() {
      try {
        ftc.cluster.restartNameNode(1);
      } catch (IOException e) {
        LOG.info("start NameNode failed!!");
      }
    }
  }

  private interface FailoverTest {
    public void runTest(FailoverTestContext ftc) throws IOException,
        URISyntaxException;
  }

  @Test
  public void testBasicFailOver() throws Exception {
    FailoverTest ft = new FailoverTest() {
      public void runTest(FailoverTestContext ftc) throws IOException,
          URISyntaxException {
        FileSystem fs = ftc.fs;
        // Test basic failover
        Path testDir1 = new Path("/dir1");
        fs.mkdirs(testDir1);
        assertTrue(fs.exists(testDir1));
        ftc.cluster.shutdownNameNode(0);
        assertTrue(fs.exists(testDir1));
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setup(false);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testAddNewNameNode() throws Exception {
    FailoverTest ft = new FailoverTest() {
      public void runTest(FailoverTestContext ftc) throws IOException,
          URISyntaxException {
        FileSystem fs = ftc.fs;
        // Test basic failover
        Path testDir1 = new Path("/dir1");
        fs.mkdirs(testDir1);
        assertTrue(fs.exists(testDir1));
        try {
          ftc.setupSecondNN(nnPort3);
        } catch (Exception e) {
          throw new IOException("Fail to setup second Namenode", e);
        }
        ftc.cluster.shutdownNameNode(0);
        assertTrue(fs.exists(testDir1));
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setup(true);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testFailOverDefaultStrategy() throws Exception {
    FailoverTest ft = new FailoverTest() {
      public void runTest(FailoverTestContext ftc) throws IOException,
          URISyntaxException {
        FileSystem fs = ftc.fs;
        // Test failover timeout
        Path testDir1 = new Path("/dir1");
        fs.mkdirs(testDir1);
        assertTrue(fs.exists(testDir1));

        ftc.cluster.shutdownNameNode(0);
        ftc.cluster.shutdownNameNode(1);
        //zk re-election may last around 20s, using current 5 * 6s will success
        new Timer().schedule(new StartNameNodeTask(ftc), 20000);

        assertTrue(fs.exists(testDir1));
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setup(false);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testFailOverTimeOut() throws Exception {
    FailoverTest ft = new FailoverTest() {
      public void runTest(FailoverTestContext ftc) throws IOException,
          URISyntaxException {
        FileSystem fs = ftc.fs;
        // Test failover timeout
        Path testDir1 = new Path("/dir1");
        fs.mkdirs(testDir1);
        assertTrue(fs.exists(testDir1));

        ftc.cluster.shutdownNameNode(0);
        ftc.cluster.shutdownNameNode(1);
        //zk re-election may last around 20s
        new Timer().schedule(new StartNameNodeTask(ftc), 20000);

        boolean getActiveNNSuccess = true;
        try {
          fs.exists(testDir1);
        } catch (Exception e) {
          getActiveNNSuccess = false;
        }
        assertFalse(getActiveNNSuccess);
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    //inappropriate config, maybe too short to handle zk re-election
    ftc.setRetryTime(3, 5000);
    ftc.setup(false);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  // TBD: Add test cases for different combination of configuration

  private void waitForHAState(MiniDFSCluster cluster, int nnidx,
      final HAServiceState state)
      throws TimeoutException, InterruptedException {
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

  /**
   * Test-thread which runs a ZK Failover Controller corresponding to a given
   * NameNode in the minicluster.
   */
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
}
