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
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.tools.DFSZKFailoverController;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
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
    private int durationBetweenRetryZk;
    private final String nameservice = nameservicePrefix
        + MiniDFSCluster.nextInstanceId();

    private FailoverTest failoverTest;

    public FailoverTestContext(FailoverTest ft) {
      failoverTest = ft;
      conf = new Configuration();
    }

    public void setup(boolean single) throws Exception {
      super.setUp();
      // Specify the quorum per-nameservice, to ensure that these configs
      // can be nameservice-scoped.
      /*
       * conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY + "." + nameservice,
       * hostPort);
       */
      conf.set(CommonConfigurationKeys.ZK_OBSERVER, hostPort);
      conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
      conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY,
          AlwaysSucceedFencer.class.getName());
      conf.setBoolean(DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY, true);

      // Turn off IPC client caching, so that the suite can handle
      // the restart of the daemons between test cases.
      conf.setInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          0);

      // default to 0
      conf.setInt(
          DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK,
          0);


      //Minimize the timecost of ut in default cases
      conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
              10);
      conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY, 3000);
      conf.setLong(DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK, 5000);

      //Change the failover retry times and interval
      if (configRetryTime) {
        conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
                retryTimes);
        conf.setInt(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK,
            durationBetweenRetryZk);
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
      LOG.info("nn1 become active");
      ctx.addThread(thr2 = new ZKFCThread(cluster, ctx, 1));
      thr2.start();
      ZKFCTestUtil.waitForHealthState(thr2.zkfc,
              HealthMonitor.State.SERVICE_HEALTHY, ctx);
    }

    public void setRetryTime(int times, int interval) {
      configRetryTime = true;
      retryTimes = times;
      durationBetweenRetryZk = interval;
    }

    public void resetFs() throws IOException {
      try {
        fs.close();
        fs = HATestUtil.configureZKBasedFailoverFs(cluster, conf);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    public void setClientName(String cname) {
      conf.set("mapreduce.task.attempt.id", cname);
    }

    public void setDurationBetweenZkRetry(long dbz) {
      conf.setLong(
          DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK,
          dbz);
    }

    public void setZkQuorum(String quorum) {
      conf.set(CommonConfigurationKeys.ZK_QUORUM_KEY, quorum);
    }

    public void runTest() throws Exception {
      failoverTest.runTest(this);
    }

    public void waitForActive(int idx) throws IOException {
      try {
        waitForHAState(cluster, idx, HAServiceState.ACTIVE);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    public void shutdown() throws Exception {
      UserGroupInformation.setLoginUser(null);
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
        new Timer().schedule(new StartNameNodeTask(ftc), 30000);

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
    ftc.setRetryTime(2, 0);
    ftc.setup(false);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testRetryZkDuration1() throws Exception {
    FailoverTest ft = new FailoverTest() {
      public void runTest(FailoverTestContext ftc) throws IOException,
          URISyntaxException {
        // cannot accesss zk, after nn failover we can still access hdfs
        FileSystem fs = ftc.fs;
        Path testDir1 = new Path("/dir1");
        fs.mkdirs(testDir1);
        ftc.setDurationBetweenZkRetry(60000);
        ftc.resetFs();
        fs = ftc.fs;
        assertTrue(fs.exists(testDir1));
        fs.getConf().set(CommonConfigurationKeys.ZK_QUORUM_KEY,
            "127.0.0.1:" + 80);
        ftc.cluster.shutdownNameNode(0);
        ftc.waitForActive(1);
        long start = Time.now();
        assertTrue(fs.exists(testDir1));
        assertTrue(Time.now() - start < 1000);
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setup(false);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testRetryZkDuration2() throws Exception {
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
        String origQuorum =
            fs.getConf().get(CommonConfigurationKeys.ZK_QUORUM_KEY);
        Configuration tmpConf = new Configuration(fs.getConf());
        tmpConf.set(CommonConfigurationKeys.ZK_QUORUM_KEY,
            "127.0.0.1:" + 80);
        tmpConf.set(CommonConfigurationKeys.ZK_OBSERVER,
            "127.0.0.1:" + 80);
        tmpConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        FileSystem nfs = FileSystem.get(tmpConf);
        ftc.cluster.shutdownNameNode(0);
        ftc.waitForActive(1);
        try {
          nfs.exists(testDir1);
          // we should not be able to access the hdfs
          assertTrue(false);
        } catch (Exception e) {
        }
        fs.getConf().set(CommonConfigurationKeys.ZK_QUORUM_KEY, origQuorum);
        try {
          Thread.sleep(16000);
        } catch (Exception e) {
          throw new IOException(e);
        }
        long start = Time.now();
        assertTrue(fs.exists(testDir1));
        // max retry is 2, max interval is 3s, so it must succeed at 2nc retry
        // because using hedging proxy, and we introduce random factors for
        // trying to access zk, we could only guarantee the access would success
        // at or before the last try
        assertTrue(Time.now() - start < 4000);
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setRetryTime(2, 8000);
    ftc.setup(true);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testNoRpcAddressConfigured() throws Exception {
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

        Configuration tmpConf = new Configuration(fs.getConf());
        Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(tmpConf);
        for (String nsId : nameserviceIds) {
          Collection<String> nnIds = DFSUtil.getNameNodeIds(tmpConf, nsId);
          for (String nnid : nnIds) {
            tmpConf.unset(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nsId + "." + nnid);
          }
        }
        tmpConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        FileSystem fs1 = FileSystem.get(tmpConf);
        ftc.cluster.shutdownNameNode(0);
        ftc.waitForActive(1);
        try {
          fs1.exists(testDir1);
          // should not success
          Assert.assertTrue(false);
        } catch (Exception e) {
          Assert.assertTrue(e instanceof RuntimeException);
        }

        tmpConf.setBoolean(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROVIDER_TOLERATE_EMPTY_NNADDR, true);
        FileSystem fs2 = FileSystem.get(tmpConf);
        Assert.assertTrue(fs2.exists(testDir1));

        // if no node on zk, and no addresses in configuration, should not do failover
        ftc.thr1.interrupt();
        ftc.thr2.interrupt();
        FileSystem fs3 = FileSystem.get(tmpConf);
        try {
          fs3.exists(testDir1);
          // should not success
          Assert.assertTrue(false);
        } catch (Exception e) {
          Assert.assertTrue(e instanceof RuntimeException);
        }
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setRetryTime(2, 8000);
    ftc.setup(true);
    ft.runTest(ftc);
    ftc.shutdown();
  }

  @Test
  public void testDelegationTokenForNewNamenode() throws Exception {
    FailoverTest ft = new FailoverTest() {
      public void runTest(FailoverTestContext ftc) throws IOException,
          URISyntaxException {
        // create a fake delegation token manager
        FSNamesystem mockNameSys = mock(FSNamesystem.class);
        DelegationTokenSecretManager sm =
            new DelegationTokenSecretManager(
                DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
                DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
                DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
                3600000, mockNameSys);
        sm.startThreads();

        // create a fake delegation token and add to ugi
        DelegationTokenIdentifier dtId =
            new DelegationTokenIdentifier(new Text("mi"), new Text("test"),
                new Text("mi"));
        Token<DelegationTokenIdentifier> token =
            new Token<DelegationTokenIdentifier>(dtId, sm);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        Text logicalServiceName =
            HAUtil.buildTokenServiceForLogicalUri(
                HATestUtil.getLogicalUri(ftc.cluster),
                HdfsConstants.HDFS_URI_SCHEME);
        token.setService(logicalServiceName);
        ugi.addToken(logicalServiceName, token);

        // access the namenode and failover
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

        Exception expectedException = null;
        try {
          fs.exists(testDir1);
        } catch (RemoteException e) {
          // When the fs init, since there is no delegation token in ugi,
          // the connection to namenode is established by SIMPLE authentication
          // method. After we added a new namenode and force the failover
          // happen, ZkConfiguredFailoverProvider cloned the delegation token
          // we just added to logical address to new address, so the new
          // connection is established by DELEGATION_TOKEN authentication
          // method.
          // But the token is fake and not exist on namenode, the call should
          // fail
          expectedException = e;
        }
        Assert.assertNotNull(expectedException);

        Collection<Token<? extends TokenIdentifier>> set = ugi.getTokens();
        assertEquals(set.size(), 2);

        String newNNAddress = "127.0.0.1:" + nnPort3;
        boolean hasNewNNAddress = false;
        for (Token<? extends TokenIdentifier> t : ugi.getTokens()) {
          if (t.getService().toString().equals(newNNAddress)) {
            hasNewNNAddress = true;
            break;
          }
        }
        assertTrue(hasNewNNAddress);
      }
    };

    FailoverTestContext ftc = new FailoverTestContext(ft);
    ftc.setup(true);
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
