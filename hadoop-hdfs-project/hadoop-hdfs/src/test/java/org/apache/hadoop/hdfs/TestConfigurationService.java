package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ConfigurationService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ljl on 17-7-25.
 */
public class TestConfigurationService {
  private static final String ZK_BASE = "/configuration-service";
  private static final String NAMESERVICE = "testing-nameservice";
  private MiniDFSCluster dfsCluster;
  public static final String ZK_HOST =
      "10.108.83.16:11000,10.108.83.17:11000,10.108.83.24:11000";

  @Before
  public void setup() throws IOException, QuorumPeerConfig.ConfigException,
      KeeperException, InterruptedException {
    // config data in ZK
    Configuration conf = new Configuration(false);
    conf.set("dfs.nameservices", NAMESERVICE);
    conf.set("dfs.ha.namenodes." + NAMESERVICE, "host0,host1");
    conf.set("dfs.namenode.rpc-address." + NAMESERVICE + ".host0", "localhost:57200");
    conf.set("dfs.namenode.rpc-address." + NAMESERVICE + ".host1", "localhost:57000");
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICE, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    conf.write(new DataOutputStream(out));
    out.flush();
    byte[] bytes = out.toByteArray();
    out.close();

    ZooKeeper zk = new ZooKeeper(ZK_HOST, 3000, null);
    Stat stat = zk.exists(ZK_BASE, false);
    if (null == stat) {
      zk.create(ZK_BASE, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    }
    stat = zk.exists(ZK_BASE + "/" + NAMESERVICE, false);
    if (null == stat) {
      zk.create(ZK_BASE + "/" + NAMESERVICE, "test".getBytes(),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zk.setData(ZK_BASE + "/" + NAMESERVICE, bytes, -1);
    zk.close();

    // start minidfscluster
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf(NAMESERVICE)
            .addNN(new MiniDFSNNTopology.NNConf("host0").setIpcPort(57200))
            .addNN(new MiniDFSNNTopology.NNConf("host1").setIpcPort(57000)));
    dfsCluster = new MiniDFSCluster.Builder(new Configuration())
        .nnTopology(topology).numDataNodes(1).build();
    dfsCluster.waitActive();

    dfsCluster.transitionToActive(0);
  }

  @After
  public void cleanup() {
    dfsCluster.shutdown();
  }

  @Test
  // BlackBox Test
  public void testVisitingUnconfiguredHDFS() throws Exception {
    System.setProperty(ConfigurationService.CONFIGURATION_SERVICE,
        "org.apache.hadoop.fs.ZookeeperConfigurationService");
    System.setProperty(
        ConfigurationService.CONFIGURATION_SERVICE_ZOOKEEPER_HOST, ZK_HOST);
    // create an empty configuration, and use it to create FileSystem
    Configuration defaultConf = new Configuration(false);
    FileSystem tstFs =
        FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), defaultConf);
    // test writing files to hdfs
    assertFalse(tstFs.exists(new Path("hdfs://" + NAMESERVICE + "/abc")));
    BufferedOutputStream bos = new BufferedOutputStream(
        tstFs.create(new Path("hdfs://" + NAMESERVICE + "/abc"), true));
    byte[] bytes = "hello world".getBytes();
    bos.write(bytes);
    bos.close();
    assertTrue(tstFs.exists(new Path("hdfs://" + NAMESERVICE + "/abc")));
  }

  @Test
  // BlackBox Test
  public void testAutoUpdateNNAddress() throws Exception {
    System.setProperty(ConfigurationService.CONFIGURATION_SERVICE,
        "org.apache.hadoop.fs.ZookeeperConfigurationService");
    System.setProperty(
        ConfigurationService.CONFIGURATION_SERVICE_ZOOKEEPER_HOST, ZK_HOST);
    // create an configuration with outdated NN address
    Configuration conf = new Configuration(false);
    conf.set("dfs.nameservices", NAMESERVICE);
    conf.set("dfs.ha.namenodes." + NAMESERVICE, "host0,host1");
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICE,
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    FileSystem tstFs =
        FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), conf);
    // test writing files to hdfs
    assertFalse(tstFs.exists(new Path("hdfs://" + NAMESERVICE + "/abc")));
    BufferedOutputStream bos = new BufferedOutputStream(
        tstFs.create(new Path("hdfs://" + NAMESERVICE + "/abc"), true));
    byte[] bytes = "hello world".getBytes();
    bos.write(bytes);
    bos.close();
    assertTrue(tstFs.exists(new Path("hdfs://" + NAMESERVICE + "/abc")));
  }
}
