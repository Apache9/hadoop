package org.apache.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FederatedHdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.FederatedDFSFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.TestFederatedDFSFileSystem;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;

public class TestFederated extends ClientBaseWithFixes {
  protected static final Log LOG =
      LogFactory.getLog(TestFederatedDFSFileSystem.class);
  protected static Configuration gConf;
  protected static MiniDFSCluster cluster;
  protected static FileSystem fs1;
  protected static FileSystem fs2;
  protected static FileSystem fs3;
  protected static String nn1Address;
  protected static String nn2Address;
  protected static String nn3Address;

  protected Configuration conf;

  @BeforeClass
  public static void setup() throws Exception {
    LOG.info("before test");
    gConf = new Configuration();
    gConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    // Bump up replication interval so that we only run replication
    // checks explicitly.
    gConf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 600);
    // Increase max streams so that we re-replicate quickly.
    gConf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 1000);
    gConf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);

    gConf.setBoolean("dfs.namenode.acls.enabled", true);

    try {
      cluster = setupNewDFSCluster();
      setupFederationConfig();
    } catch (Exception e) {
      LOG.info("Setup test env failed ", e);
    }
  }

  @Before
  public void setupBeforePerTest() {
    conf = new Configuration(gConf);
  }

  @After
  public void teardownAfterPerTest() {
    conf = null;
  }

  @AfterClass
  public static void teardown() throws IOException {
    LOG.info("before test");
    cluster.shutdown();
  }

  protected static MiniDFSCluster setupNewDFSCluster() throws IOException {
    /*
    File baseDir = new File("./target/test-dir-"
        + UUID.randomUUID().toString().substring(0, 4) + "/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    tmpConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    */

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration(gConf))
        .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(4))
        .numDataNodes(5).format(true).build();
    cluster.waitClusterUp();
    return cluster;
  }

  protected static void setupFederationConfig() throws Exception {
    // make the 1st nn of namespace 1 and namespace 2 to active
    cluster.transitionToActive(0);
    cluster.transitionToActive(2);
    cluster.transitionToActive(4);

    fs1 = cluster.getFileSystem(0);
    fs2 = cluster.getFileSystem(2);
    fs3 = cluster.getFileSystem(4);
    fs1.mkdirs(new Path("/home"));
    // make sure fs1 and fs2 is not connect to same namespace
    assert(!fs2.exists(new Path("/home")));
    fs2.mkdirs(new Path("/user"));

    // disable hdfs impl cache
    // conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    gConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://test-cluster/");
    gConf.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    gConf.set("fs.AbstractFileSystem.hdfs.impl", FederatedHdfs.class.getName());

    addNSAccessConfig(gConf, "test-cluster-1", 1);

    nn1Address =
        "hdfs://" + cluster.getNameNode(0).getHostAndPort();
    nn2Address =
        "hdfs://" + cluster.getNameNode(2).getHostAndPort();
    nn3Address =
        "hdfs://" + cluster.getNameNode(4).getHostAndPort();
    ConfigUtil.addLink(gConf, "test-cluster", "/home",
        new URI(nn1Address+ "/home"));
    ConfigUtil.addLink(gConf, "test-cluster", "/user",
        new URI("hdfs://" + "test-cluster-1" + "/user"));
    ConfigUtil.addLink(gConf, "test-cluster", "/", new URI(nn3Address + "/"));
  }

  protected static void addNSAccessConfig(Configuration config, String ns, int namenodeGroupId) {
    String prens = config.get(DFSConfigKeys.DFS_NAMESERVICES);
    config.set(DFSConfigKeys.DFS_NAMESERVICES,
        prens == null ? ns : prens + ", " + ns);
    config.set(
        DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX, ns),
        ConfiguredFailoverProxyProvider.class.getName());
    config.set(
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, ns),
        "host0,host1");
    config.set(
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, ns,
            "host0"),
        cluster.getNameNode(namenodeGroupId * 2).getHostAndPort());
    config.set(
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, ns,
            "host1"),
        cluster.getNameNode(namenodeGroupId * 2 + 1).getHostAndPort());
  }
}
