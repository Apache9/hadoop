package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.RequestHedgingProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;

public class TestHAUtil {
  private static String NAMESERVICE = "testCluster";
  private static String nnId1 = "host0", nnId2 = "host1";
  private static MiniDFSCluster cluster;
  private static String address1, address2;
  private static Configuration conf;

  private static final Log LOG = LogFactory.getLog(TestHAUtil.class);

  @BeforeClass
  public static void setupClass() throws IOException {
    MiniDFSNNTopology topology = new MiniDFSNNTopology().addNameservice(
        new MiniDFSNNTopology.NSConf(NAMESERVICE)
            .addNN(new MiniDFSNNTopology.NNConf(nnId1))
            .addNN(new MiniDFSNNTopology.NNConf(nnId2)));
    cluster =
        new MiniDFSCluster.Builder(new Configuration()).nnTopology(topology)
            .numDataNodes(1).build();
    cluster.transitionToActive(0);
    cluster.waitActive();

    address1 = cluster.getNameNode(0).getHostAndPort();
    address2 = cluster.getNameNode(1).getHostAndPort();
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setup() {
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, NAMESERVICE);
    conf.set(DFSUtil
            .addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, NAMESERVICE),
        "host0,host1");
    conf.set(DFSUtil
            .addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, NAMESERVICE, nnId1),
        address1);
    conf.set(DFSUtil
            .addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, NAMESERVICE, nnId2),
        address2);
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX,
        NAMESERVICE), ZkConfiguredFailoverProxyProvider.class.getName());
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + NAMESERVICE + "/");
    // conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    // conf.set("fs.AbstractFileSystem.hdfs.impl", Hdfs.class.getName());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
  }

  @Test
  public void testGetAddressOfActive() throws IOException, URISyntaxException {

    Configuration config = new Configuration(conf);
    FileSystem fs = null;
    InetSocketAddress activeNN = null;

    config.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX,
        NAMESERVICE), ZkConfiguredFailoverProxyProvider.class.getName());
    fs = FileSystem.get(config);
    activeNN = HAUtil.getAddressOfActive(fs);
    assertEquals(address1, activeNN.getHostName() + ":" + activeNN.getPort());

    config.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX,
        NAMESERVICE), RequestHedgingProxyProvider.class.getName());
    fs = FileSystem.get(config);
    activeNN = HAUtil.getAddressOfActive(fs);
    assertEquals(address1, activeNN.getHostName() + ":" + activeNN.getPort());

    config.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX,
        NAMESERVICE), ConfiguredFailoverProxyProvider.class.getName());
    fs = FileSystem.get(config);
    activeNN = HAUtil.getAddressOfActive(fs);
    assertEquals(address1, activeNN.getHostName() + ":" + activeNN.getPort());

    fs = FileSystem.get(new URI("hdfs://" + address1 + "/"), conf);
    activeNN = HAUtil.getAddressOfActive(fs);
    assertEquals(address1, activeNN.getHostName() + ":" + activeNN.getPort());
  }

}
