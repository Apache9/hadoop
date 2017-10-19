package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;

public class TestConfigurationServiceInDFSClient {
  private static final String NAMESERVICE = "c4tst-nonexisting";
  private MiniDFSCluster dfsCluster;

  @Before
  public void setup() throws IOException, QuorumPeerConfig.ConfigException,
      KeeperException, InterruptedException {
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

  @Test
  public void testgetNNProxyWithConfigurationService()
          throws NoSuchMethodException, URISyntaxException, IOException, InvocationTargetException, IllegalAccessException {
    Method method = DFSClient.Renewer.class.getDeclaredMethod(
        "getNNProxyWithConfigurationService", Token.class, Configuration.class);
    method.setAccessible(true);
    DFSClient.Renewer renewer = new DFSClient.Renewer();
    method.invoke(renewer,null,null);
  }
}
