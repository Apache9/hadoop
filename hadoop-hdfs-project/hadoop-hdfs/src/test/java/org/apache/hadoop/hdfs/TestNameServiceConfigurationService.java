package org.apache.hadoop.hdfs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ConfigurationService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.NameServiceConfigurationService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNameServiceConfigurationService {
  private static final String NAMESERVICE = "c4tst-nonexisting";
  private static String address1;
  private static String address2;
  private static MiniDFSCluster dfsCluster;
  private static HttpServer httpServer;

  static class GetHandler implements HttpHandler {
    String content;
    public GetHandler(String content) {
      super();
      this.content = content;

    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      String method = httpExchange.getRequestMethod();
      if (method.equals("GET")) {
        // construct response
        Headers responseHeaders = httpExchange.getResponseHeaders();
        responseHeaders.set("Context-Type","text/plain");
        httpExchange.sendResponseHeaders(200,0);

        OutputStream responseBodyOut = httpExchange.getResponseBody();
        String jsonString =
                "{\n" +
                        "\"code\": 200,\n" +
                        "\"data\": "+content+",\n" +
                        "\"description\": \"save success\"\n" +
                        "}";
        responseBodyOut.write(jsonString.getBytes());
        responseBodyOut.close();
      }
      else if(method.equals("POST")) {

      }
      else {

      }
    }
  }

  @BeforeClass
  public static void setup()
      throws IOException, QuorumPeerConfig.ConfigException,
      KeeperException, InterruptedException {

    // start minidfscluster
    MiniDFSNNTopology topology = new MiniDFSNNTopology().addNameservice(
        new MiniDFSNNTopology.NSConf(NAMESERVICE)
            .addNN(new MiniDFSNNTopology.NNConf("host0").setIpcPort(0))
            .addNN(new MiniDFSNNTopology.NNConf("host1").setIpcPort(0)));
    dfsCluster =
        new MiniDFSCluster.Builder(new Configuration()).nnTopology(topology)
            .numDataNodes(1).build();
    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive();

    address1 = dfsCluster.getNameNode(0).getHostAndPort();
    address2 = dfsCluster.getNameNode(0).getHostAndPort();

    // config data in HttpServer
    Configuration conf = new Configuration(false);
    conf.set("dfs.nameservices", NAMESERVICE);
    conf.set("dfs.ha.namenodes." + NAMESERVICE, "host0,host1");
    conf.set("dfs.namenode.rpc-address." + NAMESERVICE + ".host0", address1);
    conf.set("dfs.namenode.rpc-address." + NAMESERVICE + ".host1", address2);
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICE,
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    conf.write(new DataOutputStream(out));
    out.flush();
    byte[] bytes = out.toByteArray();
    out.close();

    String content = new String(Base64.encodeBase64(bytes));
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    httpServer.createContext("/", new GetHandler(content));
    httpServer.setExecutor(Executors.newCachedThreadPool());
    httpServer.start();
  }

  @AfterClass
  public static void cleanup() {
    dfsCluster.shutdown();
    httpServer.stop(0);
  }

  @Test
  public void testCache() throws URISyntaxException, IOException {
    Configuration conf = new Configuration(false);
    conf.set("dfs.nameservices", NAMESERVICE);
    conf.set("dfs.ha.namenodes." + NAMESERVICE, "host0,host1");
    conf.set("dfs.namenode.rpc-address." + NAMESERVICE + ".host0", address1);
    conf.set("dfs.namenode.rpc-address." + NAMESERVICE + ".host1", address2);
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICE,
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");

    conf.set("fs.hdfs.impl.disable.cache","true");
    URI uri = new URI("hdfs://"+NAMESERVICE+"");
    FileSystem fs1 = FileSystem.get(uri,conf);
    FileSystem fs2 = FileSystem.get(uri,conf);
    assertTrue(fs1!=fs2);
    
    conf.set("fs.hdfs.impl.disable.cache","false");
    fs1 = FileSystem.get(uri,conf);
    fs2 = FileSystem.get(uri,conf);
    assertTrue(fs1==fs2);
  }

  @Test
  // BlackBox Test
  public void testVisitingUnconfiguredHDFS() throws Exception {
    // create an empty configuration, and use it to create FileSystem
    Configuration defaultConf = new Configuration(false);
    defaultConf.set("configuration.service.unit.test","unit.test");
    defaultConf.set(ConfigurationService.CONFIGURATION_SERVICE,
        "org.apache.hadoop.fs.NameServiceConfigurationService");
    defaultConf.setInt(
        NameServiceConfigurationService.CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
        httpServer.getAddress().getPort());
    FileSystem tstFs =
        FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), defaultConf);
    // test writing files to hdfs
    assertFalse(tstFs.exists(
        new Path("hdfs://" + NAMESERVICE + "/testVisitingUnconfiguredHDFS")));
    BufferedOutputStream bos = new BufferedOutputStream(tstFs.create(
        new Path("hdfs://" + NAMESERVICE + "/testVisitingUnconfiguredHDFS"),
        true));
    byte[] bytes = "hello world".getBytes();
    bos.write(bytes);
    bos.close();
    assertTrue(tstFs.exists(
        new Path("hdfs://" + NAMESERVICE + "/testVisitingUnconfiguredHDFS")));
  }

  @Test
  // BlackBox Test
  public void testAutoUpdateNNAddress() throws Exception {
    // create an configuration with outdated NN address
    Configuration conf = new Configuration(false);
    conf.set("dfs.nameservices", NAMESERVICE);
    conf.set("dfs.ha.namenodes." + NAMESERVICE, "host0,host1");
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICE,
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.set("configuration.service.unit.test","unit.test");
    conf.set(ConfigurationService.CONFIGURATION_SERVICE,
        "org.apache.hadoop.fs.NameServiceConfigurationService");
    conf.setInt(
        NameServiceConfigurationService.CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
        httpServer.getAddress().getPort());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    FileSystem tstFs =
        FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), conf);
    // test writing files to hdfs
    assertFalse(tstFs.exists(
        new Path("hdfs://" + NAMESERVICE + "/testAutoUpdateNNAddress")));
    BufferedOutputStream bos = new BufferedOutputStream(tstFs
        .create(new Path("hdfs://" + NAMESERVICE + "/testAutoUpdateNNAddress"),
            true));
    byte[] bytes = "hello world".getBytes();
    bos.write(bytes);
    bos.close();
    assertTrue(tstFs.exists(
        new Path("hdfs://" + NAMESERVICE + "/testAutoUpdateNNAddress")));
  }

  @Test
  public void testFetchConfiguration() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(NameServiceConfigurationService.CONFIGURATION_SERVICE_NAME_TEAM_ID,
        "CL7198");
    // conf.set(ConfigurationService.CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX+".mi","cnbj3");//模拟c4机房机器
    conf.set(
        NameServiceConfigurationService.CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX
            + ".c4",
        "cnbj3");
    conf.set(NameServiceConfigurationService.CONFIGURATION_SERVICE_IDC,
        "alsg,azbj,azde,azor,azsg,c3,c4,dp,hy,lg,tjwq");
    NameServiceConfigurationService nscs = new NameServiceConfigurationService(conf);
    Configuration remoteConf = nscs.fetchConfiguration("c4tst-xiaomi");
    assertTrue(remoteConf != null);
  }

  @Test
  public void testUpdateConfFromNameService() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
    Configuration conf = new Configuration(false);
    conf.set(NameServiceConfigurationService.CONFIGURATION_SERVICE_NAME_TEAM_ID,
        "CL7198");
    NameServiceConfigurationService name = new NameServiceConfigurationService(conf);
    Field field = ConfigurationService.class.getDeclaredField("conf");
    field.setAccessible(true);
    Configuration reConf = (Configuration)field.get(name);
    assertTrue(reConf.get("configuration.service.name.domain.c4").equals("cnbj3"));

    Method method = NameServiceConfigurationService.class.getDeclaredMethod("getClusterIDCName",String.class);
    method.setAccessible(true);
    method.invoke(name,"zjy");
  }

  @Test
  public void testDFSNameService() throws Exception {
    Configuration conf = new Configuration(false);
    // HA case
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICE,
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    // exclude nameservice from dfs.nameservices
    conf.set("dfs.nameservices", "");
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    boolean gotException = false;
    try {
      FileSystem.get(new URI("hdfs://" + NAMESERVICE),conf);
      fail("Successfully got proxy provider for misconfigured FS");
    } catch (IOException e) {
      assertExceptionContains(
          "Could not find any configured addresses for URI " + new URI(
              "hdfs://" + NAMESERVICE), e.getCause().getCause());
    }
    // test get FileSystem with NameService
    conf.set("configuration.service.unit.test","unit.test");
    conf.set(ConfigurationService.CONFIGURATION_SERVICE,
            "org.apache.hadoop.fs.NameServiceConfigurationService");
    conf.setInt(
            NameServiceConfigurationService.CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
        httpServer.getAddress().getPort());
    FileSystem fs = FileSystem.get(new URI("hdfs://" + NAMESERVICE),conf);
    assertTrue(testWriteWithFileSystem(fs));
  }

  @Test
  public void testConnectToNNWithIpAddr() throws Exception {
    URI uri = null;
    if (dfsCluster.getNameNode(0).isActiveState()) {
      uri = new URI("hdfs://localhost:" + dfsCluster.getNameNodePort(0));
    } else {
      uri = new URI("hdfs://localhost:" + dfsCluster.getNameNodePort(1));
    }
    FileSystem fs = FileSystem.get(uri,new Configuration(false));
    assertTrue(testWriteWithFileSystem(fs));
  }

  private boolean testWriteWithFileSystem(FileSystem fs) throws Exception {
    Path path = new Path("/abc");
    if (fs.exists(path)) {
      fs.delete(path);
    }
    OutputStream out = fs.create(path);
    out.close();
    return fs.exists(path);
  }
}
