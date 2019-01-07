package org.apache.hadoop.hdfs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ConfigurationService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.NameServiceConfigurationService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.NameServiceUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
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
    String conf;
    String catalog;
    public static AtomicBoolean sleep = new AtomicBoolean(false);
    public GetHandler(String catalog, String conf) {
      super();
      this.catalog = catalog;
      this.conf = conf;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      String method = httpExchange.getRequestMethod();
      if (method.equals("GET")) {
        URI uri = httpExchange.getRequestURI();
        String path = uri.getQuery();
        String data = null;
        if (path.contains(
            NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT)) {
          data = catalog;
        } else if (path.contains(NAMESERVICE)) {
          data = conf;
        }
        // construct response
        Headers responseHeaders = httpExchange.getResponseHeaders();

        String jsonString =
            "{\n" + "\"code\": 200,\n" + "\"data\": " + data + ",\n"
                + "\"description\": \"save success\"\n" + "}";
        responseHeaders.set("Context-Type", "application/json");
        if (sleep.get()) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            NameServiceConfigurationService.LOG.warn("Interrupted", e);
          }
        }
        httpExchange.sendResponseHeaders(200, 0);
        OutputStream responseBodyOut = httpExchange.getResponseBody();
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
    String confString = new String(Base64.encodeBase64(bytes));

    Configuration catalogConf = new Configuration(false);
    catalogConf.set(NameServiceUtil.CONFIGURATION_SERVICE_IDC, "c4");
    catalogConf
        .set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + ".c4",
            "127.0.0.1");
    catalogConf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_VERSION, "v1");
    catalogConf
        .set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX + ".c4",
            "xns_c4xnssrv");
    JSONObject obj = new JSONObject();
    try {
      for (Map.Entry<String, String> entry : catalogConf) {
        String key = entry.getKey();
        String value = entry.getValue();
        obj.put(key, value);
      }
    } catch (JSONException e) {
      throw new IOException("Convert Configuration to Json error.", e);
    }
    String catalogString = obj.toString();

    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    httpServer.createContext("/", new GetHandler(catalogString, confString));
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
    setNameServiceConf(defaultConf);
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

  @Test(timeout=10000)
  public void testSocketTimeout() throws Exception {
    // create an empty configuration, and use it to create FileSystem
    Configuration defaultConf = new Configuration(false);
    setNameServiceConf(defaultConf);
    try {
      GetHandler.sleep.set(true);
      FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), defaultConf);
      fail("Expected java.lang.IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("java.net.UnknownHostException", e);
    } finally {
      GetHandler.sleep.set(false);
    }
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
    setNameServiceConf(conf);
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
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_TEAM_ID,
        "CL7198");
    // conf.set(ConfigurationService.CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX+".mi","cnbj3");//模拟c4机房机器
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX
            + ".c4",
        "cnbj3");
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_IDC,
        "alsg,azbj,azde,azor,azsg,c3,c4,dp,hy,lg,tjwq");
    NameServiceConfigurationService nscs = new NameServiceConfigurationService(conf);
    Configuration remoteConf = nscs.fetchConfiguration("c4tst-xiaomi");
    assertTrue(remoteConf != null);
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
    setNameServiceConf(conf);
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

  @Test
  // BlackBox Test
  public void testPreserveUserConf() throws Exception {
    // create an empty configuration, and use it to create FileSystem
    Configuration defaultConf = new Configuration(false);
    setNameServiceConf(defaultConf);
    FileSystem tstFs =
        FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), defaultConf);
    assertEquals(null, tstFs.getConf().get("fs.permissions.umask-mode"));

    defaultConf = new Configuration(false);
    defaultConf.set("fs.hdfs.impl.disable.cache", "true");
    defaultConf.set("fs.permissions.umask-mode", "177");
    setNameServiceConf(defaultConf);
    tstFs = FileSystem.get(new URI("hdfs://" + NAMESERVICE + "/"), defaultConf);
    assertEquals("177", tstFs.getConf().get("fs.permissions.umask-mode"));
    // test writing files to hdfs
    assertFalse(tstFs.exists(
        new Path("hdfs://" + NAMESERVICE + "/testVisitingUnconfiguredHDFS")));
    BufferedOutputStream bos = new BufferedOutputStream(tstFs.create(
        new Path("hdfs://" + NAMESERVICE + "/testVisitingUnconfiguredHDFS"),
        true));
    byte[] bytes = "hello world".getBytes();
    bos.write(bytes);
    bos.close();
    FileStatus status = tstFs.getFileStatus(
        new Path("hdfs://" + NAMESERVICE + "/testVisitingUnconfiguredHDFS"));
    FsPermission permission = status.getPermission();
    assertEquals(FsPermission.createImmutable((short)384), permission);
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

  private void setNameServiceConf(Configuration conf) {
    conf.set("configuration.service.unit.test", "unit.test");
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_GET,
        "http://127.0.0.1:" + httpServer.getAddress().getPort()
            + "/v1/api/ns/get");
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_PUT,
        "http://127.0.0.1:" + httpServer.getAddress().getPort()
            + "/v1/api/ns/put");
    conf.setInt(NameServiceUtil.CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
        httpServer.getAddress().getPort());
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_URL_GET,
        "http://%s:%d/%s/api/ns/get?teamId=%s&path=%s&cluster=%s");
    conf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_URL_PUT,
        "http://%s:%d/%s/api/ns/put");
    conf.set(ConfigurationService.CONFIGURATION_SERVICE,
        "org.apache.hadoop.fs.NameServiceConfigurationService");
  }
}
