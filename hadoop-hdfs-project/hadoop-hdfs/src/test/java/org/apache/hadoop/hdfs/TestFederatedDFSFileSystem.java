package org.apache.hadoop.hdfs;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FederatedHdfs;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;

import static org.apache.hadoop.fs.FileContext.FILE_DEFAULT_PERM;

public class TestFederatedDFSFileSystem {
  private static final Log LOG =
      LogFactory.getLog(TestFederatedDFSFileSystem.class);
  private Configuration conf;
  private MiniDFSCluster cluster1;
  private MiniDFSCluster cluster2;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    // Bump up replication interval so that we only run replication
    // checks explicitly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 600);
    // Increase max streams so that we re-replicate quickly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 1000);
    conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);

    try {
      // disable hdfs impl cache
      conf.setBoolean("fs.hdfs.impl.disable.cache", true);
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
              "hdfs://test-cluster/");
      conf.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
      conf.set("fs.AbstractFileSystem.hdfs.impl", FederatedHdfs.class.getName());

      cluster1 = setupNewDFSCluster();
      cluster2 = setupNewDFSCluster();
      setupFederationConfig();
    } catch (Exception e) {
      LOG.info("Setup test env failed " + e.getMessage());
    }
  }

  private MiniDFSCluster setupNewDFSCluster() throws IOException {
    Configuration tmpConf = new Configuration(conf);
    File baseDir = new File("./target/test-dir-"
        + UUID.randomUUID().toString().substring(0, 4) + "/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    tmpConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(tmpConf).numDataNodes(3)
        .format(true).build();
    cluster.waitClusterUp();
    return cluster;
  }

  private void setupFederationConfig() throws Exception {
    String cluster1NNAddress =
        "hdfs://" + cluster1.getNameNode().getHostAndPort();
    String cluster2NNAddress =
        "hdfs://" + cluster2.getNameNode().getHostAndPort();
    cluster1.getFileSystem().mkdir(new Path("/home"), null);
    cluster2.getFileSystem().mkdir(new Path("/user"), null);
    ConfigUtil.addLink(conf, "test-cluster", "/home",
        new URI(cluster1NNAddress + "/home"));
    ConfigUtil.addLink(conf, "test-cluster", "/user",
        new URI(cluster2NNAddress + "/user"));
  }

  @Test
  public void testBasicOperation() throws Exception {
    String testText = "hello, federation";
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    Assert.assertTrue(dfs instanceof DistributedFileSystem);
    Assert.assertTrue(dfs instanceof FederatedDFSFileSystem);

    Path fooPath = new Path("/home/foo");
    Path barPath = new Path("/user/bar");
    Path fooFilePath = new Path(fooPath, "bar");
    dfs.mkdirs(fooPath);
    dfs.mkdirs(barPath);
    OutputStream out = dfs.create(fooFilePath);
    out.write(testText.getBytes());
    out.close();

    FileSystem fs1 = cluster1.getFileSystem();
    FileSystem fs2 = cluster2.getFileSystem();
    Assert.assertTrue(fs1.exists(fooPath));
    Assert.assertTrue(fs2.exists(barPath));
    InputStream in = fs1.open(fooFilePath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String res = reader.readLine();
    in.close();
    Assert.assertEquals(testText, res);

    FileStatus[] statuses = dfs.listStatus(fooPath);
    for (FileStatus status : statuses) {
      Assert.assertEquals(status.getPath().toUri().getScheme(), "hdfs");
      Assert.assertEquals(status.getPath().toUri().getAuthority(),
          "test-cluster");
    }

    Assert.assertEquals(dfs.getHomeDirectory().toString(), "hdfs://test-cluster/user/chen");
    Assert.assertEquals(dfs.getWorkingDirectory().toString(), "hdfs://test-cluster/user/chen");
  }

  private void addClusterToConf(Configuration config, String clusterName,
      String address) {
    config.set(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "."
        + clusterName, ConfiguredFailoverProxyProvider.class.getName());
    config.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + clusterName,
        "host0");
    config.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + clusterName
        + ".host0", address);
  }

  @Test
  public void testClustersMixConfigureation() throws Exception{
    // test get fs instance with Federation Cluster as defaultFs
    FileSystem fs =
        (DistributedFileSystem) FileSystem.get(new URI("/user/foo"), conf);
    Assert.assertTrue(fs instanceof FederatedDFSFileSystem);

    // test configuration with non-federation cluster
    Configuration config = new Configuration(conf);
    config.set(DFSConfigKeys.DFS_NAMESERVICES, "dfs-cluster-a, dfs-cluster-b");
    addClusterToConf(config, "dfs-cluster-a",
        cluster1.getNameNode().getHostAndPort());
    addClusterToConf(config, "dfs-cluster-b",
        cluster2.getNameNode().getHostAndPort());
    fs = FileSystem.get(new URI("/user/foo"), config);
    Assert.assertTrue(fs instanceof FederatedDFSFileSystem);
    fs = FileSystem.get(new URI("hdfs://dfs-cluster-a/user/foo"), config);
    Assert.assertTrue(fs instanceof DistributedFileSystem);
    Assert.assertFalse(fs instanceof FederatedDFSFileSystem);

    // set defaultFs as a non-federation cluster
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            "hdfs://dfs-cluster-a/");
    fs = FileSystem.get(new URI("/user/foo"), config);
    Assert.assertTrue(fs instanceof DistributedFileSystem);
    Assert.assertFalse(fs instanceof FederatedDFSFileSystem);
    fs = FileSystem.get(new URI("hdfs://test-cluster/user/foo"), config);
    Assert.assertTrue(fs instanceof FederatedDFSFileSystem);
    // mkdir without scheme
    fs.mkdirs(new Path("/user/test"));
    Assert.assertTrue(cluster2.getFileSystem().exists(new Path("/user/test")));

    // add more federation cluster
    ConfigUtil.addLink(config, "fed-cluster", "/data",
            new URI("hdfs://" + cluster1.getNameNode().getHostAndPort()  + "/data"));
    cluster1.getFileSystem().mkdirs(new Path("/data"));
    fs = FileSystem.get(new URI("hdfs://fed-cluster/user/foo"), config);
    Assert.assertTrue(fs instanceof FederatedDFSFileSystem);
    FileSystemTestHelper.createFile(fs, new Path("/data/file"));
    Assert.assertTrue(cluster1.getFileSystem().exists(new Path("/data/file")));
  }

  @Test
  public void testListLocatedFileStatus() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    dfs.mkdirs(new Path("/user/foo"));
    dfs.mkdirs(new Path("/user/bar"));
    FileSystemTestHelper.createFile(dfs, new Path("/user/testFile"));
    RemoteIterator<LocatedFileStatus> fileList = dfs.listLocatedStatus(new Path("/user"));
    while (fileList.hasNext()) {
      LocatedFileStatus status = fileList.next();
      Assert.assertEquals(status.getPath().toUri().getScheme(), "hdfs");
      if (status.isFile()) {
        BlockLocation[] locations = status.getBlockLocations();
        for (BlockLocation loc : locations) {
          Assert.assertTrue(loc instanceof HdfsBlockLocation);
        }
      }
    }
  }

  // This UT is for a very tricky bug about path with fragment
  // Some user may submit request using URI with fragment(start with #, like /user/foo/bar#1, the URI 
  // fragments usually used for browser, http request will ignore this part, only if using %23 instead
  // of #. In the previous implementation of FederatedDFSFileSystem, before calling ViewFileSystem
  // corresponding method with path, it always convert the path with hdfs scheme to viewfs scheme, the
  // conversion first convert path to string, then replace hdfs with viewfs. But the conversion also
  // convert # to %23 during path.toString(), this will cause fail in the subsequent process
  @Test
  public void testUriWithFragment() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    Path xmlPath = new Path("/user/foo/test.xml");
    FileSystemTestHelper.createFile(dfs, xmlPath);
    URI uriWithFragment =
        new URI("hdfs", "test-cluster", xmlPath.toString(), "test.xml");
    Path pathWithFragment = new Path(uriWithFragment);
    FileStatus status = dfs.getFileStatus(pathWithFragment);
    Assert.assertEquals(status.getPath().toUri().getPath(), xmlPath.toString());
  }

  @Test
  public void testAbstractFS() throws Exception {
    String testText = "hello, federation";
    AbstractFileSystem afs =
        AbstractFileSystem.get(new URI("hdfs://test-cluster/"), conf);
    Path fooPath = new Path("/home/afs-foo");
    Path barPath = new Path("/user/afs-bar");
    Path fooFilePath = new Path(fooPath, "bar");
    afs.mkdir(fooPath, null, false);
    afs.mkdir(barPath, null, false);

    final Options.CreateOpts[] opts =
        { Options.CreateOpts.perms(FILE_DEFAULT_PERM) };
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
    OutputStream out = afs.create(fooFilePath, createFlag, opts);
    out.write(testText.getBytes());
    out.close();

    FileSystem fs1 = cluster1.getFileSystem();
    FileSystem fs2 = cluster2.getFileSystem();
    Assert.assertTrue(fs1.exists(fooPath));
    Assert.assertTrue(fs2.exists(barPath));
    InputStream in = fs1.open(fooFilePath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String res = reader.readLine();
    in.close();
    Assert.assertEquals(testText, res);
  }

  @Test
  public void testShell() throws Exception {
    FsShell shell = new FsShell(conf);
    File out = new File("testfile");
    PrintWriter pw = new PrintWriter(new FileWriter(out));
    pw.println("foobar");
    pw.close();
    String[] argv = new String[] { "-copyFromLocal", "testfile", "/user/" };
    shell.run(argv);
  }
}
