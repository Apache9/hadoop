package org.apache.hadoop.hdfs;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.TestFederated;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FederatedHdfs;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaSummary;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.FileContext.FILE_DEFAULT_PERM;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFederatedDFSFileSystem extends TestFederated {
  private static final Log LOG =
      LogFactory.getLog(TestFederatedDFSFileSystem.class);

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

    Assert.assertEquals(dfs.getHomeDirectory().toString(),
        "hdfs://test-cluster/user/"
            + UserGroupInformation.getLoginUser().getShortUserName());
    Assert.assertEquals(dfs.getWorkingDirectory().toString(),
        "hdfs://test-cluster/user/"
            + UserGroupInformation.getLoginUser().getShortUserName());
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
    addClusterToConf(config, "dfs-cluster-a", nn1Address);
    addClusterToConf(config, "dfs-cluster-b", nn2Address);
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
    Assert.assertTrue(fs2.exists(new Path("/user/test")));

    // add more federation cluster
    ConfigUtil.addLink(config, "fed-cluster", "/data",
            new URI(nn1Address  + "/data"));
    fs1.mkdirs(new Path("/data"));
    fs = FileSystem.get(new URI("hdfs://fed-cluster/user/foo"), config);
    Assert.assertTrue(fs instanceof FederatedDFSFileSystem);
    FileSystemTestHelper.createFile(fs, new Path("/data/file"));
    Assert.assertTrue(fs1.exists(new Path("/data/file")));
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

    Assert.assertTrue(fs1.exists(fooPath));
    Assert.assertTrue(fs2.exists(barPath));
    InputStream in = fs1.open(fooFilePath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String res = reader.readLine();
    in.close();
    Assert.assertEquals(testText, res);
  }

  @Test
  public void testFederatedHdfsReplicationFactor() throws Exception {
    String testText = "hello, federation";
    AbstractFileSystem afs = AbstractFileSystem.get(new URI("hdfs://test-cluster"), conf);
    final Options.CreateOpts[] opts =
            { Options.CreateOpts.perms(FILE_DEFAULT_PERM) };
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
    Path filePath = new Path("/user/foo/bar-file");
    afs.mkdir(new Path("/user/foo"), null, false);
    OutputStream out = afs.create(filePath, createFlag, opts);
    out.write(testText.getBytes());
    out.close();

    FileStatus status = afs.getFileStatus(filePath);
    Assert.assertEquals(3, status.getReplication());
  }

  @Test
  public void testShellCopy() throws Exception {
    FsShell shell = new FsShell(conf);
    File out = new File("testfile");
    PrintWriter pw = new PrintWriter(new FileWriter(out));
    pw.println("foobar");
    pw.close();
    String[] argv = new String[] { "-copyFromLocal", "testfile", "hdfs:///user/" };
    shell.run(argv);
    FileSystem fs = FileSystem.get(conf);
    Assert.assertTrue(fs.exists(new Path("/user/testfile")));
  }

  @Test
  public void testDFSConcat() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    Path srcs[] = new Path[] { new Path("/user/foo/concat_f1"),
        new Path("/user/foo/concat_f2") };
    char[] data = new char[(int)dfs.getDefaultBlockSize()];
    for (int i = 0; i < data.length; i++) {
      data[i] = 'a';
    }

    OutputStream out = dfs.create(srcs[0]);
    byte[] b = new String(data).getBytes();
    out.write(b);
    out.close();
    out = dfs.create(srcs[1]);
    out.write("xyz".getBytes());
    out.close();

    Path target = new Path("/user/foo/tgt");
    for (int i = 0; i < data.length; i++) {
      data[i] = 'b';
    }
    out = dfs.create(target);
    out.write(new String(data).getBytes());
    out.close();

    dfs.concat(target, srcs);
    FileStatus status = dfs.getFileStatus(target);
    Assert.assertEquals(status.getLen(), 2048+3);
  }

  @Test
  public void testSetQuota() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    Path p = new Path("/user/test_quota");
    if (dfs.exists(p)) {
      dfs.delete(p, true);
    }
    dfs.mkdirs(p);

    QuotaSummary qs = dfs.getQuotaSummary(p);
    Assert.assertEquals(qs.getQuota(), -1);
    Assert.assertEquals(qs.getSpaceQuota(), -1);

    dfs.setQuota(p, 1024, 1024*1024);
    qs = dfs.getQuotaSummary(p);
    Assert.assertEquals(qs.getQuota(), 1024);
    Assert.assertEquals(qs.getSpaceQuota(), 1024*1024);

    // cleanup
    dfs.delete(p, true);
  }
  
  @Test
  public void testCreateOnDefaultMountpoint() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path p = new Path("/non-exists");
    fs.mkdirs(p);
    Assert.assertTrue(fs3.exists(p));
  }

  @Test
  public void testQuotaOfInternalNode() throws Exception {
    fs1.mkdirs(new Path("/work/project1"));
    fs2.mkdirs(new Path("/work/project2"));
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, "test-cluster", "/work/project1",
            new URI(nn1Address+ "/work/project1"));
    ConfigUtil.addLink(config, "test-cluster", "/work/project2",
            new URI(nn2Address+ "/work/project2"));

    FileSystem fs = FileSystem.get(config);
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    try {
      dfs.setQuota(new Path("/work"), 1000, 2000);
      Assert.assertTrue(false);
    } catch (IOException e) {
      // should not success, since the path is not available on default mountpoint
    }

    try {
      fs.getQuotaSummary(new Path("/work"));
      Assert.assertTrue(false);
    } catch (IOException e) {
      // should not success, since the path is not available on default mountpoint
    }

    fs.mkdirs(new Path("/work/project3"));

    dfs.setQuota(new Path("/work"), 1000, 2000);
    QuotaSummary summary = fs.getQuotaSummary(new Path("/work"));
    Assert.assertEquals(1000, summary.getQuota());
    Assert.assertEquals(2000, summary.getSpaceQuota());
    summary = fs.getQuotaSummary(new Path("/work"));
    Assert.assertEquals(1000, summary.getQuota());
    Assert.assertEquals(2000, summary.getSpaceQuota());
    summary = fs3.getQuotaSummary(new Path("/work"));
    Assert.assertEquals(1000, summary.getQuota());
    Assert.assertEquals(2000, summary.getSpaceQuota());

    // test path with schema
    summary = fs.getQuotaSummary(new Path("hdfs://test-cluster/work"));
    Assert.assertEquals(1000, summary.getQuota());
    Assert.assertEquals(2000, summary.getSpaceQuota());
  }

  boolean checkAllDnBalanderBandWidthLimit(long limit) {
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getBalancerBandwidth() != limit) {
        LOG.warn("bandwidith limit on dn(cluster1) is "
            + dn.getBalancerBandwidth() + ", expect: " + limit);
        return false;
      }
    }
    return true;
  }

  @Test
  public void testSetBalancerBandwidth() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    long defaultBandwidth =
        conf.getLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT);
    Assert.assertTrue(checkAllDnBalanderBandWidthLimit(defaultBandwidth));

    long newBandwidth = 12 * 1024 * 1024;
    dfs.setBalancerBandwidth(newBandwidth);

    // Give it a few seconds to propogate new the value to the datanodes.
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
    }

    Assert.assertTrue(checkAllDnBalanderBandWidthLimit(newBandwidth));
  }

  boolean compairReports(DatanodeInfo[] report1, DatanodeInfo[] report2) {
    if (report1.length != report2.length)
      return false;
    Map<String, DatanodeInfo> report1Map = new HashMap<String, DatanodeInfo>();
    for (DatanodeInfo dn : report1) {
      report1Map.put(dn.getInfoAddr(), dn);
    }
    for (DatanodeInfo dn : report2) {
      String addr = dn.getInfoAddr();
      if (!report1Map.containsKey(addr))
        return false;
      DatanodeInfo dn1 = report1Map.get(addr);
      if (dn1.getXceiverCount() != dn.getXceiverCount())
        return false;
      if (dn1.getDfsUsed() != dn.getDfsUsed())
        return false;
    }
    return true;
  }

  @Test
  public void testDatanodeReports() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    DatanodeInfo[] dnStats = dfs.getDataNodeStats();
    DatanodeInfo[] dnStatsFs1 = ((DistributedFileSystem)fs1).getDataNodeStats();
    DatanodeInfo[] dnStatsFs2 = ((DistributedFileSystem)fs2).getDataNodeStats();
    Assert.assertTrue(compairReports(dnStatsFs1, dnStatsFs2));
    Assert.assertTrue(compairReports(dnStats, dnStatsFs1));
    Assert.assertTrue(compairReports(dnStats, dnStatsFs2));
  }

  @Test
  public void testShellDnReports() throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      DFSAdmin shell = new DFSAdmin(conf);
      String[] argv = new String[] { "-report"};
      Assert.assertEquals(0, shell.run(argv));
      results = bytes.toString();
      Assert.assertTrue(results.contains("Live datanodes"));
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
  }

  @Test
  public void testFileContextRename() throws Exception {
    // simulate the usage of yarn history server
    Path testPathDir = new Path("/user/foo/intermediate");
    Path testPath = FileContext.getFileContext(conf).makeQualified(testPathDir);
    FileContext testFc = FileContext.getFileContext(
            testPath.toUri(), conf);

    Path intermediateFile = new Path(testPathDir, "tmp.log");
    Path doneDir = testFc.makeQualified(new Path("/user/foo/done"));
    Path doneFile = new Path(doneDir, "done.log");

    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    dfs.mkdirs(testPathDir);
    dfs.mkdirs(doneDir);
    OutputStream out = dfs.create(intermediateFile);
    out.write("hello world".getBytes());
    out.close();

    testFc.rename(intermediateFile, doneFile);
    Assert.assertTrue(dfs.exists(doneFile));
  }

  @Test
  public void testSchemeIssueWhenListStatusOnInternalNode() throws Exception {
    // prepare the mounttable
    fs1.mkdirs(new Path("/internal/dir1"));
    fs2.mkdirs(new Path("/internal/dir2"));
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, "test-cluster", "/internal/dir1",
            new URI(nn1Address + "/internal/dir1"));
    ConfigUtil.addLink(config, "test-cluster", "/internal/dir2",
            new URI(nn2Address + "/internal/dir2"));

    FileSystem fs = FileSystem.get(config);
    fs.mkdirs(new Path("/internal/dir3"));

    //start the case
    FileStatus[] res = fs.listStatus(new Path("/internal"));
    for (FileStatus status : res) {
      Assert.assertEquals("hdfs", status.getPath().toUri().getScheme());
    }
  }

  @Test
  public void testSchemeIssueOnTrashDir() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path p = new Path("/user/foo/test_data");
    fs.mkdirs(p);
    Trash.moveToAppropriateTrash(fs, p, conf);
    Path trashPath = new Path(
        "/user/" + System.getProperty("user.name") + "/.Trash/Current/");
    FileStatus[] statuses = fs.listStatus(trashPath);
    Assert.assertTrue(statuses.length > 0);
    String defaultAuthority = fs.getUri().getAuthority();
    String authority = null;
    for (FileStatus status : statuses) {
      authority = status.getPath().toUri().getAuthority();
      Assert.assertTrue(authority == null || authority == defaultAuthority);
    }

    FileStatus status = fs.getFileStatus(trashPath);
    authority = status.getPath().toUri().getAuthority();
    Assert.assertTrue(authority == null || authority == defaultAuthority);
  }

  @Test
  public void testListStatusOnMountPointParent() throws Exception {
    // prepare the mounttable
    fs1.mkdirs(new Path("/internal1/dir1"));
    fs1.setOwner(new Path("/internal1/dir1"), "foo", "hadoop");
    fs2.mkdirs(new Path("/internal1/dir2"));
    fs2.setOwner(new Path("/internal1/dir2"), "foo", "hadoop");
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, "test-cluster", "/internal1/dir1",
            new URI(nn1Address + "/internal1/dir1"));
    ConfigUtil.addLink(config, "test-cluster", "/internal1/dir2",
            new URI(nn2Address + "/internal1/dir2"));

    FileSystem fs = FileSystem.get(conf);
    FileStatus[] statuses = fs.listStatus(new Path("/internal1"));
    for (FileStatus status : statuses) {
      Assert.assertEquals(status.getOwner(), "foo");
      Assert.assertEquals(status.getGroup(), "hadoop");
    }
  }

  @Test
  public void testFsCacheCleanup() throws Exception {
    FileSystem fsa = FileSystem.get(conf);
    fsa.close();
    FileSystem fsb = FileSystem.get(conf);
    fsb.close();

    // Because (ViewFileSystem)fsb.close() will close all child FileSystem,
    // We need re-get.
    fs1 = cluster.getFileSystem(0);
    fs2 = cluster.getFileSystem(2);
    fs3 = cluster.getFileSystem(4);

    Assert.assertNotSame(fsa, fsb);
  }

  @Test
  public void testMultiFedConfigCase() throws Exception {
    // Set two federation cluster config in one configuration.
    // Test whether it could be used to access both two clusters.

    Configuration configuration = new Configuration();
    File baseDir = new File("./target/test/" + "testMultiFedConfigCase").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    String NAMESERVICE = "simple-cluster";
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf(NAMESERVICE)
            .addNN(new MiniDFSNNTopology.NNConf("host1")));
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(configuration)
        .nnTopology(topology).numDataNodes(1).build();
    dfsCluster.waitActive();
    int port = dfsCluster.getNameNodePort(0);
    dfsCluster.getFileSystem(0).mkdirs(new Path("/user"));

    Configuration config = new Configuration(conf);
    config.set("dfs.nameservices",
        conf.get("dfs.nameservices") + "," + NAMESERVICE);
    config.set(
        Constants.CONFIG_VIEWFS_PREFIX + "." + NAMESERVICE + ".link./user",
        "hdfs://localhost:" + port + "/user");

    // test FederatdDFSFileSystem
    FederatedDFSFileSystem ffs = (FederatedDFSFileSystem) FileSystem
        .get(new URI("hdfs://" + NAMESERVICE), config);
    ffs.mkdirs(new Path("/user/abc"));
    assertTrue(dfsCluster.getFileSystem(0).exists(new Path("/user/abc")));

    ffs = (FederatedDFSFileSystem) FileSystem
        .get(new URI("hdfs://test-cluster/"), config);
    ffs.mkdirs(new Path("/home/abc"));
    if (cluster.getNameNode(0).isActiveState()) {
      assertTrue(cluster.getFileSystem(0).exists(new Path("/home/abc")));
    } else {
      assertTrue(cluster.getFileSystem(1).exists(new Path("/home/abc")));
    }

    // test FederatedHdfs
    FileSystem destFs = null;
    FileContext context = FileContext.getFileContext(new URI("hdfs://"+NAMESERVICE),config);
    destFs = dfsCluster.getFileSystem(0);
    if (destFs.exists(new Path("/user/abc"))) {
      destFs.delete(new Path("/user/abc"));
    }
    context.mkdir(new Path("/user/abc"), FsPermission.getDefault(),false);
    assertTrue(destFs.exists(new Path("/user/abc")));

    context = FileContext.getFileContext(new URI("hdfs://test-cluster/"),config);
    if (cluster.getNameNode(0).isActiveState()) {
      destFs = cluster.getFileSystem(0);
    } else {
      destFs = cluster.getFileSystem(1);
    }
    if (destFs.exists(new Path("/home/abc"))) {
      destFs.delete(new Path("/home/abc"));
    }
    context.mkdir(new Path("/home/abc"), FsPermission.getDefault(),false);
    assertTrue(destFs.exists(new Path("/home/abc")));

    dfsCluster.shutdown();
  }

  @Test
  public void testIsFedrationUri() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(Constants.CONFIG_VIEWFS_PREFIX + ".fed-1.link./user",
        "hdfs://cluster-0/user");
    conf.set(Constants.CONFIG_VIEWFS_PREFIX + ".fed-1.link./home",
        "hdfs://cluster-1/home");
    conf.set(Constants.CONFIG_VIEWFS_PREFIX + ".fed-2.link./foo",
        "hdfs://cluster-2/foo");
    conf.set(Constants.CONFIG_VIEWFS_PREFIX + ".fed-2.link./",
        "hdfs://cluster-3/");
    assertTrue(HAUtil.isFederationUri(conf,new URI("hdfs://fed-1")));
    assertTrue(HAUtil.isFederationUri(conf,new URI("hdfs://fed-2")));
  }

  /**
   * Test the whole process of updating mount point table:
   * 1. superuser uses DFSAdmin to update new mount point to zk;
   * 2. normal user uses FederatedDFSFilesystem to access new mount point dir;
   * */
  @Test
  public void testAddNewNameSpaceByRenewer() throws Exception {
    conf.set(DFSConfigKeys.DFS_CLIENT_ZOOKEEPER_OBSERVER, hostPort);
    // Add a new namespace
    Configuration tmpConf = new Configuration(conf);
    String clusterName = "test-cluster";
    String newNs = clusterName + "-3";
    addNSAccessConfig(tmpConf, newNs, 3);
    ConfigUtil.addLink(tmpConf, clusterName, "/new-mpt",
            new URI("hdfs://" + newNs + "/new-mpt"));
    cluster.transitionToActive(6);
    FileSystem fs4 = cluster.getFileSystem(6);
    fs4.mkdirs(new Path("/new-mpt"));

    // upload the config to zk with DFSAdmin
    HdfsMountpointRenewer hmpr = new HdfsMountpointRenewer();
    hmpr.initialize(clusterName, tmpConf,
        new MountpointRenewer.RenewMountpoint() {
          @Override
          public void doUpdateMountpoint() {
          }
        });
    String newMptConfString =
        HdfsMountpointRenewer.serializeMountpoint2String(tmpConf, clusterName);
    final DFSAdmin dfsAdmin = new DFSAdmin(tmpConf);
    final String[] argv = new String[]{"-updateMptOnZk"};
    UserGroupInformation ugi = UserGroupInformation
        .createUserForTesting("hdfs", new String[] { "hdfs" });
    ugi.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        int res = 0;
        try {
          res = ToolRunner.run(dfsAdmin, argv);
        } catch (Exception e) {
          assert false;
        }
        assertEquals(0, res);
        return null;
      }
    });
    byte[] zkData = hmpr.getMptConfFromZookeeper(tmpConf);
    assertTrue(Arrays.equals(zkData, newMptConfString.getBytes()));

    // use FederatedDFSFileSystem to access new mount point dir
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    FederatedDFSFileSystem fs = (FederatedDFSFileSystem)FileSystem.get(conf);
    Assert.assertEquals(fs.getChildFileSystems().length, 4);
    Path testPath = new Path("/new-mpt/test-dir");
    fs.mkdirs(testPath);
    Assert.assertTrue(fs4.exists(testPath));
  }

  @Test
  public void testFederationCommand() throws Exception {
    Configuration conf = new Configuration(gConf);
    addNSAccessConfig(conf, "test-cluster-0", 0);
    addNSAccessConfig(conf, "test-cluster-1", 1);

    Path apiPatha = new Path("/testFederationMethod/api/a");
    Path apiPathb = new Path(apiPatha, "b");
    Path shellPatha = new Path("/testFederationMethod/shell/a");
    Path shellPathb = new Path(shellPatha, "b");

    fs1.mkdirs(apiPatha);
    fs2.mkdirs(apiPathb);

    ConfigUtil.addLink(conf, "test-cluster", apiPatha.toString(),
        new URI("hdfs://" + "test-cluster-0" + apiPatha));
    ConfigUtil.addLink(conf, "test-cluster", apiPathb.toString(),
        new URI("hdfs://" + "test-cluster-1" + apiPathb));

    fs1.mkdirs(shellPatha);
    fs2.mkdirs(shellPathb);

    ConfigUtil.addLink(conf, "test-cluster", shellPatha.toString(),
        new URI("hdfs://" + "test-cluster-0" + shellPatha));
    ConfigUtil.addLink(conf, "test-cluster", shellPathb.toString(),
        new URI("hdfs://" + "test-cluster-1" + shellPathb));

    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    FileSystem fs = FileSystem.get(conf);
    assertTrue("No FederatedDFSFileSystem.",
        fs instanceof FederatedDFSFileSystem);
    FederatedDFSFileSystem dfs = (FederatedDFSFileSystem) fs;

    FileStatus status;
    // test fedchown

    FsShell shell = new DFSAdmin(conf);
    shell.run(
        new String[] { "-fedchown", "testFederationMethod:testFederationMethod",
            shellPatha.toString() });

    status = fs1.getFileStatus(shellPatha);
    assertEquals("testFederationMethod", status.getOwner());
    status = fs2.getFileStatus(shellPatha);
    assertEquals("testFederationMethod", status.getOwner());

    // test fedchmod
    shell.run(new String[] { "-fedchmod", "002", shellPatha.toString() });
    status = fs1.getFileStatus(shellPatha);
    assertEquals(2, status.getPermission().toShort());
    status = fs2.getFileStatus(shellPatha);
    assertEquals(2, status.getPermission().toShort());

    // test fedsetfacl
    List<AclEntry> entries;
    List<AclEntry> aclEntries;
    FsPermission perm;

    entries = Lists.newArrayList(aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "foo", ALL), aclEntry(ACCESS, GROUP, ALL),
        aclEntry(ACCESS, GROUP, "bar", ALL), aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, ALL));

    shell.run(new String[] { "-fedsetfacl", "--set",
        "user::rwx,user:foo:rwx,group::rwx,group:bar:rwx,mask::rwx,other::rwx",
        shellPatha.toString() });
    perm = fs1.getFileStatus(shellPatha).getPermission();
    aclEntries = fs1.getAclStatus(shellPatha).getEntries();
    aclEntries = AclUtil.getAclFromPermAndEntries(perm, aclEntries);
    assertEquals(entries, aclEntries);
    perm = fs2.getFileStatus(shellPatha).getPermission();
    aclEntries = fs2.getAclStatus(shellPatha).getEntries();
    aclEntries = AclUtil.getAclFromPermAndEntries(perm, aclEntries);
    assertEquals(entries, aclEntries);

    // test fedsetquota
    shell.run(new String[] { "-fedsetquota", "5", shellPatha.toString() });
    assertEquals(5, fs1.getQuotaSummary(shellPatha).getQuota());
    assertEquals(5, fs2.getQuotaSummary(shellPatha).getQuota());

    // test fedmkdirs
    Path shellDir = new Path("/testFederationCommand/shellDir/mkdirs");
    shell.run(new String[] { "-fedmkdirs", "-p", shellDir.toString() });
    assertTrue(fs1.exists(shellDir));
    assertTrue(fs2.exists(shellDir));
  }
}
