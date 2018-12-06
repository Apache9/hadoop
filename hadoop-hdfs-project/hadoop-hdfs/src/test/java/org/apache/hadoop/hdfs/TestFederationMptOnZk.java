package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileContext.FILE_DEFAULT_PERM;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FederatedHdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.NotInMountpointException;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.hdfs.FederationConfigKeys;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFederationMptOnZk extends ClientBaseWithFixes {

  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs1;
  private static FileSystem fHdfs2;
  private static FileSystem fHdfs3;
  private static FileSystem fHdfs4;
  private static final Log LOG = LogFactory.getLog(TestFederationMptOnZk.class);

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSER_KEY, UserGroupInformation
        .getCurrentUser().getShortUserName());
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setLong(DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT, 10000);
    CONF.setLong(DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT, 10000);
    CONF.setLong(FS_TRASH_INTERVAL_KEY, 10); // 10 mins
    cluster =
        new MiniDFSCluster.Builder(CONF)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(4))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs1 = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
    fHdfs3 = cluster.getFileSystem(2);
    fHdfs4 = cluster.getFileSystem(3);
    ConfigUtil.addLink(CONF, "zkmpt", "/fs1", fHdfs1.getUri());
    ConfigUtil.addLink(CONF, "zkmpt", "/fs2", fHdfs2.getUri());
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    CONF.set("fs.AbstractFileSystem.hdfs.impl", FederatedHdfs.class.getName());
    CONF.set("fs.defaultFS", "hdfs://zkmpt");
    CONF.setLong(FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL, 3000);
    CONF.setLong(
        FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL_RANDOMFACTOR,
        2000);
    CONF.setLong(FederationConfigKeys.FEDFS_MOUT_TABLE_RENEW_RETRY_INTERVAL,
        1000);
  }

  private static String runUpdateMptOnZk(final DFSAdmin shell)
      throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      shell.run(new String[] { "-updateMptOnZk" });
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("updateMptOnZk results:\n" + "<results>\n " + results
        + "</results>");
    return results;
  }

  @Test
  public void testFederatedDFSFileSystem() throws Exception {
    // Test steps:
    // 1. Verify that we can not create files under a non-existing mount point
    // 2. Add a new mount point in configuration and update zk
    // 3. Verify that we can create files under the new mount point with the
    // same fs client in step 2
    String str = "testFederatedDFSFileSystem";
    CONF.set(CommonConfigurationKeys.ZK_OBSERVER, hostPort);
    Configuration tmpCONF = new Configuration(CONF);
    tmpCONF.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
    DFSAdmin admin = new DFSAdmin(tmpCONF);
    runUpdateMptOnZk(admin);
    OutputStream out = dfs.create(new Path("hdfs://zkmpt/fs1/testfile"));
    out.write(str.getBytes());
    out.close();
    boolean excepted = false;
    // step 1
    try {
      out = dfs.create(new Path("hdfs://zkmpt/fs3/testfile"));
    } catch (NotInMountpointException nme) {
      LOG.info("Create hdfs://zkmpt/fs3/testfile failed", nme);
      excepted = true;
    }
    Assert.assertTrue(excepted);

    // step 2
    tmpCONF = new Configuration(CONF);
    tmpCONF.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
    ConfigUtil.addLink(tmpCONF, "zkmpt", "/fs3", fHdfs3.getUri());
    tmpCONF.setBoolean("fs.hdfs.impl.disable.cache", true);
    admin = new DFSAdmin(tmpCONF);
    runUpdateMptOnZk(admin);

    // step 3 verify the new configuration is caught by client
    Thread.sleep(7000);
    out = dfs.create(new Path("hdfs://zkmpt/fs3/testfile"));
    out.write(str.getBytes());
    out.close();
    Assert.assertTrue(dfs.exists(new Path("hdfs://zkmpt/fs3/testfile")));
  }

  @Test
  public void testFederatedHDFS() throws Exception {
    // Test steps:
    // 1. Verify that we can not create files under a non-existing mount point
    // 2. Add a new mount point in configuration and update zk
    // 3. Verify that we can create files under the new mount point with the
    // same fs client in step 2
    String str = "testFederatedHDFS";
    CONF.set(CommonConfigurationKeys.ZK_OBSERVER, hostPort);
    Configuration tmpCONF = new Configuration(CONF);
    tmpCONF.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
    DFSAdmin admin = new DFSAdmin(tmpCONF);
    runUpdateMptOnZk(admin);
    AbstractFileSystem afs =
        AbstractFileSystem.get(new URI("hdfs://zkmpt/"), CONF);
    final Options.CreateOpts[] opts =
        { Options.CreateOpts.perms(FILE_DEFAULT_PERM) };
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
    OutputStream out = afs.create(new Path("/fs1/testfile2"), createFlag, opts);
    out.write(str.getBytes());
    out.close();
    boolean excepted = false;
    // step 1
    try {
      out = afs.create(new Path("/fs4/testfile2"), createFlag, opts);
    } catch (FileNotFoundException fnfe) {
      LOG.info("Create /fs4/testfile2 failed", fnfe);
      excepted = true;
    }
    Assert.assertTrue(excepted);

    // step 2
    tmpCONF = new Configuration(CONF);
    tmpCONF.set(CommonConfigurationKeys.ZK_QUORUM_KEY, hostPort);
    ConfigUtil.addLink(tmpCONF, "zkmpt", "/fs4", fHdfs4.getUri());
    tmpCONF.setBoolean("fs.hdfs.impl.disable.cache", true);
    admin = new DFSAdmin(tmpCONF);
    runUpdateMptOnZk(admin);

    // step 3 verify the new configuration is caught by client
    Thread.sleep(7000);
    out = afs.create(new Path("/fs4/testfile2"), createFlag, opts);
    out.write(str.getBytes());
    out.close();
    Assert.assertTrue(fHdfs4.exists(new Path("/testfile2")));
  }
}
