package org.apache.hadoop.tools;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FederatedHdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.FederatedDFSFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFedMv {
  private static MiniDFSCluster cluster;
  private static MiniMRYarnCluster mrCluster;
  private static final Configuration CONF = new Configuration();
  private static final Configuration MRCONF = new Configuration();
  private static FileSystem fHdfs1;
  private static FileSystem fHdfs2;

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setLong(DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT, 10000);
    CONF.setLong(DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT, 10000);
    CONF.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);
    Configuration tmpConf = new Configuration(CONF);
    File baseDir =
        new File("./target/test-dir-"
            + UUID.randomUUID().toString().substring(0, 4) + "/")
            .getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    tmpConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    cluster =
        new MiniDFSCluster.Builder(tmpConf)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs1 = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
    ConfigUtil.addLink(CONF, "/fs1", fHdfs1.getUri());
    ConfigUtil.addLink(CONF, "/fs2", fHdfs2.getUri());
    ConfigUtil.addLink(MRCONF, "/fs1", fHdfs1.getUri());
    ConfigUtil.addLink(MRCONF, "/fs2", fHdfs2.getUri());
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    CONF.set("fs.defaultFS", "hdfs://default");
    MRCONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://default");
    MRCONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    MRCONF
        .set("fs.AbstractFileSystem.hdfs.impl", FederatedHdfs.class.getName());
    mrCluster = new MiniMRYarnCluster(TestFedMv.class.getName(), 3);
    MRCONF
        .set(MRJobConfig.MR_AM_STAGING_DIR, "/fs1/user/yarn/apps_staging_dir");
    MRCONF.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);
    mrCluster.init(MRCONF);
    mrCluster.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    mrCluster.close();
    cluster.shutdown();
  }

  @Test
  public void testFedMv() throws Exception {
    String str = "testbasictrashop";
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
    Assert.assertTrue(dfs instanceof DistributedFileSystem);
    Assert.assertTrue(dfs instanceof FederatedDFSFileSystem);
    dfs.mkdirs(new Path("/fs1/dir1"), null);
    dfs.mkdirs(new Path("/fs1/dir1/dir2"));
    OutputStream out = dfs.create(new Path("/fs1/dir1/testfile"));
    out.write(str.getBytes());
    out.close();

    JobConf jobConf = new JobConf(mrCluster.getConfig());
    FedMv fm = new FedMv();
    fm.setConf(jobConf);
    String arg[] = new String[] { "-m 3", "/fs1/dir1", "/fs2/dir1" };
    int ret = fm.run(arg);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(dfs.exists(new Path("/fs1/dir1")));
    Assert.assertTrue(dfs.exists(new Path("/fs2/dir1")));
    Assert.assertTrue(dfs.exists(new Path("/fs2/dir1/dir2")));
    Assert.assertTrue(dfs.exists(new Path("/fs2/dir1/testfile")));
    FSDataInputStream in = dfs.open(new Path("/fs2/dir1/testfile"));
    byte[] readout = new byte[128];
    int len = in.read(readout);
    Assert.assertTrue(str.equals(new String(readout, 0, len)));
  }
}
