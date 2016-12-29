package org.apache.hadoop.hdfs;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

public class TestFederatedDFSFileSystem {
  private static final Log LOG = LogFactory.getLog(TestFederatedDFSFileSystem.class);
  private Configuration conf;
  private MiniDFSCluster cluster1;
  private MiniDFSCluster cluster2;

  @Before
  public void setup() throws IOException{
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    // Bump up replication interval so that we only run replication
    // checks explicitly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 600);
    // Increase max streams so that we re-replicate quickly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 1000);
    conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);

    try {
      cluster1 = setupNewDFSCluster();
      cluster2 = setupNewDFSCluster();
      setupFederationConfig();
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
      conf.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    } catch (Exception e) {
      LOG.info("Setup test env failed " + e.getMessage());
    }
  }

  private MiniDFSCluster setupNewDFSCluster() throws IOException { Configuration tmpConf = new Configuration(conf);
    File baseDir = new File(
        "./target/test-dir-" + UUID.randomUUID().toString().substring(0, 4)
            + "/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    tmpConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(tmpConf)
        .numDataNodes(3)
        .format(true)
        .build();
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
    ConfigUtil.addLink(conf, "/home", new URI(cluster1NNAddress + "/home"));
    ConfigUtil.addLink(conf, "/user", new URI(cluster2NNAddress + "/user"));
  }

  @Test
  public void testBasic() throws Exception {
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
  }
}
