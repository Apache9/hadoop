package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.junit.Before;
import org.junit.Test;

public class TestFederationRename {
  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs1;
  private static FileSystem fHdfs2;

  @Before
  public void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    cluster =
        new MiniDFSCluster.Builder(CONF)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs1 = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
  }

  private void basicTestEnvSetup() throws IOException {

    fHdfs1.mkdirs(new Path("/a/b"), null);
    fHdfs1.mkdirs(new Path("/a/c"), null);
    fHdfs2.mkdirs(new Path("/c/d"), null);
    ConfigUtil.addLink(CONF, "/home", fHdfs1.getUri());
    ConfigUtil.addLink(CONF, "/user", fHdfs2.getUri());
  }

  private void dumpDir(FileSystem fs, Path p) throws IOException {
    FileStatus[] sts = fs.listStatus(p);
    System.out.println("File " + p.toString() + " and its children are:");
    for (FileStatus st : sts) {
      System.out.println(st.getPath().toString());
      if (st.isDirectory()) {
        dumpDir(fs, st.getPath());
      }
    }
  }

  @Test
  public void doBasicTest() throws IOException {
    String str = "Just a test";
    basicTestEnvSetup();
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    Assert.assertTrue(fHdfs1 instanceof DistributedFileSystem);
    Assert.assertTrue(fHdfs2 instanceof DistributedFileSystem);
    Assert.assertFalse(fHdfs1 instanceof FederatedDFSFileSystem);
    Assert.assertFalse(fHdfs2 instanceof FederatedDFSFileSystem);
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
    Assert.assertTrue(dfs instanceof DistributedFileSystem);
    Assert.assertTrue(dfs instanceof FederatedDFSFileSystem);

    OutputStream out = dfs.create(new Path("/home/a/b/testfile"));
    out.write(str.getBytes());
    out.close();

    boolean rename = dfs.rename(new Path("/home/a"), new Path("/user/c/a"));
    Assert.assertTrue(rename);

    dumpDir(fHdfs1, new Path("/"));
    dumpDir(fHdfs2, new Path("/"));
    Assert.assertTrue(dfs.exists(new Path("/user/c/a/b/testfile")));
    Assert.assertFalse(dfs.exists(new Path("/home/a")));

    InputStream in = dfs.open(new Path("/user/c/a/b/testfile"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String res = reader.readLine();
    in.close();
    Assert.assertEquals(str, res);
  }

}
