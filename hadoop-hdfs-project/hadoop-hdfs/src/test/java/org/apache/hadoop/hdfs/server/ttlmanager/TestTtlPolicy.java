/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.ttlmanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.ttlmanager.TtlPolicy.TtlInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTtlPolicy {

  private static Configuration conf;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem dfs;
  private static FsShell fsShell;
  private TtlPolicy ttlPolicy;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.HDFS_TTLMANAGER_ENABLE_TRASH_KEY, false);

    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();
    dfs = dfsCluster.getFileSystem();

    fsShell = new FsShell();
    fsShell.setConf(conf);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    ttlPolicy = new TtlPolicy(conf);
  }

  @After
  public void tearDown() throws Exception {
    fsShell.run(new String[] {"-rm", "-R", "/*"});
  }

  @Test
  public void testGetTtlInfo() throws Exception {
    Path path = new Path("/dir1");
    dfs.mkdirs(path);
    setTtl(new Path("/dir1"), "12345");

    TtlInfo ttlInfo = ttlPolicy.getTtlInfo(path);
    Assert.assertNotNull(ttlInfo);
    Assert.assertEquals(path, ttlInfo.getPath());
    Assert.assertEquals(12345, ttlInfo.getTtl());
  }

  @Test
  public void testTraverseDirectoryTreeWithOneNode() throws Exception {
    ttlPolicy.traverseDirectoryTree(new Path("/"));
    Assert.assertTrue(dfs.exists(new Path("/")));
  }

  @Test
  public void testTraverseDirectoryTreeWithoutTtl() throws Exception {
    createTestDirectoryTree();
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    Assert.assertTrue(dfs.exists(new Path("/user/test1/file")));
    Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));
  }

  @Test
  public void testTraverseDirectoryTreeWithFileTtlExpired() throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1/file"), "12345");
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    Assert.assertFalse(dfs.exists(new Path("/user/test1/file")));
    Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));
  }

  @Test
  public void testTraverseDirectoryTreeWithDirectoryTtlExpired()
      throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1"), "12345");
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    Assert.assertFalse(dfs.exists(new Path("/user/test1/file")));
    Assert.assertFalse(dfs.exists(new Path("/user/test1/dir")));
    Assert.assertFalse(dfs.exists(new Path("/user/test1")));
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));
  }

  @Test
  public void testTraverseDirectoryTreeWithChildFileTtlExpired()
      throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1"), "10M");
    setTtl(new Path("/user/test1/file"), "12345");
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    Assert.assertFalse(dfs.exists(new Path("/user/test1/file")));
    Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));
  }

  @Test
  public void testTraverseDirectoryTreeWithChildDirectoryTtlExpired()
      throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1"), "10M");
    setTtl(new Path("/user/test1/dir"), "12345");
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    Assert.assertTrue(dfs.exists(new Path("/user/test1/file")));
    Assert.assertFalse(dfs.exists(new Path("/user/test1/dir")));
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));
  }

  private void createTestDirectoryTree() throws Exception {
    Path path1 = new Path("/user/test1");
    dfs.mkdirs(path1);
    Path path2 = new Path("/user/test2");
    dfs.mkdirs(path2);
    Path path3 = new Path("/user/test3");
    dfs.mkdirs(path3);
    Path path4 = new Path("/user/test1/file");
    dfs.create(path4).close();
    Path path5 = new Path("/user/test1/dir");
    dfs.mkdirs(path5);
  }

  private void setTtl(Path path, String ttl) throws Exception {
    int exitCode = fsShell.run(new String[] {"-setTtl", ttl, path.toString()});
    Assert.assertEquals(0, exitCode);
  }
}
