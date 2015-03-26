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

import java.util.LinkedList;
import java.util.List;

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

  private static final int SINCELASTWRITE = 0x1;
  private static final int KEEPEMPTYDIR = 0x2;
  private static final int KEEPEMPTYSUBDIR = 0x4;

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
    ttlPolicy = new TtlPolicy(conf, TtlMetrics.create());
  }

  @After
  public void tearDown() throws Exception {
    fsShell.run(new String[] {"-rm", "-R", "/*"});
  }

  public void testGetTtlInfo(String ttlArgs, int ttlProperty) throws Exception {
    Path path = new Path("/dir1");
    dfs.mkdirs(path);
    setTtl(new Path("/dir1"), ttlArgs);

    TtlInfo ttlInfo = ttlPolicy.getTtlInfo(path);
    Assert.assertNotNull(ttlInfo);
    Assert.assertEquals(path, ttlInfo.getPath());
    Assert.assertEquals(12345, ttlInfo.getTtl());
    Assert.assertEquals(ttlProperty, ttlInfo.getProperty());
    dfs.delete(path, true);
  }

  @Test
  public void testGetTtlInfo() throws Exception {
    testGetTtlInfo("12345", 0);
    testGetTtlInfo("-sinceLastWrite 12345", SINCELASTWRITE);
    testGetTtlInfo("-sinceLastWrite -keepEmptyDir 12345", SINCELASTWRITE
        | KEEPEMPTYDIR);
    testGetTtlInfo("-sinceLastWrite -keepEmptySubDir 12345", SINCELASTWRITE
        | KEEPEMPTYSUBDIR);
    testGetTtlInfo("-keepEmptyDir -keepEmptySubDir 12345", KEEPEMPTYDIR
        | KEEPEMPTYSUBDIR);
    testGetTtlInfo("-sinceLastWrite -keepEmptyDir -keepEmptySubDir 12345",
        SINCELASTWRITE | KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
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

    cleanupTestDirectoryTree();
  }

  public void testTraverseDirectoryTreeWithFileTtlExpired(String ttlArgs,
      int ttlProperty) throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1/file"), ttlArgs);

    try {
      ttlPolicy.traverseDirectoryTree(new Path("/"));
    } catch (Exception e) {
      // Ignore
    }
    boolean sinceLastWrite = (ttlProperty & SINCELASTWRITE) != 0;
    boolean keepEmptyDir = (ttlProperty & KEEPEMPTYDIR) != 0;
    boolean keepEmptySubDir = (ttlProperty & KEEPEMPTYSUBDIR) != 0;

    if (!keepEmptyDir && !keepEmptySubDir) {
      if (!sinceLastWrite) {
        Assert.assertFalse(dfs.exists(new Path("/user/test1/file")));
      } else {
        Assert.assertTrue(dfs.exists(new Path("/user/test1/file")));
      }
      Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
      Assert.assertTrue(dfs.exists(new Path("/user/test2")));
      Assert.assertTrue(dfs.exists(new Path("/user/test3")));
    }

    cleanupTestDirectoryTree();
  }

  @Test
  public void testTraverseDirectoryTreeWithFileTtlExpired() throws Exception {
    testTraverseDirectoryTreeWithFileTtlExpired("12345", 0);
    testTraverseDirectoryTreeWithFileTtlExpired("-sinceLastWrite 12345",
        SINCELASTWRITE);
    testTraverseDirectoryTreeWithFileTtlExpired(
        "-sinceLastWrite -keepEmptyDir 12345", SINCELASTWRITE | KEEPEMPTYDIR);
    testTraverseDirectoryTreeWithFileTtlExpired(
        "-sinceLastWrite -keepEmptySubDir 12345", SINCELASTWRITE
            | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithFileTtlExpired(
        "-keepEmptyDir -keepEmptySubDir 12345", KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithFileTtlExpired(
        "-sinceLastWrite -keepEmptyDir -keepEmptySubDir 12345", SINCELASTWRITE
            | KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
  }
  
  @Test
  public void testTtlPolicyMetrics() throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1"), "12345");
    ttlPolicy.traverseDirectoryTree(new Path("/"));
    
    Assert.assertTrue(ttlPolicy.getMetrics().filesDeletedByTTL.value()==3);
  }

  public void testTraverseDirectoryTreeWithDirectoryTtlExpired(String ttlArgs,
      int ttlProperty) throws Exception {
    createTestDirectoryTree();
    setTtl(new Path("/user/test1"), ttlArgs);
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    boolean sinceLastWrite = (ttlProperty & SINCELASTWRITE) != 0;
    boolean keepEmptyDir = (ttlProperty & KEEPEMPTYDIR) != 0;
    boolean keepEmptySubDir = (ttlProperty & KEEPEMPTYSUBDIR) != 0;

    if (!sinceLastWrite) {
      Assert.assertFalse(dfs.exists(new Path("/user/test1/file")));
    } else {
      Assert.assertTrue(dfs.exists(new Path("/user/test1/file")));
    }
    if (!sinceLastWrite && !keepEmptySubDir) {
      Assert.assertFalse(dfs.exists(new Path("/user/test1/dir")));
    } else {
      Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
    }
    if (!sinceLastWrite && !keepEmptyDir) {
      Assert.assertFalse(dfs.exists(new Path("/user/test1")));
    } else {
      Assert.assertTrue(dfs.exists(new Path("/user/test1")));
    }
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));

    cleanupTestDirectoryTree();
  }

  @Test
  public void testTraverseDirectoryTreeWithDirectoryTtlExpired()
      throws Exception {
    testTraverseDirectoryTreeWithDirectoryTtlExpired("12345", 0);
    testTraverseDirectoryTreeWithDirectoryTtlExpired("-sinceLastWrite 12345",
        SINCELASTWRITE);
    testTraverseDirectoryTreeWithDirectoryTtlExpired(
        "-sinceLastWrite -keepEmptyDir 12345", SINCELASTWRITE | KEEPEMPTYDIR);
    testTraverseDirectoryTreeWithDirectoryTtlExpired(
        "-sinceLastWrite -keepEmptySubDir 12345", SINCELASTWRITE
            | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithDirectoryTtlExpired(
        "-keepEmptyDir -keepEmptySubDir 12345", KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithDirectoryTtlExpired(
        "-sinceLastWrite -keepEmptyDir -keepEmptySubDir 12345", SINCELASTWRITE
            | KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
  }

  public void testTraverseDirectoryTreeWithChildFileTtlExpired(String ttlArgs,
      int ttlProperty) throws Exception {
    createTestDirectoryTree();
    String ttl1 = ttlArgs + " 10M";
    String ttl2 = ttlArgs + " 12345";

    try {
      setTtl(new Path("/user/test1"), ttl1);
    } catch (Exception e) {
      // Ignore
    }
    try {
      setTtl(new Path("/user/test1/file"), ttl2);
    } catch (Exception e) {
      // Ignore
    }
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    boolean sinceLastWrite = (ttlProperty & SINCELASTWRITE) != 0;
    boolean keepEmptyDir = (ttlProperty & KEEPEMPTYDIR) != 0;
    boolean keepEmptySubDir = (ttlProperty & KEEPEMPTYSUBDIR) != 0;

    if (!sinceLastWrite && !keepEmptyDir && !keepEmptySubDir) {
      Assert.assertFalse(dfs.exists(new Path("/user/test1/file")));
    } else {
      Assert.assertTrue(dfs.exists(new Path("/user/test1/file")));
    }

    Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));

    cleanupTestDirectoryTree();
  }

  @Test
  public void testTraverseDirectoryTreeWithChildFileTtlExpired()
      throws Exception {
    testTraverseDirectoryTreeWithChildFileTtlExpired("", 0);
    testTraverseDirectoryTreeWithChildFileTtlExpired("-sinceLastWrite",
        SINCELASTWRITE);
    testTraverseDirectoryTreeWithChildFileTtlExpired(
        "-sinceLastWrite -keepEmptyDir", SINCELASTWRITE | KEEPEMPTYDIR);
    testTraverseDirectoryTreeWithChildFileTtlExpired(
        "-sinceLastWrite -keepEmptySubDir", SINCELASTWRITE
            | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithChildFileTtlExpired(
        "-keepEmptyDir -keepEmptySubDir", KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithChildFileTtlExpired(
        "-sinceLastWrite -keepEmptyDir -keepEmptySubDir", SINCELASTWRITE
            | KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
  }

  public void testTraverseDirectoryTreeWithChildDirectoryTtlExpired(
      String ttlArgs, int ttlProperty) throws Exception {
    createTestDirectoryTree();

    String ttl1 = ttlArgs + " 10M";
    String ttl2 = ttlArgs + " 12345";
    setTtl(new Path("/user/test1"), ttl1);
    setTtl(new Path("/user/test1/dir"), ttl2);
    ttlPolicy.traverseDirectoryTree(new Path("/"));

    boolean sinceLastWrite = (ttlProperty & SINCELASTWRITE) != 0;
    boolean keepEmptyDir = (ttlProperty & KEEPEMPTYDIR) != 0;

    Assert.assertTrue(dfs.exists(new Path("/user/test1/file")));
    if (!sinceLastWrite && !keepEmptyDir) {
      Assert.assertFalse(dfs.exists(new Path("/user/test1/dir")));
    } else {
      Assert.assertTrue(dfs.exists(new Path("/user/test1/dir")));
    }
    Assert.assertTrue(dfs.exists(new Path("/user/test2")));
    Assert.assertTrue(dfs.exists(new Path("/user/test3")));

    cleanupTestDirectoryTree();
  }

  @Test
  public void testTraverseDirectoryTreeWithChildDirectoryTtlExpired()
      throws Exception {
    testTraverseDirectoryTreeWithChildDirectoryTtlExpired("", 0);
    testTraverseDirectoryTreeWithChildDirectoryTtlExpired("-sinceLastWrite",
        SINCELASTWRITE);
    testTraverseDirectoryTreeWithChildDirectoryTtlExpired(
        "-sinceLastWrite -keepEmptyDir", SINCELASTWRITE | KEEPEMPTYDIR);
    testTraverseDirectoryTreeWithChildDirectoryTtlExpired(
        "-sinceLastWrite -keepEmptySubDir", SINCELASTWRITE | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithChildDirectoryTtlExpired(
        "-keepEmptyDir -keepEmptySubDir", KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
    testTraverseDirectoryTreeWithChildDirectoryTtlExpired(
        "-sinceLastWrite -keepEmptyDir -keepEmptySubDir", SINCELASTWRITE
            | KEEPEMPTYDIR | KEEPEMPTYSUBDIR);
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

  private void cleanupTestDirectoryTree() throws Exception {
    Path path = new Path("/user");
    dfs.delete(path, true);
  }

  private void setTtl(Path path, String ttlArgs) throws Exception {
    List<String> args = new LinkedList<String>();
    args.add("-setTtl");
    for (String subArg : ttlArgs.trim().split("\\s+")) {
      args.add(subArg);
    }
    args.add(path.toString());
    String[] cmdArgs = new String[args.size()];
    args.toArray(cmdArgs);
    int exitCode = fsShell.run(cmdArgs);
    if (dfs.getFileStatus(path).isDirectory()) {
      Assert.assertEquals(0, exitCode);
    } else {
      boolean dirOpt =
          args.contains("-keepEmptyDir") || args.contains("-keepEmptySubDir");
      Assert.assertTrue((exitCode == 0) || dirOpt);
    }
  }
}
