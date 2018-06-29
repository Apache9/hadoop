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
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static junit.framework.TestCase.fail;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestViewFileSystemMergedInodeTree extends ViewFileSystemBaseTest {
  private static FileSystem fsDefault;
  private static MiniDFSCluster cluster;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final String TEST_TABLE_NAME = "tmt";
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final String[] FS_HDFS_URI = new String[NAME_SPACES_COUNT];
  private static final Configuration CONF = new Configuration();
  private static final File TEST_DIR = PathUtils.getTestDir(
      TestViewFileSystemMergedInodeTree.class);
  private static final String TEST_TEMP_PATH =
      "/tmp/TestViewFileSystemMergedInodeTree";

  @BeforeClass
  public static void clusterSetupAtBeginning() throws IOException,
          LoginException, URISyntaxException {
    SupportsBlocks = true;
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(CONF)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(
            NAME_SPACES_COUNT))
        .numDataNodes(DATA_NODES_COUNT)
        .build();
    cluster.waitClusterUp();

    for (int i = 0; i < NAME_SPACES_COUNT; i++) {
      FS_HDFS[i] = cluster.getFileSystem(i);
      FS_HDFS_URI[i] = "hdfs://" + cluster.getNameNode(i).getHostAndPort();
    }
    fsDefault = FS_HDFS[FS_INDEX_DEFAULT];
  }

  @AfterClass
  public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    fsTarget = fsDefault;
    targetTestRoot = fsDefault.makeQualified(new Path("/"));
    super.setUp();
    FS_HDFS[1].mkdirs(new Path("/log_collector/"));
    FS_HDFS[2].mkdirs(new Path("/log_collector/web"));
    FS_HDFS[2].mkdirs(new Path("/log_collector/web/orders"));
    FS_HDFS[2].mkdirs(new Path("/log_collector/foo/bar"));
  }

  /**
   * Override this so that we don't set the targetTestRoot to any path under the
   * root of the FS, and so that we don't try to delete the test dir, but rather
   * only its contents.
   */
  @Override
  void initializeTargetTestRoot() throws IOException {
    for (int i = 0; i < 3; i++) {
      for (FileStatus status : FS_HDFS[i].listStatus(new Path("/"))) {
        FS_HDFS[i].delete(status.getPath(), true);
      }
    }
  }

  @Override
  void setupMountPoints() {
    super.setupMountPoints();
    ConfigUtil.addLink(conf, "/log_collector/",
            new Path(FS_HDFS_URI[1], "/log_collector").toUri());
    ConfigUtil.addLink(conf, "/log_collector/web",
            new Path(FS_HDFS_URI[2], "/log_collector/web").toUri());
    ConfigUtil.addLink(conf, "/log_collector/web/orders",
            new Path(FS_HDFS_URI[2], "/log_collector/web/orders").toUri());
    ConfigUtil.addLink(conf, "/log_collector/foo/bar",
            new Path(FS_HDFS_URI[2], "/log_collector/foo/bar").toUri());
  }

  @Override
  int getExpectedDelegationTokenCount() {
    return 3;
  }

  @Override
  int getExpectedDelegationTokenCountWithCredentials() {
    return 3;
  }

  @Override
  int getExpectedMountPoints() {
    return 12; // Base test setup 8 mount points, plus 4 in this test
  }

  @Override
  int getExpectedDirPaths() {
    return 8;
  }

  @Test
  public void testConfigRootMountPointsOnly() throws Exception {
    TEST_DIR.mkdirs();
    String clusterName = "ClusterX";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    String testFileName = "testRootMnt";

    File infile = new File(TEST_DIR, testFileName);
    final byte[] content = "HelloWorld".getBytes();
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(infile);
      fos.write(content);
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
    assertEquals((long)content.length, infile.length());

    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, clusterName, "/", TEST_DIR.toURI());

    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
    FileStatus stat = vfs.getFileStatus(new Path(viewFsUri.toString() +
        testFileName));

    System.out.println(stat);
    vfs.close();
  }

  /**
   * Test path resolve
   */

  private FileSystem getFsWithRootMountPoints()
      throws URISyntaxException, IOException {
    // add a root mountpoint
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, "/", new URI(FS_HDFS_URI[1]));
    return FileSystem.get(FsConstants.VIEWFS_URI, config);
  }
 
  @Test
  public void testNestedMountPointResolve() throws Exception {
    fsView.mkdirs(new Path("/log_collector/cloud"));
    assertTrue(FS_HDFS[1].exists(new Path("/log_collector/cloud")));
    fsView.mkdirs(new Path("/log_collector/foo/chen"));
    assertTrue(FS_HDFS[1].exists(new Path("/log_collector/foo/chen")));
    fsView.mkdirs(new Path("/log_collector/web/access"));
    assertTrue(FS_HDFS[2].exists(new Path("/log_collector/web/access")));
    assertTrue(fsView.mkdirs(new Path("/log_collector/web")));
    assertTrue(fsView.mkdirs(new Path("/log_collector/foo/bar")));
    assertFalse(FS_HDFS[1].exists(new Path("/log_collector/foo/bar")));
    fsView.mkdirs(new Path("/log_collector/foo/bar/x"));
    assertTrue(FS_HDFS[2].exists(new Path("/log_collector/foo/bar/x")));

    try {
      fsView.mkdirs(new Path("/internalDir/newDir"));
      Assert.fail("/internalDir/newDir should not create succeed!");
    } catch (AccessControlException e) {}

    FileSystem fsv = getFsWithRootMountPoints();
    fsv.mkdirs(new Path("/internalDir/newDir"));
    assertTrue(FS_HDFS[1].exists(new Path("/internalDir/newDir")));
  }

  @Test
  public void testResolvePathsInMountTable() throws Exception {
    ViewFileSystem vfs = (ViewFileSystem) fsView;
    // resolve internal nodes path
    assertTrue(vfs.getTargetFileSystem(new Path(
        "/internalDir")) instanceof ViewFileSystem.InternalDirOfViewFs);
    assertTrue(vfs.getTargetFileSystem(new Path(
        "/internalDir/internalDir2")) instanceof ViewFileSystem.InternalDirOfViewFs);

    // resolve leaf nodes path
    String fsDefaultUri = fsDefault.getUri().toString().replaceAll("/$", "");
    FileSystem res = vfs.getTargetFileSystem(new Path("/targetRoot"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""), fsDefaultUri);
    res = vfs.getTargetFileSystem(new Path("/user"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""),
        fsDefaultUri + "/user");

    // resolve nested mountpoint inter node
    res = vfs.getTargetFileSystem(new Path("/log_collector"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""),
        FS_HDFS_URI[1] + "/log_collector");
    res = vfs.getTargetFileSystem(new Path("/log_collector/foo"));
    assertTrue(res instanceof ViewFileSystem.InternalDirOfViewFs);
    res = vfs.getTargetFileSystem(new Path("/log_collector/foo/chen"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""),
            FS_HDFS_URI[1] + "/log_collector");

    // resolve root
    res = vfs.getTargetFileSystem(new Path("/"));
    assertTrue(res instanceof ViewFileSystem.InternalDirOfViewFs);
    // add root mountpoint
    vfs = (ViewFileSystem)getFsWithRootMountPoints();
    res = vfs.getTargetFileSystem(new Path("/"));
    assertTrue(res instanceof ChRootedFileSystem);
  }

  /**
   * Test basic operations involve rootDefault FS
   */

  @Test
  public void testBasicWriteOperation() throws Exception {
    try {
      FileSystemTestHelper.createFile(fsView, new Path("/internalDir/afile"));
      fail("Crerate file on internalDir should fail");
    } catch (AccessControlException e) {}
    try {
      FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web"));
      fail("/log_collector/web already exists as a directory, should not success");
    } catch (AccessControlException e) {}
    FileSystemTestHelper.createFile(fsView,
        new Path("/log_collector/new_user_file"));
    assertTrue(FS_HDFS[1].exists(new Path("/log_collector/new_user_file")));
    FileSystemTestHelper.createFile(fsView,
        new Path("/log_collector/web/orders/new_order"));
    assertTrue(
        FS_HDFS[2].exists(new Path("/log_collector/web/orders/new_order")));
    try {
      FileSystemTestHelper.createFile(fsView, new Path("/notExistDir/afile"));
      fail("/notExistDir not exist, should not create file success");
    } catch (AccessControlException e) {}

    try {
      fsView.delete(new Path("/log_collector/web/"), true);
      fail("/log_collector/web/ is an internal path, can't delete");
    } catch (AccessControlException e) {}

    try {
      assertTrue(fsView.mkdirs(new Path("/log_collector/foo")));
      fsView.delete(new Path("/log_collector/foo/"), true);
      fail("/log_collector/foo/ is an internal path, can't delete");
    } catch (AccessControlException e) {}
  }

  /**
   * Test ListStatus
   */

  @Test
  public void testListStatusNormal() throws Exception {
    FileStatus[] res = fsView.listStatus(new Path("/internalDir"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/internalDir/internalDir2"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/internalDir/linkToDir2"));
  }

  @Test
  public void testListStatusPureNestedMT() throws Exception {
    fsView.mkdirs(new Path("/log_collector/client/cn"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/zfile"));
    // test an internal mountpoint
    FileStatus[] res = fsView.listStatus(new Path("/log_collector"));
    assertEquals(res[0].getPath(), new Path("viewfs://", "/log_collector/client"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/log_collector/foo"));
    assertEquals(res[2].getPath(), new Path("viewfs://", "/log_collector/web"));
    assertEquals(res[3].getPath(), new Path("viewfs://", "/log_collector/zfile"));

    fsView.mkdirs(new Path("/log_collector/foo/adir/afile"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/foo/zfile"));
    // test an internal node, with ancestor mountpoints
    res = fsView.listStatus(new Path("/log_collector/foo"));
    assertEquals(res[0].getPath(), new Path("viewfs://", "/log_collector/foo/adir"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/log_collector/foo/bar"));
    assertEquals(res[2].getPath(), new Path("viewfs://", "/log_collector/foo/zfile"));
  }

  @Test
  public void testListStatusOnRoot() throws Exception {
    FileSystem fsv = getFsWithRootMountPoints();
    fsv.mkdirs(new Path("/newDir"));
    FileSystemTestHelper.createFile(fsv, new Path("/zfile"));
    FileStatus[] res = fsv.listStatus(new Path("/"));
    assertEquals(res.length, 10);
    assertEquals(res[9].getPath(), new Path("viewfs://", "/zfile"));
  }

  @Test
  public void testListStatusInternaldirWithRootDefault() throws Exception {
    FileSystem fsv = getFsWithRootMountPoints();
    FileStatus[] res = fsv.listStatus(new Path("/internalDir"));
    assertEquals(res.length, 2);
  }

  @Test
  public void testListStatusOnlyResolveOnLeafNode() throws Exception {
    fsView.mkdirs(new Path("/log_collector/web/orders/old_orders"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web/orders/order1"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web/orders/order2"));
    FileStatus[] res = fsView.listStatus(new Path("/log_collector/web/orders/"));
    assertEquals(res.length, 3);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/log_collector/web/orders/old_orders"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/log_collector/web/orders/order1"));
    assertEquals(res[2].getPath(), new Path("viewfs://", "/log_collector/web/orders/order2"));
  }

  @Test
  public void testListStatusWithScheme() throws Exception {
    FileStatus[] res = fsView.listStatus(new Path("viewfs:/internalDir"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/internalDir/internalDir2"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/internalDir/linkToDir2"));
  }

  /**
   * Test getContentSummary
   */

  @Test
  public void testGetContentSummaryOnInternalNode() throws Exception {
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web/file1"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web/file2"));
    ContentSummary summary = fsView.getContentSummary(new Path("/log_collector/web"));
    assertEquals(summary.getFileCount(), 2);
    // for the internal mountpoints, only show content summary of it's target filesystem
    // not including the children in mountable
    assertEquals(summary.getDirectoryCount(), 2);

    summary = fsView.getContentSummary(new Path("/"));
    assertEquals(summary.getFileCount(), 2);
    assertEquals(summary.getDirectoryCount(), 7);

    // After add a mountpoint on root, the content summary not including the children
    // in mounttable
    FileSystem fsv = getFsWithRootMountPoints();
    fsv.mkdirs(new Path("/newDir"));
    FileSystemTestHelper.createFile(fsv, new Path("/newfile"));
    summary = fsv.getContentSummary(new Path("/"));
    assertEquals(summary.getFileCount(), 1);
    assertEquals(summary.getDirectoryCount(), 3);
  }

  @Test
  public void testGetContentSummaryOnLeafNode() throws Exception {
    fsView.mkdirs(new Path("/log_collector/web/orders/old_orders"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web/orders/order1"));
    FileSystemTestHelper.createFile(fsView, new Path("/log_collector/web/orders/order2"));
    ContentSummary summary =
        fsView.getContentSummary(new Path("/log_collector/web/orders/"));
    assertEquals(summary.getFileCount(), 2);
    assertEquals(summary.getDirectoryCount(), 2);
  }

  /**
   * test rename between nn
   */
  @Test
  public void testRenameBetweenNN() throws Exception {
    FileSystemTestHelper.createFile(fsView,
        new Path("/log_collector/foo/chen/newfile"));
    assertTrue(FS_HDFS[1].exists(new Path("/log_collector/foo/chen/newfile")));
    FS_HDFS[2].mkdirs(new Path("/log_collector/foo/bar"));
    fsView.rename(new Path("/log_collector/foo/chen/newfile"),
        new Path("/log_collector/foo/bar/newfile"));
    assertFalse(FS_HDFS[1].exists(new Path("/log_collector/foo/chen/newfile")));
    assertTrue(FS_HDFS[2].exists(new Path("/log_collector/foo/bar/newfile")));
  }

  @Test
  public void testFedRenameFeature() throws Exception {
    DistributedFileSystem dfs = (DistributedFileSystem)FS_HDFS[2];
    assertTrue(dfs.mkdirs(new Path("/log_collector/foo/A/A1/A2/A3/A4/A5")));
    assertTrue(dfs.mkdirs(new Path("/log_collector/foo/A/A")));
    long renameId = dfs.renameSrcPhase1("/log_collector/foo/A/A1/A2/A3", "srcid",
        "/log_collector/foo", "dstId").getRenameId();
    // test parent
    try {
      dfs.delete(new Path("/log_collector/foo/A"));
      assert false;
    } catch (IOException e) {
      assertExceptionContains("federation rename", e);
    }
    // test child
    try {
      dfs.delete(new Path("/log_collector/foo/A/A1/A2/A3/A4"));
      assert false;
    } catch (IOException e) {
      assertExceptionContains("federation rename", e);
    }
    // test unrelated path
    assertTrue(dfs.delete(new Path("/log_collector/foo/A/A")));
    // remove FederationRenameFeature and delete
    dfs.renameSrcPhase2(renameId, true);
    assertTrue(dfs.delete(new Path("/log_collector/foo/A/A1/A2/A3/A4")));
    assertTrue(dfs.delete(new Path("/log_collector/foo/A")));
  }

  @Test
  public void testRenameOnStandbyNN() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(new Configuration(conf))
              .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(2))
              .numDataNodes(3).format(true).build();
      cluster.waitClusterUp();
      cluster.transitionToActive(0);
      cluster.transitionToActive(2);
      DistributedFileSystem standbyNameNodeFs = cluster.getFileSystem(1);;

      try {
        standbyNameNodeFs.renameSrcPhase1("/log_collector/foo/chen/newfile",
            standbyNameNodeFs.getUri().toString(),
            "/log_collector/foo/chen/newfile",
            cluster.getFileSystem(2).getUri().toString());
      } catch (Exception e) {
        assertTrue(e instanceof RemoteException);
        Exception unwrapped = ((RemoteException) e).unwrapRemoteException(
                StandbyException.class);
        assertTrue(unwrapped instanceof StandbyException);
      }
    } finally {
      if (cluster!=null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testViewFileSystemCache() throws Exception {
    Configuration configuration = new Configuration(conf);
    configuration.set("fs.hdfs.impl.disable.cache", "false");
    ViewFileSystem viewFs =
        (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, configuration);
    FileSystem[] cachedFs = viewFs.getCachedFileSystems();
    // test ViewFileSystem.childs not in FileSystem.cache
    for (FileSystem child : cachedFs) {
      FileSystem tFs = FileSystem.get(child.getUri(), CONF);
      assertTrue(tFs != child);
    }
    // test independent cache for each ViewFileSystem object
    ViewFileSystem viewFs2 =
        (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, configuration);
    FileSystem[] cachedFs2 = viewFs2.getCachedFileSystems();
    assertEquals(cachedFs.length, cachedFs2.length);
    for (int i = 0; i < cachedFs.length; i++) {
      for (int j = 0; j < cachedFs2.length; j++) {
        assertTrue("Unexpected " + i + "==" + j,
            cachedFs[i] != cachedFs2[j]);
      }
    }
    // test renewFsState
    cachedFs = viewFs.getCachedFileSystems();
    viewFs.renewFsState(conf, FsConstants.VIEWFS_URI.getAuthority());
    cachedFs2 = viewFs.getCachedFileSystems();
    assertEquals(cachedFs.length, cachedFs2.length);
    for (int i = 0; i < cachedFs.length; i++) {
      try {
        cachedFs[i].exists(new Path("/"));
      } catch (IOException e) {
        assertTrue("Filesystem shouldn't be closed", false);
      }
      int j = 0;
      for (; j < cachedFs2.length; j++) {
        if (cachedFs[i] == cachedFs2[j]) break;
      }
      assert(j <cachedFs2.length);
    }
    // test close
    viewFs.close();
    viewFs.renewFsState(conf, FsConstants.VIEWFS_URI.getAuthority());
    try {
      viewFs.exists(new Path("/log_collector/foo/bar"));
    } catch (IOException e) {
      assertExceptionContains("Filesystem closed", e);
    }
    FileSystem[] childs = viewFs.getChildFileSystems();
    for (int i = 0; i < childs.length; i++) {
      try {
        childs[i].exists(new Path("/"));
        assert false;
      } catch (IOException e) {
        assertExceptionContains("Filesystem closed", e);
      }
    }
  }
}
