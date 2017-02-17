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

import com.google.protobuf.Internal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
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
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestViewFileSystemRootDefault extends ViewFileSystemBaseTest {
  private static FileSystem fsDefault;
  private static FileSystem fsRoot;
  private static MiniDFSCluster cluster;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final String TEST_TABLE_NAME = "tmt";
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final String[] FS_HDFS_URI = new String[NAME_SPACES_COUNT];
  private static final Configuration CONF = new Configuration();
  private static final File TEST_DIR = PathUtils.getTestDir(
      TestViewFileSystemRootDefault.class);
  private static final String TEST_TEMP_PATH =
      "/tmp/TestViewFileSystemRootDefault";

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
    fsRoot = FS_HDFS[2];
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
    super.setUp();
  }

  /**
   * Override this so that we don't set the targetTestRoot to any path under the
   * root of the FS, and so that we don't try to delete the test dir, but rather
   * only its contents.
   */
  @Override
  void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fsDefault.makeQualified(new Path("/"));
    for (FileStatus status : fsDefault.listStatus(targetTestRoot)) {
      fsDefault.delete(status.getPath(), true);
    }
    for (FileStatus status : fsRoot.listStatus(new Path("/"))) {
      fsRoot.delete(status.getPath(), true);
    }
  }

  @Override
  void setupMountPoints() {
    super.setupMountPoints();
    try {
      ConfigUtil.addRootDefault(conf, new URI(FS_HDFS_URI[2]));
    } catch (URISyntaxException e) {
      // won't happen
    }
  }

  @Override
  int getExpectedDelegationTokenCount() {
    return 2;
  }

  @Override
  int getExpectedDelegationTokenCountWithCredentials() {
    return 2;
  }

  @Override
  int getExpectedMountPoints() {
    return 9; // Base test setup 8 mount points, plus a rootDefault in this test
  }


  /**
   * Basic tests for new config items of rootDefault
   */

  @Test
  public void testConfRootDefault() throws Exception {
    TEST_DIR.mkdirs();
    String clusterName = "ClusterX";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    String testFileName = "testRootDefault";

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
    ConfigUtil.addRootDefault(conf, clusterName, TEST_DIR.toURI());

    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
    FileStatus stat = vfs.getFileStatus(new Path(viewFsUri.toString() +
        testFileName));

    System.out.println(stat);
    vfs.close();
  }

  @Test
  public void testConfRootDefaultWithMountPoint() throws Exception {
    TEST_DIR.mkdirs();
    Configuration conf = new Configuration();
    String clusterName = "ClusterX";
    String mountPoint = "/user";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    String expectedErrorMsg =  "Invalid rootDefault entry in config: " +
        "root.default./user";
    String mountTableEntry = Constants.CONFIG_VIEWFS_PREFIX + "."
        + clusterName + "." + Constants.CONFIG_VIEWFS_ROOT_DEFAULT
        + "." + mountPoint;
    conf.set(mountTableEntry, TEST_DIR.toURI().toString());

    try {
      FileSystem.get(viewFsUri, conf);
      fail("Shouldn't allow linkMergeSlash to take extra mount points!");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(expectedErrorMsg));
    }
  }

  /**
   * Test path resolve
   */

  @Test
  public void testRootDefaultResolve() throws Exception {
    fsView.mkdirs(new Path("/internalDir/dirOnRootDefault"));
    fsView.mkdirs(new Path("/internalDir/linkToDir2/foo"));
    assertTrue(fsDefault.exists(new Path("/dir2/foo")));
    assertTrue(fsRoot.exists(new Path("/internalDir/dirOnRootDefault")));
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
    res = vfs.getTargetFileSystem(new Path("/user2"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""),
        fsDefaultUri + "/user");
    res = vfs.getTargetFileSystem(new Path("/data"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""),
        fsDefaultUri + "/data");
    // resolve root
    res = vfs.getTargetFileSystem(new Path("/"));
    assertTrue(res instanceof ViewFileSystem.InternalDirOfViewFs);
  }

  @Test
  public void testResolvePathsNotInMountTable() throws Exception {
    ViewFileSystem vfs = (ViewFileSystem) fsView;
    String fsRootUri = fsRoot.getUri().toString().replaceAll("/$", "");
    String fsDefaultUri = fsDefault.getUri().toString().replaceAll("/$", "");
    // should resolve to rootDefault
    FileSystem res = vfs.getTargetFileSystem(new Path("/home/"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""), fsRootUri);
    res = vfs.getTargetFileSystem(new Path("/internalDir/notInMountTable"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""), fsRootUri);

    // should resolve to /user
    res = vfs.getTargetFileSystem(new Path("/user/notInMountTable"));
    assertTrue(res instanceof ChRootedFileSystem);
    assertEquals(res.getUri().toString().replaceAll("/$", ""),
        fsDefaultUri + "/user");
  }
  
  @Test
  public void testResolvePathsWhenRootDefaultNotConfigured() throws Exception {
    String confKey = Constants.CONFIG_VIEWFS_PREFIX + "."
        + Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE + "."
        + Constants.CONFIG_VIEWFS_ROOT_DEFAULT;
    conf.unset(confKey);
    ViewFileSystem vfs =
        (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, conf);
    try {
      vfs.getTargetFileSystem(new Path("/home"));
      fail("rootDefault not configured ,resolve should fail");
    } catch (FileNotFoundException e) {
    }

    FileSystem res = vfs.getTargetFileSystem(new Path("/internalDir"));
    assertTrue(res instanceof ViewFileSystem.InternalDirOfViewFs);
  }

  /**
   * Test basic operations involve rootDefault FS
   */

  @Test
  public void testBasicWriteOperation() throws Exception {
    FileSystemTestHelper.createFile(fsView, new Path("/internalDir/afile"));
    fsView.mkdirs(new Path("/internalDir/adir"));
    assertTrue(fsRoot.exists(new Path("/internalDir/afile")));
    assertTrue(fsRoot.exists(new Path("/internalDir/adir")));
    fsView.delete(new Path("/internalDir/adir"), false);
    fsView.delete(new Path("/internalDir/afile"), false);
    assertFalse(fsRoot.exists(new Path("/internalDir/afile")));
    assertFalse(fsRoot.exists(new Path("/internalDir/adir")));
  }

  /**
   * Test ListStatus
   */

  @Test
  public void testListStatus() throws Exception {
    fsView.mkdirs(new Path("/internalDir/adir"));
    fsView.mkdirs(new Path("/internalDir/zdir"));
    FileStatus[] res = fsView.listStatus(new Path("/internalDir"));
    assertEquals(res.length, 4);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/internalDir/adir"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/internalDir/internalDir2"));
    assertEquals(res[2].getPath(), new Path("viewfs://", "/internalDir/linkToDir2"));
    assertEquals(res[3].getPath(), new Path("viewfs://", "/internalDir/zdir"));
  }

  @Test
  public void testListStatusRoot() throws Exception {
    fsView.mkdirs(new Path("/home"));
    FileSystemTestHelper.createFile(fsView, new Path("/zfile"));
    FileStatus[] res = fsView.listStatus(new Path("/"));
    assertEquals(res.length, 9);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/danglingLink"));
    assertEquals(res[2].getPath(), new Path("viewfs://", "/home"));
    assertEquals(res[8].getPath(), new Path("viewfs://", "/zfile"));
  }

  @Test
  public void testListStatusOnlyResolveOnRootDefault() throws Exception {
    fsView.mkdirs(new Path("/home/work"));
    FileSystemTestHelper.createFile(fsView, new Path("/home/file"));
    FileStatus[] res = fsView.listStatus(new Path("/home/"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/home/file"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/home/work"));
  }

  @Test
  public void testListStatusOnlyResolveOnMountTable() throws Exception {
    fsView.mkdirs(new Path("/home/work"));
    FileStatus[] res = fsView.listStatus(new Path("/internalDir"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/internalDir/internalDir2"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/internalDir/linkToDir2"));
  }

  @Test
  public void testListStatusWithScheme() throws Exception {
    FileStatus[] res = fsView.listStatus(new Path("viewfs:/internalDir"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/internalDir/internalDir2"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/internalDir/linkToDir2"));
  }

  @Test
  public void testListStatusOnlyRootDefault() throws Exception {
    Configuration config = new Configuration();
    ConfigUtil.addRootDefault(config, new URI(FS_HDFS_URI[2]));
    ViewFileSystem vfs =
        (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, config);
    vfs.mkdirs(new Path("/home/work"));
    FileSystemTestHelper.createFile(vfs, new Path("/user"));
    FileStatus[] res = vfs.listStatus(new Path("/"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/home/"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/user"));
  }

  @Test
  public void testListStatusNoRootDefault() throws Exception {
    String confKey = Constants.CONFIG_VIEWFS_PREFIX + "."
            + Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE + "."
            + Constants.CONFIG_VIEWFS_ROOT_DEFAULT;
    conf.unset(confKey);
    ViewFileSystem vfs =
            (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, conf);
    FileStatus[] res = vfs.listStatus(new Path("/internalDir"));
    assertEquals(res.length, 2);
    assertEquals(res[0].getPath(), new Path("viewfs://", "/internalDir/internalDir2"));
    assertEquals(res[1].getPath(), new Path("viewfs://", "/internalDir/linkToDir2"));
  }

  /**
   * Test getContentSummary
   */

  @Test
  public void testGetContentSummaryOnInternalNode() throws Exception {
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, "/home/work/hadoop",
        new Path(targetTestRoot, "home/work/hadoop").toUri());
    ConfigUtil.addLink(config, "/home/work/zookeeper",
            new Path(targetTestRoot, "home/work/zookeeper").toUri());
    ConfigUtil.addLink(config, "/home/play",
            new Path(targetTestRoot, "home/play").toUri());
    ViewFileSystem vfs =
            (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, config);
    ContentSummary summary = vfs.getContentSummary(new Path("/home"));
    assertEquals(summary.getFileCount(), 1);
    assertEquals(summary.getDirectoryCount(), 2);
    summary = vfs.getContentSummary(new Path("/"));
    assertEquals(summary.getFileCount(), 6);
    assertEquals(summary.getDirectoryCount(), 3);
  }

  @Test
  public void testGetContentSummaryOnRootDefault() throws Exception {
    FileSystemTestHelper.createFile(fsView, new Path("/home/testfile"));
    FileSystemTestHelper.createFile(fsView, new Path("/home/work/testfile"));
    ContentSummary summary = fsView.getContentSummary(new Path("/home"));
    assertEquals(summary.getFileCount(), 2);
    assertEquals(summary.getDirectoryCount(), 2);
    assertTrue(summary.getLength() > 0);
  }

  @Test
  public void testGetContentSummary() throws Exception {
    FileSystemTestHelper.createFile(fsView, new Path("/user/foo/barfile"));
    ContentSummary summary = fsView.getContentSummary(new Path("/user"));
    assertEquals(summary.getFileCount(), 1);
    assertEquals(summary.getDirectoryCount(), 2);
    assertTrue(summary.getLength() > 0);
  }

  /**
   * test rename from/to rootDefault
   */
  @Test
  public void testRenameWithRootDefault() throws Exception {
    FileSystemTestHelper.createFile(fsView, new Path("/user/foo/barfile"));
    fsView.mkdirs(new Path("/tmp"));
    fsView.rename(new Path("/user/foo"), new Path("/tmp/foo"));
    assertTrue(fsRoot.exists(new Path("/tmp/foo/barfile")));

    fsView.rename(new Path("/tmp/foo"), new Path("/internalDir/linkToDir2/foo"));
    assertFalse(fsRoot.exists(new Path("/tmp/foo")));
    assertTrue(fsDefault.exists(new Path("/dir2/foo/barfile")));
  }

  /**
   * Following are base tests of super class, add rootDefault will affect them,
   * need to override these cases
   */

  @Override
  @Test
  public void testInternalDeleteNonExisting2() throws IOException {
    assertFalse(fsView.delete(new Path("/internalDir/NonExisting"), false));
  }

  @Override
  @Test
  public void testInternalMkdirNew2() throws IOException {
    assertTrue(fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/internalDir/dirNew")));
  }

  @Override
  @Test
  public void testInternalCreate1() throws IOException {
    assertTrue(fileSystemTestHelper.createFile(fsView, "/foo") >= 0); // 1
                                                                      // component
  }

  @Override
  @Test
  public void testInternalCreate2() throws IOException { // 2 component
    assertTrue(
        fileSystemTestHelper.createFile(fsView, "/internalDir/foo") >= 0);
  }

  @Override
  @Test
  public void testInternalCreateMissingDir() throws IOException {
    assertTrue(fileSystemTestHelper.createFile(fsView, "/missingDir/foo") >= 0);
  }

  @Override
  @Test
  public void testInternalCreateMissingDir2() throws IOException {
    assertTrue(
        fileSystemTestHelper.createFile(fsView, "/missingDir/miss2/foo") >= 0);
  }

  @Override
  @Test
  public void testInternalCreateMissingDir3() throws IOException {
    assertTrue(
        fileSystemTestHelper.createFile(fsView, "/internalDir/miss2/foo") >= 0);
  }

  @Override
  @Test
  public void testInternalDeleteNonExisting() throws IOException {
    assertFalse(fsView.delete(new Path("/NonExisting"), false));
  }

  @Override
  @Test
  public void testInternalMkdirNew() throws IOException {
    assertTrue(fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/dirNew")));
  }
}
