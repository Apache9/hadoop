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

package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.getRemoteUser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

public class TestFSNamesystem {

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * Tests that the namenode edits dirs are gotten with duplicates removed
   */
  @Test
  public void testUniqueEditDirs() throws IOException {
    Configuration config = new Configuration();

    config.set(DFS_NAMENODE_EDITS_DIR_KEY, "file://edits/dir, "
        + "file://edits/dir1,file://edits/dir1"); // overlapping internally

    // getNamespaceEditsDirs removes duplicates
    Collection<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(config);
    assertEquals(2, editsDirs.size());
  }

  /**
   * Test that FSNamesystem#clear clears all leases.
   */
  @Test
  public void testFSNamespaceClearLeases() throws Exception {
    Configuration conf = new HdfsConfiguration();
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());

    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);
    LeaseManager leaseMan = fsn.getLeaseManager();
    leaseMan.addLease("client1", "importantFile");
    assertEquals(1, leaseMan.countLease());
    fsn.clear();
    leaseMan = fsn.getLeaseManager();
    assertEquals(0, leaseMan.countLease());
  }

  @Test
  /**
   * Test that isInStartupSafemode returns true only during startup safemode
   * and not also during low-resource safemode
   */
  public void testStartupSafemode() throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = Mockito.mock(FSImage.class);
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    FSNamesystem fsn = new FSNamesystem(conf, fsImage);

    fsn.leaveSafeMode();
    assertTrue("After leaving safemode FSNamesystem.isInStartupSafeMode still "
      + "returned true", !fsn.isInStartupSafeMode());
    assertTrue("After leaving safemode FSNamesystem.isInSafeMode still returned"
      + " true", !fsn.isInSafeMode());

    fsn.enterSafeMode(true);
    assertTrue("After entering safemode due to low resources FSNamesystem."
      + "isInStartupSafeMode still returned true", !fsn.isInStartupSafeMode());
    assertTrue("After entering safemode due to low resources FSNamesystem."
      + "isInSafeMode still returned false",  fsn.isInSafeMode());
  }

  @Test
  public void testReplQueuesActiveAfterStartupSafemode() throws IOException, InterruptedException{
    Configuration conf = new Configuration();

    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);

    FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
    FSNamesystem fsn = Mockito.spy(fsNamesystem);

    //Make shouldPopulaeReplQueues return true
    HAContext haContext = Mockito.mock(HAContext.class);
    HAState haState = Mockito.mock(HAState.class);
    Mockito.when(haContext.getState()).thenReturn(haState);
    Mockito.when(haState.shouldPopulateReplQueues()).thenReturn(true);
    Whitebox.setInternalState(fsn, "haContext", haContext);

    //Make NameNode.getNameNodeMetrics() not return null
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);

    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues were being populated during very first "
        + "safemode", !fsn.isPopulatingReplQueues());
    fsn.leaveSafeMode();
    assertTrue("FSNamesystem didn't leave safemode", !fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated even after leaving "
      + "safemode", fsn.isPopulatingReplQueues());
    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated after entering "
      + "safemode 2nd time", fsn.isPopulatingReplQueues());
  }
  
  @Test
  public void testFsLockFairness() throws IOException, InterruptedException{
    Configuration conf = new Configuration();

    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);

    conf.setBoolean("dfs.namenode.fslock.fair", true);
    FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
    assertTrue(fsNamesystem.getFsLockForTests().isFair());
    
    conf.setBoolean("dfs.namenode.fslock.fair", false);
    fsNamesystem = new FSNamesystem(conf, fsImage);
    assertFalse(fsNamesystem.getFsLockForTests().isFair());
  }  
  
  @Test
  public void testFSNamesystemLockCompatibility() {
    FSNamesystemLock rwLock = new FSNamesystemLock(true);

    assertEquals(0, rwLock.getReadHoldCount());
    rwLock.readLock().lock();
    assertEquals(1, rwLock.getReadHoldCount());

    rwLock.readLock().lock();
    assertEquals(2, rwLock.getReadHoldCount());

    rwLock.readLock().unlock();
    assertEquals(1, rwLock.getReadHoldCount());

    rwLock.readLock().unlock();
    assertEquals(0, rwLock.getReadHoldCount());

    assertFalse(rwLock.isWriteLockedByCurrentThread());
    assertEquals(0, rwLock.getWriteHoldCount());
    rwLock.writeLock().lock();
    assertTrue(rwLock.isWriteLockedByCurrentThread());
    assertEquals(1, rwLock.getWriteHoldCount());
    
    rwLock.writeLock().lock();
    assertTrue(rwLock.isWriteLockedByCurrentThread());
    assertEquals(2, rwLock.getWriteHoldCount());

    rwLock.writeLock().unlock();
    assertTrue(rwLock.isWriteLockedByCurrentThread());
    assertEquals(1, rwLock.getWriteHoldCount());

    rwLock.writeLock().unlock();
    assertFalse(rwLock.isWriteLockedByCurrentThread());
    assertEquals(0, rwLock.getWriteHoldCount());
  }

  @Test
  public void testReset() throws Exception {
    Configuration conf = new Configuration();
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    FSNamesystem fsn = new FSNamesystem(conf, fsImage);
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
    fsn.clear();
    assertFalse(fsn.isImageLoaded());
    final INodeDirectory root = (INodeDirectory) fsn.getFSDirectory()
            .getINode("/");
    assertTrue(root.getChildrenList(Snapshot.CURRENT_STATE_ID).isEmpty());
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
  }

  @Test
  public void testDeleteWithExistingFileTrash() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      fsn.forceToTrash = true;

      String src = "/user/" + getRemoteUser().getUserName() + "/b";
      String src2 = "/user/" + getRemoteUser().getUserName() + "/b/a";
      DistributedFileSystem hdfs = cluster.getFileSystem();
      hdfs.create(new Path(src));
      fsn.delete(src, false);
      assertTrue(hdfs.getClient()
          .exists("/user/" + getRemoteUser().getShortUserName()
              + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
              + "/b"));
      HdfsFileStatus hdfsFileStatus = hdfs.getClient()
          .getFileInfo("/user/" + getRemoteUser().getShortUserName()
              + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
              + "/b");
      assertTrue(!hdfsFileStatus.isDir());

      hdfs.create(new Path(src2));
      fsn.delete(src2, false);
      assertTrue(!hdfs.getClient()
          .exists("/user/" + getRemoteUser().getShortUserName()
              + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
              + "/b/a"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
