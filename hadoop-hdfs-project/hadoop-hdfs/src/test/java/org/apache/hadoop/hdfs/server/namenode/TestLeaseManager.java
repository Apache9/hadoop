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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;


public class TestLeaseManager {
  final Configuration conf = new HdfsConfiguration();
  public static long maxLockHoldToReleaseLeaseMs = 100;

  @Test
  public void testRemoveLeaseWithPrefixPath() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();

    LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
    lm.addLease("holder1", "/a/b");
    lm.addLease("holder2", "/a/c");
    assertNotNull(lm.getLeaseByPath("/a/b"));
    assertNotNull(lm.getLeaseByPath("/a/c"));

    lm.removeLeaseWithPrefixPath("/a");

    assertNull(lm.getLeaseByPath("/a/b"));
    assertNull(lm.getLeaseByPath("/a/c"));

    lm.addLease("holder1", "/a/b");
    lm.addLease("holder2", "/a/c");

    lm.removeLeaseWithPrefixPath("/a/");

    assertNull(lm.getLeaseByPath("/a/b"));
    assertNull(lm.getLeaseByPath("/a/c"));
  }

  /** Check that LeaseManager.checkLease release some leases
   */
  @Test
  public void testCheckLease() throws IOException {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    long numLease = 100;

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(0, 0);

    for (long i = 0; i <= numLease - 1; i++) {
      //Add some leases to the LeaseManager
      lm.addLease("holder" + i, Long.toString(INodeId.ROOT_INODE_ID + i));
    }
    assertEquals(numLease, lm.countLease());

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
    assertEquals(numLease - 1, lm.countLease());
  }


  private static FSNamesystem makeMockFsNameSystem() throws IOException {
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getMaxLockHoldToReleaseLeaseMs()).thenReturn(maxLockHoldToReleaseLeaseMs);
    /* Sleep as long as twice the time the LeaseManager checks for the leases during one round
      and throw IOException to help th test
     */
    when(fsn.internalReleaseLease(any(Lease.class), anyString(), anyString())).thenAnswer(new Answer<String>() {
      @Override public String answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(2 * maxLockHoldToReleaseLeaseMs);
        throw new IOException("IO Exception for test");
      }
    });
    return fsn;
  }
}
