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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestReplicationPolicyOverscheduled {
  {
    ((Log4JLogger) BlockPlacementPolicy.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 6;
  private static NetworkTopology cluster;
  private static NameNode namenode;
  private static BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static DatanodeDescriptor dataNodes[];
  private static DatanodeStorageInfo[] storages;
  private static final int overScheduledThreshold = 30;


  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    for (DatanodeStorageInfo storageInfo : dn.getStorageInfos()) {
      storageInfo.setUtilizationForTesting(capacity, dfsUsed, remaining,
          blockPoolUsed);
    }
    dn.updateHeartbeat(BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/d1/r1",
        "/d1/r1",
        "/d1/r2",
        "/d1/r2",
        "/d2/r3",
        "/d2/r3"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks, 3);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());

    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENT_MAX_SCHEDULED,
        overScheduledThreshold);
    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().addDatanode(
          dataNodes[i]);
    }
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }

  private void setBloscksScheduled(DatanodeDescriptor dn, int numScheduled) {
    while (dn.getBlocksScheduled() != numScheduled) {
      if (dn.getBlocksScheduled() > numScheduled) {
        dn.decrementBlocksScheduled();
      } else {
        dn.incrementBlocksScheduled();
      }
    }
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas) {
    return chooseTarget(numOfReplicas, dataNodes[0]);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer) {
    return chooseTarget(numOfReplicas, writer,
        new ArrayList<DatanodeStorageInfo>());
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes,
      Set<Node> excludedNodes) {
    return replicator.chooseTarget(filename, numOfReplicas, writer, chosenNodes,
        false, excludedNodes, BLOCK_SIZE, StorageType.DEFAULT);
  }

  @Test
  public void testChooseTargetLocalNodeOverScheduled() throws Exception {
    // 10 blocks per disk, 3 disk per node
    // oversheculed set as 10 will not affect chooseTarget
    setBloscksScheduled(dataNodes[0], 10);
    DatanodeStorageInfo[] targets = chooseTarget(1);
    assertEquals(1, targets.length);
    assertTrue(dataNodes[0] == targets[0].getDatanodeDescriptor());

    setBloscksScheduled(dataNodes[0], 30);
    targets = chooseTarget(1);
    assertEquals(1, targets.length);
    assertTrue(dataNodes[0] != targets[0].getDatanodeDescriptor());
  }

  @Test
  public void testChoosTagetFailDueToOverScheduled() throws Exception {
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      setBloscksScheduled(dataNodes[i], 2 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * 3);
    }
    DatanodeStorageInfo[] targets = chooseTarget(3);
    assertEquals(0, targets.length);
  }

  @Test
  public void testChooseTargetSuccessWithMaxScheduledLimit() throws Exception {
    long capacity = 4 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    long dfsUsed = 5 * BLOCK_SIZE;

    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      setBloscksScheduled(dataNodes[i], 4 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * 3);
    }
    // max.scheduled set to 30, each disk have 15 blocks remaining, 3 disk available
    // so the allocation should success
    updateHeartbeatWithUsage(dataNodes[0], capacity, dfsUsed, capacity
        - dfsUsed, dfsUsed, 0, 0, 0, 0);
    DatanodeStorageInfo[] targets = chooseTarget(1);
    assertEquals(1, targets.length);
    assertEquals(dataNodes[0], targets[0].getDatanodeDescriptor());
  }
}