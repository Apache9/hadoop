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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestReplicationPolicyOverused {
  {
    ((Log4JLogger)BlockPlacementPolicy.LOG).getLogger().setLevel(Level.ALL);
  }


  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 6;
  private static NetworkTopology cluster;
  private static NameNode namenode;
  private static BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static DatanodeDescriptor dataNodes[];
  private static DatanodeStorageInfo[] storages;
  private static final int overUsedPercentageThreshold = 60;
  private static final long overUsedFreespaceThreshold =
      HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE + 3 * BLOCK_SIZE;
  
  @Rule
  public ExpectedException exception = ExpectedException.none();
  
  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
    long capacity, long dfsUsed, long remaining, long blockPoolUsed,
    long dnCacheCapacity, long dnCacheUsed, int xceiverCount, int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
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
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
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
    conf.setInt(DFSConfigKeys.DFS_DATANODE_OVERUSED_PERCENTAGE_THRESHOLD,
        overUsedPercentageThreshold);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_OVERUSED_FREESPACE_THRESHOLD,
        overUsedFreespaceThreshold);
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
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
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

  private static DatanodeStorageInfo[] chooseTarget(
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeStorageInfo> chosenNodes,
      Set<Node> excludedNodes) {
    return replicator.chooseTarget(filename, numOfReplicas, writer, chosenNodes,
        false, excludedNodes, BLOCK_SIZE, StorageType.DEFAULT);
  }

  private boolean containsWithinRange(DatanodeStorageInfo target,
      DatanodeDescriptor[] nodes, int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < nodes.length;
    assert endIndex >= startIndex && endIndex < nodes.length;
    for (int i = startIndex; i <= endIndex; i++) {
      if (nodes[i].equals(target.getDatanodeDescriptor())) {
        return true;
      }
    }
    return false;
  }
  
  private boolean containsWithinRange(DatanodeDescriptor target,
      DatanodeStorageInfo[] nodes, int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < nodes.length;
    assert endIndex >= startIndex && endIndex < nodes.length;
    for (int i = startIndex; i <= endIndex; i++) {
      if (nodes[i].getDatanodeDescriptor().equals(target)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testChooseTargetWithOverusedNodes() throws Exception {

    // Condition 1 : choose self w/o overused
    DatanodeStorageInfo[] targets;
    // We set the datanode[0] as stale, thus should choose datanode[1] since
    // datanode[1] is on the same rack with datanode[0] (writer)
    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertEquals(storages[0], targets[0]);

    System.out.println("Test Condition1");
    // Condition 2 : Overused from percentage point of view but not from
    // freespace
    long capacity = 13 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    long dfsUsed = 9 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    updateHeartbeatWithUsage(dataNodes[0], capacity, dfsUsed, capacity
        - dfsUsed, dfsUsed, 0, 0, 0, 0);
    targets = chooseTarget(1);
    assertEquals(storages[1], targets[0]);
    
    System.out.println("Test Condition2");
    // Condition 3: Overused from freespace point of view but not percentage
    capacity = 3 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    dfsUsed = 8 * BLOCK_SIZE;
    updateHeartbeatWithUsage(dataNodes[0], capacity, dfsUsed, capacity
        - dfsUsed, dfsUsed, 0, 0, 0, 0);
    targets = chooseTarget(1);
    assertEquals(storages[1], targets[0]);

    System.out.println("Test Condition3");
    // Condition 3: Overused from freespace point of view but not percentage
    capacity = 4 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    dfsUsed = 14 * BLOCK_SIZE;
    updateHeartbeatWithUsage(dataNodes[0], capacity, dfsUsed, capacity
        - dfsUsed, dfsUsed, 0, 0, 0, 0);
    targets = chooseTarget(1);
    assertEquals(storages[1], targets[0]);

    // reset
    updateHeartbeatWithUsage(dataNodes[0], 2
        * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 2
        * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    dataNodes[0].setLastUpdate(Time.now());
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }


  /**
   * In this testcase, we set 3 nodes (dataNodes[0] ~ dataNodes[2]) as overused,
   * and when the number of replicas is less or equal to 3, all the healthy
   * datanodes should be returned by the chooseTarget method. When the number of
   * replicas is 4, a overused node should be included.
   * 
   * @throws Exception
   */
  @Test
  public void testChooseTargetWithHalfOverusedNodes() throws Exception {
    // Set dataNodes[0], dataNodes[1], and dataNodes[2] as overused
    long capacity = 4 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    long dfsUsed = 14 * BLOCK_SIZE;
    for (int i = 0; i < 3; i++) {
      updateHeartbeatWithUsage(dataNodes[i], capacity, dfsUsed, capacity
          - dfsUsed, dfsUsed, 0, 0, 0, 0);
    }
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();

    DatanodeStorageInfo[] targets = chooseTarget(0);
    assertEquals(targets.length, 0);

    // Since we have 6 datanodes total, stale nodes should
    // not be returned until we ask for more than 3 targets
    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertFalse(containsWithinRange(targets[0], dataNodes, 0, 2));

    targets = chooseTarget(2);
    assertEquals(targets.length, 2);
    assertFalse(containsWithinRange(targets[0], dataNodes, 0, 2));
    assertFalse(containsWithinRange(targets[1], dataNodes, 0, 2));

    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    assertTrue(containsWithinRange(targets[0], dataNodes, 3, 5));
    assertTrue(containsWithinRange(targets[1], dataNodes, 3, 5));
    assertTrue(containsWithinRange(targets[2], dataNodes, 3, 5));

    targets = chooseTarget(4);
    assertEquals(targets.length, 4);
    assertTrue(containsWithinRange(dataNodes[3], targets, 0, 3));
    assertTrue(containsWithinRange(dataNodes[4], targets, 0, 3));
    assertTrue(containsWithinRange(dataNodes[5], targets, 0, 3));

    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i], 2
          * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 2
          * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    for (int i = 0; i < dataNodes.length; i++) {
      dataNodes[i].setLastUpdate(Time.now());
    }
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }
}
