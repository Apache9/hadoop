/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.HdfsRaidConfigKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.BlockPlacementPolicyRaid;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockPlacementPolicyRaid {

  private final static int numRacks = 4;
  private final static int nodesPerRack = 5;
  private final static int blockSize = 1024;
  private final static int chooseTimes = 10000;
  private final static String policyStr = "/tobers:3600 /tmp:7200";
  private final static String file = "/tobers/test";
  private final static int replica = 3;

  private static DatanodeStorageInfo[] storages;
  private static DatanodeDescriptor[] dataNodes;
  private static Configuration conf;
  private static NameNode namenode;
  private static BlockPlacementPolicy placementPolicy;
  private static NetworkTopology cluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    String[] racks = new String[numRacks];
    for (int i = 0; i < numRacks; i++) {
      racks[i] = "/rack" + i;
    }

    String[] owerRackOfNodes = new String[numRacks * nodesPerRack];
    for (int i = 0; i < nodesPerRack; i++) {
      for (int j = 0; j < numRacks; j++) {
        owerRackOfNodes[i * numRacks + j] = racks[j];
      }
    }

    storages = DFSTestUtil.createDatanodeStorageInfos(owerRackOfNodes);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestBlockPlacementPolicyRaid.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, new File(baseDir, "name").getPath());
    conf.set(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_POLICY_KEY, policyStr);
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
      BlockPlacementPolicyRaid.class.getName());

    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    placementPolicy = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    for (int i = 0; i < nodesPerRack * numRacks; i++) {
      cluster.add(dataNodes[i]);
    }

    setupDataNodeCapacity();
  }

  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn, long capacity, long dfsUsed,
      long remaining, long blockPoolUsed, long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(BlockManagerTestUtil.getStorageReportsForDatanode(dn), dnCacheCapacity,
      dnCacheUsed, xceiverCount, volFailures);
  }

  private static void setupDataNodeCapacity() {
    for (int i = 0; i < nodesPerRack * numRacks; i++) {
      updateHeartbeatWithUsage(dataNodes[i], 2 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        0L, 2 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * blockSize, 0L, 0L, 0L, 0, 0);
    }
  }

  /*
   * To verify that the BlockPlacementPolicy can be replaced by BlockPlacementPolicyRaid via
   * changing the configuration.
   */
  @Test
  public void testPolicyReplacement() {
    Assert.assertTrue((placementPolicy instanceof BlockPlacementPolicyRaid));
  }

  /*
   * Call choose target many times and verify that the gap between the most chosen DN and the fewest
   * chosen DN should be not too large (for e.g. half of total chosen number). Also verify that the
   * total number should be correct.
   */
  @Test
  public void testChooseTarget() {
    Map<DatanodeStorageInfo, Integer> chosenHist = new HashMap<DatanodeStorageInfo, Integer>();
    for (DatanodeStorageInfo ds : storages) {
      chosenHist.put(ds, 0);
    }

    for (int i = 0; i < chooseTimes; i++) {
      DatanodeStorageInfo[] targets = namenode
          .getNamesystem()
          .getBlockManager()
          .getBlockPlacementPolicy()
          .chooseTarget(file, replica, null, new ArrayList<DatanodeStorageInfo>(), false, null,
            blockSize, StorageType.DEFAULT);

      Assert.assertTrue(targets.length == replica);
      for (int j = 0; j < replica; j++) {
        Assert.assertTrue(chosenHist.containsKey(targets[j]));
        int hit = chosenHist.get(targets[j]) + 1;
        chosenHist.put(targets[j], hit);
      }
    }

    int total = 0, min = Integer.MAX_VALUE, max = 0;
    for (Map.Entry<DatanodeStorageInfo, Integer> entry : chosenHist.entrySet()) {
      total += entry.getValue();
      if (entry.getValue() < min) {
        min = entry.getValue();
      }
      if (entry.getValue() > max) {
        max = entry.getValue();
      }
    }
    Assert.assertTrue(total == replica * chooseTimes);
    Assert.assertTrue(min <= max);
    Assert.assertTrue((max - min) < chooseTimes);
  }

  @AfterClass
  public static void teardownCluster() {
    if (namenode != null) {
      namenode.stop();
    }
  }
}