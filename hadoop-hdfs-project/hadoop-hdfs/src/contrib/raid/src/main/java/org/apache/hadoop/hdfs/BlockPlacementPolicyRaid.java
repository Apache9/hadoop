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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.BlockCodec;
import org.apache.hadoop.contrib.raid.HdfsRaidConfigKeys;
import org.apache.hadoop.contrib.raid.Policy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/*
 * The default policy to choose targets is to choose one target in the local rack and the other two
 * targets in another rack. The passed-in params of those plug-in hooks do not provide enough
 * information regarding locations of blocks in a raid group. It is hard to make stringent
 * allocation decision which can satisfy raid's requirement. To make it simple, we will try to
 * allocate blocks random and rely on RaidNode's Mover to fix mis-placed blocks.
 */
/*
 * For chooseReplicaToDelete, it's hard to get location informations of blocks in a raid group from
 * the pass-in params. Given that, donot override the super's method.
 */

@InterfaceAudience.Private
public class BlockPlacementPolicyRaid extends BlockPlacementPolicyDefault {

  private static final Log LOG = LogFactory.getLog(BlockPlacementPolicyRaid.class);
  private Policy policy;
  private int dataBlocksNum;
  private int codingBlocksNum;

  protected BlockPlacementPolicyRaid(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap) {
    super(conf, stats, clusterMap);
    initialize(conf, stats, clusterMap);
  }

  protected BlockPlacementPolicyRaid() {
  }

  @Override
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap) {
    super.initialize(conf, stats, clusterMap);
    try {
      policy = new Policy(conf);
      policy.loadPolicy(conf);
    } catch (IOException ioe) {
      // Ignore the exception. We will fail back to the super in all override methods.
      LOG.warn("Fail to initialize BlockPlacementPolicyRaid", ioe);
    }
    dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
    codingBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas, Node writer,
      List<DatanodeStorageInfo> chosen, boolean returnChosenNodes, Set<Node> excludedNodes,
      long blocksize, StorageType storageType) {
    // This is called from replication worker. If the existing replica is larger than 0, it is not a
    // replication for raid file.
    if (chosen.size() > 0) {
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosen, returnChosenNodes,
        excludedNodes, blocksize, storageType);
    }

    // If not raid related, go through to the super.
    boolean isCodingFile = BlockCodec.isCodingFile(srcPath);
    if (!policy.isRaidCandidate(srcPath) && !isCodingFile) {
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosen, returnChosenNodes,
        excludedNodes, blocksize, storageType);
    }

    // if block replication is not the default, we would not encode the file and pass through to the
    // super.
    if (numOfReplicas != DFSConfigKeys.DFS_REPLICATION_DEFAULT) {
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosen, returnChosenNodes,
        excludedNodes, blocksize, storageType);
    }

    // Choose for possible raid file - as random as possible
    int clusterSize = clusterMap.getNumOfLeaves();

    if (clusterSize == 0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }

    if (clusterSize < dataBlocksNum + codingBlocksNum) {
      // The raid should not encode files since there are too few nodes
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosen, returnChosenNodes,
        excludedNodes, blocksize, storageType);
    }

    int replicas = numOfReplicas + chosen.size();
    if (replicas > clusterSize) {
      numOfReplicas -= (replicas - clusterSize);
    }

    if (numOfReplicas == 0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }

    if (writer != null && !clusterMap.contains(writer)) {
      writer = null;
    }

    if (excludedNodes == null) {
      excludedNodes = new HashSet<Node>();
    }

    for (DatanodeStorageInfo storage : chosen) {
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }

    List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>(chosen);

    try {
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize, 1, results, true, true,
        storageType);
    } catch (NotEnoughReplicasException e1) {
      LOG.warn("Failed to place enough replicas, still need " + (numOfReplicas - results.size())
          + ", will try stale nodes.", e1);
      final Set<Node> oldExcludedNodes = new HashSet<Node>(excludedNodes);
      for (DatanodeStorageInfo resultStorage : results) {
        addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
      }
      try {
        chooseRandom(numOfReplicas, NodeBase.ROOT, oldExcludedNodes, blocksize, 1, results, false,
          false, storageType);
      } catch (NotEnoughReplicasException e2) {
        LOG.warn("Failed to place enough replicas even tried stale and over used nodes, still need "
            + (numOfReplicas - results.size()), e2);
      }
    }

    if (!returnChosenNodes) {
      results.removeAll(chosen);
    }

    if (results.size() == 0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    } else {
      return getPipeline((writer == null) ? results.get(0).getDatanodeDescriptor() : writer,
        results.toArray(new DatanodeStorageInfo[results.size()]));
    }
  }

  // This is copied from BlockPlacementPolicyDefault
  private DatanodeStorageInfo[] getPipeline(Node writer, DatanodeStorageInfo[] storages) {
    if (storages.length == 0) {
      return storages;
    }

    synchronized (clusterMap) {
      int index = 0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = storages[0].getDatanodeDescriptor();
      }
      for (; index < storages.length; index++) {
        DatanodeStorageInfo shortestStorage = storages[index];
        int shortestDistance = clusterMap.getDistance(writer,
          shortestStorage.getDatanodeDescriptor());
        int shortestIndex = index;
        for (int i = index + 1; i < storages.length; i++) {
          int currentDistance = clusterMap.getDistance(writer, storages[i].getDatanodeDescriptor());
          if (shortestDistance > currentDistance) {
            shortestDistance = currentDistance;
            shortestStorage = storages[i];
            shortestIndex = i;
          }
        }
        // switch position index & shortestIndex
        if (index != shortestIndex) {
          storages[shortestIndex] = storages[index];
          storages[index] = shortestStorage;
        }
        writer = shortestStorage.getDatanodeDescriptor();
      }
    }
    return storages;
  }
}
