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
package org.apache.hadoop.contrib.raid;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRaidNode {

  private static Configuration conf, rdConf;
  private static RaidNode rd;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem dfs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    dfsCluster.waitActive();
    dfs = dfsCluster.getFileSystem();
    if (!(dfs instanceof DistributedFileSystem)) {
      throw new IOException("Non-distributed filesystem not supported");
    }

    dfs.getConf().set(HdfsRaidConfigKeys.HDFS_RAIDNODE_IPC_ADDRESS_KEY, "127.0.0.1:12345");
    rdConf = dfs.getConf();
    rd = new RaidNode(rdConf);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    rd.stop();
  }

  @Test
  public void testThreadsKickedNumber() throws Exception {
    final long encodeInterval = 2000;
    final long zombieSweeperInterval = 1000;
    final long fixerInterval = 1500;
    rdConf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL, encodeInterval);
    rdConf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL, zombieSweeperInterval);
    rdConf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL, fixerInterval);
    rd.start();
    Thread.sleep(8500);
    Assert.assertTrue(rd.getEncodeTaskDone() == (8500 / encodeInterval));
    Assert.assertTrue(rd.getZombieSweeperTaskDone() == (8500 / zombieSweeperInterval));
  }
}
