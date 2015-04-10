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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRaidShell {

  private static Configuration conf;
  private static RaidShell shell;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem dfs;
  private static RaidNode rd;

  @BeforeClass
  public static void setUpClass() throws IOException {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    dfsCluster.waitActive();
    dfs = dfsCluster.getFileSystem();
    if (!(dfs instanceof DistributedFileSystem)) {
      throw new IOException("Non-distributed filesystem not supported");
    }
    dfs.getConf().set(HdfsRaidConfigKeys.HDFS_RAIDNODE_IPC_ADDRESS_KEY, "127.0.0.1:12345");
    rd = new RaidNode(dfs.getConf());
    rd.start();
    shell = new RaidShell(dfs.getConf());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    rd.stop();
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Test
  public void testGetPolicyInfos() throws Exception {
    Policy policy = new Policy(conf);
    rd.setPolicy(policy);
    Policy clPolicy = shell.getPolicy();
    Assert.assertTrue(policy.equals(clPolicy));
    policy.addNewPolicy("/test1", 36000);
    clPolicy = shell.getPolicy();
    clPolicy.showPolicy();
    Assert.assertTrue(policy.equals(clPolicy));
    policy.addNewPolicy("/test2", 72000);
    clPolicy = shell.getPolicy();
    clPolicy.showPolicy();
    Assert.assertTrue(policy.equals(clPolicy));
  }
}
