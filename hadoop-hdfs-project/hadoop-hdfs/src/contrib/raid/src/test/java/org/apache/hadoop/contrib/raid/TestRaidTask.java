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
import org.apache.hadoop.contrib.raid.RaidTask.CollectRaidInfoTask;
import org.apache.hadoop.contrib.raid.RaidTask.ZombieSweeperTask;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRaidTask {

  private static Configuration conf;
  private static MiniDFSCluster dfsCluster;
  private static MiniMRClientCluster mrCluster;
  private static FileSystem dfs;
  private static RaidNode rd;

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
    rd = new RaidNode(dfs.getConf());
    rd.start();

    mrCluster = MiniMRClientClusterFactory.create(TestRaidTask.class, 3, dfs.getConf());
    mrCluster.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    rd.stop();

    if (mrCluster != null) {
      mrCluster.stop();
    }

    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  private void setupFsEnv() throws IOException {
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        Path dir = new Path("/" + i + "/" + j);
        Path file = new Path("/" + i + "/" + j + "/f");
        dfs.mkdirs(dir);
        FSDataOutputStream out = dfs.create(file);
        out.write((dir.toString() + file.toString()).getBytes());
        out.close();
      }
    }
    dfs.mkdirs(new Path("/raid/collect"));
    dfs.mkdirs(new Path("/raid/coder"));
  }

  private Policy setupPolicy() {
    Policy policy = new Policy(null);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        String dir = "/" + i + "/" + j;
        policy.addNewPolicy(dir, 3600);
      }
    }
    return policy;
  }

  private void verifyResult() throws IOException {
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        Path file = new Path("/" + i + "/" + j + "/f");
        Path codeFile = BlockCodec.getCodingFile(file);
        FileStatus fileStatus = dfs.getFileStatus(file);
        Assert.assertTrue(dfs.exists(codeFile));
        FileStatus codeFileStatus = dfs.getFileStatus(codeFile);
        Assert.assertEquals(fileStatus.getReplication(), 1);
        Assert.assertEquals(codeFileStatus.getReplication(), 1);
      }
    }
  }

  @Test
  public void testCollectorAndCoder() throws Exception {
    // Set up fs env
    setupFsEnv();

    // Set up policy
    Policy policy = setupPolicy();

    // Get configuration
    Configuration jobConf = new Configuration(mrCluster.getConfig());
    jobConf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 2000);
    Thread.sleep(3000);
    // In case that MapReduce task out of memory during encoding
    jobConf.setInt(HdfsRaidConfigKeys.HDFS_RAID_CODEC_CODE_BUF_SIZE, 16 * 1024 * 1024);
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY, "/raid/collect");
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_KEY, "/raid/coder");

    CollectRaidInfoTask crti = new CollectRaidInfoTask(rd, policy, jobConf);
    rd.submitTask(crti);

    // Wait job done
    long jobDone = rd.getEncodeTaskDone();
    while (jobDone != 1) {
      Thread.sleep(1000);
      jobDone = rd.getEncodeTaskDone();
    }

    // Verify codings
    verifyResult();
  }

  // Use encoded files left by last test case.
  @Test
  public void testZombieSweeper() throws Exception {
    final Path file = new Path("/0/1/f");
    final Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(file));
    Assert.assertTrue(dfs.exists(codingFile));
    dfs.delete(file, false);
    Thread.sleep(1000);
    Assert.assertTrue(dfs.exists(codingFile));
    final Thread self = Thread.currentThread();

    final class TstZombieSweeperTask extends ZombieSweeperTask {
      public boolean success;

      private TstZombieSweeperTask(RaidNode rd, Path dir, Configuration conf) {
        super(rd, dir, conf);
        success = false;
      }

      @Override
      public void onSuccess(TaskResult result) {
        super.onSuccess(result);
        try {
          if (!dfs.exists(codingFile)) {
            success = true;
          }
        } catch (IOException ioe) {
          success = false;
        }
        Assert.assertTrue(success);
        self.interrupt();
      }

      @Override
      public void onFailure(Throwable t) {
        success = false;
        t.printStackTrace();
        self.interrupt();
        Assert.assertTrue(success);
      }
    }
    TstZombieSweeperTask zst = new TstZombieSweeperTask(rd, BlockCodec.getRaidRoot(), conf);
    rd.submitTask(zst);
    try {
      Thread.sleep(60000); // 60 seconds
    } catch (InterruptedException ie) {
      // Ignore
    }
    Assert.assertTrue(zst.success);
    Assert.assertTrue(zst.numOfCleanedZombie() == 1);
  }
}
