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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.RaidTask.CollectRaidInfoTask;
import org.apache.hadoop.contrib.raid.RaidTask.FixerTask;
import org.apache.hadoop.contrib.raid.RaidTask.RaidTaskUtils;
import org.apache.hadoop.contrib.raid.RaidTask.ZombieSweeperTask;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
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
  private static DFSClient dfsClient;
  private static FileSystem dfs;
  private static RaidNode rd;
  final private static String collectResult = "/raid/collect";
  final private static String coderResult = "/raid/coder";
  final private static String fixerResult = "/raid/fixer";

  // The interval to mark DN dead is : 2*hbRecheckInterval+10*1000*heartBeatInterval. Pls refer
  // DatanodeManager
  final private static long heartBeatInterval = 1l;
  final private static int hbRecheckInterval = 3000;
  final private static long deadShowup = 2 * hbRecheckInterval + 10 * 1000 * heartBeatInterval;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    // To make DN dead detection quickly
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, heartBeatInterval);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, hbRecheckInterval);

    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    dfsCluster.waitActive();
    dfs = dfsCluster.getFileSystem();
    if (!(dfs instanceof DistributedFileSystem)) {
      throw new IOException("Non-distributed filesystem not supported");
    }

    dfsClient = ((DistributedFileSystem) dfs).getClient();

    dfs.getConf().set(HdfsRaidConfigKeys.HDFS_RAIDNODE_IPC_ADDRESS_KEY, "127.0.0.1:12345");
    // To prevent fixer from being scheduled by RaidNode automatically
    dfs.getConf().setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL, 24 * 3600 * 1000);
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
    dfs.mkdirs(new Path(collectResult));
    dfs.mkdirs(new Path(coderResult));
    dfs.mkdirs(new Path(fixerResult));
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
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY, collectResult);
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_KEY, coderResult);

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

  // Use encoded files left by the encode test case.
  @Test
  public void testZombieSweeper() throws Exception {
    final Path file = new Path("/0/1/f");
    final Path codingFile = BlockCodec.getCodingFile(file);
    final Thread self = Thread.currentThread();
    Assert.assertTrue(dfs.exists(file));
    Assert.assertTrue(dfs.exists(codingFile));
    dfs.delete(file, false);
    Thread.sleep(1000);
    Assert.assertTrue(dfs.exists(codingFile));

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

  // Use encoded files left by the encode test case.
  @Test
  public void testFixerTaskWithCorruptBlocks() throws Exception {
    final Path file = new Path("/0/2/f");
    final Thread self = Thread.currentThread();
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(), 0);
    LocatedBlock[] lblks = new LocatedBlock[blocks.getLocatedBlocks().size()];
    blocks.getLocatedBlocks().toArray(lblks);

    // Mark this file's block as corrupted
    dfsClient.reportBadBlocks(lblks);
    blocks = dfsClient.getLocatedBlocks(file.toString(), 0);
    lblks = new LocatedBlock[blocks.getLocatedBlocks().size()];
    blocks.getLocatedBlocks().toArray(lblks);
    for (LocatedBlock blk : lblks) {
      Assert.assertTrue(blk.isCorrupt());
    }

    final class TstFixerTask extends FixerTask {
      public boolean success;

      private TstFixerTask(RaidNode rd, Configuration conf) throws IOException {
        super(rd, conf);
        success = false;
      }

      @Override
      public void onSuccess(TaskResult result) {
        super.onSuccess(result);
        success = true;
        // Sleep a while so that NN can get DN's reporting
        try {
          LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(), 0);

          LocatedBlock[] lblks = new LocatedBlock[blocks.getLocatedBlocks().size()];
          blocks.getLocatedBlocks().toArray(lblks);

          for (LocatedBlock blk : lblks) {
            Assert.assertFalse(blk.isCorrupt());
            if (blk.isCorrupt()) {
              success = false;
              break;
            }
          }
        } catch (Exception e) {
          // Simply mark task as fail
          success = false;
          e.printStackTrace();
        }
        self.interrupt();
      }

      @Override
      public void onFailure(Throwable t) {
        success = false;
        t.printStackTrace();
        self.interrupt();
      }
    }

    // Get configuration
    Configuration jobConf = new Configuration(mrCluster.getConfig());
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_RESULT_DIR_KEY, fixerResult);

    TstFixerTask ft = new TstFixerTask(rd, jobConf);
    rd.submitTask(ft);

    try {
      Thread.sleep(60000); // 60 seconds
    } catch (InterruptedException ie) {
      // Ignore
    }

    Assert.assertTrue(ft.success);
  }

  // Test that if some DNs are stopped we can get a corrupt list.
  // TBD: After block placement policy work is done, add test case to verify that we can use Fixer
  // to fix blocks which are hosted by the dead DN.
  @Test
  public void testGetCorruptList() throws Exception {
    Map<RaidTaskUtils.FixerItem, Set<Integer>> fixerInfo = Fixer.collectFixerInfo(dfs.getConf());
    Assert.assertTrue(fixerInfo.entrySet().size() == 0);
    int numDNs = dfsCluster.getDataNodes().size();

    for (int i = 0; i < numDNs - 1; i++) {
      dfsCluster.stopDataNode(i);
    }

    Thread.sleep(deadShowup + 6000);

    fixerInfo = Fixer.collectFixerInfo(dfs.getConf());
    // Dump fixerInfo and DFSck output to see if their outputs are consistent.
    /**
     * for (Map.Entry<RaidTaskUtils.FixerItem, Set<Integer>> entry : fixerInfo.entrySet()) {
     * StringBuilder sb = new StringBuilder();
     * sb.append(entry.getKey().getFile().toString()).append("\t"); for (int blk : entry.getValue())
     * { sb.append("\t").append(blk); } System.out.println(sb.toString()); }
     * System.out.println("\n\n Runing fsck : \n"); DFSck fsck = new DFSck(dfs.getConf());
     * fsck.run(new String[] { "-list-corruptfileblocks", "-blocks", "-locations" });
     */
    Assert.assertTrue(fixerInfo.entrySet().size() != 0);
  }
}
