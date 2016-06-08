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
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.RaidTask.CollectRaidInfoTask;
import org.apache.hadoop.contrib.raid.RaidTask.FixerTask;
import org.apache.hadoop.contrib.raid.RaidTask.RaidTaskUtils;
import org.apache.hadoop.contrib.raid.RaidTask.TaskPurpose;
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
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
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
  final private static String moverResult = "/raid/mover";
  final private static long blockSize = 1024 * 1024;

  // The interval to mark DN dead is : 2*hbRecheckInterval+10*1000*heartBeatInterval. Pls refer
  // DatanodeManager
  final private static long heartBeatInterval = 1l;
  final private static int hbRecheckInterval = 3000;
  final private static long deadShowup = 2 * hbRecheckInterval + 10 * 1000 * heartBeatInterval;

  // Make the group a little bit small so that the UT would not take too much time
  final static int dataBlocksNum = 3;
  final static int codingBlocksNum = 2;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    // To make DN dead detection quickly
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, heartBeatInterval);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, hbRecheckInterval);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_SLOW_LOG_THRESHOLD_MS_KEY, 10000);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, dataBlocksNum);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, codingBlocksNum);
    // Set the block size to a small value so that the test can finish in a short time
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, (long) 0);
    
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(dataBlocksNum + codingBlocksNum)
        .build();
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

    Map<String, String> env = System.getenv();
    String ldPath = env.get("LD_LIBRARY_PATH");
    String origPath = mrCluster.getConfig().get(MRJobConfig.MAPRED_ADMIN_USER_ENV,
      MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV);
    ldPath = origPath + ":" + ldPath;

    mrCluster.getConfig().set(MRJobConfig.MAPRED_ADMIN_USER_ENV, ldPath);
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
    byte[] buf = new byte[dataBlocksNum * (int) blockSize];
    Arrays.fill(buf, (byte) 0xff);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        Path dir = new Path("/" + i + "/" + j);
        Path file = new Path("/" + i + "/" + j + "/f");
        dfs.mkdirs(dir);
        FSDataOutputStream out = dfs.create(file);
        out.write(buf);
        out.close();
      }
    }
    dfs.mkdirs(new Path(collectResult));
    dfs.mkdirs(new Path(coderResult));
    dfs.mkdirs(new Path(fixerResult));
    dfs.mkdirs(new Path(moverResult));
  }

  private Policy setupPolicy() throws IOException {
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

    CollectRaidInfoTask crti = new CollectRaidInfoTask(rd, policy, TaskPurpose.Encode, jobConf);
    rd.submitTask(crti);

    // Wait job done
    long jobDone = rd.getEncodeTaskDone();
    while (jobDone != 1) {
      Thread.sleep(1000);
      jobDone = rd.getEncodeTaskDone();
    }

    // Verify codings
    verifyResult();
    
    Assert.assertEquals(rd.getMetrics().filesScannedForCoder.value(), 9);
    Assert.assertEquals(rd.getMetrics().filesCoded.value(), 9);
    Assert.assertEquals(rd.getMetrics().bytesCoded.value(), 9 * dataBlocksNum * blockSize);
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
    Assert.assertEquals(rd.getMetrics().zombieFilesSweeped.value(), zst.numOfCleanedZombie());
    Assert.assertEquals(rd.getMetrics().failedSweeping.value(), 0);
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
    LocatedBlock[] corruptBlks = new LocatedBlock[] { lblks[0], lblks[1] };
    dfsClient.reportBadBlocks(corruptBlks);
    blocks = dfsClient.getLocatedBlocks(file.toString(), 0);
    lblks = new LocatedBlock[blocks.getLocatedBlocks().size()];
    blocks.getLocatedBlocks().toArray(lblks);
    corruptBlks[0] = lblks[0];
    corruptBlks[1] = lblks[1];
    for (LocatedBlock blk : corruptBlks) {
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
    Assert.assertEquals(rd.getMetrics().blocksFixed.value(), 2);
    Assert.assertEquals(rd.getMetrics().failedFixing.value(), 0);
  }

  // Make some blocks on the same node and then let mover to fix it
  private void setupBlockPlacement(Path file) throws Exception {
    Assert.assertTrue(BlockCodec.isFileEncoded(dfs, file));
    Path codingFile = BlockCodec.getCodingFile(file);

    waitNNReduceReplica(file, codingFile);

    LocatedBlocks dataBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
      file.toString(), 0);
    LocatedBlocks codingBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
      codingFile.toString(), 0);

    // Simply move blocks in the first raid group
    Assert.assertTrue(dataBlks.getLocatedBlocks().size() >= 1);
    Assert.assertTrue(codingBlks.getLocatedBlocks().size() >= 1);
    int connectTimeout = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_CONNECT_TIMEOUT,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_CONNECT_TIMEOUT_DEFAULT);
    int moveTimeout = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MOVEONEBLOCK_TIMEOUT,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MOVEONEBLOCK_TIMEOUT_DEFAULT);

    if (!codingBlks.getLocatedBlocks().get(1).getLocations()[0].equals(dataBlks.getLocatedBlocks()
        .get(0).getLocations()[0])) {
      Mover.moveOneBlock(connectTimeout, moveTimeout, codingBlks.getLocatedBlocks().get(1),
        dataBlks.getLocatedBlocks().get(0).getLocations()[0]);
    }

    waitNNReduceReplica(file, codingFile);

    // Verify the moveOneBlock actually do its job
    dataBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(file.toString(), 0);
    codingBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(codingFile.toString(),
      0);

    Assert.assertTrue(codingBlks.getLocatedBlocks().get(1).getLocations()[0].equals(dataBlks
        .getLocatedBlocks().get(0).getLocations()[0]));
  }

  private void checkBlockPlacement(Path file) throws Exception {
    Assert.assertTrue(BlockCodec.isFileEncoded(dfs, file));
    Path codingFile = BlockCodec.getCodingFile(file);

    waitNNReduceReplica(file, codingFile);

    LocatedBlocks dataBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
      file.toString(), 0);
    LocatedBlocks codingBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
      codingFile.toString(), 0);

    // Check that the first raid group now is distributed on different nodes
    int numDataBlks = dataBlks.getLocatedBlocks().size();
    numDataBlks = (numDataBlks > dataBlocksNum) ? dataBlocksNum : numDataBlks;
    int numCodingBlks = codingBlks.getLocatedBlocks().size();
    numCodingBlks = (numCodingBlks > codingBlocksNum) ? codingBlocksNum : numCodingBlks;
    boolean satisfyRaidPlace = true;

    Set<LocatedBlock> blks = new HashSet<LocatedBlock>();
    blks.addAll(dataBlks.getLocatedBlocks());
    blks.addAll(codingBlks.getLocatedBlocks());

    for (LocatedBlock blk : blks) {
      for (LocatedBlock tmpBlk : blks) {
        if (tmpBlk == blk) {
          continue;
        }
        if (tmpBlk.getLocations()[0].equals(blk.getLocations()[0])) {
          satisfyRaidPlace = false;
        }
      }
    }

    Assert.assertTrue(satisfyRaidPlace);
    Assert.assertTrue(rd.getMetrics().blocksMoved.value() >= 1);
    Assert.assertEquals(rd.getMetrics().filesScannedForMover.value(), 8);
    Assert.assertEquals(rd.getMetrics().failedMoving.value(), 0);
  }

  private void waitNNReduceReplica(Path file, Path codingFile) throws Exception {
    Assert.assertTrue(dfs.getFileStatus(file).getReplication() == 1);
    Assert.assertTrue(dfs.getFileStatus(codingFile).getReplication() == 1);
    LocatedBlocks dataBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
      file.toString(), 0);
    LocatedBlocks codingBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
      codingFile.toString(), 0);
    int numDataBlks = dataBlks.getLocatedBlocks().size();
    int numCodingBlks = codingBlks.getLocatedBlocks().size();
    // Wait until all replicas has been reduced by NN
    for (int i = 0; i < numDataBlks; i++) {
      while (dataBlks.getLocatedBlocks().get(i).getLocations().length != 1) {
        Thread.sleep(1000);
        dataBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(file.toString(), 0);
      }
    }
    for (int i = 0; i < numCodingBlks; i++) {
      while (codingBlks.getLocatedBlocks().get(i).getLocations().length != 1) {
        Thread.sleep(1000);
        codingBlks = ((DistributedFileSystem) dfs).getClient().getLocatedBlocks(
          codingFile.toString(), 0);
      }
    }
  }

  // Use encoded files left by the encode test case.
  @Test
  public void testCollectorAndMover() throws Exception {
    // Set up fs env
    Path file = new Path("/1/0/f");
    setupBlockPlacement(file);

    // Get configuration
    Configuration jobConf = new Configuration(mrCluster.getConfig());
    // In case that MapReduce task out of memory during encoding
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY, collectResult);
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_RESULT_DIR_KEY, moverResult);

    CollectRaidInfoTask crti = new CollectRaidInfoTask(rd, new Policy(conf),
        TaskPurpose.BlockMover, jobConf);
    rd.submitTask(crti);

    // Wait job done
    long jobDone = rd.getMoverTaskDone();
    while (jobDone != 1) {
      Thread.sleep(1000);
      jobDone = rd.getMoverTaskDone();
    }

    // Verify mover result
    checkBlockPlacement(file);
  }

  // Test that if some DNs are stopped we can get a corrupt list.
  // TBD: After block placement policy work is done, add test case to verify that we can use Fixer
  // to fix blocks which are hosted by the dead DN.
  @Test
  public void testGetCorruptList() throws Exception {
    Map<RaidTaskUtils.FixerItem, Set<Integer>> fixerInfo = Fixer.collectFixerInfo(dfs.getConf());
    Assert.assertEquals(fixerInfo.entrySet().size(), 0);
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

  @Test
  public void testCollectorSplitRootDirs() throws  Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();

    FileSystem dfs = dfsCluster.getFileSystem();
    dfs.mkdirs(new Path("/user/test1"));
    dfs.mkdirs(new Path("/user/test2"));
    dfs.mkdirs(new Path("/user/test3"));
    dfs.mkdirs(new Path("/user/test1/foo_a"));
    dfs.mkdirs(new Path("/user/test1/foo_b"));
    dfs.mkdirs(new Path("/user/test1/foo_c"));
    dfs.mkdirs(new Path("/user/test2/bar_a"));
    dfs.mkdirs(new Path("/user/test2/bar_b"));
    dfs.create(new Path("/user/test3/a.txt")).close();
    dfs.create(new Path("/user/test1/foo_c/a.txt")).close();

    FileSystem fs = FileSystem.get(conf);
    Queue<Path> res = RaidTask.RaidTaskUtils.getSubDirectoriesAndFiles(fs, new Path("/user/"), 2);
    for (Path p : res) {
      System.out.println(p.toString());
    }
    Assert.assertEquals(res.size(), 6);
  }

  private void createFile(Path p) throws IOException {
    byte[] buf = new byte[dataBlocksNum * (int) blockSize];
    Arrays.fill(buf, (byte) 0xff);
    FSDataOutputStream out = dfs.create(p);
    out.write(buf);
    out.close();
  }

  private void resetFsEnv() throws Exception {
    int numDNs = dfsCluster.getDataNodes().size();
    for (int i = 0; i < numDNs - 1; i++) {
      dfsCluster.restartDataNode(i);
    }
    for (int i = 0; i < 3; i++) {
      dfs.delete(new Path("/" + i), true);
    }
    dfs.delete(new Path("/raid"), true);
  }

  private void setupFsEnv2() throws IOException {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        for (int k = 0; k < 2; k++) {
          Path dir = new Path("/" + i + "/" + j + "/" + k);
          dfs.mkdirs(dir);
          Path f1 = new Path("/" + i + "/" + j + "/" + k + "/f1");
          Path f2 = new Path("/" + i + "/" + j + "/" + k + "/f2");
          createFile(f1);
          createFile(f2);
        }
      }
    }
    createFile(new Path("/0/foo"));
    createFile(new Path("/1/bar"));
    createFile(new Path("/0/1/foo"));
    createFile(new Path("/1/0/bar"));

    dfs.mkdirs(new Path(collectResult));
    dfs.mkdirs(new Path(coderResult));
    dfs.mkdirs(new Path(fixerResult));
    dfs.mkdirs(new Path(moverResult));
  }

  @Test
  public void testRaidTaskWithRootDirSplits() throws Exception {
    resetFsEnv();
    setupFsEnv2();
    List<Path> paths = new LinkedList<Path>();
    paths.add(new Path("/0"));
    paths.add(new Path("/1"));

    // Get configuration
    Configuration jobConf = new Configuration(mrCluster.getConfig());
    jobConf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 2000);
    Thread.sleep(3000);
    // In case that MapReduce task out of memory during encoding
    jobConf.setInt(HdfsRaidConfigKeys.HDFS_RAID_CODEC_CODE_BUF_SIZE, 16 * 1024 * 1024);
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY, collectResult);
    jobConf.setStrings(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_KEY, coderResult);
    jobConf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_SCAN_ROOT_DIRS_SPLIT_DEPTH, 2);

    Collector collector = new Collector(paths,
        new Path(collectResult + "/" + System.currentTimeMillis()),
        TaskPurpose.Encode, jobConf);
    collector.run();

    Assert.assertEquals(
        collector.getCounter(Collector.CounterName.FilesScannedForCoder)
            .getValue(), 8 * 2 + 4);
    Assert.assertEquals(
        collector.getCounter(JobCounter.TOTAL_LAUNCHED_MAPS).getValue(), 8 + 4);
  }

  @Test public void testTraverseDirectoryTree() throws Exception {
    resetFsEnv();
    List<Path> paths = new LinkedList<Path>();
    paths.add(new Path("/test/a"));
    paths.add(new Path("/test/b/a/a"));
    paths.add(new Path("/test/b/a/b"));
    paths.add(new Path("/test/b/b"));
    paths.add(new Path("/test/b/c/a/a"));
    paths.add(new Path("/test/b/d"));
    paths.add(new Path("/test/c"));
    paths.add(new Path("/test/d/a"));

    for (Path p : paths) {
      createFile(p);
    }

    Queue<Path> files = RaidTaskUtils
        .traverseDirectoryTree(dfs, new Path("/test/"), new RaidTaskUtils.Filter() {
          public boolean check(Path file, RaidMetrics metrics)
              throws IOException {
            return dfs.isFile(file);
          }
        });

    int i = 0;
    for (Path p : files) {
      Assert.assertEquals(p, paths.get(i++));
    }
  }
}
