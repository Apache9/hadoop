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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;

/**
 * RaidTask defines the base task that the RaidNode runs.
 */
public abstract class RaidTask<R> implements Callable<R>, FutureCallback<R> {

  protected final RaidNode raidNode;
  protected boolean isTaskKicked;

  private static final Log LOG = LogFactory.getLog(RaidTask.class);

  protected RaidTask(RaidNode raidNode) {
    this.raidNode = raidNode;
  }

  public enum TaskStatus {
    Success, Fail
  }

  /**
   * The task result class.
   */
  public static class TaskResult {

    private TaskStatus status;
    private long startTimeMs;
    private long endTimeMs;

    public TaskResult(TaskStatus status, long startTimeMs, long endTimeMs) {
      this.status = status;
      this.startTimeMs = startTimeMs;
      this.endTimeMs = endTimeMs;
    }

    public TaskStatus getStatus() {
      return status;
    }

    public long getStartTimeMs() {
      return startTimeMs;
    }

    public long getEndTimeMs() {
      return endTimeMs;
    }

    public long getTimeConsumedMs() {
      return getEndTimeMs() - getStartTimeMs();
    }
  }

  /**
   * Task to encode specified blocks.
   */
  public static class EncodeTask extends RaidTask<TaskResult> {

    private final Path file;

    public EncodeTask(RaidNode raidNode, Path file) {
      super(raidNode);
      Preconditions.checkArgument(file != null);
      this.file = file;
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      raidNode.getCodec().encode(file);
      long endTimeMs = System.currentTimeMillis();
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Encode file " + file + " successful, consumed " + result.getStartTimeMs() + " ms");
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Encode file " + file + " failed", t);
    }
  }

  /**
   * Task to decode specified blocks.
   */
  public static class DecodeTask extends RaidTask<TaskResult> {

    private final Path file;
    private final int[] corruptedBlocks;

    public DecodeTask(RaidNode raidNode, Path file, int[] corruptedBlocks) {
      super(raidNode);
      Preconditions.checkArgument(file != null);
      Preconditions.checkArgument(corruptedBlocks != null && corruptedBlocks.length > 0);
      this.file = file;
      this.corruptedBlocks = corruptedBlocks;
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      raidNode.getCodec().decode(file, corruptedBlocks,
        MRUtils.getBlockTokenSecretManager(raidNode.getConf()));
      long endTimeMs = System.currentTimeMillis();
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Decode corrupted blocks " + Arrays.toString(corruptedBlocks) + " of file " + file
          + " successful");
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.info("Decode corrupted blocks " + Arrays.toString(corruptedBlocks) + " of file " + file
          + " failed");
    }
  }

  // TBD: Make it a piggyback job of CollectRaidInfoTask for Mover.
  public static class ZombieSweeperTask extends RaidTask<TaskResult> {

    private final Path dirToSweep;
    private final Configuration conf;
    private int cleanedFiles;
    private int dataBlocksNum;
    private int codingBlocksNum;
    private long zombieSweeperGracePeriod;
    private long orphanFileGracePeriod;
    private short replicaAfterEncode;

    public ZombieSweeperTask(RaidNode raidNode, Path dir, Configuration conf) {
      super(raidNode);
      this.dirToSweep = dir;
      this.conf = conf;
      this.dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
      this.codingBlocksNum = conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
      this.zombieSweeperGracePeriod = conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL_DEFAULT);
      this.orphanFileGracePeriod = conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ORPHAN_FILE_GRACE_PERIOD,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ORPHAN_FILE_GRACE_PERIOD_DEFAULT);
      this.replicaAfterEncode = (short) conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_FILE_REPLICA,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_FILE_REPLICA_DEFAULT);
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      raidNode.getMetrics().incrZombieSweeperTaskScheduled();
      sweepDirectory(dirToSweep);
      long endTimeMs = System.currentTimeMillis();
      raidNode.getMetrics().addZombieSweeperDurationInMs(endTimeMs - startTimeMs);
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("ZombieSweeper cleanup orphan coding files successfully and took "
          + (result.getEndTimeMs() - result.getStartTimeMs()) / 1000 + " seconds");
      raidNode.increaseZombieSweeperTaskDone();
      raidNode.scheduleZombieSweeperTask(zombieSweeperGracePeriod);
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.info("ZombieSweeper fail to cleanup orphan coding files ");
      ReflectionUtils.logThreadInfo(LOG, "Thread dump from ZombieSweeperTask:onFailure", 1000);
      raidNode.getMetrics().incrZombieSweeperTaskFailed();
      raidNode.increaseZombieSweeperTaskDone();
      raidNode.scheduleZombieSweeperTask(zombieSweeperGracePeriod);
    }

    private void sweepDirectory(Path dir) throws IOException {
      final FileSystem fs = FileSystem.get(conf);

      Queue<Path> toCleanup = RaidTask.RaidTaskUtils.traverseDirectoryTree(fs, dirToSweep,
              new RaidTaskUtils.Filter() {
                public boolean check(Path file, RaidMetrics metrics)
                    throws IOException {
                  if (fs.isDirectory(file)) {
                    return false;
                  }
                  if (BlockCodec.isCodingFile(file)) {
                    Path sourceFile = BlockCodec.getCodingFileSource(file);
                    if (!fs.exists(sourceFile)) {
                      return true;
                    }
                    FileStatus sourceFileStatus = fs.getFileStatus(sourceFile);
                    FileStatus codeFileStatus = fs.getFileStatus(file);
                    if (System.currentTimeMillis() < codeFileStatus
                        .getModificationTime() + orphanFileGracePeriod
                        || !((DistributedFileSystem) fs).isFileClosed(file)) {
                      // The coding file has been changed recently or is not
                      // closed yet, which implies the
                      // coder might be in progress.
                      return false;
                    }

                    if (sourceFileStatus.getReplication() <= replicaAfterEncode) {
                      // The source file's replica has been reduced. It implies
                      // the source file has ever
                      // been encoded successfully.
                      // Do not treat the file as zombie file even if the coding
                      // file's
                      // length is not correct. Let fixer handle it.
                      return false;
                    } else {
                      return true;
                    }
                    }
                  return false;
                  }
              });

      for (Path orphanFile : toCleanup) {
        try {
          fs.delete(orphanFile, false);
          raidNode.getMetrics().incrZombieFilesSweeped();
          LOG.info("Cleaned a zombie file " + orphanFile.toString());
          cleanedFiles++;
        } catch (IOException ioe) {
          // Log a message and try to delete other zombie files
          LOG.warn("Fail to delete zombie file " + orphanFile.toString(), ioe);
          raidNode.getMetrics().incrFailedSweeping();
        }
      }
    }

    @VisibleForTesting
    public int numOfCleanedZombie() {
      return cleanedFiles;
    }
  }

  public static enum TaskPurpose {
    Encode, BlockMover, InvalidType
  }

  /**
   * Task to collect information of files need to encode or need to adjust blocks layout.
   */
  public static class CollectRaidInfoTask extends RaidTask<TaskResult> {

    private final Configuration conf;
    private Collector collector;
    private String lastRealWorkTaskId;
    private String lastCollectRaidInfoTaskId;
    private Path resultDirPath;
    private boolean nothingToDo;
    private TaskPurpose purpose;

    public CollectRaidInfoTask(RaidNode raidNode, Policy policy, TaskPurpose purpose,
 Configuration inConf) throws IOException {
      super(raidNode);
      this.conf = new Configuration(inConf);
      this.isTaskKicked = false;
      this.purpose = purpose;
      try {
        if (purpose == TaskPurpose.Encode) {
          this.lastRealWorkTaskId = MRUtils.readJobId(this.conf, Coder.getJobIdFilePath());
        } else if (purpose == TaskPurpose.BlockMover) {
          this.lastRealWorkTaskId = MRUtils.readJobId(this.conf, Mover.getJobIdFilePath());
        }
      } catch (FileNotFoundException e) {
        this.lastRealWorkTaskId = null;
      }

      try {
        this.lastCollectRaidInfoTaskId = MRUtils.readJobId(this.conf,
          Collector.getJobIdFilePath(purpose));
      } catch (FileNotFoundException e) {
        this.lastCollectRaidInfoTaskId = null;
      }

      List<Path> rootDirs = null;

      // TBD: Go deeper in the path to dispatch directories among map tasks so that it has a higher
      // possibility of load balance.
      if (purpose == TaskPurpose.Encode) {
        rootDirs = policy.getCandidateDirs();
      } else if (purpose == TaskPurpose.BlockMover) {
        rootDirs = new LinkedList<Path>();
        rootDirs.add(BlockCodec.getRaidRoot());
        String queue = conf.get(HdfsRaidConfigKeys.HDFS_RAID_MOVER_JOB_QUEUE);
        if (queue != null) {
          conf.set("mapreduce.job.queuename", queue);
        }
      }

      // Eliminate empty dirs
      FileSystem fs = FileSystem.get(conf);
      Iterator<Path> it = rootDirs.iterator();
      while (it.hasNext()) {
        Path dir = it.next();
        if (!fs.exists(dir) || !fs.isDirectory(dir) || fs.listStatus(dir).length == 0) {
          it.remove();
        }
      }

      if (rootDirs.size() != 0) {
        String resultDir = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_DEFAULT);
        Preconditions.checkNotNull(resultDir);
        resultDirPath = new Path(resultDir + "/" + purpose.toString() + System.currentTimeMillis());

        nothingToDo = false;
      } else {
        nothingToDo = true;
      }

      if (nothingToDo) {
        return;
      }

      // If the number of live nodes is too small, we should not kick-off any Encode or Mover tasks.
      if (!(fs instanceof DistributedFileSystem)) {
        IOException ioe = new IOException("Non-distributed filesystem is not supported");
        LOG.warn("Non-distributed filesystem is cnofigured", ioe);
        throw ioe;
      }
      DatanodeInfo[] liveNodes = ((DistributedFileSystem) fs).getClient().datanodeReport(
        DatanodeReportType.LIVE);
      int dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
      int codingBlocksNum = conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
      if (liveNodes.length < dataBlocksNum + codingBlocksNum) {
        LOG.debug("Not kicking encoding since the number of datanodes is not enough to ensure data availability of raid");
        nothingToDo = true;
      }

      if (!nothingToDo) {
        this.collector = new Collector(rootDirs, resultDirPath, purpose, conf);
      }
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      // Ensure that legacy tasks are killed
      if (this.lastRealWorkTaskId != null) {
        MRUtils.killJob(conf, lastRealWorkTaskId);
      }
      if (this.lastCollectRaidInfoTaskId != null) {
        MRUtils.killJob(conf, lastCollectRaidInfoTaskId);
      }

      raidNode.getMetrics().incrCollectorTaskScheduled();
      // Run the collector
      if (!nothingToDo) {
        LOG.debug("Collecting files for " + purpose.toString());
        isTaskKicked = true;
        collector.run();
      } else {
        LOG.debug("Idle collecting task for " + purpose.toString());
        raidNode.getMetrics().incrIdleCollectorTaskScheduled();
      }
      long endTimeMs = System.currentTimeMillis();
      raidNode.getMetrics().addCollectorDurationInMs(endTimeMs - startTimeMs);
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Collect raid info task success, timeConsumedMs=" + result.getTimeConsumedMs()
          + " purpose is " + purpose.toString());
      try {
        if (isTaskKicked) {
            raidNode.getMetrics().incrFilesScannedForCoder(
            collector.getCounter(Collector.CounterName.FilesScannedForCoder).getValue());
            raidNode.getMetrics().incrFilesScannedForMover(
            collector.getCounter(Collector.CounterName.FilesScannedForMover).getValue());
        }
      } catch (IOException ioe) {
        // Just ignore
      }

      try {
        // Start the batch raid task
        if (!nothingToDo) {
          BatchRaidTask task = new BatchRaidTask(raidNode, resultDirPath, purpose, conf);
          raidNode.submitTask(task);
        } else {
          reKickTask();
        }
      } catch (IOException e) {
        LOG.error("Cannot start task for " + purpose.toString(), e);
        reKickTask();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Collect raid info task failed purpose is " + purpose.toString(), t);
      raidNode.getMetrics().incrCollectorTaskFailed();
      try {
        if (isTaskKicked) {
            raidNode.getMetrics().incrFilesScannedForCoder(
            collector.getCounter(Collector.CounterName.FilesScannedForCoder).getValue());
            raidNode.getMetrics().incrFilesScannedForMover(
            collector.getCounter(Collector.CounterName.FilesScannedForMover).getValue());
        }
      } catch (IOException ioe) {
        // Just ignore
      }
      reKickTask();
    }

    private void reKickTask() {
      switch (purpose) {
      case Encode:
        raidNode.increaseEncodeTaskDone();
        raidNode.scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
        break;
      case BlockMover:
        raidNode.increaseMoverTaskDone();
        raidNode.scheduleMoverTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_INTERVAL,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_INTERVAL_DEFAULT));
        break;
      }
    }

    private List<Path> convertPaths(String[] paths) {
      Preconditions.checkNotNull(paths);
      Preconditions.checkArgument(paths.length > 0);
      List<Path> result = new ArrayList<Path>(paths.length);

      for (String path : paths) {
        result.add(new Path(path));
      }
      return result;
    }
  }

  /**
   * Task to do batch encoding and decoding work.
   */
  public static class BatchRaidTask extends RaidTask<TaskResult> {

    private final String lastCollectRaidInfoTaskId;
    private final Configuration conf;
    private Coder coder = null;
    private Mover mover = null;
    private final Path collectResultDir;
    private final FileSystem fs;
    private final TaskPurpose purpose;

    public BatchRaidTask(RaidNode raidNode, Path collectResultDir, TaskPurpose purpose,
        Configuration conf) throws IOException {
      super(raidNode);
      this.conf = conf;
      this.isTaskKicked = false;
      this.purpose = purpose;
      this.lastCollectRaidInfoTaskId = MRUtils.readJobId(this.conf,
        Collector.getJobIdFilePath(purpose));
      this.collectResultDir = collectResultDir;
      this.fs = FileSystem.get(this.conf);

      String resultDir = null;
      switch (purpose) {
      case Encode:
        resultDir = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_KEY,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_DEFAULT);
        break;
      case BlockMover:
        resultDir = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_RESULT_DIR_KEY,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_RESULT_DIR_DEFAULT);
        break;
      default:
        throw new IOException("Unknow task type");
      }

      Preconditions.checkNotNull(resultDir);
      Path resultDirPath = new Path(resultDir + "/" + System.currentTimeMillis());

      // TBD: Put constants to a seprated file named HdfsRaidConstants.java
      switch (purpose) {
      case Encode:
        this.coder = new Coder(new Path(this.collectResultDir.toString() + "/part-r-00000"),
            conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY,
              HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT), resultDirPath, conf);
        break;
      case BlockMover:
        this.mover = new Mover(new Path(this.collectResultDir.toString() + "/part-r-00000"),
            conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_KEY,
              HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_DEFAULT), resultDirPath, conf);
        break;
      default:
        throw new IOException("Unknow task type");
      }
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      // Ensure that last collect task is successful
      Path successFile = new Path(collectResultDir.toString() + "/" + "_SUCCESS");
      // TBD: Put all this kind of constant to a file named HdfsRaidConstants.java
      Path resultFile = new Path(collectResultDir.toString() + "/part-r-00000");
      if (!fs.exists(successFile) || !fs.exists(resultFile)) {
        throw new IOException("The last collect task is failed, "
            + "can't start the batch raid task.");
      }

      long endTimeMs;
      switch (purpose) {
      case Encode:
        raidNode.getMetrics().incrCoderTaskScheduled();
        isTaskKicked = true;
        coder.run();
        endTimeMs = System.currentTimeMillis();
        raidNode.getMetrics().addCoderDurationInMs(endTimeMs - startTimeMs);
        break;
      case BlockMover:
        raidNode.getMetrics().incrMoverTaskScheduled();
        isTaskKicked = true;
        mover.run();
        endTimeMs = System.currentTimeMillis();
        raidNode.getMetrics().addMoverDurationInMs(endTimeMs - startTimeMs);
        break;
      default:
        throw new IllegalArgumentException("Unknown task type");
      }

      TaskResult result = new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
      return result;
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Batch raid task finished successfully, timeConsumedMs="
          + result.getTimeConsumedMs());

      if (isTaskKicked && coder != null) {
        try {
          raidNode.getMetrics().incrFilesCoded(
            coder.getCounter(Coder.CounterName.EncodeFiles).getValue());
          raidNode.getMetrics().incrBytesCoded(
            coder.getCounter(Coder.CounterName.EncodeBytes).getValue());
          raidNode.getMetrics().incrFailedCoding(
            coder.getCounter(Coder.CounterName.EncodeFail).getValue());
        } catch (IOException ioe) {
          // Just ignore
          LOG.warn("Fail to get metrics of Coder job ", ioe);
        }
      }

      if (isTaskKicked && mover != null) {
        try {
          raidNode.getMetrics().incrBlocksMoved(
            mover.getCounter(Mover.CounterName.MovedBlocks).getValue());
          raidNode.getMetrics().incrFailedBuildingMovingMap(
            mover.getCounter(Mover.CounterName.FailedBuildingMovingMap).getValue());
          raidNode.getMetrics().incrFailedMoving(
            mover.getCounter(Mover.CounterName.FailedMoving).getValue());
        } catch (IOException ioe) {
          // Just ignore
          LOG.warn("Fail to get metrics of Mover job ", ioe);
        }
      }

      reKickTask();
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.warn("Batch raid task failed", t);
      switch (purpose) {
      case Encode:
        raidNode.getMetrics().incrCoderTaskFailed();
        break;
      case BlockMover:
        raidNode.getMetrics().incrMoverTaskFailed();
      default:
        assert false : "Unknown task type";
      }
      // There may be some partial success encoding
      if (coder != null && isTaskKicked) {
        try {
          raidNode.getMetrics().incrFilesCoded(
            coder.getCounter(Coder.CounterName.EncodeFiles).getValue());
          raidNode.getMetrics().incrBytesCoded(
            coder.getCounter(Coder.CounterName.EncodeBytes).getValue());
          raidNode.getMetrics().incrFailedCoding(
            coder.getCounter(Coder.CounterName.EncodeFail).getValue());
        } catch (IOException ioe) {
          // Just ignore
        }
      }

      if (mover != null && isTaskKicked) {
        try {
          raidNode.getMetrics().incrBlocksMoved(
            mover.getCounter(Mover.CounterName.MovedBlocks).getValue());
          raidNode.getMetrics().incrFailedBuildingMovingMap(
            mover.getCounter(Mover.CounterName.FailedBuildingMovingMap).getValue());
          raidNode.getMetrics().incrFailedMoving(
            mover.getCounter(Mover.CounterName.FailedMoving).getValue());
        } catch (IOException ioe) {
          // Just ignore
        }
      }
      reKickTask();
    }

    private void startCollectTask() throws IOException {
      CollectRaidInfoTask task = new CollectRaidInfoTask(raidNode, raidNode.getPolicyInfos(null),
          purpose, conf);
      raidNode.submitTask(task);
    }

    private void reKickTask() {
      switch (purpose) {
      case Encode:
        raidNode.increaseEncodeTaskDone();
        raidNode.scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
        break;
      case BlockMover:
        raidNode.increaseMoverTaskDone();
        raidNode.scheduleMoverTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_INTERVAL,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_INTERVAL_DEFAULT));
        break;
      }
    }
  }

  /**
   * Task to do fixer work.
   */
  public static class FixerTask extends RaidTask<TaskResult> {

    private final Configuration conf;
    private final Fixer fixer;

    public FixerTask(RaidNode raidNode, Configuration conf) throws IOException {
      super(raidNode);
      this.conf = conf;
      this.isTaskKicked = false;

      String resultDir = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_RESULT_DIR_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_RESULT_DIR_DEFAULT);
      Preconditions.checkNotNull(resultDir);
      Path resultDirPath = new Path(resultDir + "/" + System.currentTimeMillis());

      fixer = new Fixer(resultDirPath, conf);
    }

    @Override
    public TaskResult call() throws Exception {

      long startTimeMs = System.currentTimeMillis();
      raidNode.getMetrics().incrFixerTaskScheduled();
      isTaskKicked = true;
      fixer.run();
      long endTimeMs = System.currentTimeMillis();
      raidNode.getMetrics().addFixerDurationInMs(endTimeMs - startTimeMs);

      TaskResult result = new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
      return result;
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Fixer task finished successfully, timeConsumedMs=" + result.getTimeConsumedMs());

      if (isTaskKicked) {
        try {
          raidNode.getMetrics().incrBlocksFixed(
            fixer.getCounter(Fixer.CounterName.FixedBlocks).getValue());
          raidNode.getMetrics().incrFailedFixing(
            fixer.getCounter(Fixer.CounterName.FixFail).getValue());
        } catch (IOException ioe) {
          // Just ignore
        }
      }
      raidNode.increaseFixerTaskDone();
      raidNode.scheduleFixerTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL_DEFAULT));
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.warn("Fixer task failed", t);

      raidNode.increaseFixerTaskDone();
      raidNode.getMetrics().incrFixerTaskFailed();
      if (isTaskKicked) {
        try {
          raidNode.getMetrics().incrBlocksFixed(
            fixer.getCounter(Fixer.CounterName.FixedBlocks).getValue());
          raidNode.getMetrics().incrFailedFixing(
            fixer.getCounter(Fixer.CounterName.FixFail).getValue());
        } catch (IOException ioe) {
          // Just ignore
        }
      }
      raidNode.scheduleFixerTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL_DEFAULT));
    }
  }

  public static class RaidTaskUtils {

    private static RaidMetrics metrics;

    public static void setMetrics(RaidMetrics m) {
      metrics = m;
    }

    public static interface Filter {
      public boolean check(Path file, RaidMetrics m) throws IOException;
    }

    private static class ChildrenInfo {
      private int nextChildId = 0;
      private final FileStatus[] children;

      public ChildrenInfo(FileStatus[] children) {
        this.children = children;
      }

      public boolean hasNextChild() {
        if (children == null || children.length == 0) {
          return false;
        }
        return nextChildId < children.length;
      }

      public FileStatus nextChild() {
        return children[nextChildId++];
      }
    }

    /**
     * Depth first traverse the specified directory tree, return all files or sub-directories at given depth
     */
    public static Queue<Path> getSubDirectoriesAndFiles(FileSystem fs, Path rootDir, int depth) {
      Queue<Path> result = new LinkedList<Path>();
      try {
        if (!fs.isDirectory(rootDir) || depth == 0) {
          result.add(rootDir);
          return result;
        }
        FileStatus[] children = fs.listStatus(rootDir);
        for (FileStatus f : children) {
          result.addAll(getSubDirectoriesAndFiles(fs, f.getPath(), depth-1));
        }
      } catch (IOException e) {
        LOG.warn("Error occured while processing path " + rootDir, e);
      }
      return result;
    }

    /**
     * Depth first traverses the specified directory tree to get all appropriate files.
     */
    public static Queue<Path> traverseDirectoryTree(FileSystem fs, Path rootDir, Filter filter) {
      Preconditions.checkArgument(rootDir != null);
      Stack<Path> stack = new Stack<Path>();
      Map<Path, ChildrenInfo> childrenInfos = new HashMap<Path, ChildrenInfo>();
      Queue<Path> result = new LinkedList<Path>();

      stack.add(rootDir);
      while (!stack.isEmpty()) {
        Path path = null;
        try {
          path = stack.peek();
          boolean isDirectory = fs.isDirectory(path);

          ChildrenInfo childrenInfo = childrenInfos.get(path);
          if (childrenInfo == null && isDirectory) {
            FileStatus[] children = fs.listStatus(path);
            childrenInfo = new ChildrenInfo(children);
            childrenInfos.put(path, childrenInfo);
          }

          if (isDirectory && childrenInfo.hasNextChild()) {
            FileStatus nextChild = childrenInfo.nextChild();
            stack.add(nextChild.getPath());
          } else {
            if (filter.check(path, metrics)) {
              result.add(new Path(path.toUri().getPath()));
            }
            stack.pop();
            childrenInfos.remove(path);
          }
        } catch (IOException e) {
          stack.pop();
          LOG.warn("Error occured while processing path " + path, e);
        }
      }
      return result;
    }

    public static class FixerItem implements Comparable<FixerItem> {
      private Path file;
      private int group;

      public FixerItem(Path file, int group) {
        this.file = file;
        this.group = group;
      }

      public Path getFile() {
        return file;
      }

      public int getGroup() {
        return group;
      }

      @Override
      public int compareTo(FixerItem item) {
        int res = file.toString().compareTo(item.getFile().toString());
        if (res == 0) {
          res = (group - item.getGroup());
        }
        return res;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }

        if (!(o instanceof FixerItem)) {
          return false;
        }

        return (compareTo((FixerItem) o) == 0);
      }
    }
  }
}
