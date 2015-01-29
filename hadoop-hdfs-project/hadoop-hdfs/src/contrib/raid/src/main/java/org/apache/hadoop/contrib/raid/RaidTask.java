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
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;

/**
 * RaidTask defines the base task that the RaidNode runs.
 */
public abstract class RaidTask<R> implements Callable<R>, FutureCallback<R> {

  protected final RaidNode raidNode;

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
      raidNode.getCodec().decode(file, corruptedBlocks);
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

  public static class ZombieSweeperTask extends RaidTask<TaskResult> {

    private final Path dirToSweep;
    private final Configuration conf;
    private int cleanedFiles;

    public ZombieSweeperTask(RaidNode raidNode, Path dir, Configuration conf) {
      super(raidNode);
      this.dirToSweep = dir;
      this.conf = conf;
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      sweepDirectory(dirToSweep);
      long endTimeMs = System.currentTimeMillis();
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("ZombieSweeper cleanup orphan coding files successfully and took "
          + (result.getEndTimeMs() - result.getStartTimeMs()) / 1000 + " seconds");
      raidNode.increaseZombieSweeperTaskDone();
      raidNode.scheduleZombieSweeperTask(conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL_DEFAULT));
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.info("ZombieSweeper fail to cleanup orphan coding files ");
      ReflectionUtils.logThreadInfo(LOG, "Thread dump from ZombieSweeperTask:onFailure", 1000);
      raidNode.increaseZombieSweeperTaskDone();
      raidNode.scheduleZombieSweeperTask(conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL_DEFAULT));
    }

    private void sweepDirectory(Path dir) throws IOException {
      final FileSystem fs = FileSystem.get(conf);
      Queue<Path> toCleanup = RaidTask.RaidTaskUtils.traverseDirectoryTree(fs, dirToSweep,
        new RaidTaskUtils.Filter() {
          public boolean check(Path file) throws IOException {
            if (fs.isDirectory(file)) {
              return false;
            }
            if (BlockCodec.isCodingFile(file)) {
              Path sourceFile = BlockCodec.getCodingFileSource(file);
              if (!fs.exists(sourceFile)) {
                return true;
              }
            }
            return false;
          }
        });

      for (Path orphanFile : toCleanup) {
        try {
          fs.delete(orphanFile, false);
          LOG.info("Cleaned a zombie file " + orphanFile.toString());
          cleanedFiles++;
        } catch (IOException ioe) {
          // Log a message and try to delete other zombie files
          LOG.warn("Fail to delete zombie file " + orphanFile.toString(), ioe);
        }
      }
    }

    @VisibleForTesting
    public int numOfCleanedZombie() {
      return cleanedFiles;
    }
  }

  /**
   * Task to collect information of files need to encode and decode.
   */
  public static class CollectRaidInfoTask extends RaidTask<TaskResult> {

    private final Configuration conf;
    private Collector collector;
    private String lastBatchRaidTaskId;
    private String lastCollectRaidInfoTaskId;
    private Path resultDirPath;
    private boolean nothingToDo;

    public CollectRaidInfoTask(RaidNode raidNode, Policy policy, Configuration conf)
        throws IOException {
      super(raidNode);
      this.conf = conf;
      try {
        this.lastBatchRaidTaskId = MRUtils.readJobId(this.conf, Coder.getJobIdFilePath());
      } catch (FileNotFoundException e) {
        this.lastBatchRaidTaskId = null;
      }

      try {
        this.lastCollectRaidInfoTaskId = MRUtils.readJobId(this.conf, Collector.getJobIdFilePath());
      } catch (FileNotFoundException e) {
        this.lastCollectRaidInfoTaskId = null;
      }

      List<Path> rootDirs = policy.getCandidateDirs();

      if (rootDirs.size() != 0) {
        String resultDir = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY);
        Preconditions.checkNotNull(resultDir);
        resultDirPath = new Path(resultDir + "/" + System.currentTimeMillis());
        this.collector = new Collector(rootDirs, resultDirPath, conf);
        nothingToDo = false;
      } else {
        nothingToDo = true;
      }
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      // Ensure that legacy tasks are killed
      if (this.lastBatchRaidTaskId != null) {
        MRUtils.killJob(conf, lastBatchRaidTaskId);
      }
      if (this.lastCollectRaidInfoTaskId != null) {
        MRUtils.killJob(conf, lastCollectRaidInfoTaskId);
      }

      // Run the collector
      if (!nothingToDo) {
        collector.run();
      }
      long endTimeMs = System.currentTimeMillis();
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Collect raid info task success, timeConsumedMs=" + result.getTimeConsumedMs());

      try {
        // Start the batch raid task
        if (!nothingToDo) {
          BatchRaidTask task = new BatchRaidTask(raidNode, resultDirPath, conf);
          raidNode.submitTask(task);
        } else {
          raidNode.increaseEncodeTaskDone();
          raidNode.scheduleEncodeTask(conf.getLong(
            HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
            HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
        }
      } catch (IOException e) {
        raidNode.increaseEncodeTaskDone();
        raidNode.scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
        LOG.fatal("Cannot start batch raid task", e);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Collect raid info task failed", t);
      raidNode.increaseEncodeTaskDone();
      // Should we retry a little bit earlier?
      raidNode.scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
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
    private final Coder coder;
    private final Path collectResultDir;
    private final FileSystem fs;

    public BatchRaidTask(RaidNode raidNode, Path collectResultDir, Configuration conf)
        throws IOException {
      super(raidNode);
      this.conf = conf;
      this.lastCollectRaidInfoTaskId = MRUtils.readJobId(this.conf, Collector.getJobIdFilePath());
      this.collectResultDir = collectResultDir;
      this.fs = FileSystem.get(this.conf);

      String resultDir = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_KEY);
      Preconditions.checkNotNull(resultDir);
      Path resultDirPath = new Path(resultDir + "/" + System.currentTimeMillis());

      int mapTaskNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT);

      this.coder = new Coder(new Path(this.collectResultDir.toString() + "/part-r-00000"),
          mapTaskNum, resultDirPath, conf);
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      // Ensure that last collect task is successful
      Path successFile = new Path(collectResultDir.toString() + "/" + "_SUCCESS");
      Path resultFile = new Path(collectResultDir.toString() + "/part-r-00000");
      if (!fs.exists(successFile) || !fs.exists(resultFile)) {
        throw new IOException("The last collect task is failed, "
            + "can't start the batch raid task.");
      }

      coder.run();
      long endTimeMs = System.currentTimeMillis();
      TaskResult result = new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);

      return result;
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Batch raid task finished successfully, timeConsumedMs="
          + result.getTimeConsumedMs());

      raidNode.increaseEncodeTaskDone();
      raidNode.scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.warn("Batch raid task failed", t);

      raidNode.increaseEncodeTaskDone();
      raidNode.scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));
    }

    private void startCollectTask() throws IOException {
      CollectRaidInfoTask task = new CollectRaidInfoTask(raidNode, raidNode.getPolicyInfos(null),
          conf);
      raidNode.submitTask(task);
    }
  }

  public static class RaidTaskUtils {

    public static interface Filter {
      public boolean check(Path file) throws IOException;
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
     * Depth first traverses the specified directory tree to get all appropriate files.
     */
    public static Queue<Path> traverseDirectoryTree(FileSystem fs, Path rootDir, Filter filter) {
      Preconditions.checkArgument(rootDir != null);
      Stack<Path> stack = new Stack<Path>();
      Set<Path> visited = new HashSet<Path>();
      Map<Path, ChildrenInfo> childrenInfos = new HashMap<Path, ChildrenInfo>();
      Queue<Path> result = new LinkedList<Path>();

      stack.add(rootDir);
      visited.add(rootDir);
      while (!stack.isEmpty()) {
        Path path = null;
        try {
          path = stack.peek();

          ChildrenInfo childrenInfo = childrenInfos.get(path);
          if (childrenInfo == null) {
            FileStatus[] children = fs.listStatus(path);
            childrenInfo = new ChildrenInfo(children);
            childrenInfos.put(path, childrenInfo);
          }

          if (fs.isDirectory(path) && childrenInfo.hasNextChild()) {
            FileStatus nextChild = childrenInfo.nextChild();
            if (!visited.contains(nextChild.getPath())) {
              visited.add(nextChild.getPath());
              stack.add(nextChild.getPath());
            }
          } else {
            if (filter.check(path)) {
              result.add(new Path(path.toUri().getPath()));
            }
            stack.pop();
          }
        } catch (IOException e) {
          stack.pop();
          LOG.warn("Error occured while processing path " + path, e);
        }
      }
      return result;
    }
  }
}
