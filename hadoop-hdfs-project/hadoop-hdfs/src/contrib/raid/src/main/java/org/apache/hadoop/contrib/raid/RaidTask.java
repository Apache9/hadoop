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
package org.apache.hadoop.contrib.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
    Success,
    Fail
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
      LOG.info("Encode file " + file + " successful, consumed "
          + result.getStartTimeMs() + " ms");
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
      Preconditions.checkArgument(corruptedBlocks != null &&
          corruptedBlocks.length > 0);
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
      LOG.info("Decode corrupted blocks " + Arrays.toString(corruptedBlocks)
          + " of file " + file + " successful");
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.info("Decode corrupted blocks " + Arrays.toString(corruptedBlocks)
          + " of file " + file + " failed");
    }
  }

  /**
   * Task to collect information of files need to encode and decode.
   */
  public static class CollectRaidInfoTask extends RaidTask<TaskResult> {

    private final Configuration conf;
    private final Collector collector;
    private final String lastBatchRaidTaskId;
    private final String lastCollectRaidInfoTaskId;
    private final Path resultDirPath;

    public CollectRaidInfoTask(RaidNode raidNode,  Configuration conf)
        throws IOException {
      super(raidNode);
      this.conf = conf;
      this.lastBatchRaidTaskId = MRUtils.readJobId(this.conf,
          Coder.getJobIdFilePath());
      this.lastCollectRaidInfoTaskId = MRUtils.readJobId(this.conf,
          Collector.getJobIdFilePath());

      String[] rootDirs = conf.getStrings(
          HdfsRaidConfigKeys.HDFS_RAIDNODE_RAIDABLE_ROOT_DIRS_KEY);
      Preconditions.checkNotNull(rootDirs);
      String resultDir = conf.get(
          HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY);
      Preconditions.checkNotNull(resultDir);
      resultDirPath = new Path(resultDir + "/" + System.currentTimeMillis());
      this.collector = new Collector(convertPaths(rootDirs),
          resultDirPath, conf);
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      // Ensure that legacy tasks are killed
      MRUtils.killJob(conf, lastBatchRaidTaskId);
      MRUtils.killJob(conf, lastCollectRaidInfoTaskId);

      // Run the collector
      collector.run();
      long endTimeMs = System.currentTimeMillis();
      return new TaskResult(TaskStatus.Success, startTimeMs, endTimeMs);
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Collect raid info task success, timeConsumedMs=" +
          result.getTimeConsumedMs());

      try {
        // Start the batch raid task
        BatchRaidTask task = new BatchRaidTask(raidNode, resultDirPath, conf);
        raidNode.submitTask(task);
      } catch (IOException e) {
        LOG.fatal("Cannot start batch raid task", e);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Collect raid info task failed", t);

      // Retry collecting
      try {
        CollectRaidInfoTask task = new CollectRaidInfoTask(
            raidNode, raidNode.getConf());
        raidNode.submitTask(task);
      } catch (IOException e) {
        LOG.fatal("Cannot retry collecting", e);
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
    private final Coder coder;
    private final Path collectResultDir;
    private final FileSystem fs;

    public BatchRaidTask(RaidNode raidNode, Path collectResultDir,
        Configuration conf) throws IOException {
      super(raidNode);
      this.conf = conf;
      this.lastCollectRaidInfoTaskId = MRUtils.readJobId(this.conf,
          Collector.getJobIdFilePath());
      this.collectResultDir = collectResultDir;
      this.fs = FileSystem.get(this.conf);

      String resultDir = conf.get(
          HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_RESULT_DIR_KEY);
      Preconditions.checkNotNull(resultDir);
      Path resultDirPath = new Path(resultDir + "/" +
          System.currentTimeMillis());

      int mapTaskNum = conf.getInt(
          HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT);

      this.coder = new Coder(new Path(this.collectResultDir.toString() +
          "/part-r-00000"), mapTaskNum, resultDirPath, conf);
    }

    @Override
    public TaskResult call() throws Exception {
      long startTimeMs = System.currentTimeMillis();
      // Ensure that last collect task is successful
      Path successFile = new Path(collectResultDir.toString() + "/"
          + "_SUCCESS");
      Path resultFile = new Path(collectResultDir.toString() + "/part-r-00000");
      if (!fs.exists(successFile) || !fs.exists(resultFile)) {
        throw new IOException("The last collect task is failed, " +
            "can't start the batch raid task.");
      }

      coder.run();
      long endTimeMs = System.currentTimeMillis();
      TaskResult result = new TaskResult(TaskStatus.Success, startTimeMs,
          endTimeMs);

      // Sleep some time in case that coder job is finished too quickly.
      if (result.getTimeConsumedMs() < 10000) {
        Thread.sleep(5000);
      }
      return result;
    }

    @Override
    public void onSuccess(TaskResult result) {
      LOG.info("Batch raid task finished successfully, timeConsumedMs="
          + result.getTimeConsumedMs());

      try {
        startCollectTask();
      } catch (IOException e) {
        LOG.fatal("Cannot start collect raid info task");
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.warn("Batch raid task failed", t);

      try {
        startCollectTask();
      } catch (IOException e) {
        LOG.fatal("Cannot start collect raid info task");
      }
    }

    private void startCollectTask() throws IOException {
      CollectRaidInfoTask task = new CollectRaidInfoTask(raidNode, conf);
      raidNode.submitTask(task);
    }
  }
}
