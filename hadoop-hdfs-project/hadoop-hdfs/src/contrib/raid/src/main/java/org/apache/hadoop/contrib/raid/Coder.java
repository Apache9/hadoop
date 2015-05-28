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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Preconditions;

/**
 * A Coder is used to do the encoding and decoding jobs according to the files information collected
 * by the Collector{@link Collector}. It will use a MapReduce job to do the work.
 */
public class Coder {

  private static final Log LOG = LogFactory.getLog(Coder.class);

  public enum CounterName {
    EncodeFiles, EncodeFail, EncodeBytes, DecodeSuccess, DecodeFail, InvalidTaskType
  }

  private final Path collectorResultFile;
  private final int mapTaskNum;
  private final Path outputPath;
  private final Configuration conf;
  private Job job;

  public Coder(Path collectorResultFile, int mapTaskNum, Path outputPath, Configuration conf) {
    Preconditions.checkNotNull(collectorResultFile);
    Preconditions.checkArgument(mapTaskNum > 0);
    Preconditions.checkNotNull(conf);
    this.collectorResultFile = collectorResultFile;
    this.mapTaskNum = mapTaskNum;
    this.outputPath = outputPath;
    this.conf = conf;
  }

  public void run() throws IOException, ClassNotFoundException, InterruptedException {
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY, mapTaskNum);
    conf.set(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_FILE_KEY,
      collectorResultFile.toString());

    job = Job.getInstance(conf, "RaidNode-Coder");
    MRUtils.cacheCodecLib(conf, job);
    job.setJarByClass(Coder.class);
    job.setMapperClass(CoderMapper.class);

    job.setInputFormatClass(RaidFileInfoInputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setSpeculativeExecution(false);
    job.setNumReduceTasks(0);
    job.submit();
    MRUtils.writeJobId(conf, getJobIdFilePath(), getJobId());

    if (!job.waitForCompletion(true)) {
      throw new IOException("Wait for job completion failed");
    }
  }

  public Counter getCounter(CounterName name) throws IOException {
    Preconditions.checkNotNull(job);
    return job.getCounters().findCounter(name);
  }

  public String getJobId() {
    Preconditions.checkNotNull(job);
    return job.getJobID().toString();
  }

  public static Path getJobIdFilePath() {
    Path raidRoot = BlockCodec.getRaidRoot();
    return new Path(raidRoot.toString() + "/" + "coder.jobid");
  }

  /**
   * Map task to do encode/decode work.
   */
  public static class CoderMapper extends Mapper<Object, Text, Object, Object> {

    private Configuration conf;
    private BlockCodec blockCodec;
    private Counter encodeFiles;
    private Counter encodeBytes;
    private Counter encodeFail;
    private Counter decodeSuccess;
    private Counter decodeFail;
    private Counter invalidTaskType;
    private long encodeTimeWindow;
    private DistributedFileSystem dfs;
    private long maxFilesPerMapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
      this.blockCodec = new BlockCodec(this.conf);
      this.encodeFiles = context.getCounter(CounterName.EncodeFiles);
      this.encodeBytes = context.getCounter(CounterName.EncodeBytes);
      this.encodeFail = context.getCounter(CounterName.EncodeFail);
      this.decodeSuccess = context.getCounter(CounterName.DecodeSuccess);
      this.decodeFail = context.getCounter(CounterName.DecodeFail);
      this.invalidTaskType = context.getCounter(CounterName.InvalidTaskType);
      this.encodeTimeWindow = this.conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT);
      this.dfs = (DistributedFileSystem) FileSystem.get(this.conf);
      this.maxFilesPerMapper = this.conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAX_FILES_PER_MAPPER,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAX_FILES_PER_MAPPER_DEFAULT);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
      String info = value.toString();
      String[] tokens = info.split("\t");
      Path file = new Path(tokens[0].trim());
      // TBD: The second token should be the group num to be encoded
      try {
        if (encodeFiles.getValue() >= maxFilesPerMapper) {
          // Use the HDFS_RAIDNODE_CODER_MAX_FILES_PER_MAPPER to prevent coder from running too long
          return;
        }
        FileStatus status = FileSystem.get(conf).getFileStatus(file);
        long fileModTime = status.getModificationTime();
        long currentTimeMs = System.currentTimeMillis();
        if ((fileModTime + encodeTimeWindow > currentTimeMs) || !dfs.isFileClosed(file)) {
          // The file might have been re-opened for write
          return;
        }
        blockCodec.encode(file);
        LOG.debug("Encoded file " + file.toString());
        encodeFiles.increment(1);
        encodeBytes.increment(((status.getLen() + status.getBlockSize() - 1) / status
            .getBlockSize()) * status.getBlockSize());
      } catch (Exception e) {
        encodeFail.increment(1);
      }
    }
  }

  /**
   * The raid file information input format class.
   */
  // TBD: Load balance - creating splits according to blocks to be encoded.
  // Collecting task should also put the file blocks information in its result file
  private static class RaidFileInfoInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);

      String collectResultFile = conf
          .get(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_FILE_KEY);
      int mapTaskNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT);
      List<InputSplit> result = new ArrayList<InputSplit>(mapTaskNum);

      FSDataInputStream in = fs.open(new Path(collectResultFile));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line;
      int count = 0;
      while ((line = reader.readLine()) != null) {
        RaidFileInfoSplit split;
        if (result.size() < mapTaskNum) {
          split = new RaidFileInfoSplit();
          result.add(split);
        } else {
          split = (RaidFileInfoSplit) result.get(count++ % mapTaskNum);
        }
        split.addFileInfo(line.trim());
      }
      reader.close();
      LOG.debug("Get " + result.size() + " splits for coder");
      return result;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      RaidFileInfoSplit raidFileInfoSplit = (RaidFileInfoSplit) split;
      RaidFileInfoReader reader = new RaidFileInfoReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  /**
   * The raid file information split class.
   */
  private static class RaidFileInfoSplit extends InputSplit implements Writable {

    private List<String> raidFileInfos;

    public RaidFileInfoSplit() {
      raidFileInfos = new LinkedList<String>();
    }

    public void addFileInfo(String fileInfo) {
      raidFileInfos.add(fileInfo);
    }

    public List<String> getRaidFileInfos() {
      return raidFileInfos;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return raidFileInfos.size();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      String[] infos = new String[raidFileInfos.size()];
      raidFileInfos.toArray(infos);
      WritableUtils.writeStringArray(out, infos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String[] infos = WritableUtils.readStringArray(in);
      raidFileInfos.addAll(Arrays.asList(infos));
    }
  }

  /**
   * The raid file information record reader class.
   */
  private static class RaidFileInfoReader extends RecordReader<Object, Text> {

    private RaidFileInfoSplit split;
    private int nextIndex;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.split = (RaidFileInfoSplit) split;
      this.nextIndex = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      List<String> raidFileInfos = split.getRaidFileInfos();
      if (raidFileInfos == null || raidFileInfos.isEmpty()) {
        return false;
      }
      return nextIndex < raidFileInfos.size();
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return new Text(split.getRaidFileInfos().get(nextIndex++));
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return nextIndex * 1.0f / split.getRaidFileInfos().size();
    }

    @Override
    public void close() throws IOException {
    }
  }
}
