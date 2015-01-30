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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.Coder.CounterName;
import org.apache.hadoop.contrib.raid.RaidTask.RaidTaskUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A Fixer is used to fix corrupted blocks in the HDFS. It will use a MapReduce job to do the work.
 */
public class Fixer {

  private static final Log LOG = LogFactory.getLog(Fixer.class);

  private final Configuration conf;
  private final Path outputPath;
  private Job job;

  public Fixer(Path outputPath, Configuration conf) {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
    this.outputPath = outputPath;
  }

  public void run() throws IOException, ClassNotFoundException, InterruptedException {

    job = Job.getInstance(conf, "RaidNode-Fixer");
    job.setJarByClass(Fixer.class);
    job.setMapperClass(FixerMapper.class);

    job.setInputFormatClass(FixerInfoInputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);

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
    return new Path(raidRoot.toString() + "/" + "fixer.jobid");
  }

  // If a DN fails, after the heat beat expires, the NN will mark the DN as dead and will move
  // blocks hosted by the DN to the list of needReplication.
  // And since those blocks' original replications are 1, they would be placed in the
  // corruption queue in the list. Given that, if we fix all corrupted blocks, we also fixed
  // blocks whose DN is dead. Hopefully, the NN would not change the behavior.
  //
  // Usually, there should not be too many corrupt blocks, so it would be ok to handle it in
  // RaidNode with one thread.
  @VisibleForTesting
  public static Map<RaidTaskUtils.FixerItem, Set<Integer>> collectFixerInfo(Configuration conf)
      throws IOException {
    Map<RaidTaskUtils.FixerItem, Set<Integer>> info = new TreeMap<RaidTaskUtils.FixerItem, Set<Integer>>();
    Set<Path> corruptFiles = new HashSet<Path>();
    FileSystem fs = FileSystem.get(conf);
    RemoteIterator<Path> rip = fs.listCorruptFileBlocks(new Path("/"));
    int dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
    int codingBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);

    while (true) {
      try {
        if (rip.hasNext()) {
          Path file = rip.next();
          // For corrupted files other than encoded files, the NN will do extra replication to fix
          // it.
          // We only care about encoded files and their coding files.
          if (BlockCodec.isCodingFile(file) || BlockCodec.isFileEncoded(fs, file)) {
            corruptFiles.add(file);
          }
        } else {
          break;
        }
      } catch (IOException ioe) {
        // Fix what we got so far
        LOG.warn("Something wrong to get corrupt file list", ioe);
        break;
      }
    }

    for (Path file : corruptFiles) {
      try {
        if (!BlockCodec.isCodingFile(file)) {
          // This is the source file corruption
          FileStatus fileStatus = fs.getFileStatus(file);
          BlockLocation[] blkLocs = fs.getFileBlockLocations(file, 0, fileStatus.getLen());
          for (BlockLocation blk : blkLocs) {
            if (blk.isCorrupt()) {
              int blkIndex = (int) (blk.getOffset() / fileStatus.getBlockSize());
              int group = blkIndex / dataBlocksNum;
              RaidTaskUtils.FixerItem item = new RaidTaskUtils.FixerItem(file, group);
              if (info.containsKey(item)) {
                info.get(item).add(blkIndex);
              } else {
                Set<Integer> corruptBlks = new TreeSet<Integer>();
                corruptBlks.add(blkIndex);
                info.put(item, corruptBlks);
              }
            }
          }
        } else {
          // The corrupted file is a coding file
          Path sourceFile = BlockCodec.getCodingFileSource(file);
          FileStatus sourceStatus = fs.getFileStatus(sourceFile);
          FileStatus fileStatus = fs.getFileStatus(file);
          BlockLocation[] blkLocs = fs.getFileBlockLocations(file, 0, fileStatus.getLen());
          for (BlockLocation blk : blkLocs) {
            if (blk.isCorrupt()) {
              int sourceFileBlks = (int) ((sourceStatus.getLen() + sourceStatus.getBlockSize() - 1) / sourceStatus
                  .getBlockSize());
              int blkIndex = (int) (blk.getOffset() / fileStatus.getBlockSize());
              int group = blkIndex / codingBlocksNum;
              // In the implementation of decode, it will calculate the coding file's index from the
              // last block of source file. We do the same thing here.
              blkIndex += sourceFileBlks;
              RaidTaskUtils.FixerItem item = new RaidTaskUtils.FixerItem(file, group);
              if (info.containsKey(item)) {
                info.get(item).add(blkIndex);
              } else {
                Set<Integer> corruptBlks = new TreeSet<Integer>();
                corruptBlks.add(blkIndex);
                info.put(item, corruptBlks);
              }
            }
          }
        }
      } catch (IOException ioe) {
        // Ignore
        LOG.warn("Some thing wrong when collecting fixer task info", ioe);
      }
    }
    return info;
  }

  /**
   * Map task to do fixer work.
   */
  public static class FixerMapper extends Mapper<Object, Text, Object, Object> {

    private Configuration conf;
    private BlockCodec blockCodec;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
      this.blockCodec = new BlockCodec(this.conf);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
      String info = value.toString();
      String[] tokens = info.split("\\s+");
      Path file = new Path(tokens[0].trim());
      int[] blocks = new int[tokens.length - 1];

      for (int i = 0; i < blocks.length; i++) {
        try {
          blocks[i] = Integer.parseInt(tokens[i + 1]);
        } catch (NumberFormatException e) {
          // The input is generated from the recorder, the FormatExpection should not happen.
          LOG.warn("Incorrect format found in fixer task info", e);
          throw new IOException("Wrong block numbers");
        }
      }

      if (blocks.length > blockCodec.getCodingBlocksNum()) {
        // TBD : Add metrics
        // Fail earlier if we cannot fix it.
        StringBuilder sb = new StringBuilder();
        sb.append("Detect corruption in blocks ");
        for (int blk : blocks) {
          sb.append("\t").append(blk);
        }
        sb.append(" in file ").append(file.toString()).append(".")
            .append(" The corrupted blks is larger than ").append(blockCodec.getCodingBlocksNum())
            .append(", which can not be reconstructed.");
        LOG.warn(sb.toString());
        return;
      }

      try {
        blockCodec.decode(file, blocks);
      } catch (Exception e) {
        // Something wrong
        StringBuilder sb = new StringBuilder();
        sb.append("Fail to decode blocks of file ").append(file.toString()).append(" :");
        for (int blk : blocks) {
          sb.append("\t" + blk);
        }
        LOG.warn(sb.toString(), e);
        throw new IOException("Fail to decode");
      }
    }
  }

  /**
   * The fixer information input format class.
   */
  // Each map task will handle one group of blocks (default is 6+3). We cannot do it in finer grain.
  // This level of load balance should be ok.
  private static class FixerInfoInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Map<RaidTaskUtils.FixerItem, Set<Integer>> fixerInfo = Fixer.collectFixerInfo(conf);
      int mapTaskNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT);
      List<InputSplit> result = new ArrayList<InputSplit>(mapTaskNum);

      int count = 0;
      for (Map.Entry<RaidTaskUtils.FixerItem, Set<Integer>> item : fixerInfo.entrySet()) {
        FixerInfoSplit split;
        if (result.size() < mapTaskNum) {
          split = new FixerInfoSplit();
          result.add(split);
        } else {
          split = (FixerInfoSplit) result.get((count++ % mapTaskNum));
        }

        StringBuilder sb = new StringBuilder();
        sb.append(item.getKey().getFile().toString()).append("\t");
        for (int blk : item.getValue()) {
          sb.append(blk).append("\t");
        }
        split.addInfo(sb.toString());
      }

      return result;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Preconditions.checkState((split instanceof FixerInfoSplit));
      FixerInfoReader reader = new FixerInfoReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  /**
   * The raid file information split class.
   */
  private static class FixerInfoSplit extends InputSplit implements Writable {

    private List<String> splitInfos;

    public FixerInfoSplit() {
      splitInfos = new LinkedList<String>();
    }

    public void addInfo(String item) {
      splitInfos.add(item);
    }

    public List<String> getFixerSplitInfo() {
      return splitInfos;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return splitInfos.size();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      String[] infos = new String[splitInfos.size()];
      splitInfos.toArray(infos);
      WritableUtils.writeStringArray(out, infos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String[] infos = WritableUtils.readStringArray(in);
      splitInfos.addAll(Arrays.asList(infos));
    }
  }

  /**
   * The raid file information record reader class.
   */
  private static class FixerInfoReader extends RecordReader<Object, Text> {

    private FixerInfoSplit split;
    private int nextIndex;
    private Text currentVal;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.split = (FixerInfoSplit) split;
      this.nextIndex = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      List<String> fixerInfo = split.getFixerSplitInfo();
      if (fixerInfo == null || fixerInfo.isEmpty()) {
        return false;
      }

      if (nextIndex < fixerInfo.size()) {
        currentVal = new Text(split.getFixerSplitInfo().get(nextIndex++));
        return true;
      }

      return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return currentVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return nextIndex * 1.0f / split.getFixerSplitInfo().size();
    }

    @Override
    public void close() throws IOException {
    }
  }
}
