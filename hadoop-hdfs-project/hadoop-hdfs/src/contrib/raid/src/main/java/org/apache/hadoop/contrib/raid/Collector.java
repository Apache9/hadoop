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
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.RaidTask.RaidTaskUtils;
import org.apache.hadoop.contrib.raid.RaidTask.TaskPurpose;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * A Collector is used to collect the information of files that need encoding and decoding. It will
 * use a MapReduce job to do the work.
 */
public class Collector {

  private static final Log LOG = LogFactory.getLog(Collector.class);

  private final List<Path> rootDirs;
  private final Path resultDir;
  private final Configuration conf;
  private Job job;
  private TaskPurpose purpose;

  public enum CounterName {
    FilesScannedForCoder, FilesScannedForMover, RecordReaderCreated, ReaderInitialized
  }

  public Collector(List<Path> rootDirs, Path resultDir, TaskPurpose purpose, Configuration conf) {
    Preconditions.checkArgument(rootDirs != null && rootDirs.size() > 0);
    Preconditions.checkNotNull(resultDir);
    Preconditions.checkNotNull(conf);
    this.rootDirs = rootDirs;
    this.resultDir = resultDir;
    this.conf = conf;
    this.purpose = purpose;
  }

  /**
   * Runs the collect raid file information MapReduce job.
   */
  public void run() throws IOException, ClassNotFoundException, InterruptedException {
    String strRootDirs = StringUtils.arrayToString(getStrDirs(rootDirs));
    conf.set(HdfsRaidConfigKeys.HDFS_RAIDNODE_SCAN_ROOT_DIRS_KEY, strRootDirs);
    conf.setEnum(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_TASK_TYPE, purpose);

    job = Job.getInstance(conf, "RaidNode-Collector-" + purpose.toString());
    job.setJarByClass(Collector.class);
    job.setMapperClass(CollectorMapper.class);
    job.setReducerClass(CollectorReducer.class);

    job.setInputFormatClass(RaidDirInfoInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, resultDir);

    job.setSpeculativeExecution(false);
    job.setNumReduceTasks(1);
    job.submit();
    MRUtils.writeJobId(conf, getJobIdFilePath(purpose), getJobId());

    if (!job.waitForCompletion(true)) {
      throw new IOException("Wait for job completion failed");
    }
  }

  public String getJobId() {
    Preconditions.checkNotNull(job);
    return job.getJobID().toString();
  }

  public Counter getCounter(Enum<?> name) throws IOException {
    Preconditions.checkNotNull(job);
    return job.getCounters().findCounter(name);
  }

  public static Path getJobIdFilePath(TaskPurpose purpose) {
    Path raidRoot = BlockCodec.getRaidRoot();
    return new Path(raidRoot.toString() + "/" + purpose.toString() + "collector.jobid");
  }

  private String[] getStrDirs(List<Path> dirs) {
    Preconditions.checkArgument(dirs != null && !dirs.isEmpty());
    String[] result = new String[dirs.size()];
    int index = 0;
    for (Path dir : dirs) {
      result[index++] = dir.toString();
    }
    return result;
  }

  /**
   * The map task definition for the Collector.
   */
  public static class CollectorMapper extends Mapper<Object, Text, Text, Text> {

    private FileSystem fs;
    private NetworkTopology topology;
    private int dataBlocksNum;
    private int codingBlocksNum;
    private boolean shuffleBlksAmongRacks;
    private TaskPurpose purpose;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      purpose = conf.getEnum(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_TASK_TYPE,
        TaskPurpose.InvalidType);

      if (purpose == TaskPurpose.InvalidType) {
        throw new IOException("Unknow task type for CollectorMapper");
      }

      if (purpose == TaskPurpose.BlockMover) {
        topology = new NetworkTopology();
        if (!(fs instanceof DistributedFileSystem)) {
          throw new IOException("The file system is not a distributed file system");
        }
        DatanodeInfo[] liveNodes = ((DistributedFileSystem) fs).getClient().datanodeReport(
          DatanodeReportType.LIVE);
        for (DatanodeInfo di : liveNodes) {
          topology.add(di);
        }

        dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
        codingBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
        shuffleBlksAmongRacks = conf.getBoolean(
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_SHUFFLE_RACKS,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_SHUFFLE_RACKS_DEFAULT);
      }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
      Path file = new Path(value.toString());

      if (purpose == TaskPurpose.BlockMover) {

        if (!BlockCodec.isCodingFile(file)) {
          // This is not a coding file in the /raid directory, which might be a temporary or other
          // meta files. Just skip it.
          LOG.debug(file.toString() + " is not a coding file, skip moving check");
          return;
        }
        // This is scanning coding files, which is used to collect blocks that need to be moved.
        Path sourceFile = BlockCodec.getCodingFileSource(file);
        if (!fs.exists(sourceFile)) {
          // Zombie coding file. Nothing to do.
          LOG.debug(file.toString() + " is a zombie file, skip moving check");
          return;
        }

        FileStatus sourceStatus = fs.getFileStatus(sourceFile);
        FileStatus fileStatus = fs.getFileStatus(file);
        int blksNum = (int) ((sourceStatus.getLen() + sourceStatus.getBlockSize() - 1) / sourceStatus
            .getBlockSize());
        int groupNum = (blksNum + dataBlocksNum - 1) / dataBlocksNum; 

        if (!((DistributedFileSystem) fs).isFileClosed(file)) {
          // The encoding is on-going. Do nothing.
          LOG.debug(file.toString() + " is not closed yet, skip moving check");
          return;
        }

        if (groupNum * codingBlocksNum * fileStatus.getBlockSize() != fileStatus.getLen()) {
          StringBuilder sb = new StringBuilder();
          sb.append("Something goes wrong with ").append(file.toString())
              .append(" - the expected coding file size is ")
              .append(groupNum * codingBlocksNum * fileStatus.getBlockSize())
              .append(" whereas actually is ").append(fileStatus.getLen())
              .append(", dataBlocksNum is ").append(dataBlocksNum)
              .append(", codingBlocksNum is ").append(codingBlocksNum)
              .append(", blockSize is ").append(fileStatus.getBlockSize())
              .append(", file size is ").append(fileStatus.getLen())
              .append(". Source file size is ").append(sourceStatus.getLen());
          LOG.warn(sb.toString());
          // The file might be left by a unfinished coder job.
          return;
        }

        LocatedBlocks sourceBlks = ((DistributedFileSystem) fs).getClient().getLocatedBlocks(
          sourceFile.toString(), 0, sourceStatus.getLen());
        LocatedBlocks codingBlks = ((DistributedFileSystem) fs).getClient().getLocatedBlocks(
          file.toString(), 0, fileStatus.getLen());
        Map<Integer, Set<LocatedBlock>> groupToLoc = new TreeMap<Integer, Set<LocatedBlock>>();

        for (LocatedBlock loc : sourceBlks.getLocatedBlocks()) {
          int blkIndex = (int) (loc.getStartOffset() / sourceStatus.getBlockSize());
          int groupIndex = blkIndex / dataBlocksNum;
          if (groupToLoc.containsKey(groupIndex)) {
            groupToLoc.get(groupIndex).add(loc);
          } else {
            Set<LocatedBlock> locs = new HashSet<LocatedBlock>();
            locs.add(loc);
            groupToLoc.put(groupIndex, locs);
          }
        }

        for (LocatedBlock loc : codingBlks.getLocatedBlocks()) {
          int blkIndex = (int) (loc.getStartOffset() / fileStatus.getBlockSize());
          int groupIndex = blkIndex / codingBlocksNum;
          if (groupToLoc.containsKey(groupIndex)) {
            groupToLoc.get(groupIndex).add(loc);
          } else {
            Set<LocatedBlock> locs = new HashSet<LocatedBlock>();
            locs.add(loc);
            groupToLoc.put(groupIndex, locs);
          }
        }

        for (Map.Entry<Integer, Set<LocatedBlock>> entry : groupToLoc.entrySet()) {
          if (needMove(entry)) {
            LOG.debug("Added file " + sourceFile.toString() + " as mover candidate.");
            context.write(new Text(sourceFile.toString()), new Text(entry.getKey().toString()));
          }
        }

      } else if (purpose == TaskPurpose.Encode) {
        // This is scanning source files, which is used to collect files to be encoded.
        Path codingFile = BlockCodec.getCodingFile(file);
        if (!fs.exists(codingFile)) {
          // TBD: Change this to write out group index to be encoded
          context.write(value, new Text("Encode"));
        }
      }
    }

    private boolean needMove(Map.Entry<Integer, Set<LocatedBlock>> grpLocs) {
      for (LocatedBlock lb : grpLocs.getValue()) {
        // If this group contains corrupt blocks or the replication is not 1 yet, skip this group.
        // It may be handled next time.
        if (lb.isCorrupt() || (lb.getLocations().length != 1)) {
          LOG.debug("Block " + lb.toString() + " is skipped.");
          return false;
        }
      }
      for (LocatedBlock lb : grpLocs.getValue()) {
        LOG.debug("Need move check of block " + lb.toString());
        for (LocatedBlock tmpLb : grpLocs.getValue()) {
          if (tmpLb == lb) {
            continue;
          }
          if ((lb.getLocations()[0]).equals(tmpLb.getLocations()[0])) {
            LOG.debug("Block " + lb.toString() + "and block " + 
                tmpLb.toString() + " are on same node.");
            return true;
          }
          if (shuffleBlksAmongRacks) {
            if (topology.isOnSameRack(lb.getLocations()[0], tmpLb.getLocations()[0])) {
              LOG.debug("Block " + lb.toString() + "and block " + 
                  tmpLb.toString() + " are on same rack.");
              return true;
            }
          }
        }
      }
      LOG.debug("Do not need to move group " + grpLocs.getKey());
      return false;
    }
  }

  /**
   * The reduce task definition for the Collector.
   */
  public static class CollectorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      for (Text value : values) {
        context.write(key, value);
      }
    }
  }

  /**
   * The raid directory information input format class for the Collector MapReduce job.
   */
  private static class RaidDirInfoInputFormat extends InputFormat {

    private Counter recordReaderCreated;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String rootDirs = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_SCAN_ROOT_DIRS_KEY);
      if (rootDirs == null || rootDirs.isEmpty()) {
        throw new IOException(HdfsRaidConfigKeys.HDFS_RAIDNODE_SCAN_ROOT_DIRS_KEY
            + " isn't configured");
      }
      int depth = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_SCAN_ROOT_DIRS_SPLIT_DEPTH,
              HdfsRaidConfigKeys.HDFS_RAIDNODE_SCAN_ROOT_DIRS_SPLIT_DEPTH_DEFAULT);

      String[] dirs = rootDirs.split(",");
      List<Path> moreDirs = new LinkedList<Path>();
      FileSystem fs = FileSystem.get(conf);
      for (String dir : dirs) {
        moreDirs.addAll(RaidTaskUtils.getSubDirectoriesAndFiles(fs, new Path(dir.trim()), depth));
      }
      List<InputSplit> splits = new ArrayList<InputSplit>(moreDirs.size());
      for (Path dir : moreDirs) {
        splits.add(new RaidDirInfoSplit(dir));
      }
      return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      RaidDirInfoReader reader = new RaidDirInfoReader();
      this.recordReaderCreated = context.getCounter(CounterName.RecordReaderCreated);
      recordReaderCreated.increment(1);
      // reader.initialize(split, context);
      return reader;
    }
  }

  /**
   * The raid directory information split class.
   */
  private static class RaidDirInfoSplit extends InputSplit implements Writable {

    private Path rootDir;

    public RaidDirInfoSplit() {
    }

    public RaidDirInfoSplit(Path rootDir) {
      this.rootDir = rootDir;
    }

    public Path getRootDir() {
      return rootDir;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, rootDir.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      rootDir = new Path(WritableUtils.readString(in));
    }
  }

  /**
   * The raid directory information reader class.
   */
  private static class RaidDirInfoReader extends RecordReader<Object, Text> {

    private RaidDirInfoSplit split;
    private Configuration conf;
    private FileSystem fs;
    private Queue<Path> dirs;
    private long raidFileTimeWindow;
    private int totalNum;
    private TaskPurpose purpose;
    private int dataBlocksNum;
    private int codingBlocksNum;
    private boolean skipSpaceCheckForTest;

    private Counter filesScannedForCoder;
    private Counter filesScannedForMover;
    private Counter readerInitialized;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.split = (RaidDirInfoSplit) split;
      this.conf = context.getConfiguration();
      this.fs = FileSystem.get(this.conf);
      this.raidFileTimeWindow = this.conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT);
      this.purpose = conf.getEnum(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_TASK_TYPE,
        TaskPurpose.InvalidType);
      this.filesScannedForCoder = context.getCounter(CounterName.FilesScannedForCoder);
      this.filesScannedForMover = context.getCounter(CounterName.FilesScannedForMover);
      this.readerInitialized = context.getCounter(CounterName.ReaderInitialized);
      this.dataBlocksNum = this.conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
      this.codingBlocksNum = this.conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
      this.skipSpaceCheckForTest = this.conf.getBoolean(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_SKIP_ENCODE_SPACE_CHECK_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_SKIP_ENCODE_SPACE_CHECK_DEAFULT);

      readerInitialized.increment(1);
      this.dirs = RaidTaskUtils.traverseDirectoryTree(fs, this.split.getRootDir(),
        new RaidTaskUtils.Filter() {
          public boolean check(Path file, RaidMetrics metrics) throws IOException {
            assert metrics == null : "MR metrics are collected through counters";
            if (!fs.isFile(file)) {
              return false;
            }

            if (purpose == TaskPurpose.Encode) {
              FileStatus fileStatus = fs.getFileStatus(file);
              long currentTimeMs = System.currentTimeMillis();
              long fileModTime = fileStatus.getModificationTime();
              filesScannedForCoder.increment(1);
              // Skip already encoded files
              if (BlockCodec.isFileEncoded(fs, file)) {
                LOG.debug("Skip " + file.toString() + " since it is already encoded.");
                return false;
              }
              long blocks = (fileStatus.getLen() + fileStatus.getBlockSize() - 1)
                  / fileStatus.getBlockSize();
              long grps = (blocks + dataBlocksNum - 1) / dataBlocksNum;
              long origSpace = fileStatus.getReplication() * fileStatus.getLen();
              long encodedSapce = fileStatus.getLen()
                  + (grps * codingBlocksNum * fileStatus.getBlockSize());
              if (origSpace <= encodedSapce && skipSpaceCheckForTest == false) {
                LOG.debug("Skip " + file.toString()
                    + " since encoding it would not save any space: original space consumed is "
                    + origSpace + ", estimated space comsumption after encoding is " + encodedSapce);
                return false;
              }
              if (fs instanceof DistributedFileSystem) {
                DistributedFileSystem dfs = (DistributedFileSystem) fs;
                if ((fileModTime + raidFileTimeWindow < currentTimeMs) && dfs.isFileClosed(file)) {
                  LOG.debug("Selected " + file.toString() + "to encode: consumed space is "
                      + origSpace + ", estimated space comsumption after encoding is  "
                      + encodedSapce);
                  return true;
                }
                LOG.debug("Skip "
                    + file.toString()
                    + " as it is still open for write or its last modification time is still in grace period ");
              }
            } else if (purpose == TaskPurpose.BlockMover) {
              LOG.debug("Checking file " + file.toString() + " for BlockMover.");
              if (BlockCodec.isCodingFile(file)) {
                if (fs instanceof DistributedFileSystem) {
                  DistributedFileSystem dfs = (DistributedFileSystem) fs;
                  filesScannedForMover.increment(1);
                  if (dfs.isFileClosed(file)) {
                    LOG.debug("File " + file.toString() + " is picked for BlockMover check.");
                    return true;
                  }
                  LOG.debug("File " + file.toString() + " is skipped since it is not closed.");
                }
              }
            }
            return false;
          }
        });
      this.totalNum = this.dirs.size();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return !dirs.isEmpty();
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      Path currentDir = dirs.poll();
      return new Text(currentDir.toString());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return dirs.size() * 1.0f / totalNum;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
