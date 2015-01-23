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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * A Collector is used to collect the information of files that need encoding and decoding. It will
 * use a MapReduce job to do the work.
 */
public class Collector {

  public enum TaskType {
    Encode, Decode
  }

  private static final Log LOG = LogFactory.getLog(Collector.class);

  private final List<Path> rootDirs;
  private final Path resultDir;
  private final Configuration conf;
  private Job job;

  public Collector(List<Path> rootDirs, Path resultDir, Configuration conf) {
    Preconditions.checkArgument(rootDirs != null && rootDirs.size() > 0);
    Preconditions.checkNotNull(resultDir);
    Preconditions.checkNotNull(conf);
    this.rootDirs = rootDirs;
    this.resultDir = resultDir;
    this.conf = conf;
  }

  /**
   * Runs the collect raid file information MapReduce job.
   */
  public void run() throws IOException, ClassNotFoundException, InterruptedException {
    String strRootDirs = StringUtils.arrayToString(getStrDirs(rootDirs));
    conf.set(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAIDABLE_ROOT_DIRS_KEY, strRootDirs);

    job = Job.getInstance(conf, "RaidNode-Collector");
    job.setJarByClass(Collector.class);
    job.setMapperClass(CollectorMapper.class);
    job.setReducerClass(CollectorReducer.class);

    job.setInputFormatClass(RaidDirInfoInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, resultDir);

    job.setNumReduceTasks(1);
    job.submit();
    MRUtils.writeJobId(conf, getJobIdFilePath(), getJobId());

    if (!job.waitForCompletion(true)) {
      throw new IOException("Wait for job completion failed");
    }
  }

  public String getJobId() {
    Preconditions.checkNotNull(job);
    return job.getJobID().toString();
  }

  public static Path getJobIdFilePath() {
    Path raidRoot = BlockCodec.getRaidRoot();
    return new Path(raidRoot.toString() + "/" + "collector.jobid");
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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
      // Currently, only encode task is collected. Decode is triggered on
      // demand and may be supported in the future.
      Path codingFile = BlockCodec.getCodingFile(new Path(value.toString()));
      if (!fs.exists(codingFile)) {
        context.write(value, new Text(TaskType.Encode.name()));
      }
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
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String rootDirs = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAIDABLE_ROOT_DIRS_KEY);
      if (rootDirs == null || rootDirs.isEmpty()) {
        throw new IOException(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAIDABLE_ROOT_DIRS_KEY
            + " isn't configured");
      }

      String[] dirs = rootDirs.split(",");
      List<InputSplit> splits = new ArrayList<InputSplit>(dirs.length);
      for (int i = 0; i < dirs.length; ++i) {
        splits.add(new RaidDirInfoSplit(new Path(dirs[i].trim())));
      }
      return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      RaidDirInfoReader reader = new RaidDirInfoReader();
      reader.initialize(split, context);
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

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.split = (RaidDirInfoSplit) split;
      this.conf = context.getConfiguration();
      this.fs = FileSystem.get(this.conf);
      this.raidFileTimeWindow = this.conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT);
      this.dirs = traverseDirectoryTree(this.split.getRootDir());
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

    /**
     * Depth first traverses the specified directory tree to get all raidable files.
     */
    private Queue<Path> traverseDirectoryTree(Path rootDir) {
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
            if (checkFile(path)) {
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

    /**
     * Checks whether the specified is raidable.
     */
    private boolean checkFile(Path file) throws IOException {
      if (!fs.isFile(file)) {
        return false;
      }

      FileStatus fileStatus = fs.getFileStatus(file);
      long currentTimeMs = System.currentTimeMillis();
      long fileModTime = fileStatus.getModificationTime();
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        if ((fileModTime + raidFileTimeWindow < currentTimeMs) && dfs.isFileClosed(file)) {
          return true;
        }
      } else {
        if (fileModTime + raidFileTimeWindow < currentTimeMs) {
          return true;
        }
      }
      return false;
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
  }
}
