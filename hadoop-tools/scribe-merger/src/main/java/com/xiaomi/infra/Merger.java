package com.xiaomi.infra;
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

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.zip.CRC32;

import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * This class provides methods that can be used
 * to merge scribe logs.
 */
public class Merger {
  public static final int VERSION = 1;
  private static final Log LOG = LogFactory.getLog(Merger.class);

  private static final String SCRIBE_PATH_PREFIX = "/user/h_scribe/";
  private static final String NAME = "scribe-file-merge";
  static final String SRC_LIST_KEY = NAME + ".src.list";
  static final String JOB_DIR_KEY = NAME + ".job.dir";

  static final String SRC_COUNT_KEY = NAME + ".src.count";
  static final String TOTAL_SIZE_KEY = NAME + ".total.size";

  /** the size of the blocks that will be created when merging **/
  static final String BLOCK_SIZE_KEY = NAME + ".block.size";
  /**the size of the part files that will be created when merging **/
  static final String MERGED_FILE_SIZE_KEY = NAME + ".partfile.size";
  /**the average size above which bypass merging**/
  static final String MERGED_BYPASS_SIZE_KEY = NAME + ".bypass.size";

  static final String MAP_VERIFY_ENABLED_KEY = NAME + ".map.verify.enable";
  static final String DELETE_MEDIATE_OUTPUT_KEY = NAME + ".delete.mediate.output";
  static final String DO_REPLACE_KEY = NAME + ".do.replacement";
  static final String BLOCK_MISSING_TAG = "BLOCK_MISSING";

  static final String START_DATE_KEY = NAME + ".startdate";
  static final String END_DATE_KEY = NAME + ".enddate";

  static final long DEFAULT_BLOCK_SIZE = 512 * 1024 * 1024l;
  static final long DEFAULT_MERGED_FILE_SIZE = 2 * 1024 * 1024 * 1024l;
  static final long DEFAULT_MERGED_BYPYSS_SIZE = 256 * 1024 * 1024l;

  /** the desired replication degree; default is 10 **/
  short repl = 5;

  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
  private static final String usage = "merge <basePath> <startDate> <endDate> <output>\n" +
      "Date format is: %Y-%m-%d";

  private Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Merger(Configuration conf) {
    setConf(conf);
  }

  /** MergeEntry is used in the {@link com.xiaomi.infra.Merger.MergeEntry} as the input value. */
  public static class MergeEntry implements Writable {
    String parent;
    String filePath;
    Long length;
    // modification timestamp;
    Long timestamp;
    MergeEntry() {}

    MergeEntry(String parent, String filePath, Long length, Long timestamp) {
      this.parent = parent;
      this.filePath = filePath;
      this.length = length;
      this.timestamp = timestamp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      parent = Text.readString(in);
      filePath = Text.readString(in);
      length = in.readLong();
      timestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, parent);
      Text.writeString(out, filePath);
      out.writeLong(length);
      out.writeLong(timestamp);
    }
  }

  /**
   * Input format of a merge job responsible for
   * generating splits of the file list
   */
  static class MergeInputFormat extends InputFormat<LongWritable, MergeEntry> {

    //generate input splits from the src file lists
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
      Configuration conf = context.getConfiguration();
      String srcfilelist = conf.get(SRC_LIST_KEY, "");
      if ("".equals(srcfilelist)) {
        throw new IOException("Unable to get the " +
            "src file for merge generation.");
      }
      long totalSize = conf.getLong(TOTAL_SIZE_KEY, -1);
      long mergeSize = conf.getLong(MERGED_FILE_SIZE_KEY, DEFAULT_MERGED_FILE_SIZE);
      if (totalSize == -1) {
        throw new IOException("Invalid size of files to merge");
      }
      //we should be safe since this is set by our own code
      Path src = new Path(srcfilelist);
      FileSystem fs = src.getFileSystem(conf);
      FileStatus fstatus = fs.getFileStatus(src);
      ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
      LongWritable key = new LongWritable();
      final MergeEntry value = new MergeEntry();
      SequenceFile.Reader reader = null;

      // the count of sizes calculated till now
      long currentCount = 0L;
      // the endposition of the split
      long lastPos = 0L;
      // the start position of the split
      long startPos = 0L;
      // the partition id in the split
      int partitionId = 0;
      String lastPath = "";
      // create splits of size target size so that all the maps
      // have equals sized data to read and write to.
      try {
        reader = new SequenceFile.Reader(fs, src, conf);
        if (reader.next(key, value)) {
          lastPath = value.parent;
          currentCount += value.length;
          lastPos = reader.getPosition();
        }
        while(reader.next(key, value)) {
          // different days
          if (!value.parent.equals(lastPath)) {
            long size = lastPos - startPos;
            splits.add(new FileSplit(src, startPos, size, (String[]) null));
            startPos = lastPos;
            currentCount = 0L;
            lastPath = value.parent;
          } else if (currentCount + value.length > mergeSize && currentCount != 0){
            long size = lastPos - startPos;
            splits.add(new FileSplit(src, startPos, size, (String[]) null));
            startPos = lastPos;
            currentCount = 0L;
          }
          currentCount += value.length;
          lastPos = reader.getPosition();
        }
        if (startPos != lastPos) {
          splits.add(new FileSplit(src, startPos, lastPos - startPos, (String[]) null));
        }
      } finally {
        reader.close();
      }
      return splits;
    }

    @Override
    public RecordReader<LongWritable, MergeEntry> createRecordReader(InputSplit split,
                                                                     TaskAttemptContext context) throws IOException {
      RecordReader<LongWritable, MergeEntry> reader =  new SequenceFileRecordReader<LongWritable, MergeEntry>();
      return reader;
    }
  }

  private void append(SequenceFile.Writer srcWriter, long len,
                      String path, String children, Long timestamp) throws IOException {
    srcWriter.append(new LongWritable(0l), new MergeEntry(path, children, len, timestamp));
  }

  boolean needMerge(Path path) throws IOException{
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return false;
    }
    FileStatus[] fileStatuses = fs.listStatus(path);
    if (fileStatuses.length == 0) {
      return false;
    }
    long bypassThreshold = conf.getLong(MERGED_BYPASS_SIZE_KEY, DEFAULT_MERGED_BYPYSS_SIZE);
    long fileCount = 0, fileSize = 0;
    for (FileStatus status : fileStatuses) {
      fileCount ++;
      fileSize += status.getLen();
    }
    if (fileSize / fileCount > bypassThreshold) {
      return false;
    }
    return true;
  }
  /**
   * merge the given source paths into
   * the dest
   * @param srcPaths the src paths to be merged
   */
  void merge(List<Path> srcPaths, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
    int numFiles = 0;
    long totalSize = 0;
    FileSystem fs = FileSystem.get(conf);
    boolean deleteMediateOutput = conf.getBoolean(DELETE_MEDIATE_OUTPUT_KEY, false);
    Path stagingArea;
    stagingArea = JobSubmissionFiles.getStagingDir(new Cluster(conf),
        conf);

    Path jobDirectory = new Path(stagingArea,
        String.format("%s_%s", NAME, System.currentTimeMillis()));
    FsPermission perms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    fs.mkdirs(jobDirectory, perms);
    conf.set(JOB_DIR_KEY, jobDirectory.toString());
    //get a tmp directory for input splits
    Path srcFiles = new Path(jobDirectory, "_merge_src_files");
    LOG.info("merge split file is: " + srcFiles);
    conf.set(SRC_LIST_KEY, srcFiles.toString());
    for (Path path : srcPaths) {
      String removePrefix = removeScribePrefix(path.toUri().getPath());
      Path newPath = new Path(jobDirectory, removePrefix);
      fs.mkdirs(newPath);
    }

    SequenceFile.Writer srcWriter = SequenceFile.createWriter(fs, conf,
        srcFiles, LongWritable.class, MergeEntry.class,
        SequenceFile.CompressionType.NONE);
    // get the list of files
    // create single list of files and dirs
    try {
      for (Path parent: srcPaths) {
        FileStatus[] allFiles = fs.listStatus(parent);
        for (FileStatus fileStatus: allFiles) {
          final Path path = fileStatus.getPath();
          long len = fileStatus.getLen();
          if (len == 0) {
            LOG.info(String.format("skipping path %s which has length 0", path));
            continue;
          }
          append(srcWriter, len, parent.toString(), path.toString(), fileStatus.getModificationTime());
          srcWriter.sync();
          numFiles++;
          totalSize += len;
        }
      }
    } finally {
      srcWriter.close();
    }
    //increase the replication of src files
    fs.setReplication(srcFiles, repl);
    conf.setInt(SRC_COUNT_KEY, numFiles);
    conf.setLong(TOTAL_SIZE_KEY, totalSize);
    Job job = Job.getInstance(conf, "Scribe-Merge");
    job.setNumReduceTasks(1);
    job.setJarByClass(Merger.class);
    job.setMapperClass(MergerMapper.class);
    job.setReducerClass(MergerReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(MergeInputFormat.class);
    FileInputFormat.addInputPath(job, jobDirectory);
    FileOutputFormat.setOutputPath(job, outputPath);
    //make sure no speculative execution is done
    job.setSpeculativeExecution(false);
    job.waitForCompletion(true);

    //delete the tmp job directory
    if (deleteMediateOutput) {
      try {
        fs.delete(jobDirectory, true);
      } catch (IOException ie) {
        LOG.info("Unable to clean tmp directory " + jobDirectory);
      }
    }
  }

  static class MergerMapper extends Mapper<LongWritable, MergeEntry, Text, Text> {
    private Configuration conf = null;
    Path tmpOutputDir = null;
    Path tmpOutput = null;
    String parentPath;
    String partname = null;
    FileSystem destFs = null;
    int buf_size = 128 * 1024;
    Long blockSize = 0l;
    SequenceFile.Writer writer = null;
    SequenceFile.Reader reader = null;
    CRC32 crc = null;
    boolean verifyEnabled;
    boolean blockmissing = false;

    public void setup(Context context) throws IOException, InterruptedException {
      // this is tightly tied to map reduce
      // since it does not expose an api
      // to get the partition
      conf = context.getConfiguration();
      destFs = FileSystem.get(conf);
      tmpOutputDir = new Path(conf.get(JOB_DIR_KEY, ""));
      int partId = conf.getInt(MRJobConfig.TASK_PARTITION, -1);
      partname = String.format("merge-%06d", partId);

      blockSize = conf.getLong(BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
      verifyEnabled = conf.getBoolean(MAP_VERIFY_ENABLED_KEY, false);
      crc = new CRC32();
      // get the output path and write to the tmp
      // directory
    }

    // copy raw data.
    public void copyData(SequenceFile.Reader reader,
                         SequenceFile.Writer writer, Context context) throws IOException {
      BytesWritable key = (BytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
      try {
        while (reader.next(key, value)) {
          crc.update(key.getBytes(), 0, key.getLength());
          crc.update(value.getBytes(), 0, value.getLength());
          writer.append(key, value);
        }
      } catch (EOFException eofe) {
        LOG.warn(eofe.getMessage());
      }
    }

    // read files from the split input
    // and write it onto the part files.
    public void map(LongWritable key, MergeEntry value,
                    Context context) throws IOException, InterruptedException {
      Path srcPath = new Path(value.filePath);
      FileSystem srcFs = srcPath.getFileSystem(conf);
      LOG.info(String.format("parent: %s, file: %s, length: %s, timestamp: %s", value.parent, value.filePath, value.length, value.timestamp));
      try {
        reader = new SequenceFile.Reader(srcFs, srcPath, conf);
      } catch (BlockMissingException e) {
        LOG.warn(e.getMessage());
        blockmissing = true;
        return;
      }
      if (null == writer) {
        parentPath = new Path(value.parent).toUri().getPath();
        String removePrefix = removeScribePrefix(parentPath);
        tmpOutput = new Path(tmpOutputDir, removePrefix + "/" + partname);
        LOG.info("temp output file path is: " + destFs.makeQualified(tmpOutput));
        writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(tmpOutput),
            SequenceFile.Writer.keyClass(reader.getKeyClass()),
            SequenceFile.Writer.valueClass(reader.getValueClass()),
            SequenceFile.Writer.compression(reader.getCompressionType(), reader.getCompressionCodec()),
            SequenceFile.Writer.blockSize(blockSize),
            SequenceFile.Writer.bufferSize(buf_size),
            SequenceFile.Writer.progressable(context)
        );
      }
      if (!blockmissing) {
        try {
          copyData(reader, writer, context);
          writer.hflush();
        } finally {
          reader.close();
        }
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      try {
        writer.close();
        if (blockmissing) {
          context.write(new Text(parentPath), new Text(BLOCK_MISSING_TAG));
          return;
        }
        // close the part files.
        if (verifyEnabled) {
          SequenceFile.Reader outputReader = new SequenceFile.Reader(destFs, tmpOutput, conf);
          CRC32 outputCRC = new CRC32();
          BytesWritable key = (BytesWritable) ReflectionUtils.newInstance(outputReader.getKeyClass(), conf);
          BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(outputReader.getValueClass(), conf);
          while (outputReader.next(key, value)) {
            outputCRC.update(key.getBytes(), 0, key.getLength());
            outputCRC.update(value.getBytes(), 0, value.getLength());
          }
          if (crc.getValue() != outputCRC.getValue()) {
            throw new IOException("map side check failed");
          } else {
            LOG.info("map side crc check passed");
          }
        }
      } catch (IOException ie) {
        destFs.delete(tmpOutput);
        throw ie;
      }
      context.write(new Text(parentPath), new Text(destFs.makeQualified(tmpOutput).toString()));
    }
  }

  static class MergerReducer extends Reducer<Text, Text, Text, Text> {
    Configuration conf = null;
    FileSystem fs = null;
    int partId;
    boolean doReplace;

    public void setup(Context context) throws IOException{
      conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      partId = 0;
      doReplace = conf.getBoolean(DO_REPLACE_KEY, true);
    }
    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
      Path srcDirectory = new Path(key.toString());
      FileStatus srcStatus = fs.getFileStatus(srcDirectory);
      FileStatus[] statuses = fs.listStatus(srcDirectory);
      Text[] mergedFiles = Iterables.toArray(values, Text.class);
      Path mergedDir;
      FsPermission permission;
      if (statuses.length > 0 && mergedFiles.length > 0) {
        permission = statuses[0].getPermission();
        mergedDir = new Path(mergedFiles[0].toString()).getParent();
      } else {
        String errorMessage = "path " + srcDirectory + " has no file";
        throw new IOException(errorMessage);
      }
      for (Text text: mergedFiles) {
        String filename = text.toString();
        if (BLOCK_MISSING_TAG.equals(filename)) {
          LOG.warn(String.format("file %s skipped due to missing block", srcDirectory));
          return;
        }
      }
      LOG.debug(String.format("origin: %s, replace: %s, doReplace: %s", srcDirectory, mergedDir, doReplace));
      context.write(new Text(srcDirectory.toString()), new Text(mergedDir.toString()));
      if (doReplace) {
        fs.setOwner(mergedDir, srcStatus.getOwner(), srcStatus.getGroup());
        fs.setPermission(mergedDir, srcStatus.getPermission());
        FileStatus fstatus = fs.getFileStatus(srcDirectory);

        for (Text text : mergedFiles) {
          Path mergedFile = new Path(text.toString());
          fs.setOwner(mergedFile, fstatus.getOwner(), fstatus.getGroup());
          fs.setPermission(mergedFile, permission);
        }
        LOG.info(String.format("moving from %s to %s", mergedDir, srcDirectory));
        fs.delete(srcDirectory, true);
        fs.rename(mergedDir, srcDirectory);
        LOG.info(String.format("successfully move to %s", srcDirectory));
      }
    }
  }
  /** the main driver for creating merges
   *  it takes at least three command line parameters. The parent path,
   *  The src and the dest.
   *  The mapper created merges
   */

  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    List<Path> srcPaths = new ArrayList<Path>();
    if (args.length != 4) {
      System.out.println(usage);
      throw new IOException("Invalid usage.");
    }
    String pathName = args[0];
    FileSystem fs = FileSystem.get(conf);
    Set<String> categories = new HashSet<String>(Arrays.asList(pathName.split(",")));
    for (String category : categories) {
      Path path = new Path(category);
      if (!fs.exists(path)) {
        System.out.println(usage);
        throw new IOException("Path " + path + " is not exist");
      }
      FileStatus fstatus = fs.getFileStatus(path);
      if (!fstatus.isDirectory()) {
        System.out.println(usage);
        throw new IOException("Path " + path + " is not a directory");
      }
      String start = args[1];
      String end = args[2];
      Date startDate, endDate, today = new Date();
      try {
        startDate = format.parse(start);
        endDate = format.parse(end);
      } catch (ParseException e) {
        LOG.error("date parse error: " + e.getMessage());
        System.out.println(usage);
        throw new IOException("Invalid date format for merge. start date: " + start + ". end date: " + end);
      }
      // if endDate is after today, set endDate = today - 1 week to avoid data corrupt
      if (endDate.after(today)) {
        Calendar tmp = Calendar.getInstance();
        tmp.setTime(today);
        tmp.add(Calendar.DATE, -7);
        endDate = tmp.getTime();
      }
      conf.set(START_DATE_KEY, start);
      conf.set(END_DATE_KEY, end);
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(startDate);
      LOG.info(String.format("start date %s, end date %s", startDate, endDate));
      for (; calendar.getTime().before(endDate); calendar.add(Calendar.DATE, 1)) {
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int date = calendar.get(Calendar.DATE);
        String fullPath = String.format("%s/year=%s/month=%02d/day=%02d", path, year, month, date);
        Path tmp = new Path(fullPath);
        if (needMerge(tmp)) {
          srcPaths.add(tmp);
        }
      }
    }
    Path outputPath = new Path(args[3]);
    if (fs.exists(outputPath)) {
      LOG.info("output exists, delete it");
      fs.delete(outputPath, true);
    }
    merge(srcPaths, outputPath);
    return 0;
  }

  public static String removeScribePrefix(String path) {
    String removePrefix = path;
    if (removePrefix.startsWith(SCRIBE_PATH_PREFIX)) {
      removePrefix = removePrefix.substring(SCRIBE_PATH_PREFIX.length());
    }
    return removePrefix;
  }

  /** the main functions **/
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    Merger merger = new Merger(conf);
    merger.run(otherArgs);
  }
}
