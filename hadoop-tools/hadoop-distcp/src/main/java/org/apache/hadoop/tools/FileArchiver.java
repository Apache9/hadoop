package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileArchiver implements Tool {
  private class DirSummary implements Writable {
    private String path;
    private States state;
    private ContentSummary summary;
    private String jobID;
    private int mapCount;
    DirSummary() {
      summary = new ContentSummary();
    }
    DirSummary(String p, ContentSummary sum) {
      path = p;
      summary = sum;
      state = States.NOT_START;
      jobID = "";
      mapCount = 0;
    }

    @Override
    @InterfaceAudience.Private
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, path);
      WritableUtils.writeEnum(out, state);
      summary.write(out);
      WritableUtils.writeString(out, jobID);
      out.writeInt(mapCount);
    }

    @Override
    @InterfaceAudience.Private
    public void readFields(DataInput in) throws IOException {
      path = WritableUtils.readString(in);
      state = WritableUtils.readEnum(in, States.class);
      summary.readFields(in);
      jobID = WritableUtils.readString(in);
      mapCount = in.readInt();
    }
  }

  static private abstract class PathFilter {
    public abstract void initialize(Configuration conf)
        throws IllegalArgumentException;
    public abstract boolean ifQualified (Path path) throws Exception;

    static public PathFilter getInstance(String type, Configuration conf)
        throws IllegalArgumentException {
      PathFilter filter = null;
      if (type.equals("pattern")) {
        filter = new PatternPathFilter();
        filter.initialize(conf);
      } else {
        throw new IllegalArgumentException("Illegal PathFilter type. Only "
            + " \"pattern\"/\"pattern-date\" available now");
      }
      return filter;
    }
  }

  static private class PatternPathFilter extends PathFilter {
    Pattern includePattern = null;
    Pattern excludePattern = null;
    public void initialize(Configuration conf) throws IllegalArgumentException{
      String includePatternStr = conf.get(PATTERN_PATH_FILTER_INCLUDE);
      String excludePatternStr = conf.get(PATTERN_PATH_FILTER_EXCLUDE);
      LOG.info("Using pattern based path filter, include pattern: " +
          includePatternStr + " exclude pattern: " + excludePatternStr);

      if (includePatternStr == null && excludePatternStr == null) {
        throw new IllegalArgumentException(
            "missing required config items " + PATTERN_PATH_FILTER_INCLUDE
                + " or " + PATTERN_PATH_FILTER_EXCLUDE);
      }
      if (includePatternStr != null) {
        includePattern = Pattern.compile(includePatternStr);
      }
      if (excludePatternStr != null) {
        excludePattern = Pattern.compile(excludePatternStr);
      }
    }

    public boolean ifQualified(Path path) throws Exception{
      boolean res = true;
      if (includePattern != null) {
        Matcher matcher = includePattern.matcher(path.toString());
        if (!matcher.find()) {
          res = false;
        }
      }
      if (res && excludePattern != null) {
        Matcher matcher = excludePattern.matcher(path.toString());
        if (matcher.find()) {
          res = false;
        }
      }
      return res;
    }
  }

  private class Options {
    public String startFrom = null;
    public String SpecifiedSubDir = null;
  }

  private static enum States {
    NOT_START,
    IN_FLIGHT,
    COMPLETE,
    IN_CONSISTENT,
    DELETED
  };

  // Configrations
  private static final String SOURCE_CLUSTER_CONF_KEY =
      "dfs.file.archiver.source.cluster";
  private static final String DEST_CLUSTER_CONF_KEY =
      "dfs.file.archiver.dest.cluster";
  private static final String WORK_DIR_CONF_KEY =
      "dfs.file.archiver.work.dir";
  private static final String WORK_DIR_CONF_KEY_DEFAULT =
      "/user/hdfs_admin";
  private static final String SOURCE_BASE_DIR =
      "dfs.file.archiver.source.base.dir";
  private static final String DEST_BASE_DIR =
      "dfs.file.archiver.dest.base.dir";
  private static final String SOURCE_PATH_FILTER =
      "dfs.file.archiver.source.path.filter";
  private static final String SOURCE_PATH_FILTER_DEFAULT =
      "pattern";
  private static final String PATTERN_PATH_FILTER_INCLUDE =
      "dfs.file.archiver.pattern.path.filter.include";
  private static final String PATTERN_PATH_FILTER_EXCLUDE =
      "dfs.file.archiver.pattern.path.filter.exclude";
  private static final String DISTCP_PARAMETER =
      "dfs.file.archiver.distcp.parameter";
  private static final String DISTCP_PARAMETER_DEFAULT =
      "-async -update -prugpca -ignoreDeleted";
  private static final String SCHEDULE_INTERNAL_SEC =
      "dfs.file.archiver.schedule.interval.sec";
  private static final long SCHEDULE_INTERVAL_SEC_DEFAULT = 3600;
  private static final String DELETE_INTERNAL_SEC =
      "dfs.file.archiver.delete.interval.sec";
  private static final long DELETE_INTERVAL_SEC_DEFAULT = 43200; // half day
  private static final String DELETE_AFTER_COPY =
      "dfs.file.archiver.delete.after.copy";
  private static final boolean DELETE_AFTER_COPY_DEFAULT = false;
  private static final String MAX_MAPS =
      "dfs.file.archiver.max.maps";
  private static final int MAX_MAPS_DEFAULT = 512;
  private static final String BANDWIDTH_LIMIT_MB =
      "dfs.file.archiver.bandwidth.limit.mb";
  private static final int BANDWIDTH_LIMIT_MB_DEFAULT = 1;
  private static final String BLACK_LIST =
      "dfs.file.archiver.black.list";

  // Working files
  private static final String DIR_STAT_FILE = "dir_stats";
  private static final String DIR_STAT_RAW = "dir_stats.raw";
  private static final String IN_FLIGHT_FILE = "copy_in_flight";
  private static final String COMPLETED_FILE = "completed";
  private static final String INCONSISTENT_FILE = "inconsistent";
  private static final String DELETED_FILE = "removed";

  private static final Log LOG = LogFactory.getLog(FileArchiver.class);

  private Configuration conf;
  private DistributedFileSystem srcFs;
  private DistributedFileSystem destFs;
  private JobClient jobClient;
  private Path workDir;
  private Path taskDir;
  private Path srcBaseDir;
  private Path destBaseDir;
  private List<String> distCpArgs;
  private long scheduleInterval;
  private long deleteInterval;
  private boolean deleteAfterCopy;
  private int availableMaps;
  private int maxMaps;
  private int bandWidthLimit;
  private boolean shouldStop = false;
  private PathFilter pathFilter;
  private Options options = new Options();

  private TreeMap<String, DirSummary> srcDirs =
      new TreeMap<String, DirSummary>();
  private TreeSet<String> unscheduledSet = new TreeSet<String>();
  private TreeSet<String> inFlightSet = new TreeSet<String>();
  private TreeSet<String> completeSet = new TreeSet<String>();
  private TreeMap<String, Long> inconsistentMap = new TreeMap<String, Long>();
  private TreeSet<String> deletedSet = new TreeSet<String>();

  private LinkedList<String> blackList = new LinkedList<String>();
  private int pathesInBlackList = 0;

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public int run(String[] args) throws Exception {
    initialize();
    execute();
    return 0;
  }

  private void execute() throws IOException,InterruptedException {
    long lastDeteTime = Time.now();
    while (!shouldStop()) {
      if (unscheduledSet.isEmpty() && inFlightSet.isEmpty()
          && inconsistentMap.isEmpty()
          && (!deleteAfterCopy || completeSet.isEmpty())) {
        LOG.info("All copy job completed, exiting");
        break;
      }
     
      LOG.info("Check all in-flight tasks");
      for (String p : new TreeSet<String>(inFlightSet)) {
        checkAndUpdateDirState(p);
      }
      LOG.info("Check all inconsistent tasks");
      for (String p: new TreeSet<String>(inconsistentMap.keySet())) {
        LOG.info(p + " is inconsistent, need manually check and fix");
        checkAndUpdateDirState(p);
      }
      submitCopyTasks(scheduleCopyTasks());
      serializeDirStats();
      serializeProgressFile();
      Thread.sleep(scheduleInterval * 1000);

      long current = Time.now();
      if (deleteAfterCopy && current > lastDeteTime + deleteInterval*1000) {
        moveCompletedTaskToTrash();
        lastDeteTime = current;
      }
    }
  }

  // for testing
  public synchronized void setShouldStop(boolean val) {
    shouldStop = val;
  }

  private synchronized boolean shouldStop() {
    return shouldStop;
  }

  private void initialize() throws Exception {
    LOG.info("Initializing...");
    String srcClusterUri = conf.get(SOURCE_CLUSTER_CONF_KEY);
    String destClusterUri = conf.get(DEST_CLUSTER_CONF_KEY);
    if (srcClusterUri == null || destClusterUri == null) {
      throw new IllegalArgumentException(
          "Missing required source/dest cluster configuration");
    }
    LOG.info("Source cluster: " + srcClusterUri +
        ", dest cluster " + destClusterUri);

    srcFs =
        (DistributedFileSystem) FileSystem.get(new URI(srcClusterUri), conf);
    destFs =
        (DistributedFileSystem) FileSystem.get(new URI(destClusterUri), conf);
    jobClient = new JobClient(conf);

    // check workDir configuration
    workDir = new Path(conf.get(WORK_DIR_CONF_KEY, WORK_DIR_CONF_KEY_DEFAULT));
    LOG.info("Work directory: " + workDir);
    if (conf.get(SOURCE_BASE_DIR, null) == null) {
      throw new IllegalArgumentException(
          "Missing required configuration item " + SOURCE_BASE_DIR);
    }
    if (!srcFs.exists(workDir)) {
      LOG.info("Work directory not exits, create one");
      srcFs.mkdirs(workDir, null);
    }
    // check the root of source directory to copy
    String path = conf.get(SOURCE_BASE_DIR);
    if (path == null) {
      throw new IllegalArgumentException(
          "Missing required configuration item " + SOURCE_BASE_DIR);
    }
    srcBaseDir = new Path(path);
    if (!srcFs.exists(srcBaseDir)) {
      throw new FileNotFoundException(
          "Source directory " + srcBaseDir.toString() + " not exists");
    }
    // check the root of directory copy to
    path = conf.get(DEST_BASE_DIR);
    destBaseDir = path == null ? srcBaseDir : new Path(path);
    LOG.info("Source base directory: " + srcBaseDir);
    LOG.info("Destination base directory: " + destBaseDir);

    if (!destFs.exists(destBaseDir)) {
      LOG.info(String.format("%s on cluster %s not exits, will create it",
          destBaseDir.toString(), destClusterUri));
      destFs.mkdirs(destBaseDir);
    }

    String filterType = conf.get(SOURCE_PATH_FILTER, SOURCE_PATH_FILTER_DEFAULT);
    pathFilter = PathFilter.getInstance(filterType, conf);

    distCpArgs = getDistcpParameter();
    scheduleInterval =
        conf.getLong(SCHEDULE_INTERNAL_SEC, SCHEDULE_INTERVAL_SEC_DEFAULT);
    LOG.info("Schedule interval: " + scheduleInterval + " seconds");

    deleteInterval =
        conf.getLong(DELETE_INTERNAL_SEC, DELETE_INTERVAL_SEC_DEFAULT);
    deleteAfterCopy =
        conf.getBoolean(DELETE_AFTER_COPY, DELETE_AFTER_COPY_DEFAULT);
    if (deleteAfterCopy) {
      LOG.info("Enabled deleteAfterCopy, will delete completed files every " +
            deleteInterval + " seconds");
    }
    maxMaps = conf.getInt(MAX_MAPS, MAX_MAPS_DEFAULT);
    bandWidthLimit = conf.getInt(BANDWIDTH_LIMIT_MB, BANDWIDTH_LIMIT_MB_DEFAULT);
    availableMaps = maxMaps;
    LOG.info("maxMaps: " + maxMaps + ", band width limit: " +
        bandWidthLimit + " MB per map");

    loadBlackList();
    // setup working directory
    String taskID = getTaskID(srcBaseDir);
    if (taskID == null) {
      throw new RuntimeException("generate task-id failed");
    }
    taskDir = new Path(workDir, taskID);
    LOG.info("Task directory: " + taskDir);
    if (!srcFs.exists(taskDir)) {
      LOG.info("Task directory not exists, will create it");
      srcFs.mkdir(taskDir, null);
    }
    Path dirStatRawFile = new Path(taskDir, DIR_STAT_RAW);
    if (!srcFs.exists(dirStatRawFile)) {
      LOG.info("It's the 1st time run this service, will go though all " +
          "directories to copy......");
      getDirStats(getAllSourceDirs(srcBaseDir));
      LOG.info("Found " + srcDirs.size() + " directories to copy");
      serializeDirStats();
    } else {
      LOG.info("Loading contentSummaries from previous file");
      loadDirStatsFromFile(dirStatRawFile);
    }

    for (String p : inFlightSet) {
      availableMaps -= srcDirs.get(p).mapCount;
      assert(availableMaps >= 0);
    }
    LOG.info("Initialize complete");
  }

  private List<String> getDistcpParameter() {
    String parameter = conf.get(DISTCP_PARAMETER, DISTCP_PARAMETER_DEFAULT);
    return new ArrayList<String>(Arrays.asList(parameter.split(" ")));
  }

  private String getTaskID(Path taskSrcPath) {
    String[] splits = taskSrcPath.toString().split("/");
    String taskName = splits[splits.length-1];
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(taskSrcPath.toString().getBytes());
      String md5Value =  new BigInteger(1, md.digest()).toString(16);
      return md5Value.substring(0,8) + "-" + taskName;
    } catch (Exception e) {
      LOG.warn("Calculate task-id failed " + e.getMessage());
    }
    return null;
  }

  private void getDirStats(List<Path> paths) throws IOException{
    for (Path p : paths) {
      Path fullPath = new Path(srcBaseDir, p);
      DirSummary sum = new DirSummary(p.toString(), srcFs.getContentSummary(fullPath));
      srcDirs.put(p.toString(), sum);
      addPathToSets(p.toString(), sum.state);
    }
  }

  private void serializeDirStats()
      throws IOException {
    LOG.info("Generating contentSummaries for each directories to copy");
    // using text format, for human read
    Path statFile = new Path(taskDir, DIR_STAT_FILE);
    // using binary format, save content summary and copy progress for each
    // directory, and jobid for copying jobs
    Path statRawFile = new Path(taskDir, DIR_STAT_RAW);
    DataOutputStream txtOut = srcFs.create(statFile);
    DataOutputStream rawOut = srcFs.create(statRawFile);

    rawOut.writeInt(srcDirs.size());
    for (Map.Entry<String, DirSummary> entry : srcDirs.entrySet()) {
      String p = entry.getKey();
      DirSummary sum = entry.getValue();
      sum.write(rawOut);
      txtOut.write(String
          .format("%s\t%d\n", p, sum.summary.getLength())
          .getBytes());
    }
    rawOut.close();
    txtOut.close();
  }

  private void serializeProgressFile () throws IOException {
    Path inflightFile = new Path(taskDir, IN_FLIGHT_FILE);
    OutputStream out = srcFs.create(inflightFile);
    for (String p : inFlightSet){
      out.write(p.getBytes());
      out.write('\n');
    }
    out.close();

    Path completeFile = new Path(taskDir, COMPLETED_FILE);
    out = srcFs.create(completeFile);
    for (String p : completeSet) {
      out.write(p.getBytes());
      out.write('\n');
    }
    out.close();

    Path inconsistentFile = new Path(taskDir, INCONSISTENT_FILE);
    out = srcFs.create(inconsistentFile);
    for (String p : inconsistentMap.keySet()) {
      out.write(String
          .format("%s\t%d\t%d\n", p, srcDirs.get(p).summary.getLength(),
              inconsistentMap.get(p)).getBytes());
    }
    out.close();

    Path deletedFile = new Path(taskDir, DELETED_FILE);
    out = srcFs.create(deletedFile);
    for (String p : deletedSet) {
      out.write(p.getBytes());
      out.write('\n');
    }
    out.close();

    LOG.info(String.format(
        "[CURRENT STATUS] un-scheduled:%d, ongoing:%d, "
            + "complete:%d, inconsistent:%d, deleted:%d, in-blacklist:%d",
        unscheduledSet.size(), inFlightSet.size(), completeSet.size(),
        inconsistentMap.size(), deletedSet.size(), pathesInBlackList));
  }

  private void loadDirStatsFromFile(Path file) throws IOException {
    DataInputStream in = srcFs.open(file);
    int num = in.readInt();
    for (int i = 0; i < num; i++) {
      DirSummary sum = new DirSummary();
      sum.readFields(in);
      srcDirs.put(sum.path, sum);
      addPathToSets(sum.path, sum.state);
    }
  }
  
  private void loadBlackList() throws IOException {
    LOG.info("loading blacklist file...");
    String blackListFile = conf.get(BLACK_LIST, null);
    if (blackListFile != null) {
      InputStream fin = new FileInputStream(blackListFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(fin));
      String line;
      while ((line = br.readLine()) != null) {
        blackList.add(line.trim());
        LOG.info("blacklist item: " + line.trim());
      }
      fin.close();
    }
  }

  private void addPathToSets(String path, States s) {
    switch (s) {
    case NOT_START:
      unscheduledSet.add(path);
      break;
    case IN_FLIGHT:
      inFlightSet.add(path);
      break;
    case COMPLETE:
      completeSet.add(path);
      break;
    case IN_CONSISTENT:
      long destSpace = -1;
      try {
        ContentSummary sum = destFs.getContentSummary(new Path(destBaseDir, path));
        destSpace = sum.getLength();
      } catch (IOException e) {
        LOG.warn("getContentSummary failed for " + path, e);
      }
      inconsistentMap.put(path, destSpace);
      break;
    case DELETED:
      deletedSet.add(path);
      break;
    }
  }

  private void removePathFromSets(String path, States s) {
    switch (s) {
    case NOT_START:
      unscheduledSet.remove(path);
      break;
    case IN_FLIGHT:
      inFlightSet.remove(path);
      break;
    case COMPLETE:
      completeSet.remove(path);
      break;
    case IN_CONSISTENT:
      inconsistentMap.remove(path);
      break;
    case DELETED:
      deletedSet.remove(path);
      break;
    }
  }

  private List<Path> getAllSourceDirs(Path srcRootPath) throws Exception{
    List<Path> resList = new LinkedList<Path>();
    LinkedList<FileStatus> queue = new LinkedList<FileStatus>();
    try {
      FileStatus rootStatus = srcFs.getFileStatus(srcRootPath);
      queue.push(rootStatus);
      while (!queue.isEmpty()) {
        for (FileStatus child : srcFs.listStatus(queue.pop().getPath())) {
          Path childPath = child.getPath();
          if (pathFilter.ifQualified(childPath)) {
            resList.add(getRelativePath(childPath, srcRootPath));
            continue;
          }
          if (child.isDirectory()) {
            queue.push(child);
          }
        }
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage());
    }
    return resList;
  }

  private Path getRelativePath(Path fullPath, Path parent)
      throws IllegalArgumentException {
    String f = fullPath.toString();
    String p = parent.toString();
    // if path contains port, need to skip 2 ':'
    while (f.contains(":")) {
      f = f.substring(f.indexOf('/', f.indexOf(':')));
    }
    // if path don't contain port, in the schema of hdfs://cluster-name/user/x
    if (f.startsWith("//")) {
      f = f.substring(f.indexOf('/', 2));
    }

    if (!f.startsWith(p)) {
      throw new IllegalArgumentException(
          String.format("%s is not parent of %s", p, f));
    }
    return new Path(f.substring(p.length() + 1));
  }

  private void checkAndUpdateDirState (String dir) throws IOException{
    DirSummary sum = srcDirs.get(dir);
    States oldState = sum.state;
    if (sum.state == States.IN_FLIGHT && !sum.jobID.isEmpty()) {
      RunningJob job = jobClient.getJob(JobID.forName(sum.jobID));
      if (job != null) {
        if (!job.isComplete()) {
          // state not change, return
          return;
        }
        if (job.isSuccessful()) {
          sum.state = States.COMPLETE;
          LOG.info("copy complete! " + dir);
        } else {
          // copy job failed, reset to not_started status, waiting for
          // next time schedule
          sum.state = States.NOT_START;
        }
      } else {
        sum.state = States.NOT_START;
      }
      availableMaps += sum.mapCount;
      assert(availableMaps <= maxMaps);
      sum.jobID = "";
      sum.mapCount = 0;
    }

    // if inconsistency cause by src data change
    if (sum.state == States.IN_CONSISTENT) {
      ContentSummary contentSummary =
          srcFs.getContentSummary(new Path(srcBaseDir, dir));
      if (contentSummary.getLength() != sum.summary.getLength()) {
        sum.summary = contentSummary;
      }
    }

    Path destPath = new Path(destBaseDir, dir);
    if (sum.summary.getLength() == 0) {
      // this condition is actually checked before schedule copy tasks
      // if directory is empty, then mark it as complete
      sum.state = States.COMPLETE;
      availableMaps += sum.mapCount;
      assert(availableMaps <= maxMaps);
      sum.mapCount = 0;
      sum.jobID = "";
    } else if (!destFs.exists(destPath)) {
      if (sum.state == States.COMPLETE) {
        // this check is perform after copy complete, and found dest
        // path not exists. then it might caused by cached contentSummary
        // outdated, we've scheduled an actually empty directory.
        // in this case, we should update the contentSummary cache
        Path srcPath = new Path(srcBaseDir, dir);
        if (srcFs.exists(srcPath)) {
          ContentSummary contentSummary = srcFs.getContentSummary(srcPath);
          if (contentSummary.getLength() != sum.summary.getLength()) {
            sum.summary = contentSummary;
          }
          sum.state = States.NOT_START;
        } // else if srcPath not exists, then simply keep it as COMPLETE
      } else {
        sum.state = States.NOT_START;
      }
    } else {
      ContentSummary contentSummary = destFs.getContentSummary(destPath);
      if (contentSummary.getLength() == sum.summary
          .getLength()) {
        sum.state = States.COMPLETE;
        availableMaps += sum.mapCount;
        assert(availableMaps <= maxMaps);
        sum.mapCount = 0;
        sum.jobID = "";
      } else if (sum.state == States.COMPLETE) {
        sum.state = States.IN_CONSISTENT;
      }
    }
    if (oldState != sum.state) {
      removePathFromSets(sum.path, oldState);
      addPathToSets(sum.path, sum.state);
    }
  }

  private Map<String, Integer> scheduleCopyTasks() {
    TreeMap<String, Integer> tasks = new TreeMap<String, Integer>();
    long dataSizePerMap =
        bandWidthLimit * scheduleInterval * 1024 * 1024;

    TreeSet<String> tmpSet = new TreeSet<String>(unscheduledSet);
    Iterator<String> iter = tmpSet.iterator();
    while (availableMaps > 0 && iter.hasNext()) {
      String p = iter.next();
      try {
        // make sure the directory is not start copying
        checkAndUpdateDirState(p);
        DirSummary sum = srcDirs.get(p);
        if (sum.state == States.NOT_START) {
          if (isInBlackList(p)) {
              unscheduledSet.remove(p);
              pathesInBlackList++;
              LOG.info(p + " is excluded because it's in blacklist");
              continue;
          }
          int mapCount = (int)(sum.summary.getLength()/dataSizePerMap) + 1;
          // maps count should not bigger than file count
          mapCount = (int) Math.min((long)mapCount, sum.summary.getFileCount());
          // if directory too big, using few maps will spend very long time,
          // waiting for enough resources
          if (inFlightSet.size() > 0 && mapCount > 2 * availableMaps &&
              availableMaps < maxMaps/2) {
            LOG.info(
                "only " + availableMaps + " maps available, " + p + " requires "
                    + mapCount + " skip it temporarily, "
                    + "will reschedule when cluster free");
            continue;
          }
          mapCount = Math.min(mapCount, availableMaps);
          tasks.put(p, mapCount);
          availableMaps -= mapCount;
          assert(availableMaps >= 0);
        }
      } catch (IOException e) {
        LOG.warn(
            "try to getContentSummary for " + p + " failed :" + e.getMessage());
      }
    }
    return tasks;
  }

  private void submitCopyTasks(Map<String, Integer> tasks) {
    int succeed = 0;
    long dataSize = 0;
    for (Map.Entry<String, Integer> entry : tasks.entrySet()) {
      String dir = entry.getKey();
      int maps = entry.getValue();
      JobConf jobConf = new JobConf(getConf());
      String jobID = submitDistcpJob(dir, maps, jobConf);
      if (jobID != null) {
        DirSummary sum = srcDirs.get(dir);
        States oldState = sum.state;
        sum.state = States.IN_FLIGHT;
        sum.jobID = jobID;
        sum.mapCount = maps;
        removePathFromSets(dir, oldState);
        addPathToSets(dir, sum.state);
        succeed++;
        dataSize += sum.summary.getLength();
      } else {
        availableMaps += maps;
        assert(availableMaps <= maxMaps);
      }
    }
    LOG.info(String.format(
        "Scheduled %d tasks, %s submit succeed, avaiable maps:%d" +
            ", Will copy %d Bytes data",
        tasks.size(), succeed, availableMaps, dataSize));
  }

  private String submitDistcpJob(String directory, int maps, JobConf jobConf) {
    List<String> args = new ArrayList<String>(distCpArgs);
    args.addAll(Arrays.asList("-m", Integer.toString(maps), "-bandwidth",
        Integer.toString(bandWidthLimit)));
    args.add(srcFs.getUri().toString() + new Path(srcBaseDir, directory));
    args.add(destFs.getUri().toString() + new Path(destBaseDir, directory));
    try {
      DistCp distcp = new DistCp(jobConf, null);
      ToolRunner.run(jobConf, distcp,
          args.toArray(new String[args.size()]));
    } catch (Exception e) {
      LOG.error("submit job failed. " + e.getMessage());
      return null;
    }
    String jobID = jobConf.get(DistCpConstants.CONF_LABEL_DISTCP_JOB_ID);
    if (jobID != null) {
      LOG.info(
          "submit distcp job succeed for " + directory + ", jobid:" + jobID);
    } else {
      LOG.warn("can't find config " + DistCpConstants.CONF_LABEL_DISTCP_JOB_ID);
    }
    return jobID;
  }

  private void moveCompletedTaskToTrash() {
    for (String p : new TreeSet<String>(completeSet)) {
      try {
        Path srcPath = new Path(srcBaseDir, p);
        ContentSummary srcSum =
            srcFs.getContentSummary(srcPath);
        ContentSummary destSum = null;
        if (srcSum.getLength() !=0) {
          destSum = destFs.getContentSummary(new Path(destBaseDir, p));
        }
        if (srcSum.getLength() == 0 ||
            srcSum.getLength() == destSum.getLength()) {
          boolean res = Trash.moveToAppropriateTrash(srcFs, srcPath, getConf());
          if (!res) {
            LOG.warn("move to trash failed!");
          }
          DirSummary sum = srcDirs.get(p);
          removePathFromSets(p, sum.state);
          sum.state = States.DELETED;
          addPathToSets(p, sum.state);
        } else if (srcSum.getLength() != destSum.getLength()) {
          DirSummary sum = srcDirs.get(p);
          removePathFromSets(p, sum.state);
          sum.state = States.IN_CONSISTENT;
          addPathToSets(p, sum.state);
        }
      } catch (IOException e) {
        LOG.warn("move to trash failed", e);
      }
    }
  }

  private boolean isInBlackList(String path) {
    for (String p : blackList) {
      if (path.startsWith(p)) {
        return true;
      }
    }
    return false;
  }

  public static void main(String argv[]) {
    int exitcode = 0;
    try {
      FileArchiver archiver = new FileArchiver();
      exitcode = ToolRunner.run(new HdfsConfiguration(), archiver, argv);
    } catch (Exception e) {
      LOG.error("couldn't run FileArchiver, error: ", e);
    }
    System.exit(exitcode);
  }
}
