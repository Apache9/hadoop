package org.apache.hadoop.tools;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestFileArchiver {
  private static final Log LOG = LogFactory.getLog(TestFileArchiver.class);
  private static final int SMALL_BLOCK = 1024;
  private static final String UT_WORK_DIR = "/user/hdfs_ut";
  private static final String SOURCE_BASE_DIR = "/user/h_scribe";

  private Configuration dfsConf;
  private Configuration mrConf;
  private JobConf jobConf;
  private MiniDFSCluster srcCluster;
  private MiniDFSCluster targetCluster;
  private MiniMRYarnCluster mrCluster;
  private FileSystem srcFs;
  private FileSystem targetFs;
  private YarnClient yarnClient;
  private TreeSet<Path> fileList;
  private FileArchiver archiver;
  private Thread archiverThread;

  @Before
  public void setup() {
    dfsConf = new Configuration();
    mrConf = new Configuration();
    dfsConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, SMALL_BLOCK);
    // Bump up replication interval so that we only run replication
    // checks explicitly.
    dfsConf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 600);
    // Increase max streams so that we re-replicate quickly.
    dfsConf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 1000);
    dfsConf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);
    fileList = new TreeSet<Path>();

    try {

      srcCluster = setupNewDFSCluster();
      targetCluster= setupNewDFSCluster();

      for (DataNode dn :srcCluster.getDataNodes()) {
        LOG.info("datanode info of src cluster:" + dn.toString());
      }
      for (DataNode dn :targetCluster.getDataNodes()) {
        LOG.info("datanode info of target cluster:" + dn.toString());
      }

      setupMRCluster();

      // setup configuration for running fileArchiver
      jobConf = new JobConf(mrCluster.getConfig());
      jobConf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);
      jobConf.set("dfs.file.archiver.source.base.dir", SOURCE_BASE_DIR);
      String srcNNAddress =
          "hdfs://" + srcCluster.getNameNode().getHostAndPort();
      jobConf.set("dfs.file.archiver.source.cluster", srcNNAddress);
      String targetNNAddress =
          "hdfs://" + targetCluster.getNameNode().getHostAndPort();
      jobConf.set("dfs.file.archiver.dest.cluster", targetNNAddress);
      jobConf.set("dfs.file.archiver.work.dir", UT_WORK_DIR);
      jobConf.set("dfs.file.archiver.pattern.path.filter.include",
          "year=\\d+/month=\\d+");
      jobConf.set("dfs.file.archiver.distcp.parameter",
          "-async -update -prugpc -ignoreDeleted");

      prepareScribeFiles();
      srcFs = srcCluster.getFileSystem();
      targetFs = targetCluster.getFileSystem();
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(jobConf);
      yarnClient.start();
      archiver = new FileArchiver();
    } catch (Exception e) {
      LOG.info("setup test env failed " + e.getMessage());
    }
  }
  
  @After
  public void tearDown() throws Exception {
    if (srcCluster != null) {
      srcCluster.shutdown();
    }
    if (targetCluster != null) {
      targetCluster.shutdown();
    }
    if (mrCluster != null) {
      mrCluster.close();
    }
  }

  private MiniDFSCluster setupNewDFSCluster() throws IOException {
    Configuration conf = new Configuration(dfsConf);

    File baseDir = new File(
        "./target/test-dir-" + UUID.randomUUID().toString().substring(0, 4)
            + "/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(3)
          .format(true)
          .build();
    cluster.waitClusterUp();
    return cluster;
  }

  private void setupMRCluster() throws IOException {
      mrCluster = new MiniMRYarnCluster(this.getClass().getName(), 3);
      mrConf.set("fs.defaultFS", targetCluster.getFileSystem().getUri().toString());
      mrConf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      mrCluster.init(mrConf);
      mrCluster.start();
  }

  private void prepareScribeFiles() throws IOException {
    Random rand = new Random();
    DistributedFileSystem srcFs = srcCluster.getFileSystem();
    Path scribeBase = new Path("/user/h_scribe/");
    srcFs.mkdirs(scribeBase, null);
    for (int i = 0; i < 2; i++) {
      String userName = String.format("mi-user-%d", i);
      Path userPath = new Path(scribeBase, userName);
      srcFs.mkdirs(userPath, null);
      for (int y = 2015; y < 2017; y++) {
        int monthes = 0;
        for (int m = 1; m <= 12; m = m +rand.nextInt(6) + 1) {
          for (int d = 1; d <= 30; d = d + rand.nextInt(15) + 1) {
            Path datePath = new Path(userPath,
                String.format("year=%d/month=%02d/day=%02d", y, m, d));
            srcFs.mkdirs(datePath, null);
            int files = rand.nextInt(2) + 1;
            for (int n = 0; n < files; n++) {
              Path filePath = new Path(datePath, String.format("test-%d", n));
              OutputStream out = srcFs.create(filePath);
              out.write("it's a test string".getBytes());
              out.close();
              LOG.info("created file:" + filePath.toString());
              fileList.add(filePath);
            }
          }
          // avoid too much ut data, if there already generate 3 monthes data,
          // skip to next year
          if (++monthes >= 3) {
            break;
          }
        }
      } //end of year loop
    }
  }

  private Path waitForInit() throws IOException, InterruptedException{
    Path srcWorkDir = new Path(UT_WORK_DIR);
    while (!srcFs.exists(srcWorkDir)
        || srcFs.listStatus(srcWorkDir).length == 0) {
      Thread.sleep(100);
    }
    Path taskDir = srcFs.listStatus(srcWorkDir)[0].getPath();
    while (srcFs.listStatus(taskDir).length == 0) {
      Thread.sleep(100);
    }
    while (!srcFs.exists(new Path(taskDir, "dir_stats"))) {
      Thread.sleep(100);
    }
    return taskDir;
  }

  private void waitForSchedule(Path taskDir)
      throws IOException, InterruptedException {
    while (!srcFs.exists(new Path(taskDir, "copy_in_flight"))) {
      Thread.sleep(100);
    }
  }

  private Path startFileArchiverAndWaitForJobSubmit(JobConf tConf)
      throws IOException, InterruptedException, YarnException {
    if (archiverThread != null) {
      archiverThread.join();
    }
    archiver.setShouldStop(false);
    final JobConf testConf = new JobConf(tConf);
    archiverThread = new Thread() {
      @Override
      public void run() {
        try {
          ToolRunner.run(testConf, archiver, new String[] {});
        } catch (Exception e) {
          LOG.info("run file archiver failed!", e);
        }
      }
    };
    archiverThread.start();
    // waiting for FileArchiver init the task directory
    Path taskDir = waitForInit();
    waitForSchedule(taskDir);
    // waiting for distcp job submitted
    while (yarnClient.getApplications().size() == 0) {
      Thread.sleep(100);
    }
    return taskDir;
  }

  private void waitForAllJobFinished()
      throws IOException, InterruptedException, YarnException {
    EnumSet<YarnApplicationState> appState =
        EnumSet.noneOf(YarnApplicationState.class);
    appState.add(YarnApplicationState.FINISHED);
    appState.add(YarnApplicationState.FAILED);
    appState.add(YarnApplicationState.KILLED);
    while (yarnClient.getApplications(appState).size() != yarnClient
        .getApplications().size()) {
      Thread.sleep(1000);
    }
  }

  private List<ApplicationReport> getAllJobs(YarnApplicationState state)
    throws IOException, InterruptedException, YarnException {
    EnumSet<YarnApplicationState> appState =
        EnumSet.noneOf(YarnApplicationState.class);
    appState.add(state);
    return yarnClient.getApplications(appState);
  }

  private List<ApplicationReport> getAllRunningJobs()
      throws IOException, InterruptedException, YarnException {
    EnumSet<YarnApplicationState> appState =
        EnumSet.noneOf(YarnApplicationState.class);
    appState.add(YarnApplicationState.NEW);
    appState.add(YarnApplicationState.NEW_SAVING);
    appState.add(YarnApplicationState.SUBMITTED);
    appState.add(YarnApplicationState.ACCEPTED);
    appState.add(YarnApplicationState.RUNNING);
    return yarnClient.getApplications(appState);
  }

  private List<String> readFile(FileSystem fs, Path file) throws IOException {
    ArrayList<String> res = new ArrayList<String>();
    FSDataInputStream in = fs.open(file);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = br.readLine()) != null) {
      res.add(line);
    }
    in.close();
    return res;
  }

  @Test
  public void testIncludePathPattern() throws Exception {
    TreeSet<Path> monthPathSet = new TreeSet<Path>();
    for (Path p : fileList) {
      Path monthPath = p.getParent().getParent();
      monthPathSet.add(monthPath);
    }

    JobConf tmpConf = new JobConf(jobConf);
    archiver.setShouldStop(true);
    ToolRunner.run(tmpConf, archiver, new String[] {});
    Path taskDir = waitForInit();

    List<String> dirStats = readFile(srcFs, new Path(taskDir, "dir_stats"));
    TreeSet<Path> patternSet = new TreeSet<Path>();
    for (String line : dirStats) {
      String path = line.split("\t")[0];
      patternSet.add(new Path(SOURCE_BASE_DIR, path));
    }
    Assert.assertEquals(monthPathSet.size(), patternSet.size());
    for (Path p : monthPathSet) {
      Assert.assertTrue(patternSet.contains(p));
    }
  }

  @Test
  public void testExcludePathPattern() throws Exception {
    TreeSet<Path> monthPathSet = new TreeSet<Path>();
    for (Path p : fileList) {
      Path monthPath = p.getParent().getParent();
      Pattern re = Pattern.compile("year=(\\d+)/month=(\\d+)");
      Matcher matcher = re.matcher(monthPath.toString());
      if (matcher.find()) {
        if (Integer.parseInt(matcher.group(1)) >=2016 &&
            Integer.parseInt(matcher.group(2)) >= 4)
          continue;
      }
      monthPathSet.add(monthPath);
    }

    JobConf tmpConf = new JobConf(jobConf);
    tmpConf.set("dfs.file.archiver.pattern.path.filter.exclude",
        "year=2016/month=(0[4-9]|1[0-2])");
    archiver.setShouldStop(true);
    ToolRunner.run(tmpConf, archiver, new String[] {});
    Path taskDir = waitForInit();

    List<String> dirStats = readFile(srcFs, new Path(taskDir, "dir_stats"));
    TreeSet<Path> patternSet = new TreeSet<Path>();
    for (String line : dirStats) {
      String path = line.split("\t")[0];
      patternSet.add(new Path(SOURCE_BASE_DIR, path));
    }
    Assert.assertEquals(monthPathSet.size(), patternSet.size());
    for (Path p : monthPathSet) {
      Assert.assertTrue(patternSet.contains(p));
    }
  }

  @Test
  public void testOneIteration() throws Exception {
    JobConf tmpConf = new JobConf(jobConf);
    tmpConf.setInt("dfs.file.archiver.schedule.interval.sec", 10);
    tmpConf.setInt("dfs.file.archiver.max.maps", 3);
    Path taskDir = startFileArchiverAndWaitForJobSubmit(tmpConf);
    // then stop the FileArchiver thread, finish the 1st iteration
    archiver.setShouldStop(true);
    waitForAllJobFinished();

    List<String> onCopyList  = readFile(srcFs, new Path(taskDir, "copy_in_flight"));
    Assert.assertFalse("copy_in_files should not be empty", onCopyList.isEmpty());

    // verify copy completed
    boolean copySucceed = true;
    for (String dir: onCopyList) {
      copySucceed = false;
      Path parentDir = new Path(SOURCE_BASE_DIR, dir);
      for (Path file : fileList) {
        if (!file.toString().startsWith(parentDir.toString())) {
          continue;
        }
        try {
          if (!targetFs.exists(file)) {
            copySucceed = false;
            break;
          }
          FileStatus srcStatus = srcFs.getFileStatus(file);
          FileStatus targetStatus = targetFs.getFileStatus(file);
          if (srcStatus.getLen() != targetStatus.getLen() ||
              !srcStatus.getOwner().equals(targetStatus.getOwner())) {
            copySucceed = false;
            break;
          }
          copySucceed = true;
        } catch (IOException e) {
          LOG.warn("access hdfs faild");
        }
      }
      if (!copySucceed)
        break;
    }
    Assert.assertTrue("some file copy failed!", copySucceed);
  }

  @Test
  public void testCopyAllFiles() throws Exception {
    final String PATTERN = "year=2016/month=(0[4-9]|1[0-2])";
    JobConf tmpConf = new JobConf(jobConf);
    tmpConf.setInt("dfs.file.archiver.schedule.interval.sec", 10);
    tmpConf.setInt("dfs.file.archiver.max.maps", 20);
    tmpConf.set("dfs.file.archiver.pattern.path.filter.exclude",
        PATTERN);
    Path taskDir = startFileArchiverAndWaitForJobSubmit(tmpConf);

    waitForAllJobFinished();
    List<String> allFiles = readFile(srcFs, new Path(taskDir, "dir_stats"));
    Path completeFile = new Path(taskDir, "completed");
    List<String> completeFiles = readFile(srcFs, completeFile);
    while (completeFiles.size() != allFiles.size()) {
      Thread.sleep(500);
      completeFiles = readFile(srcFs, completeFile);
    }
    archiver.setShouldStop(true);

    boolean checkSucceed = true;
    Pattern re = Pattern.compile("year=(\\d+)/month=(\\d+)");

    String errMsg = "";
    for (Path file : fileList) {
      Matcher matcher = re.matcher(file.toString());
      if (!matcher.find()) {
        checkSucceed = false;
        errMsg = file + ", file name is not a normal scribe pattern";
        break;
      }
      if (!srcFs.exists(file)) {
        checkSucceed = false;
        errMsg = file + " not exists, delete_after_copy is not enabled, " +
            "it shouldn't happen";
      }
      if (Integer.parseInt(matcher.group(1)) >= 2016
          && Integer.parseInt(matcher.group(2)) >= 4) {
        if (targetFs.exists(file)) {
          checkSucceed = false;
          errMsg = file + " should not exists in target fs";
          break;
        }
      } else {
        if (!targetFs.exists(file)) {
          checkSucceed = false;
          errMsg = file + " not exists in target fs";
          break;
        }
      }
    }
    Assert.assertTrue(errMsg, checkSucceed);
  }

  @Test
  public void testRestart() throws Exception {
    final int MAX_MAPS = 5;
    JobConf tmpConf = new JobConf(jobConf);
    tmpConf.setLong("dfs.file.archiver.schedule.interval.sec", 5);
    tmpConf.setInt("dfs.file.archiver.max.maps", MAX_MAPS);
    Path taskDir = startFileArchiverAndWaitForJobSubmit(tmpConf);
    // stop the service first
    archiver.setShouldStop(true);

    List<String> onCopyList  = readFile(srcFs, new Path(taskDir, "copy_in_flight"));
    Assert.assertFalse("copy_in_files should not be empty", onCopyList.isEmpty());
    // waiting for service stopped
    while (yarnClient.getApplications().size() != onCopyList.size()) {
      Thread.sleep(100);
    }
    Thread.sleep(100);

    // restart the service
    startFileArchiverAndWaitForJobSubmit(tmpConf);
    List<String> completeList = readFile(srcFs, new Path(taskDir, "completed"));
    // waiting for next round schedule submitted
    while (completeList.isEmpty()) {
      Thread.sleep(100);
      // since test file is very small, in most case, 1 map for each copy job
      // is enough, so the job count usually equals map count.
      // this logic is also true for the following assertions
      Assert.assertTrue(getAllRunningJobs().size() <= MAX_MAPS);
      completeList = readFile(srcFs, new Path(taskDir, "completed"));
    }
    archiver.setShouldStop(true);
    List<String> newOnCopyList =
        readFile(srcFs, new Path(taskDir, "copy_in_flight"));
    Assert.assertTrue(newOnCopyList.size() <= MAX_MAPS);
    // make sure there are new task scheduled
    newOnCopyList.removeAll(onCopyList);
    Assert.assertTrue(newOnCopyList.size() > 0);
  }

  @Test
  public void testDelete() throws Exception {
    JobConf tmpConf = new JobConf(jobConf);
    tmpConf.setLong("dfs.file.archiver.schedule.interval.sec", 5);
    tmpConf.setInt("dfs.file.archiver.max.maps", 50);
    tmpConf.setBoolean("dfs.file.archiver.delete.after.copy", true);
    tmpConf.setLong("dfs.file.archiver.delete.interval.sec", 60);
    Path taskDir = startFileArchiverAndWaitForJobSubmit(tmpConf);
    Thread.sleep(1000);
    List<String> removedList = readFile(srcFs, new Path(taskDir, "removed"));
    while (removedList.isEmpty()) {
      Thread.sleep(100);
      removedList = readFile(srcFs, new Path(taskDir, "removed"));
    }
    archiver.setShouldStop(true);
    for (String p : removedList) {
      Path fullPath = new Path(SOURCE_BASE_DIR, p);
      Assert.assertFalse(srcFs.exists(fullPath));
      TrashPolicy trashPolicy =
          TrashPolicy.getInstance(tmpConf, srcFs, srcFs.getHomeDirectory());
      Path trashDir = trashPolicy.getCurrentTrashDir();
      Assert.assertTrue(srcFs.exists(trashDir));
      Assert.assertTrue(
          srcFs.exists(new Path(trashDir, fullPath.toString().substring(1))));
    }
  }

  @Test
  public void testBlackList() throws Exception {
    // create the blacklist
    FileOutputStream fos = new FileOutputStream(new File("blacklist-file"));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
    bw.write("mi-user-0");
    bw.newLine();
    bw.write("mi-user-2");
    bw.close();

    final String PATTERN = "year=2016/month=(0[4-9]|1[0-2])";
    JobConf tmpConf = new JobConf(jobConf);
    tmpConf.setInt("dfs.file.archiver.schedule.interval.sec", 10);
    tmpConf.setInt("dfs.file.archiver.max.maps", 20);
    tmpConf.set("dfs.file.archiver.pattern.path.filter.exclude",
        PATTERN);
    tmpConf.set("dfs.file.archiver.black.list", "blacklist-file");
    Path taskDir = startFileArchiverAndWaitForJobSubmit(tmpConf);

    waitForAllJobFinished();
    Path completeFile = new Path(taskDir, "completed");
    List<String> completeFiles = readFile(srcFs, completeFile);

    boolean testSucceed = true;
    for (String line : completeFiles) {
      if (line.contains("mi-user-0") || line.contains(("mi-user-2"))) {
        testSucceed = false;
        break;
      }
    }
    Assert.assertTrue(testSucceed);
  }
}
