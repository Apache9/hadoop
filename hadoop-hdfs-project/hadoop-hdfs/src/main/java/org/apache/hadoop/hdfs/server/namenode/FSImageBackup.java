package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.ThrottledInputStream;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Created by xiegang1 on 17-12-6.
 */
public class FSImageBackup extends Thread {
  private static final Log LOG = LogFactory.getLog(FSImageBackup.class);
  private static String BACKUP_FILE_PREFIX = "fsimage";

  private String backupCluster = null;
  private FSNamesystem fsNamesystem;
  private Configuration conf;
  private String backupPath = null;
  private int maxBackups = 0;
  private ExecutorService uploaderExcutor;
  private long lastTX = Long.MIN_VALUE;
  private boolean stop = false;
  private String localNameServiceName = null;
  private long backupCheckInterval = 0;
  private long bandwidth = 0;
  private int bufferSize = 0;

  // for the test
  private String lastBackupFSImageFileName = null;
  private String lastBackupFSImageMD5FileName = null;

  public FSImageBackup(FSNamesystem ns,  Configuration conf) {
    this.fsNamesystem = ns;
    this.conf = new Configuration(conf);
    this.localNameServiceName = conf.get("dfs.nameservices");
    String backupDir = conf.get(DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_DIR_KEY, DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_DIR_DEFAULT);
    this.backupPath = backupDir + "/" + getNameServiceId();
    this.maxBackups = conf.getInt(DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_MAX_BACKUP_KEY, DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_MAX_BACKUP_DEFAULT);
    this.uploaderExcutor = Executors.newFixedThreadPool(1);
    this.backupCluster = conf.get(DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_CLUSTER_KEY);
    this.backupCheckInterval = conf.getLong(DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_CHECK_INTERVAL_DEFAULT);
    this.bandwidth = conf.getLongBytes(DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_BANDWIDTH_KEY, DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_BANDWIDTH_DEFAULT);
    this.bufferSize =  conf.getInt(DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_UPLOAD_BUFFER_SIZE_KEY, DFSConfigKeys.DFS_NAMENODE_BACKUP_FSIMAGE_UPLOAD_BUFFER_SIZE_DEFAULT);
    try {
      this.conf.set("dfs.nameservices", backupCluster);
      this.conf.set("fs.defaultFS", "hdfs://" + backupCluster);
    } catch (Exception e) {
      LOG.error(e);
      LOG.error("fail to set nameservice when init FSImageBackup");
      stop = true;
      return;
    }
    this.setDaemon(true);
    LOG.info("use " + backupCluster + " for fsimage backup" + "last TXID " + lastTX);
  }

  private File getLastLocalFSImageFile () {
    return fsNamesystem.getFSImage().getStorage().getHighestFsImageName();
  }

  private long getLatestTxId () {
    return fsNamesystem.getFSImage().getStorage().getMostRecentCheckpointTxId();
  }

  private String getNameServiceId() {
    String nameServiceId = "";
    String ip = "";


    try {
      InetAddress ia = null;
      ia = InetAddress.getLocalHost();
      if (ia != null) {
        ip = ia.getHostAddress();
      }
    } catch (UnknownHostException e) {

    }


    nameServiceId = localNameServiceName + "_" + ip;

    return nameServiceId;
  }

  @VisibleForTesting
  public String getLastBackupFSImageFileName() {
    return lastBackupFSImageFileName;
  }

  @VisibleForTesting
  public String getLastBackupFSImageMD5FileName() {
    return lastBackupFSImageMD5FileName;
  }

  @VisibleForTesting
  public  void updateLastFSImageFileName(String name) {
    lastBackupFSImageFileName = backupPath + "/" + name;
  }

  @VisibleForTesting
  public  void updateLastFSImageMD5FileName(String name) {
    lastBackupFSImageMD5FileName = backupPath + "/" + name;
  }

  @VisibleForTesting
  public void setConf(Configuration conf) {
    this.conf = conf;
  }


  private String[] getOldestBackupToDelete(FileStatus[] fileStatuses) {
    if (fileStatuses == null || fileStatuses.length == 0 || fileStatuses.length < maxBackups) {
      return null;
    }
    long backups = 0;
    long oldestTS = Long.MAX_VALUE;
    String oldestTxIdStr = null;
    String oldestTsStr = null;
    for (FileStatus fileStatus : fileStatuses) {
      String filename = fileStatus.getPath().getName();
      if (filename.contains(".md5")) {
        continue;
      }
      if (filename.startsWith(BACKUP_FILE_PREFIX)) {
        String[] parts = filename.split("_");
        if (parts == null || parts.length != 3 || !parts[0].equals(BACKUP_FILE_PREFIX)) {
          continue;
        }
        long tmpTS = -1;
        String tmpTSStr = parts[2];
        String tmpTxIdStr = parts[1];
        backups ++;
        try {
          tmpTS = Long.parseLong(tmpTSStr);
        } catch (NumberFormatException e) {
          LOG.warn("wrong fsimage name format " + filename);
          continue;
        }
        if (tmpTS < oldestTS) {
          oldestTS = tmpTS;
          oldestTsStr = tmpTSStr;
          oldestTxIdStr = tmpTxIdStr;
        }
      }
    }

    if (backups < maxBackups) {
      return null;
    }

    if (oldestTxIdStr == null || oldestTsStr == null) {
      return null;
    }

    String[] results = new String[2];
    results[0] = BACKUP_FILE_PREFIX + "_" + oldestTxIdStr + "_" + oldestTsStr;
    results[1] = BACKUP_FILE_PREFIX + "_" + oldestTxIdStr + ".md5" + "_" + oldestTsStr;


    return results;
  }

  // return null means no history
  private BackupHistory getBackupHistory(FileSystem fs) throws IOException {
    if (fs == null) {
      return null;
    }
    int backups = 0;
    long oldestTS = Long.MAX_VALUE;
    long lastTx = Long.MIN_VALUE;
    String oldestTxIdStr = null;
    String oldestTsStr = null;
    BackupHistory backupHistory = new BackupHistory();

    try {
      FileStatus[] fileStatuses = fs.listStatus(new Path(backupPath));
      for (FileStatus fileStatus : fileStatuses) {
        String filename = fileStatus.getPath().getName();
        if (filename.contains(".md5")) {
          continue;
        }
        if (filename.startsWith(BACKUP_FILE_PREFIX)) {
          String[] parts = filename.split("_");
          if (parts == null || parts.length != 3 || !parts[0].equals(BACKUP_FILE_PREFIX)) {
            continue;
          }
          long tmpTS = 0;
          long tmpTx = 0;
          String tmpTSStr = parts[2];
          String tmpTxIdStr = parts[1];
          try {
            tmpTS = Long.parseLong(tmpTSStr);
            tmpTx = Long.parseLong(tmpTxIdStr);
          } catch (NumberFormatException e) {
            LOG.warn("wrong fsimage name format " + filename);
            continue;
          }
          String backupMetaFile = BACKUP_FILE_PREFIX + "_" + tmpTxIdStr + ".md5" + "_" + tmpTSStr;
          // without meta file, it should be incomplete backup, we just skip it now
          if (!fs.exists(new Path(backupPath, backupMetaFile))) {
            LOG.warn("md5 file doesn't exist " + backupMetaFile);
            continue;
          }

          //now, we found a valid backup
          backups++;

          if (lastTx < tmpTx) {
            lastTx = tmpTx;
          }


          if (tmpTS < oldestTS) {
            oldestTS = tmpTS;
            oldestTsStr = tmpTSStr;
            oldestTxIdStr = tmpTxIdStr;
          }
        }
      }
    } catch (IOException e) {
      LOG.error("fail to get the backup history");
      throw e;
    }


    if (oldestTxIdStr == null || oldestTsStr == null) {
      return null;
    }

    backupHistory.completedBackupNum = backups;
    backupHistory.oldestBackupFile = BACKUP_FILE_PREFIX + "_" + oldestTxIdStr + "_" + oldestTsStr;
    backupHistory.oldestBackupMD5File = BACKUP_FILE_PREFIX + "_" + oldestTxIdStr + ".md5" + "_" + oldestTsStr;
    backupHistory.lastTx = lastTx;


    return backupHistory;
  }

  public synchronized void updateLastTxId(long newTx) {
    this.lastTX = newTx;
  }

  public synchronized long getLastTxId() {
    return this.lastTX;
  }

  public void doBackup() throws IOException {
    if (backupCluster == null) {
      LOG.info("the HDFS cluster used to backup FSImage is not configured, skip the backup");
      return;
    }
    LOG.debug("lastBackupTX=" + lastTX + " lastTX=" + getLatestTxId());
    if (((ThreadPoolExecutor)uploaderExcutor).getActiveCount() > 0) {
      LOG.debug("there is an in progress backup, skip this time, last backup txid " + getLastTxId());
      return;
    }
    if (getLastTxId() != getLatestTxId()) {
      FSImageUploader uploader = new FSImageUploader(this);
      uploaderExcutor.submit(uploader);
    }
  }

  public synchronized void shouldStop () {
    this.stop = true;
    if (this.uploaderExcutor != null) {
      this.uploaderExcutor.shutdownNow();
    }
    LOG.info("stop fsimage backup service");
  }

  public synchronized boolean isStop () {
    return this.stop;
  }

  @Override
  public void run () {
    while (!isStop()) {
      try {
        doBackup();
      } catch (IOException e) {
        LOG.error(e);
      } finally {
        if (!isStop()) {
          try {
            Thread.sleep(backupCheckInterval);
          } catch (InterruptedException e) {
          }
        }
      }
    }
  }

  static class FSImageUploader implements Runnable {
    private FSImageBackup fsImageBackup;

    public FSImageUploader(FSImageBackup fsImageBackup) {
      this.fsImageBackup = fsImageBackup;
    }

    private boolean uploadFile (FileSystem fs, File localFile, Path path) {
      FSDataOutputStream outputStream = null;
      FileInputStream fileInputStream = null;
      try {
        outputStream = fs.create(path, true);
        fileInputStream = new FileInputStream(localFile);
        ThrottledInputStream throttledInputStream = new ThrottledInputStream(fileInputStream, fsImageBackup.bandwidth);

        IOUtils.copyBytes(throttledInputStream, outputStream, fsImageBackup.bufferSize);
      } catch (IOException ioe) {
        LOG.error(ioe);
        LOG.error("fail to upload file " + localFile.getName() + " to " + path);
        return false;
      } finally {
        if (outputStream != null) {
          IOUtils.closeStream(outputStream);
        }

        if (fileInputStream != null) {
          IOUtils.closeStream(fileInputStream);
        }
      }

      return true;
    }

    @Override
    public void run() {
      FileSystem fs = null;
      String hdfsFsimageFileName = null;
      String hdfsFsimageMD5FileName = null;
      try {
        try {
          fs = FileSystem.get(fsImageBackup.conf);
        } catch (Exception e) {
          Throwable t = e;
          while (t != null) {
            LOG.error(t);
            t = t.getCause();
          }

          LOG.error("fail to get dfs " + fsImageBackup.backupCluster);
          return;
        }

        //check the existing backup
        Path backupPath = new Path(fsImageBackup.backupPath);
        try {
          if (!fs.exists(backupPath)) {
            fs.mkdirs(backupPath);
          }
        } catch (Exception e) {
          LOG.error(e);
          LOG.error("fail to make path " + backupPath);
          return;
        }

        BackupHistory backupHistory = fsImageBackup.getBackupHistory(fs);

        if (backupHistory != null && backupHistory.lastTx == fsImageBackup.getLatestTxId()) {
          LOG.info("the fsimage with txid " + backupHistory.lastTx + " has been backup, skip it");
          return;
        }

        if (backupHistory != null && backupHistory.completedBackupNum >= fsImageBackup.maxBackups) {
          LOG.info("purge the oldest fsimage backup " + backupHistory.oldestBackupFile);
          fs.delete(new Path(fsImageBackup.backupPath + "/" + backupHistory.oldestBackupFile), true);
          fs.delete(new Path(fsImageBackup.backupPath + "/" + backupHistory.oldestBackupMD5File), true);
        }

        long tmpTxid = fsImageBackup.getLatestTxId();
        long ts = Time.now();

        File lastFSImageFile = fsImageBackup.getLastLocalFSImageFile();
        File lastFSImageMD5File = MD5FileUtils.getDigestFileForFile(lastFSImageFile);
        hdfsFsimageFileName = lastFSImageFile.getName() + "_" + ts;
        hdfsFsimageMD5FileName = lastFSImageMD5File.getName() + "_" + ts;

        Path fsImagePath = new Path(fsImageBackup.backupPath + "/" + hdfsFsimageFileName);
        Path fsImageMD5Path = new Path(fsImageBackup.backupPath + "/" + hdfsFsimageMD5FileName);

        LOG.info("start upload fsimage " + hdfsFsimageFileName);
        if (!uploadFile(fs, lastFSImageFile, fsImagePath)) {
          return;
        }

        if (!uploadFile(fs, lastFSImageMD5File, fsImageMD5Path)) {
          return;
        }

        long startTs = Time.now();
        fsImageBackup.updateLastFSImageFileName(hdfsFsimageFileName);
        fsImageBackup.updateLastFSImageMD5FileName(hdfsFsimageMD5FileName);
        fsImageBackup.updateLastTxId(tmpTxid);

        LOG.info("complete upload fsimage to " + fsImagePath + " txid=" + tmpTxid + " cost=" + (Time.now() - startTs) + "ms");

      } catch (FileNotFoundException e) {
        LOG.error(e);
        LOG.error("backup dir doesn't exist " + fsImageBackup.backupPath);
      } catch (IOException e) {
        LOG.error(e);
        LOG.error("fail to upload the fsimage ");
      } finally {
        if (fs != null) {
          try {
            fs.close();
          } catch (IOException e) {
            LOG.error("fail to close fs " + e);
          }
        }
        if (fsImageBackup != null && !fsImageBackup.isStop()) {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
          }
        }

      }

    }
  }

  private static class BackupHistory {
    public int completedBackupNum;
    public String oldestBackupFile;
    public String oldestBackupMD5File;
    long lastTx;
  }


}
