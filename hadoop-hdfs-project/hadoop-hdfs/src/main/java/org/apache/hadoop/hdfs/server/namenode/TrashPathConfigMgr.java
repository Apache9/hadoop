package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_KEY;

/**
 * Created by xiegang1 on 17-5-11.
 */
public class TrashPathConfigMgr implements Runnable {
  public static final Log LOG = LogFactory.getLog(TrashPathConfigMgr.class);
  public static final String TRASH_PATH_CONFIG_FILE = "TrashPath.conf";
  private ArrayList <String[]> trashPathList;
  Thread refreshThr;
  private long refreshInterval;
  private long lastModifiedTime = 0;
  private AtomicBoolean running = new AtomicBoolean(false);


  public TrashPathConfigMgr(Configuration conf) {
    refreshInterval = conf.getLong(DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_KEY, DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_DEFAULT);
  }

  public void start () {
    running.set(true);
    refreshThr = new Thread(this);
    refreshThr.setDaemon(true);
    refreshThr.start();
  }

  public void stop() {
    running.set(false);
    if (refreshThr != null) {
      refreshThr.interrupt();
      try {
        refreshThr.join(2000);
      } catch (InterruptedException e) {
      }
    }
    refreshThr = null;
  }

  private void addPath (ArrayList<String[]> list,  String path) {
    if (path == null) {
      return;
    }
    if (!path.startsWith("/")) {
      return;
    }

    if (path.equals("/")) {
      return;
    }

    String[] pathNodes = path.split("/");
    if (pathNodes != null) {
      LOG.info("Add trash path: " + path);
      list.add(pathNodes);
    }
  }

  protected synchronized void updateConf (ArrayList <String[]> list) {
    trashPathList = list;
  }

  protected void handle() { // for unit test

  }

  private boolean hasCommonPrefix(String[] path1, String[] path2 ) {
    if (path1 == null || path2 == null) {
      return false;
    }
    int i = 0;
    while (i < path1.length && i < path2.length) {
      if (path1[i].equals(path2[i])) {
        i++;
        continue;
      } else {
        return false;
      }

    }
    return true;

  }

  public synchronized boolean needMoveToTrash (String pathStr) {
    if (pathStr == null || trashPathList == null) {
      return false;
    }
    String[] pathNodes = pathStr.split("/");
    if (pathNodes == null) {
      return false;
    }

    for (String[] trashPathNodes : trashPathList) {
      if (trashPathNodes == null) {
        continue;
      }
      if (hasCommonPrefix(pathNodes, trashPathNodes)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public void run() {
    while (running.get()) {
      File trashPathConfFile = new File(TRASH_PATH_CONFIG_FILE);
      BufferedReader br = null;
      try {
        if (trashPathConfFile.exists() && lastModifiedTime != trashPathConfFile
            .lastModified()) {
          lastModifiedTime = trashPathConfFile.lastModified();
          ArrayList<String[]> tmpList = new ArrayList<String[]>();
          br = new BufferedReader(new FileReader(trashPathConfFile));
          String pathStr = null;
          while ((pathStr = br.readLine()) != null) {
            addPath(tmpList, pathStr);
          }
          updateConf(tmpList);
          LOG.info("update trash path conf from file:" + trashPathConfFile);
        } else {
          handle();
        }
      } catch (Throwable e) {
        LOG.error("fail to update trash path config file:" + trashPathConfFile,
            e);
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
          }
        }
        try {
          Thread.sleep(refreshInterval);
        } catch (InterruptedException e) {
        }
      }
    }
  }
}
