package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

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

  public TrashPathConfigMgr(Configuration conf) {
    refreshInterval = conf.getLong(DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_KEY, DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_DEFAULT);
    start();
  }
  public void start () {
    refreshThr = new Thread(this);
    refreshThr.setDaemon(true);
    refreshThr.start();
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

  private synchronized void updateConf (ArrayList <String[]> list) {
    trashPathList = list;
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
      int i = 0;
      while (i < pathNodes.length && i < trashPathNodes.length) {
        if (pathNodes[i].equals(trashPathNodes[i])) {
          i++;
          continue;
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public void run() {
    while (true) {
      try {
        File trashPathConfFile = new File(TRASH_PATH_CONFIG_FILE);
        if (!trashPathConfFile.exists()
            || lastModifiedTime == trashPathConfFile.lastModified()) {
          LOG.warn("the trash path config file doesn't exist or not modified: " + trashPathConfFile);
          try {
            Thread.sleep(refreshInterval);
          } catch (InterruptedException e) {
          }
          continue;
        }

        lastModifiedTime = trashPathConfFile.lastModified();

      } catch (Exception e) {
        LOG.error("fail to open trash path config file" + e);
        try {
          Thread.sleep(refreshInterval);
        } catch (InterruptedException ie) {
        }
      }

      BufferedReader br = null;
      try {
        ArrayList <String[]> tmpList = new ArrayList<String[]>();
        br = new BufferedReader(new InputStreamReader(new FileInputStream(TRASH_PATH_CONFIG_FILE)));
        String pathStr = null;
        while ((pathStr = br.readLine()) != null) {
          addPath(tmpList, pathStr);
        }
        updateConf(tmpList);
      } catch (Exception e) {
        LOG.error("failed to load trash config " + e);
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }

      try {
        Thread.sleep(refreshInterval);
      } catch (InterruptedException e) {
      }
    }
  }
}
