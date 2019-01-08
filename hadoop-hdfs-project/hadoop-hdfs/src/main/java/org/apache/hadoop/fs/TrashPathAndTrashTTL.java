package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.TRASH_PATH_CONF_FILE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.TRASH_TTL_CONF_FILE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.ATTR_TRASH_PATH_HAS_LOADED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.ATTR_TRASH_TTL_HAS_LOADED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.ATTR_TRASH_TTL_NAME;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.ATTR_HAS_LOADED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.ATTR_TRASH_PATH_NAME;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IS_TRASH_PATH;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class TrashPathAndTrashTTL {
  protected FileSystem fs; // the FileSystem
  protected FSNamesystem namesystem;
  private static final Log LOG = LogFactory.getLog(TrashPathAndTrashTTL.class);
  private static final int SLEEP_TIME_SECONDS = 10;

  public TrashPathAndTrashTTL(FileSystem fs, FSNamesystem namesystem) {
    this.fs = fs;
    this.namesystem = namesystem;
  }

  public Runnable setTrashPath() {
    return new TrashPath();
  }

  public Runnable setTrashTTL() {
    return new TrashTTL();
  }

  private class TrashPath implements Runnable {

    private void loadTrashConf() {
      File trashPathConfFile = new File(TRASH_PATH_CONF_FILE);
      if (!trashPathConfFile.exists()) {
        LOG.warn(
            "the TrashTTL config file doesn't exist: " + trashPathConfFile);
        return;
      }
      BufferedReader br = null;
      try {
        br = new BufferedReader(
            new InputStreamReader(new FileInputStream(TRASH_PATH_CONF_FILE)));
        String pathStr;
        while ((pathStr = br.readLine()) != null) {
          Path path = new Path(pathStr);
          if (fs.exists(path)) {
            Map<String, byte[]> allXAttrs = fs.getXAttrs(path);
            if (allXAttrs == null
                || !allXAttrs.containsKey(ATTR_TRASH_PATH_HAS_LOADED)) {
              LOG.info(
                  "now add path xattr trashpath for just one time:" + pathStr);
              byte[] value = StringUtils.getBytesUtf8(IS_TRASH_PATH);
              byte[] hasloaded = StringUtils.getBytesUtf8(ATTR_HAS_LOADED);
              fs.setXAttr(path, ATTR_TRASH_PATH_NAME, value);
              fs.setXAttr(path, ATTR_TRASH_PATH_HAS_LOADED, hasloaded);
            }
          }
        }
      } catch (Exception e) {
        LOG.error(
            "failed to load trash path config , exception: " + e.getMessage());
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            LOG.error("failed to close BufferedReader , exception: "
                + e.getMessage());
          }
        }
      }

    }

    @Override
    public void run() {
      while (namesystem.isInSafeMode()) {
        try {
          LOG.info("now in safe mode, will retry set trash path after"
              + SLEEP_TIME_SECONDS + " seconds");
          Thread.sleep(SLEEP_TIME_SECONDS * 1000);
        } catch (InterruptedException e) {
          LOG.error("encounter InterruptedException :" + e.getMessage());
        }
      }
      loadTrashConf();
    }
  }

  private class TrashTTL implements Runnable {

    private void loadTrashConf() {
      File trashTTLConfFile = new File(TRASH_TTL_CONF_FILE);
      if (!trashTTLConfFile.exists()) {
        LOG.warn("the TrashTTL config file doesn't exist: " + trashTTLConfFile);
        return;
      }
      BufferedReader br = null;
      try {
        br = new BufferedReader(
            new InputStreamReader(new FileInputStream(TRASH_TTL_CONF_FILE)));
        String trashTTLRecord;
        String pathStr;
        while ((trashTTLRecord = br.readLine()) != null) {
          String[] trashTTLParameter =
              org.apache.commons.lang3.StringUtils.split(trashTTLRecord, ',');
          if (trashTTLParameter == null || trashTTLParameter.length != 2) {
            LOG.error(
                "trashtll input record [" + trashTTLRecord + "] is invalid");
            continue;
          }
          pathStr = "/user/" + trashTTLParameter[0];
          Path path = new Path(pathStr);
          if (fs.exists(path)) {
            Map<String, byte[]> allXAttrs = fs.getXAttrs(path);
            if (allXAttrs == null
                || !allXAttrs.containsKey(ATTR_TRASH_TTL_HAS_LOADED)) {
              LOG.info(
                  "now add path xattr trashttl for just one time:" + pathStr);
              byte[] value = StringUtils.getBytesUtf8(trashTTLParameter[1]);
              byte[] hasloaded = StringUtils.getBytesUtf8(ATTR_HAS_LOADED);
              fs.setXAttr(path, ATTR_TRASH_TTL_NAME, value);
              fs.setXAttr(path, ATTR_TRASH_TTL_HAS_LOADED, hasloaded);
            }
          }
        }
      } catch (Exception e) {
        LOG.error(
            "failed to load trash ttl config , exception:" + e.getMessage());
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            LOG.error("failed to close BufferedReader , exception: "
                + e.getMessage());
          }
        }
      }

    }

    @Override
    public void run() {
      while (namesystem.isInSafeMode()) {
        try {
          LOG.info("now in safe mode, will retry set trash ttl after"
              + SLEEP_TIME_SECONDS + " seconds");
          Thread.sleep(SLEEP_TIME_SECONDS * 1000);
        } catch (InterruptedException e) {
          LOG.error("encounter InterruptedException :" + e.getMessage());
        }
      }
      loadTrashConf();
    }
  }
}
