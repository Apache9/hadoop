package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.server.namenode.TrashPathConfigMgr.TRASH_PATH_CONFIG_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestTrashPathConfigMgr {
  @Test
  public void testTrashPathConfigMgrWorkflow()
      throws IOException, InterruptedException {
    final AtomicInteger update = new AtomicInteger(0);
    final AtomicInteger handleNotUpdate = new AtomicInteger(0);
    class TTrashPathConfigMgr extends TrashPathConfigMgr {
      public TTrashPathConfigMgr(Configuration conf) {
        super(conf);
      }
      protected synchronized void updateConf (ArrayList<String[]> list) {
        update.incrementAndGet();
      }
      protected void handle() {
        handleNotUpdate.getAndIncrement();
      }
    }
    File trashPathConfFile = new File(TRASH_PATH_CONFIG_FILE);
    if (!trashPathConfFile.exists()) {
      trashPathConfFile.createNewFile();
    }
    Configuration conf = new Configuration();
    conf.set(DFS_NAMENODE_TRASH_PATH_CONF_REFRESH_INTERVAL_KEY, "1000");
    TrashPathConfigMgr tpcm = new TTrashPathConfigMgr(conf);
    tpcm.start();
    Thread.sleep(3000);
    assertEquals(1, update.get());
    assertEquals(2, handleNotUpdate.get());
    Thread thread = tpcm.refreshThr;
    tpcm.stop();
    assertFalse(thread.isAlive());
  }
}
