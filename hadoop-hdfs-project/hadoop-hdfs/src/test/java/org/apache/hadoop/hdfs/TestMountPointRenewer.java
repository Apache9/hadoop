package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestMountPointRenewer {
  @Test
  public void testscheduleRenewer() throws InterruptedException {
    Configuration conf = new Configuration(false);
    conf.set("dfs.mount.point.renewer.debug.updateMptFromZk", "true");
    conf.set(FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL_RANDOMFACTOR,
        "50");
    conf.set(FederationConfigKeys.FEDFS_MOUT_TABLE_RENEW_RETRY_INTERVAL, "50");
    conf.set(FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL, "50");
    final int[] times = { 0 };

    MountPointRenewer mpr = new MountPointRenewer("unittest", conf,
        new MountPointRenewer.RenewMpt() {
          @Override
          public void renewMpt(String viewName, Configuration conf)
              throws IOException {
            times[0]++;
          }
        });

    mpr.initMptFromZkAndKickoffRenewer();
    Thread.sleep(1000);
    assertTrue(times[0] >= 10);
    mpr.close();
    int tmp = times[0];
    Thread.sleep(1000);
    assertTrue(times[0]==tmp);
    try {
      mpr.initMptFromZkAndKickoffRenewer();
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }
  }
}
