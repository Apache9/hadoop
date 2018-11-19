package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHdfsMountpointRenewer {
  final String clusterName = "test-cluster";
  class TestHdfsMountpoint extends HdfsMountpointRenewer {
    @Override
    public byte[] getMptConfFromZookeeper(Configuration conf)
        throws IOException, IllegalArgumentException, KeeperException,
        InterruptedException {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 3; i++) {
        String cn = clusterName + "-" + i;
        sb.append(Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + "."
            + Constants.CONFIG_VIEWFS_LINK + "." + "/user/h_d_p/dir-" + i
            + "=" + "hdfs://"+cn+"/user/h_d_p/dir-" + i);
        sb.append(";");
        sb.append("dfs.client.failover.proxy.provider." + cn + "=" +
            "org.apache.hadoop.hdfs.server.namenode.ha."
            + "ConfiguredFailoverProxyProvider");
        sb.append(";");
        sb.append("dfs.ha.namenodes." + cn + "=" + "host0,host1");
        sb.append(";");
        sb.append("dfs.namenode.rpc-address." + cn + ".host0" + "=ip:port");
        sb.append(";");
        sb.append("dfs.namenode.rpc-address." + cn + ".host1" + "=ip:port");
        sb.append(";");
      }
      sb.append(
          "dfs.nameservices=" + clusterName + "-0," + clusterName + "-1,"
              + clusterName + "-2");
      sb.append(";");
      return sb.toString().getBytes();
    }
  }

  @Test
  public void testVerifyMountpointInZk() throws Exception {
    Configuration conf = new Configuration(false);
    String cn = clusterName + "-5";
    conf.set(Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + "."
            + Constants.CONFIG_VIEWFS_LINK + "." + "/user/h_d_p/dir-5",
        "hdfs://" + cn + "/user/h_d_p/dir-5");
    conf.set("dfs.client.failover.proxy.provider." + cn,
        "org.apache.hadoop.hdfs.server.namenode.ha."
            + "ConfiguredFailoverProxyProvider");
    conf.set("dfs.ha.namenodes." + cn, "host0,host1");
    conf.set("dfs.namenode.rpc-address." + cn + ".host0", "ip:port");
    conf.set("dfs.namenode.rpc-address." + cn + ".host1", "ip:port");
    conf.set("dfs.nameservices", clusterName + "-5");
    conf.setLong(CommonConfigurationKeys.VIEW_FS_MOUNT_TABLE_RENEW_INTERVAL,
        1000);
    final AtomicInteger counter = new AtomicInteger(0);
    MountpointRenewer mptRenewer =
        new TestHdfsMountpoint() {
          @Override
          public void verifyNewMountPoints(
              Configuration originalConf, Configuration newConf)
              throws IOException {
            try {
              super.verifyNewMountPoints(originalConf, newConf);
            } catch (IOException ioe) {
              assertExceptionContains("New mount point table is invalid", ioe);
              throw ioe;
            }
            assert false;
          }
        };
    mptRenewer.initialize(clusterName, conf, new MountpointRenewer.RenewMountpoint() {
      @Override
      public void doUpdateMountpoint() {
        counter.incrementAndGet();
      }
    });

    mptRenewer.updateMptFromZk(conf);
  }
  @Test
  public void testUpdateMountpoint() throws Exception {
    Configuration conf = new Configuration(false);
    String cn = clusterName + "-0";
    conf.set(Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + "."
            + Constants.CONFIG_VIEWFS_LINK + "." + "/user/h_d_p/dir-0",
        "hdfs://" + cn + "/user/h_d_p/dir-0");
    conf.set("dfs.client.failover.proxy.provider." + cn,
        "org.apache.hadoop.hdfs.server.namenode.ha."
            + "ConfiguredFailoverProxyProvider");
    conf.set("dfs.ha.namenodes." + cn, "host0,host1");
    conf.set("dfs.namenode.rpc-address." + cn + ".host0", "ip:port");
    conf.set("dfs.namenode.rpc-address." + cn + ".host1", "ip:port");
    conf.set("dfs.nameservices", clusterName + "-0");
    conf.setLong(CommonConfigurationKeys.VIEW_FS_MOUNT_TABLE_RENEW_INTERVAL,
        1000);
    final AtomicInteger counter = new AtomicInteger(0);
    MountpointRenewer mptRenewer =
        new TestHdfsMountpoint() {};
    mptRenewer.initialize(clusterName, conf, new MountpointRenewer.RenewMountpoint() {
      @Override
      public void doUpdateMountpoint() {
        counter.incrementAndGet();
      }
    });

    mptRenewer.updateMptFromZk(conf);
    assertEquals(1, counter.get());
    assertTrue(conf.get("dfs.nameservices").equals(
        clusterName + "-0," + clusterName + "-1," + clusterName + "-2"));
    for (int i = 0; i < 3; i++) {
      cn = clusterName + "-" + i;
      assertTrue(conf.get(
          Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + "."
              + Constants.CONFIG_VIEWFS_LINK + "." + "/user/h_d_p/dir-" + i)
          .equals("hdfs://" + cn + "/user/h_d_p/dir-" + i));
      assertTrue(conf.get("dfs.client.failover.proxy.provider." + cn).equals(
          "org.apache.hadoop.hdfs.server.namenode.ha."
              + "ConfiguredFailoverProxyProvider"));
      assertTrue(conf.get("dfs.ha.namenodes." + cn).equals("host0,host1"));
      assertTrue(conf.get("dfs.namenode.rpc-address." + cn + ".host0")
          .equals("ip:port"));
      assertTrue(conf.get("dfs.namenode.rpc-address." + cn + ".host1")
          .equals("ip:port"));
    }
  }

  @Test
  public void testCheckInterval() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setLong(CommonConfigurationKeys.VIEW_FS_MOUNT_TABLE_RENEW_INTERVAL,
        1000);
    final StringBuilder sb = new StringBuilder("key=value;");
    final AtomicInteger counter = new AtomicInteger(0);
    MountpointRenewer mptRenewer =
        new HdfsMountpointRenewer() {
          @Override
          public byte[] getMptConfFromZookeeper(Configuration conf)
              throws IOException, IllegalArgumentException, KeeperException,
              InterruptedException {
            return sb.toString().getBytes();
          }
        };
    mptRenewer.initialize(clusterName, conf, new MountpointRenewer.RenewMountpoint() {
      @Override
      public void doUpdateMountpoint() {
        counter.incrementAndGet();
      }
    });

    for (int i = 0; i < 10; i++) {
      mptRenewer.updateMptFromZk(conf);
      sb.append("key"+i+"="+"value"+i+";");
      Thread.sleep(500);
    }
    assertEquals(5, counter.get());
  }
}
