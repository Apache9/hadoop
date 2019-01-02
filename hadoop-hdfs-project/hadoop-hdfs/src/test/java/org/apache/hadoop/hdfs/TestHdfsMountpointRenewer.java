package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
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
  public void testDeprecatedKey() throws Exception {
    System.setProperty("hadoop.property.dfs.block.size", "128m");
    Configuration conf = new HdfsConfiguration(false);
    assertEquals(1, conf.size());
    final AtomicInteger counter = new AtomicInteger(0);
    MountpointRenewer mptRenewer = new TestHdfsMountpoint() {};
    mptRenewer.initialize(clusterName, conf, new MountpointRenewer.RenewMountpoint() {
      @Override
      public void doUpdateMountpoint() {
        counter.incrementAndGet();
      }
    });

    mptRenewer.updateMptFromZk(conf);
    assertEquals(1, counter.get());
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
          public void verify(Map<String, String> originalConf,
              Map<String, String> newConf)
              throws IOException {
            try {
              super.verify(originalConf, newConf);
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
    conf.set("dfs.namenode.rpc-address." + cn + ".host0", "localhost:port");
    conf.set("dfs.namenode.rpc-address." + cn + ".host1", "localhost:port");
    conf.set("dfs.nameservices", clusterName + "-0" + ",c4tst-non-exist");
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
    Collection<String> collection =
        conf.getStringCollection("dfs.nameservices");
    assertTrue(collection.contains("c4tst-non-exist"));
    for (int i = 0; i < 3; i++) {
      cn = clusterName + "-" + i;
      assertTrue(collection.contains(cn));
      assertEquals("hdfs://" + cn + "/user/h_d_p/dir-" + i, conf.get(
          Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + "."
              + Constants.CONFIG_VIEWFS_LINK + "." + "/user/h_d_p/dir-" + i));
      assertEquals(
          "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
          conf.get("dfs.client.failover.proxy.provider." + cn));
      assertEquals("host0,host1", conf.get("dfs.ha.namenodes." + cn));
      if (i == 0) {
        assertEquals("localhost:port",
            conf.get("dfs.namenode.rpc-address." + cn + ".host0"));
        assertEquals("localhost:port",
            conf.get("dfs.namenode.rpc-address." + cn + ".host1"));
      } else {
        assertEquals("ip:port",
            conf.get("dfs.namenode.rpc-address." + cn + ".host0"));
        assertEquals("ip:port",
            conf.get("dfs.namenode.rpc-address." + cn + ".host1"));
      }
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
