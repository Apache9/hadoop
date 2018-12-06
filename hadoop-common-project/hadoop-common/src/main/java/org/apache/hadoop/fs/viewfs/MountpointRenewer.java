package org.apache.hadoop.fs.viewfs;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

abstract public class MountpointRenewer {

  static final public Log LOG = LogFactory.getLog(MountpointRenewer.class);
  public static interface RenewMountpoint {
    /**
     * do UpdateMountpoint will be called when updateMptFromZk.
     * */
    void doUpdateMountpoint();
  }

  protected String clusterName;
  private long mptCheckInterval;
  private long lastCheckTime; // default value is 0
  private byte[] lastMountPointTable = null;
  private RenewMountpoint rmp;
  private boolean skipRenew = false;

  public MountpointRenewer() {
  }

  public void initialize(String clusterName, Configuration conf,
      RenewMountpoint rmp) throws IOException {
    this.clusterName = clusterName;
    this.mptCheckInterval =
        conf.getLong(CommonConfigurationKeys.VIEW_FS_MOUNT_TABLE_RENEW_INTERVAL,
            CommonConfigurationKeys.VIEW_FS_MOUNT_TABLE_RENEW_INTERVAL_DEFAULT);
    this.rmp = rmp;
    this.skipRenew =
        conf.getBoolean(CommonConfigurationKeys.VIEW_FS_SKIP_MOUNT_TABLE_RENEW,
            CommonConfigurationKeys.VIEW_FS_SKIP_MOUNT_TABLE_RENEW_DEFAULT);
  }

  /**
   * update mount table in conf with mount table from zk
   * 1. if time interval is less than mptCheckInterval since last check, ignore.
   * 2. if get mount table from zk failed, ignore.
   * 3. if update conf's mount table failed, ignore.
   * 4. otherwise, mount table will be updated, and doUpdateMountpoint will be
   *    called.
   * */
  public void updateMptFromZk(Configuration conf) {
    if (skipRenew) {
      return;
    }
    synchronized(this) {
      long curTime = Time.now();
      if (curTime - lastCheckTime < mptCheckInterval) {
        return;
      } else {
        lastCheckTime = curTime;
      }
    }
    byte[] mptFromZk = null;
    try {
      mptFromZk = getMptConfFromZookeeper(conf);
    } catch (Exception e) {
      LOG.debug("Get new mount point table inforamtion from zk failed", e);
      // Ignore, using whatever we have in the original configration
    }
    if (mptFromZk != null) {
      synchronized(this) {
        // Renew the fsstate in viewfs if the mount table in zk changed. Otherwise
        // do nothing.
        if (lastMountPointTable == null || !Arrays
            .equals(mptFromZk, lastMountPointTable)) {
          try {
            updateMountPointConfig(conf, mptFromZk);
          } catch (IOException ioe) {
            LOG.debug("Update mount point table from zk failed", ioe);
            return;
          }
          lastMountPointTable = mptFromZk;
          if (rmp != null) {
            rmp.doUpdateMountpoint();
          }
        }
      }
    }
  }
  /**
   * update conf with data from zk
   * @param  conf
   *         Configuration that is going to be updated
   * @param zkData
   *         byte[] data got from znode.
   * */
  abstract public void updateMountPointConfig(Configuration conf,
      byte[] zkData) throws IOException;

  public byte[] getMptConfFromZookeeper(Configuration conf)
      throws IOException, IllegalArgumentException, KeeperException,
      InterruptedException {
    ZooKeeper zkClient = null;
    try {
      String znode = getZnode(conf);
      zkClient = getZkClient(conf);
      byte[] activeData = zkClient.getData(znode, false, null);
      return activeData;
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  public void setMptConfToZookeeper(byte[] data, Configuration conf)
      throws IOException, IllegalArgumentException, KeeperException,
      InterruptedException {
    ZooKeeper zkClient = null;
    try {
      String znode = getZnode(conf);
      zkClient = getZkClient(conf);
      Stat stat = zkClient.exists(znode, false);
      if (stat == null) { // znode doesn't exist, create it.
        String zkAclConf = conf.get(CommonConfigurationKeys.ZK_ACL_KEY,
            CommonConfigurationKeys.ZK_ACL_DEFAULT);
        zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
        List<ACL> zkAcls = ZKUtil.parseACLs(zkAclConf);
        if (zkAcls.isEmpty()) {
          zkAcls = ZooDefs.Ids.CREATOR_ALL_ACL;
        }
        // Create all parents first
        String pathParts[] = znode.split("/");
        Preconditions
            .checkArgument(pathParts.length >= 2 && pathParts[0].isEmpty(),
                "Invalid path: %s", znode);

        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < pathParts.length - 1; i++) {
          sb.append("/").append(pathParts[i]);
          String prefixPath = sb.toString();
          LOG.debug("Ensuring existence of " + prefixPath);
          try {
            zkClient.create(prefixPath, new byte[] {}, zkAcls,
                CreateMode.PERSISTENT);
          } catch (KeeperException.NodeExistsException e) {
            // This is OK - just ensuring existence.
          }
        }
        // create the node
        zkClient.create(znode, data, zkAcls, CreateMode.PERSISTENT);
      } else {
        zkClient.setData(znode, data, stat.getVersion());
      }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  public void deleteMptConfFromZookeeper(Configuration conf)
      throws IOException, KeeperException, InterruptedException {
    ZooKeeper zkClient = null;
    try {
      String znode = getZnode(conf);
      zkClient = getZkClient(conf);
      Stat stat = zkClient.exists(znode, false);
      if (stat == null) { // znode doesn't exist, create it.
        return;
      } else {
        zkClient.delete(znode, stat.getVersion());
      }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  String getZnode(Configuration conf) {
    String znode =
        conf.get(CommonConfigurationKeys.ZK_PARENT_ZNODE_KEY,
            CommonConfigurationKeys.ZK_PARENT_ZNODE_DEFAULT)
            + "/"
            + clusterName
            + "/"
            + conf.get(CommonConfigurationKeys.VIEW_FS_ZK_MPT_NODE_KEY,
            CommonConfigurationKeys.VIEW_FS_ZK_MPT_NODE_DEFAULT);
    return znode;
  }

  ZooKeeper getZkClient(Configuration conf) throws IOException {
    String zkQuorum = ZKUtil.getZkQuorum(conf, clusterName);
    if (zkQuorum == null) {
      throw new IOException("Failed get zkQuorum. zkQuorum is null.");
    }
    ZooKeeper zkClient =
        new ZooKeeper(zkQuorum, conf.getInt(
            CommonConfigurationKeys.ZK_SESSION_TIMEOUT_KEY,
            CommonConfigurationKeys.ZK_SESSION_TIMEOUT_DEFAULT),
            new Watcher() {
              public void process(WatchedEvent event) {
                // Empty watcher handler
              }
            });
    return zkClient;
  }

  // never return null.
  public static MountpointRenewer createMountpointRenewer(String clusterName,
      Configuration conf, RenewMountpoint renewMountpoint) {
    MountpointRenewer mountpointRenewer = null;
    Class<?> clazz = conf.getClass("fs.viewfs.mount.point.renewer.impl",
        DefaultMountpointRenewer.class);
    if (clazz != null) {
      mountpointRenewer =
          (MountpointRenewer) ReflectionUtils.newInstance(clazz, conf);
      try {
        mountpointRenewer.initialize(clusterName, conf,
            renewMountpoint);
      } catch (IOException ioe) {
        LOG.info(
            "Failed when constructing mount point renewer, use default one.",
            ioe);
        mountpointRenewer = new DefaultMountpointRenewer();
      }
    }
    return mountpointRenewer;
  }
}