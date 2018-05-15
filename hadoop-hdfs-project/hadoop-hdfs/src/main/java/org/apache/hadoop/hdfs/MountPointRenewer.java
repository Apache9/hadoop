package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MountPointRenewer {
  
  private Configuration conf;
  private String viewName;
  final private Log LOG = LogFactory.getLog(MountPointRenewer.class);
  
  // mptRenewInterval is the interval to get new configuration in normal
  // condition.
  // mptRetryInterval is the interval to get new configuration if it failed to
  // do so last time.
  private long mptRenewInterval;
  private long mptRenewRandomFactor;
  private long mptRetryInterval;
  private Random renewRand;
  private String lastMountPointTable = null;
  private RenewMpt renewMpt;
  private Timer nextRenewTimer = null;

  public interface RenewMpt {
    public void renewMpt(String viewName, Configuration conf)
        throws IOException;
  }
  
  public MountPointRenewer(String inViewName, Configuration inConf, RenewMpt rm) {
    this.viewName = inViewName;
    this.conf = inConf;
    mptRenewInterval =
        conf.getLong(FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL,
            FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL_DEFAULT);
    mptRenewRandomFactor =
        conf.getLong(
            FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL_RANDOMFACTOR,
            FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL_RANDOMFACTOR_DEFAULT);
    mptRetryInterval =
        conf.getLong(
            FederationConfigKeys.FEDFS_MOUT_TABLE_RENEW_RETRY_INTERVAL,
            FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_RETRY_INTERVAL_DEFAULT);
    renewRand = new Random();
    renewMpt = rm;
  }
  
  /*
   * Return the mount point configuration in "key=value;key=value;key=value"
   * format.
   */
  public static String getMountPointConfig(final Configuration config,
      final String viewName) throws IOException {
    StringBuilder mpBuilder = new StringBuilder();
    String vName = viewName;
    if (vName == null) {
      vName = Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
    }

    final String mtPrefix = Constants.CONFIG_VIEWFS_PREFIX + "." + vName + ".";
    final String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    boolean isFirstEntry = true;
    for (Entry<String, String> si : config) {
      final String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        boolean isMergeLink = false;
        String src = key.substring(mtPrefix.length());
        if (src.startsWith(linkPrefix)) {
          src = src.substring(linkPrefix.length());
        } else if (src.startsWith(linkMergePrefix)) { // A merge link
          isMergeLink = true;
          src = src.substring(linkMergePrefix.length());
        } else if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
          // ignore - we set home dir from config
          continue;
        } else {
          throw new IOException(
              "ViewFs: Invalid entry in Mount table in config: " + src);
        }

        final String target = si.getValue(); // link or merge link
        if (!isFirstEntry) {
          mpBuilder.append(";");
        }
        mpBuilder.append(si.getKey()).append("=").append(target);
        isFirstEntry = false;
      }
    }
    return mpBuilder.toString();
  }

  public static void verifyNewMountPoints(Configuration conf, String kvConf,
      String viewName) throws IllegalArgumentException, IOException {
    String origKvConfig = getMountPointConfig(conf, viewName);
    String[] origKvs = origKvConfig.split(";");
    String[] kvs = kvConf.split(";");
    for (String origKv : origKvs) {
      boolean contiansOrigKv = false;
      for (String kv : kvs) {
        if (kv.equals(origKv)) {
          contiansOrigKv = true;
          break;
        }
      }
      if (!contiansOrigKv) {
        throw new IllegalArgumentException(
            "New mount point table is invalid since " + origKv
                + " is not contained.");
      }
    }
  }

  private static boolean updateMountPointConfig(Configuration conf,
      String kvConfig,
      String viewName) throws IllegalArgumentException, IOException {
    if (kvConfig == null) {
      return false;
    }
    boolean res = false;
    verifyNewMountPoints(conf, kvConfig, viewName);
    String[] kvs = kvConfig.split(";");
    for (String kv : kvs) {
      int splitIdx = kv.indexOf("=");
      String key = kv.substring(0, splitIdx);
      String val = kv.substring(splitIdx + 1);
      if (conf.get(key) == null) {
        res = true;
        conf.set(key, val);
      }
    }
    return res;
  }

  private static String getZkQuorum(Configuration inconf) {
    // If no observer is configured, fail back to ha quorum. The intention is to
    // support smooth upgrading.
    String zkQuorum = inconf.get(DFSConfigKeys.DFS_CLIENT_ZOOKEEPER_OBSERVER);
    if (zkQuorum == null) {
      zkQuorum = inconf.get(CommonConfigurationKeys.ZK_QUORUM_KEY);
    }
    return zkQuorum;
  }

  private static String getMptConfFromZookeeper(String viewName,
      Configuration conf)
      throws IOException,
      IllegalArgumentException, KeeperException, InterruptedException {
    ZooKeeper zkClient = null;
    try {
      String znode =
          conf.get(CommonConfigurationKeys.ZK_PARENT_ZNODE_KEY,
              CommonConfigurationKeys.ZK_PARENT_ZNODE_DEFAULT)
              + "/"
              + viewName
              + "/"
              + conf.get(FederationConfigKeys.FEDFS_ZK_MPT_NODE_KEY,
                  FederationConfigKeys.FEDFS_ZK_MPT_NODE_DEFAULT);
      String zkQuorum = getZkQuorum(conf);
      if (zkQuorum == null) {
        return null;
      }
      zkClient =
          new ZooKeeper(zkQuorum, conf.getInt(
              CommonConfigurationKeys.ZK_SESSION_TIMEOUT_KEY,
              CommonConfigurationKeys.ZK_SESSION_TIMEOUT_DEFAULT),
              new Watcher() {
                public void process(WatchedEvent event) {
                  // Empty watcher handler
                }
              });
      byte[] activeData = zkClient.getData(znode, false, null);
      return new String(activeData);
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  public static boolean updateMptFromZkOnce(String viewName, Configuration conf) {
    String mptFromZk = null;
    try {
      mptFromZk = getMptConfFromZookeeper(viewName, conf);
    } catch (Exception e) {
      // Ignore, using whatever we have in the original configration
      return false;
    }
    if (mptFromZk != null) {
      // Renew the fsstate in viewfs if the mount table in zk changed. Otherwise
      // do nothing.
      try {
        return updateMountPointConfig(conf, mptFromZk, viewName);
      } catch (IOException ioe) {
        // Ignore, using whatever we have in the original configration
        return false;
      }
    }
    return false;
  }

  public String getMptZnodePath() {
    String znode =
        conf.get(CommonConfigurationKeys.ZK_PARENT_ZNODE_KEY,
            CommonConfigurationKeys.ZK_PARENT_ZNODE_DEFAULT)
            + "/"
            + viewName
            + "/"
            + conf.get(FederationConfigKeys.FEDFS_ZK_MPT_NODE_KEY,
                FederationConfigKeys.FEDFS_ZK_MPT_NODE_DEFAULT);
    return znode;
  }

  public ZooKeeper getZkClient() throws IOException {
    String zkQuorum = conf.get(CommonConfigurationKeys.ZK_QUORUM_KEY);
    if (zkQuorum == null) {
      return null;
    }
    return new ZooKeeper(zkQuorum, conf.getInt(
        CommonConfigurationKeys.ZK_SESSION_TIMEOUT_KEY,
        CommonConfigurationKeys.ZK_SESSION_TIMEOUT_DEFAULT), new Watcher() {
          public void process(WatchedEvent event) {
            // Empty watcher handler
          }
        });
  }

  private boolean updateMptFromZk(String viewName, Configuration conf) {
    String mptFromZk = null;
    try {
      mptFromZk = getMptConfFromZookeeper(viewName, conf);
    } catch (Exception e) {
      LOG.debug("Get new mount point table inforamtion from zk failed", e);
      // Ignore, using whatever we have in the original configration
      return false;
    }
    if (mptFromZk != null) {
      // Renew the fsstate in viewfs if the mount table in zk changed. Otherwise
      // do nothing.
      if (lastMountPointTable == null || !mptFromZk.equals(lastMountPointTable)) {
        try {
          updateMountPointConfig(conf, mptFromZk, viewName);
        } catch (IOException ioe) {
          LOG.debug("Update mount point table inforamtion from zk failed", ioe);
          return false;
        }
      }
      lastMountPointTable = mptFromZk;
    }
    return true;
  }
  

  synchronized public void scheduleRenewer(final long delay) {
    nextRenewTimer = new Timer(true);
    nextRenewTimer.schedule(new TimerTask() {
      public void run() {
        long nextDelay;
        if (updateMptFromZk(viewName, conf)) {
          try {
            renewMpt.renewMpt(viewName, conf);
            nextDelay =
                mptRenewInterval
                    + renewRand.nextInt((int) mptRenewRandomFactor);
          } catch (IOException ioe) {
            // Using the old fsState
            nextDelay =
                mptRetryInterval
                    + renewRand.nextInt((int) mptRenewRandomFactor);
          }
        } else {
          nextDelay =
              mptRetryInterval + renewRand.nextInt((int) mptRenewRandomFactor);
        }
        scheduleRenewer(nextDelay);
      }
    }, delay);
  }

  public void initMptFromZkAndKickoffRenewer() {
    long initialDelay;
    boolean skip =
        conf.getBoolean(FederationConfigKeys.FEDFS_SKIP_MOUNT_TABLE_RENEW,
            FederationConfigKeys.FEDFS_SKIP_MOUNT_TABLE_RENEW_DEFAULT);
    if (!skip) {
      if (updateMptFromZk(viewName, conf)) {
        initialDelay =
            mptRenewInterval + renewRand.nextInt((int) mptRenewRandomFactor);
      } else {
        initialDelay =
            mptRetryInterval + renewRand.nextInt((int) mptRenewRandomFactor);
      }
      scheduleRenewer(initialDelay);
    }
  }

  synchronized public void close() {
    if (nextRenewTimer != null) {
      nextRenewTimer.cancel();
    }
  }
}
