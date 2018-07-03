package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
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
    nextRenewTimer = new Timer(true);// create a daemon thread
  }
  
  /*
   * Return the mount point configuration in "key=value;key=value;key=value"
   * format.
   */
  public static String getMountPointConfig(final Configuration config,
      final String viewName, final boolean includeNsConfig) throws IOException {
    StringBuilder mpBuilder = new StringBuilder();
    Set<String> nsIds = new HashSet<String>();
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

        if (includeNsConfig) {
          String nsId;
          try {
            nsId = new URI(target).getAuthority();
          } catch (URISyntaxException use) {
            throw new IOException(use);
          }
          if (nsId != null && !nsIds.contains(nsId)) {
            String nsConfig = getNsConfigs(config, nsId);
            if (nsConfig != null) {
              nsIds.add(nsId);
              mpBuilder.append(nsConfig);
            }
          }
        }

        mpBuilder.append(si.getKey()).append("=").append(target);
        isFirstEntry = false;
      }
    }
    if (includeNsConfig) {
      if (!isFirstEntry) {
        mpBuilder.append(";");
      }
      mpBuilder.append(DFSConfigKeys.DFS_NAMESERVICES).append("=")
              .append(Joiner.on(",").skipNulls().join(nsIds));
    }

    return mpBuilder.toString();
  }

  public static String getNsConfigs(Configuration conf, String nsId)
      throws IOException {
    if (nsId.contains(":")) { // it's an address
      return null;
    }

    StringBuilder builder = new StringBuilder();
    // add failover provider config
    String failoverProviderKey = DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX, nsId);
    String failoverClassName = conf.get(failoverProviderKey,
        ZkConfiguredFailoverProxyProvider.class.getName());
    builder.append(failoverProviderKey).append("=").append(failoverClassName);
    builder.append(";");

    // add dfs.ha.namenodes config
    String nsIdKey =
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, nsId);
    Collection<String> nnIds = DFSUtil.getNameNodeIds(conf, nsId);
    if (nnIds.size() != 2) {
      throw new IOException("Can't get HA configs for nsId: " + nsId);
    }
    builder.append(nsIdKey).append("=").append(conf.get(nsIdKey));
    builder.append(";");

    // add rpc-addresses
    for (String nnId : nnIds) {
      String addrKey = DFSUtil.addKeySuffixes(
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, nnId);
      String NNAddr = conf.get(addrKey);
      if (NNAddr == null) {
        throw new IOException("Can't get NN address configs for " + addrKey);
      }
      builder.append(addrKey).append("=").append(NNAddr);
      builder.append(";");
    }
    return builder.toString();
  }

  public static void verifyNewMountPoints(Configuration conf, String kvConf,
      String viewName) throws IllegalArgumentException, IOException {
    String origKvConfig = getMountPointConfig(conf, viewName, false);
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

  public static boolean updateMountPointConfig(Configuration conf,
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
      } else if (key.equals(DFSConfigKeys.DFS_NAMESERVICES)) {
        res = true;
        Set<String> nsSet = new HashSet<String>();
        nsSet.addAll(Arrays.asList(conf.get(key).split(",")));
        nsSet.addAll(Arrays.asList(val.split(",")));
        conf.set(key, Joiner.on(",").skipNulls().join(nsSet));
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

  public static String getMptConfFromZookeeper(String viewName,
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
    String zkQuorum = getZkQuorum(conf);
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

  private void scheduleRenewer(final long delay) {
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
