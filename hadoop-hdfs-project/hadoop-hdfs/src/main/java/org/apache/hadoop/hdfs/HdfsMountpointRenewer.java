package org.apache.hadoop.hdfs;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class HdfsMountpointRenewer extends MountpointRenewer {

  @Override
  public void updateMountPointConfig(Configuration conf,
      byte[] zkData) throws IOException {
    Map<String, String> newConf =
        deserializeString2Mountpoint(new String(zkData));
    Map<String, String> originalMountpoint = getMountPointEntries(conf);
    // newConf should contain all mount points in original conf.
    verify(originalMountpoint, newConf);
    // set entries from newConf to conf in incremental mode.
    Iterator<Map.Entry<String, String>> iter = newConf.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> kv = iter.next();
      String key = kv.getKey();
      String value = kv.getValue();
      if (conf.get(key) == null) {
        conf.set(key, value);
      } else if (key.equals(DFSConfigKeys.DFS_NAMESERVICES)) {
        Set<String> nsSet = new HashSet<String>();
        nsSet.addAll(conf.getStringCollection(key));
        nsSet.addAll(StringUtils.getStringCollection(value));
        conf.set(key, Joiner.on(",").skipNulls().join(nsSet));
      }
    }
  }

  // newConf should be a super set of original mount point.
  public void verify(Map<String,String> originalMountpoint,
      Map<String,String> newConf) throws IOException {
    Iterator<Map.Entry<String, String>> iter =
        originalMountpoint.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> kv = iter.next();
      if (!kv.getValue().equals(newConf.get(kv.getKey()))) {
        throw new IOException(
            "New mount point table is invalid since " + kv.getKey() + "=" + kv
                .getValue() + " is not contained.");
      }
    }
  }

  // Return the mount point configuration in "key=value;key=value;key=value"
  // format.
  protected Map<String,String> getMountPointEntries(Configuration config)
      throws IOException {
    Map<String,String> res = new HashMap<String,String>();

    final String mtPrefix =
        Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + ".";
    final String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    for (Map.Entry<String, String> si : config) {
      final String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        String src = key.substring(mtPrefix.length());
        if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
          // ignore - we set home dir from config
          continue;
        } else if (src.startsWith(linkPrefix) || src
            .startsWith(linkMergePrefix)) { // link or merge link
          res.put(si.getKey(), si.getValue());
        } else {
          throw new IOException(
              "ViewFs: Invalid entry in Mount table in config: " + src);
        }
      }
    }
    return res;
  }

  public static String serializeMountpoint2String(Configuration conf,
      String clusterName) throws IOException {
    StringBuilder mpBuilder = new StringBuilder();
    Set<String> nsIds = new HashSet<String>();

    final String mtPrefix =
        Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + ".";
    final String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    boolean isFirstEntry = true;
    for (Map.Entry<String, String> si : conf) {
      final String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        String src = key.substring(mtPrefix.length());
        if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
          // ignore - we set home dir from config
          continue;
        } else if (src.startsWith(linkPrefix) || src
            .startsWith(linkMergePrefix)) {
          final String target = si.getValue(); // link or merge link
          if (!isFirstEntry) {
            mpBuilder.append(";");
          }
          mpBuilder.append(si.getKey()).append("=").append(target);
          isFirstEntry = false;
        } else {
          throw new IOException(
              "ViewFs: Invalid entry in Mount table in config: " + src);
        }
        String nsId;
        try {
          nsId = new URI(si.getValue()).getAuthority();
        } catch (URISyntaxException use) {
          throw new IOException(use);
        }
        if (nsId != null && !nsIds.contains(nsId)) {
          String nsConfig = getNsConfigs(conf, nsId);
          if (nsConfig != null) {
            nsIds.add(nsId);
            if (!isFirstEntry) {
              mpBuilder.append(";");
            }
            mpBuilder.append(nsConfig);
          }
        }
      }
    }
    if (!isFirstEntry) {
      mpBuilder.append(";");
    }
    mpBuilder.append(DFSConfigKeys.DFS_NAMESERVICES).append("=")
        .append(Joiner.on(",").skipNulls().join(nsIds));
    return mpBuilder.toString();
  }

  public static Map<String,String> deserializeString2Mountpoint(String content)
      throws IOException {
    if (content == null) {
      throw new IOException("content is null !");
    }
    String[] kvs = content.split(";");
    Map<String,String> conf = new HashMap<String,String>();
    for (String kv : kvs) {
      if (kv.length() == 0) {
        continue;
      }
      int splitIdx = kv.indexOf("=");
      if (splitIdx < 0) {
        continue;
      }
      String key = kv.substring(0, splitIdx);
      String val = kv.substring(splitIdx + 1);
      conf.put(key, val);
    }
    return conf;
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
    if (builder.length()>0 && builder.charAt(builder.length()-1) == ';') {
      builder.deleteCharAt(builder.length()-1);
    }
    return builder.toString();
  }
}
