package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.util.ZKUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;

public class FederationUtil {

  private static final Log LOG = LogFactory.getLog(FederationUtil.class);

  private FederationUtil() {
    /* Hidden constructor */ }

  /**
   * Get all nsIds from config according to the cluster logic name.
   * @param clusterName federated cluster logic name.
   * @param config where get nsId from.
   * @return nsIds
   * @throws URISyntaxException if the given config has error.
   */
  public static Set<String> getAllNsId(String clusterName, Configuration config)
      throws URISyntaxException {
    Set<String> nsIds = new HashSet<String>();
    final String mtPrefix =
        Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + ".";
    Configuration localConfig = new Configuration(config);
    for (Map.Entry<String, String> si : localConfig) {
      String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        String target = si.getValue();
        String nsId;
        nsId = new URI(target).getAuthority();
        if (nsId != null) {
          nsIds.add(nsId);
        }
      }
    }
    return nsIds;
  }

  /**
   * For federated cluster, set items like foo.{cluster logic name} to all
   * foo.{nsId}
   * @param clusterName federated cluster logic name.
   * @param config will be set to.
   * @throws URISyntaxException if the given config has error.
   */
  public static void confAllNamespace(String clusterName, Configuration config)
      throws URISyntaxException {
    // TODO: Support custom config item maybe with lambda?
    Set<String> nsIds = getAllNsId(clusterName, config);
    String zkQuorum = ZKUtil.getZkQuorum(config, clusterName);
    String proxyProviderClass = config
        .get(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + clusterName);
    for (String nsId : nsIds) {
      if (zkQuorum != null && config
          .get(CommonConfigurationKeys.ZK_OBSERVER + "." + nsId) == null) {
        config.set(CommonConfigurationKeys.ZK_OBSERVER + "." + nsId, zkQuorum);
      }
      if (proxyProviderClass != null && config.get(
          DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nsId) == null) {
        config.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nsId,
            proxyProviderClass);
      }
    }
  }
}
