/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.AddressRpcProxyPair;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.protobuf.InvalidProtocolBufferException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;

/**
 * A FailoverProxyProvider implementation which inherit
 * ConfiguredFailoverProxyProvider. The extra functionality is to get active
 * namenode information from zookeeper.
 * 
 * With ConfiguredFailoverProxyProvider, if a namenode restart, the client might
 * be hung on that namenode for a while before failover to the active namenode
 * since there is usually full GC during bootstrap (due to block reporting burst
 * etc.)
 * 
 * ZkConfiguredFailoverProxyProvider is mean to resolve the issue since it will
 * get the active namenode from zookeeper directly.
 */
public class ZkConfiguredFailoverProxyProvider<T> extends
    RequestHedgingProxyProvider<T> implements Watcher {

  private static final Log LOG = LogFactory
      .getLog(ZkConfiguredFailoverProxyProvider.class);

  private static final String LOCK_FILENAME = "ActiveStandbyElectorLock";

  private boolean queryZkOnce;
  private boolean useUnConfiguredNN;
  private boolean noZkQuorum;
  private long durationBetweenRetryZk;
  private Random rand;
  private long lastUseZkTime;
  private long failoversBeforeTryZk;
  private int failoverAttempts = 0;

  private String nsId;

  public ZkConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface) {
    super(conf, uri, xface);
    rand = new Random();
    queryZkOnce =
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_FAILOVER_QUERY_ZOOKEEPER_ONCE,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_QUERY_ZOOKEEPER_ONCE_DEFAULT);
    useUnConfiguredNN =
        conf.getBoolean(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_USE_UNCONFIGED_NAMENODE,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_USE_UNCONFIGED_NAMENODE_DEFAULT);
    durationBetweenRetryZk =
        conf.getLong(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_GET_ACTIVE_NAMENODE_DURATION_BETWEEN_RETRYZK_DEFAULT);
    if (durationBetweenRetryZk > 0) {
      durationBetweenRetryZk += calcRandomWithLowBound(durationBetweenRetryZk);
    }

    long maxFailoverAttempts = conf.getInt(
            DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
            DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
    failoversBeforeTryZk = calcRandomWithLowBound(maxFailoverAttempts);

    noZkQuorum = false;
    lastUseZkTime = -1;
    Collection<String> nsIds = DFSUtil.getNameServiceIds(conf);

    nsId = uri.getAuthority();
    if (nsId == null) {
      throw new RuntimeException("No nameservices is configured");
    }
    String zkQuorum = ZKUtil.getZkQuorum(conf, nsId);
    if (zkQuorum == null) {
      noZkQuorum = true;
    }
  }

  @Override
  protected void initializeProxies(URI uri) {
    Map<String, Map<String, InetSocketAddress>> map =
        DFSUtil.getHaNnRpcAddresses(conf);
    Map<String, InetSocketAddress> addressesInNN = map.get(uri.getHost());

    // With ZkConfiguredFailoverProvider, there might be no namenodes configured
    // in hdfs-site.xml
    if (addressesInNN != null && addressesInNN.size() > 0) {
      Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
      for (InetSocketAddress address : addressesOfNns) {
        proxies.add(new AddressRpcProxyPair<T>(address));
      }
      // The client may have a delegation token set for the logical
      // URI of the cluster. Clone this token to apply to each of the
      // underlying IPC addresses so that the IPC code can find it.
      HAUtil.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    }
  }

  private String getZnode() {
    return conf.get(CommonConfigurationKeys.ZK_PARENT_ZNODE_KEY,
        CommonConfigurationKeys.ZK_PARENT_ZNODE_DEFAULT)
        + "/"
        + nsId
        + "/"
        + LOCK_FILENAME;
  }

  private int getActiveNNIndex() throws IOException, KeeperException,
      InterruptedException {
    InetSocketAddress activeNN = null;
    ZooKeeper zkClient = null;
    try {
      String zkQuorum = ZKUtil.getZkQuorum(conf, nsId);
      assert (zkQuorum != null);
      zkClient =
          new ZooKeeper(zkQuorum, conf.getInt(
              CommonConfigurationKeys.ZK_SESSION_TIMEOUT_KEY,
              CommonConfigurationKeys.ZK_SESSION_TIMEOUT_DEFAULT), this, true);
      try {
        byte[] activeData = zkClient.getData(getZnode(), false, null);
        ActiveNodeInfo proto;
        proto = ActiveNodeInfo.parseFrom(activeData);
        activeNN = new InetSocketAddress(proto.getHostname(), proto.getPort());
      } catch (InvalidProtocolBufferException e) {
        throw new KeeperException.RuntimeInconsistencyException();
      }
      if (activeNN == null) {
        if (proxies.isEmpty()) {
          throw new RuntimeException("No namenode rpc address available on both zk and configuration");
        }
        // Will fall back to configuration
        throw new KeeperException.SystemErrorException();
      }
      for (int i = 0; i < proxies.size(); i++) {
        if (activeNN.equals(proxies.get(i).address)) {
          return i;
        }
      }
      if (useUnConfiguredNN) {
        proxies.add(0, new AddressRpcProxyPair<T>(activeNN));
        LOG.info("clone the delegation token from: " + logicalUri + " to " + activeNN);
        Set<InetSocketAddress> addressSet = new HashSet<InetSocketAddress>();
        addressSet.add(activeNN);
        HAUtil.cloneDelegationTokenForLogicalUri(UserGroupInformation.getCurrentUser(),
          logicalUri, addressSet);
        return 0;
      } else {
        throw new RuntimeException(
            "Active namenode in zk does not exist in configure file");
      }
    } catch (KeeperException ke) {
      // In case of zk exception, will fail back to try NNs in proxies one by
      // one
      LOG.warn("Fail to get active namenode from zookeeper since " + ke
          + ",  will try to use addresses in local configuration file.");

      if (proxies.size() == 0) {
        if (ke.code() == KeeperException.Code.NONODE) {
          throw new RuntimeException("No namenode rpc address available on both zk and configuration");
        }
        throw ke;
      }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
      lastUseZkTime = Time.now();
    }
    return -1;
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    failoverAttempts++;
    boolean useZk = (!noZkQuorum) && (failoverAttempts > failoversBeforeTryZk);
    if (useZk && queryZkOnce) {
      // update failoversBeforeTryZk, then we will not try zk anymore
      failoversBeforeTryZk = Integer.MAX_VALUE;
    }

    if (useZk && lastUseZkTime > 0) {
      if (Time.now() - lastUseZkTime < durationBetweenRetryZk) {
        useZk = false;
      }
    }

    if (useZk) {
        try {
          getActiveNNIndex();
        } catch (Exception e) {
          if (e instanceof RuntimeException) {
            throw (RuntimeException)e;
          }
          LOG.warn("Get address from zk failed", e);
        }
    }
    super.performFailover(currentProxy);
  }

  public void process(WatchedEvent event) {
    // Empty watcher handler
  }

  private long calcRandomWithLowBound(long val) {
    // Return a random number which is smaller than val whereas larger than a
    // small number caculated from val.
    // The purpose is to make unit test happy.
    long lowBound = val / 10;
    long upBound = val * 9 / 10;
    return lowBound + rand.nextInt((int) upBound);
  }
}
