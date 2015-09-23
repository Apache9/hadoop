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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A FailoverProxyProvider implementation which inherit
 * ConfiguredFailoverProxyProvider. The extra functionality is to get active
 * namenode information from zookeeper.
 * 
 * With ConfiguredFailoverProxyProvider, if a nemnode restart, the client might
 * be hung on that namenode for a while before failover to the active namenode
 * since there is usually full GC during bootstrap (due to block reporting burst
 * etc.)
 * 
 * ZkConfiguredFailoverProxyProvider is mean to resolve the issue since it will
 * get the active namenode from zookeeper directly.
 */
public class ZkConfiguredFailoverProxyProvider<T> extends
    ConfiguredFailoverProxyProvider<T> implements Watcher {

  private static final Log LOG = LogFactory
      .getLog(ZkConfiguredFailoverProxyProvider.class);

  private static final int NUM_RETRIES = 3;
  private static final int SLEEP_BETWEEN_RETRY = 5000;
  private static final String LOCK_FILENAME = "ActiveStandbyElectorLock";

  private boolean queryZkOnce;
  private boolean useUnConfiguredNN;

  private String nsId;
  private long failoverFencePeriodInMs;

  public ZkConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface) {
    super(conf, uri, xface);
    queryZkOnce =
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_FAILOVER_QUERY_ZOOKEEPER_ONCE,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_QUERY_ZOOKEEPER_ONCE_DEFAULT);
    useUnConfiguredNN =
        conf.getBoolean(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_USE_UNCONFIGED_NAMENODE,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_USE_UNCONFIGED_NAMENODE_DEFAULT);
    Collection<String> nsIds = DFSUtil.getNameServiceIds(conf);

    if (nsIds.size() == 1) {
      nsId = (String) (nsIds.toArray()[0]);
    } else {
      for (String str : nsIds) {
        if (str.equals(uri.getHost())) {
          nsId = str;
          break;
        }
      }
    }
    if (nsId == null) {
      throw new RuntimeException("No nameservices is configured");
    }
    failoverFencePeriodInMs =
        conf.getLong(DFSConfigKeys.DFS_CLIENT_ZK_FAILOVER_FENCE_PERIOD_INMS,
            DFSConfigKeys.DFS_CLIENT_ZK_FAILOVER_FENCE_PERIOD_INMS_DEFAULT);
    try {
      currentProxyIndex = getActiveNNIndex();
    } catch (Exception e) {
      LOG.error("Fail to get initial active Namenode information", e);
      throw new RuntimeException(e);
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
      zkClient =
          new ZooKeeper(conf.get(CommonConfigurationKeys.ZK_QUORUM_KEY),
              conf.getInt(CommonConfigurationKeys.ZK_SESSION_TIMEOUT_KEY,
                  CommonConfigurationKeys.ZK_SESSION_TIMEOUT_DEFAULT), this);
      int retry = 0;
      while (true) {
        try {
          byte[] activeData = zkClient.getData(getZnode(), false, null);
          ActiveNodeInfo proto;
          try {
            proto = ActiveNodeInfo.parseFrom(activeData);
            activeNN =
                new InetSocketAddress(proto.getHostname(), proto.getPort());
            break;
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Invalid data in ZK: "
                + StringUtils.byteToHexString(activeData));
          }
        } catch (KeeperException ke) {
          LOG.warn("Fail to get active namenode information from zooker", ke);
          LOG.warn("Will retry " + (NUM_RETRIES - retry) + "times");
          if (++retry < NUM_RETRIES) {
            Thread.sleep(SLEEP_BETWEEN_RETRY);
            continue;
          } else {
            break;
          }
        }
      }
      if (activeNN == null) {
        throw new RuntimeException("Fail to get active namenode from zookeeper");
      }
      for (int i = 0; i < proxies.size(); i++) {
        if (activeNN.equals(proxies.get(i).address)) {
          return i;
        }
      }
      if (useUnConfiguredNN) {
        proxies.add(0, new AddressRpcProxyPair<T>(activeNN));
        return 0;
      } else {
        throw new RuntimeException(
            "Active namenode in zk does not exist in configure file");
        }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    if (queryZkOnce) {
      currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
    } else {
      try {
        currentProxyIndex = getActiveNNIndex();
      } catch (Exception e) {
        LOG.error("Fail to get initial active Namenode information", e);
        throw new RuntimeException(e);
      }
    }
    try {
      Thread.sleep(failoverFencePeriodInMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    LOG.info("Failover to namenode " + proxies.get(currentProxyIndex).address);
  }

  public void process(WatchedEvent event) {
    // Empty watcher handler
  }
}
