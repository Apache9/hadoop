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
package org.apache.hadoop.hdfs.server.ttlmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * TtlManager is used to manage the TtlPolicy{@link TtlPolicy}, and can be
 * easily extended to support managing other polices.
 */
public class TtlManager extends Thread {

  private static final Log LOG = LogFactory.getLog(TtlManager.class);

  private final Worker worker;
  private final Map<String, Policy> policies = new HashMap<String, Policy>();
  private final ScheduledExecutorService scheduler;
  TtlMetrics  metrics;

  public TtlManager(Configuration conf) {
    super(TtlManager.class.getName());
    int corePoolSize = conf.getInt(
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_CORE_POOL_SIZE_KEY,
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_CORE_POOL_SIZE_DEFAULT);
    int maxPoolSize = conf.getInt(
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_MAX_POOL_SIZE_KEY,
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_MAX_POOL_SIZE_DEFAULT);
    long keepAliveMs = conf.getLong(
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_POOL_KEEPALIVE_MS_KEY,
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_POOL_KEEPALIVE_MS_DEFAULT);
    int queueCapacity = conf.getInt(
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_QUEUE_CAPACITY_KEY,
        DFSConfigKeys.HDFS_TTLMANAGER_WORKER_QUEUE_CAPACITY_DEFAULT);
    this.worker = new Worker(corePoolSize, maxPoolSize, keepAliveMs,
        queueCapacity);

    this.scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "Scheduler");
        thread.setDaemon(true);
        return thread;
      }
    });
    
    this.metrics = TtlMetrics.create(); 
  }

  public void registerPolicy(Policy policy) {
    policies.put(policy.getName(), policy);
  }
  
  TtlMetrics getMetrics() {
    return metrics;
  }

  @Override
  public void run() {
    while (!isInterrupted()) {
      for (Entry<String, Policy> entry : policies.entrySet()) {
        Policy policy = entry.getValue();
        // The policy is disabled
        if (!policy.isEnabled()) {
          continue;
        }
        metrics.incrTtlScheduledTask();
        schedulePolicyTask(policy);
      }

      try {
        sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  private void schedulePolicyTask(final Policy policy) {
    // Before scheduling the policy, in order to avoid duplicate scheduling,
    // we should disable the policy at first.
    policy.disable();

    switch (policy.getType()) {
      case ONE_SHOT:
        scheduleOneshotTask(policy);
        break;
      case PERIOD:
        schedulePeriodTask(policy);
        break;
      default:
        LOG.error("Invalid policy type:" + policy +
            " for policy:" + policy.getName());
        break;
    }
  }

  private void scheduleOneshotTask(final Policy policy) {
    scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        worker.submitPolicyTask(policy);
      }
    }, policy.getDelayMs(), TimeUnit.MILLISECONDS);
  }

  private void schedulePeriodTask(final Policy policy) {
    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        worker.submitPolicyTask(policy);
      }
    }, policy.getDelayMs(), policy.getPeriodMs(), TimeUnit.MILLISECONDS);
  }

  public static void main(String[] args)
      throws InterruptedException, IOException {
    Configuration conf = new HdfsConfiguration();
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_TTLMANAGER_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_TTLMANAGER_KERBEROS_PRINCIPAL_KEY);
    TtlManager ttlManager = new TtlManager(conf);
    ttlManager.registerPolicy(new TtlPolicy(conf, ttlManager.getMetrics()));
    ttlManager.start();

    while (true) {
      LOG.info("TtlManager is running");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }

    ttlManager.interrupt();
    ttlManager.join();
    LOG.info("TtlManager is stopped");
  }
}
