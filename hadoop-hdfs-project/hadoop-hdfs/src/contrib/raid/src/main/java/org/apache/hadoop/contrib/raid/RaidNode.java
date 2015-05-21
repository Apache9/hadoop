/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.contrib.raid;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.ClientRaidnodeProtocolService;
import org.apache.hadoop.contrib.raid.RaidTask.CollectRaidInfoTask;
import org.apache.hadoop.contrib.raid.RaidTask.FixerTask;
import org.apache.hadoop.contrib.raid.RaidTask.RaidTaskUtils;
import org.apache.hadoop.contrib.raid.RaidTask.TaskPurpose;
import org.apache.hadoop.contrib.raid.RaidTask.ZombieSweeperTask;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.BlockingService;

/**
 * The RaidNode controls the whole process of HDFS raid and runs as a standalone daemon.
 */
public class RaidNode extends Configured implements ClientRaidnodeProtocol {

  private final Configuration conf;
  private final ListeningExecutorService taskExecutor;
  private final ExecutorService callbackExecutor;
  private final BlockCodec codec;
  private RPC.Server ipcServer;
  private Policy policy;
  private long encodeTaskDone;
  private long zombieSweeperTaskDone;
  private long fixerTaskDone;
  private long moverTaskDone;
  private boolean shouldRun;
  private Timer lastEncodeTimer;
  private Timer lastZombieSweeperTimer;
  private Timer lastFixerTimer;
  private Timer lastMoverTimer;
  private RaidMetrics metrics;
  private HttpServer2 httpServer;

  private static final int TASK_QUEUE_CAPACITY = 1024;
  private static final int CORE_POOL_SIZE = 4;
  private static final int MAX_POOL_SIZE = 100;
  private static final int THREAD_KEEP_ALIVE_SECS = 60;

  public RaidNode(Configuration conf) throws IOException {
    this.conf = conf;

    BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<Runnable>(TASK_QUEUE_CAPACITY);
    ExecutorService executorService = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE,
        THREAD_KEEP_ALIVE_SECS, TimeUnit.SECONDS, taskQueue,
        createThreadFactory("TaskExecutorThread"));
    this.taskExecutor = MoreExecutors.listeningDecorator(executorService);

    this.callbackExecutor = Executors.newFixedThreadPool(1,
      createThreadFactory("CallbackExecutorThread"));

    this.codec = new BlockCodec(this.conf);
    this.shouldRun = true;
  }

  private void initIpcServer(Configuration conf) throws IOException {
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(conf
        .get(HdfsRaidConfigKeys.HDFS_RAIDNODE_IPC_ADDRESS_KEY));

    RPC.setProtocolEngine(conf, ClientRaidnodeProtocolPB.class, ProtobufRpcEngine.class);
    ClientRaidnodeProtocolServerSideTranslatorPB clientRaidnodeProtocolXlator = new ClientRaidnodeProtocolServerSideTranslatorPB(
        this);
    BlockingService service = ClientRaidnodeProtocolService
        .newReflectiveBlockingService(clientRaidnodeProtocolXlator);
    ipcServer = new RPC.Builder(conf)
        .setProtocol(ClientRaidnodeProtocolPB.class)
        .setInstance(service)
        .setBindAddress(ipcAddr.getHostName())
        .setPort(ipcAddr.getPort())
        .setNumHandlers(
          conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_HANDLER_COUNT_KEY,
            HdfsRaidConfigKeys.HDFS_RAIDNODE_HANDLER_COUNT_DEFAULT)).setVerbose(false).build();
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new RaidACLPolicyProvider());
    }
  }

  private void initMetrics() {
    this.metrics = RaidMetrics.create();
  }

  private void shutDownMetrics() {
    if (metrics != null) {
      metrics.shutDown();
    }
  }

  public RaidMetrics getMetrics() {
    return metrics;
  }

  // TBD: Implement the logic of reload policy when users change policies.
  private void initPolicy(Configuration conf) throws IOException {
    policy = new Policy(conf);
    policy.loadPolicy(conf);
  }

  public <R> void submitTask(RaidTask<R> task) {
    try {
      ListenableFuture<R> future = taskExecutor.submit(task);
      Futures.addCallback(future, task, callbackExecutor);
    } catch (RejectedExecutionException e) {
      task.onFailure(e);
    }
  }

  public void scheduleEncodeTask(final long delay) {
    lastEncodeTimer = new Timer();
    lastEncodeTimer.schedule(new TimerTask() {
      public void run() {
        try {
          CollectRaidInfoTask task = new CollectRaidInfoTask(RaidNode.this, getPolicyInfos(null),
              TaskPurpose.Encode, conf);
          submitTask(task);
        } catch (IOException ioe) {
          // TBD: We should stop RaidNode if we retried too many times and still fail.
          scheduleEncodeTask(delay);
        }
      }
    }, delay);
  }

  public void shutDownEncodeTask() {
    if (lastEncodeTimer != null) {
      lastEncodeTimer.cancel();
    }
  }

  public void scheduleZombieSweeperTask(long delay) {
    lastZombieSweeperTimer = new Timer();
    lastZombieSweeperTimer.schedule(new TimerTask() {
      public void run() {
        // TBD: Use configuration to set/get raid root directory.
        ZombieSweeperTask task = new ZombieSweeperTask(RaidNode.this, BlockCodec.getRaidRoot(),
            conf);
        submitTask(task);
      }
    }, delay);
  }

  public void shutDownZombieSweeperTask() {
    if (lastZombieSweeperTimer != null) {
      lastZombieSweeperTimer.cancel();
    }
  }

  public void scheduleFixerTask(final long delay) {
    lastFixerTimer = new Timer();
    lastFixerTimer.schedule(new TimerTask() {
      public void run() {
        try {
          FixerTask task = new FixerTask(RaidNode.this, conf);
          submitTask(task);
        } catch (IOException ioe) {
          // TBD: We should stop RaidNode if we retried too many times and still fail?
          scheduleFixerTask(delay);
        }
      }
    }, delay);
  }

  public void shutDownFixerTask() {
    if (lastFixerTimer != null) {
      lastFixerTimer.cancel();
    }
  }

  public void scheduleMoverTask(final long delay) {
    lastMoverTimer = new Timer();
    lastMoverTimer.schedule(new TimerTask() {
      public void run() {
        try {
          CollectRaidInfoTask task = new CollectRaidInfoTask(RaidNode.this, getPolicyInfos(null),
              TaskPurpose.BlockMover, conf);
          submitTask(task);
        } catch (IOException ioe) {
          // TBD: We should stop RaidNode if we retried too many times and still fail.
          scheduleMoverTask(delay);
        }
      }
    }, delay);
  }

  public void shutDownMoverTask() {
    if (lastMoverTimer != null) {
      lastMoverTimer.cancel();
    }
  }

  private void startHttpServer(Configuration conf) throws IOException {
    final String httpAddrString = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_HTTP_ADDRESS_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_HTTP_ADDRESS_DEFAULT);
    InetSocketAddress httpAddr = NetUtils.createSocketAddr(httpAddrString,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_HTTP_PORT_DEFAULT,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_HTTP_ADDRESS_KEY);

    final String httpsAddrString = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_HTTPS_ADDRESS_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf, httpAddr, httpsAddr,
      "raidnode", HdfsRaidConfigKeys.HDFS_RAIDNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_KEYTAB_FILE_KEY);

    httpServer = builder.build();
    httpServer.start();
  }

  private void stopHttpServer() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public void start() throws IOException {

    // Initialize the policy
    initMetrics();
    initPolicy(conf);

    // Start the IPC server
    initIpcServer(conf);
    ipcServer.start();

    // Set metrics of raid task before kick-off any tasks
    RaidTaskUtils.setMetrics(metrics);

    // Start the encoding task
    scheduleEncodeTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT));

    // Start the mover task
    scheduleMoverTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_INTERVAL,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_INTERVAL_DEFAULT));

    // Start the fixer
    scheduleFixerTask(conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_FIXER_INTERVAL_DEFAULT));

    // Start the orphan file cleaner
    scheduleZombieSweeperTask(conf.getLong(
      HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL_DEFAULT));

    startHttpServer(conf);
  }

  public void stop() throws IOException {

    shutDownZombieSweeperTask();

    shutDownFixerTask();

    shutDownEncodeTask();

    if (ipcServer != null) {
      ipcServer.stop();
    }

    shutDownMetrics();

    stopHttpServer();
  }

  public BlockCodec getCodec() {
    return codec;
  }

  public Configuration getConf() {
    return conf;
  }

  private ThreadFactory createThreadFactory(final String threadName) {
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(true);
        return thread;
      }
    };
  }

  public boolean shouldRun() {
    return shouldRun;
  }

  @Override
  // ClientRaidnodeProtocol
  public Policy getPolicyInfos(String cookie) throws IOException {
    return policy.getPolicyInfos(cookie);
  }

  @VisibleForTesting
  // Only for testing now
  public void setPolicy(Policy policy) {
    this.policy = policy;
  }

  @VisibleForTesting
  // Only for testing
  public long getEncodeTaskDone() {
    return encodeTaskDone;
  }

  @VisibleForTesting
  // Only for testing
  public long getZombieSweeperTaskDone() {
    return zombieSweeperTaskDone;
  }

  @VisibleForTesting
  // Only for testing
  public long getFixerTaskDone() {
    return fixerTaskDone;
  }

  @VisibleForTesting
  // Only for testing
  public long getMoverTaskDone() {
    return moverTaskDone;
  }

  public void increaseEncodeTaskDone() {
    encodeTaskDone += 1;
  }

  public void increaseZombieSweeperTaskDone() {
    zombieSweeperTaskDone += 1;
  }

  public void increaseFixerTaskDone() {
    fixerTaskDone += 1;
  }

  public void increaseMoverTaskDone() {
    moverTaskDone += 1;
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = new HdfsConfiguration();
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, HdfsRaidConfigKeys.HDFS_RAIDNODE_KEYTAB_FILE_KEY, 
      HdfsRaidConfigKeys.HDFS_RAIDNODE_KERBEROS_PRINCIPAL_KEY);
    RaidNode raidNode = new RaidNode(conf);
    raidNode.start();

    while (raidNode.shouldRun()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
    raidNode.stop();
  }
}
