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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

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
  }

  // TBD: Implement the logic of reload policy when users change policies.
  private void initPolicy(Configuration conf) throws IOException {
    policy = new Policy(conf);
    policy.parsePolicy();
  }

  public <R> void submitTask(RaidTask<R> task) {
    try {
      ListenableFuture<R> future = taskExecutor.submit(task);
      Futures.addCallback(future, task, callbackExecutor);
    } catch (RejectedExecutionException e) {
      task.onFailure(e);
    }
  }

  public void start() throws IOException {
    // Initialize the policy
    initPolicy(conf);

    // Submit a CollectRaidInfoTask to trigger the batch job

    // TBD: Need to handle bootstrap logic (i.e. the first time
    // the RaidNode is kicked off will throw FileNotFoundException
    // in cons of CollectRaidInfoTask.
    // Temporary mask off the code to make unit test code happy.
    // Will revist when adding different monitor threads in RaidNode.
    /**
     * CollectRaidInfoTask task = new CollectRaidInfoTask(this, conf); submitTask(task);
     */

    // Start the CorruptedBlockMonitor

    // Start the orphan file cleaner

    // Start the IPC server
    initIpcServer(conf);
    ipcServer.start();
  }

  public void stop() {
    if (ipcServer != null) {
      ipcServer.stop();
    }
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

  @Override
  // ClientRaidnodeProtocol
  public Policy getPolicyInfos(String cookie) throws IOException {
    return policy.getPolicyInfos(cookie);
  }

  @VisibleForTesting
  // ONLY for testing now
  public void setPolicy(Policy policy) {
    this.policy = policy;
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = new HdfsConfiguration();
    RaidNode raidNode = new RaidNode(conf);
    raidNode.start();

    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
    raidNode.stop();
  }
}
