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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Worker is used to execute the submitted tasks.
 */
public class Worker {

  private final ListeningExecutorService executorService;
  private final ExecutorService callbackPool;

  public Worker(int corePoolSize, int maxPoolSize, long keepAliveMs,
      int workQueueCapacity) {
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(
        workQueueCapacity);
    ExecutorService executePool = new ThreadPoolExecutor(corePoolSize,
        maxPoolSize, keepAliveMs, TimeUnit.MILLISECONDS, workQueue,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "ExecutorPool");
            thread.setDaemon(true);
            return thread;
          }
        });
    executorService = MoreExecutors.listeningDecorator(executePool);

    callbackPool = Executors.newFixedThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "CallbackPool");
        thread.setDaemon(true);
        return thread;
      }
    });
  }

  /**
   * Submits a policy task to the worker.
   *
   * @param policy The policy to submit
   */
  public <R> void submitPolicyTask(Policy<R> policy) {
    AsyncTask<R> task = new AsyncTask<R>(policy);
    submitTask(task);
  }

  /**
   * Submits a task to the worker, the completion handler will
   * be called when the task finishes.
   *
   * @param task The task to submit
   * @throws RejectedExecutionException
   */
  public <R> void submitTask(AsyncTask<R> task) {
    try {
      ListenableFuture<R> future = executorService.submit(task);
      Futures.addCallback(future, task, callbackPool);
    } catch (RejectedExecutionException e) {
      task.onFailure(e);
    }
  }
}
