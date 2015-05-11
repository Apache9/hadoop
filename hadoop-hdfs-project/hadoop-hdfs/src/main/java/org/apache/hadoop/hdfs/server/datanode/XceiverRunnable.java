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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Time;

@InterfaceAudience.Private
public class XceiverRunnable implements Runnable {

  private final Runnable runnable;

  public XceiverRunnable(Runnable runnable) {
    this.runnable = runnable;
  }

  private enum State {
    NEW, RUNNING, COMPLETED, CANCELLED
  }

  private State state = State.NEW;

  private Thread runner;

  @Override
  public void run() {
    Thread currentThread = Thread.currentThread();
    synchronized (this) {
      if (state == State.CANCELLED) {
        return;
      }
      state = State.RUNNING;
      runner = currentThread;
    }
    String oldName = currentThread.getName();
    currentThread.setName(runnable.toString());
    try {
      runnable.run();
    } finally {
      synchronized (this) {
        state = State.COMPLETED;
        notifyAll();
      }
      currentThread.setName(oldName);
    }
  }

  synchronized Thread getRunner() {
    return runner;
  }

  public synchronized boolean waitUntilDone(long timeout)
      throws InterruptedException {
    if (state == State.NEW) {
      state = State.CANCELLED;
      return true;
    }
    if (runner == Thread.currentThread()) {
      // this should not happen, but to keep compatible with the old
      // ReplicaInPipeline.stopWriter, just warn and return true here.
      DataNode.LOG.warn(runner.getName() + " is trying to stop itself.");
      return true;
    }
    long startTimeMs = Time.monotonicNow();
    while (state != State.COMPLETED) {
      long timeToWait = timeout - (Time.monotonicNow() - startTimeMs);
      if (timeToWait <= 0) {
        return false;
      }
      wait(timeToWait);
    }
    return true;
  }
}
