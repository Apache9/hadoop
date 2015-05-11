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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

/**
 * Used to stop transfer which receive data from DataXceiver peer.
 */
@InterfaceAudience.Private
public class PeerXceiverStopper implements XceiverStopper {

  private final Peer peer;

  private Future<?> future;

  private XceiverRunnable xceiver;

  public PeerXceiverStopper(Peer peer) {
    this.peer = peer;
  }

  public synchronized void setFuture(Future<?> future) {
    this.future = future;
    notifyAll();
  }

  void setXceiver(XceiverRunnable xceiver) {
    this.xceiver = xceiver;
  }

  private void logAndThrow() throws IOException {
    // note that, there is a chance that the runner has already begun to run
    // other task, so the thread name and stacktrace maybe wrong.
    Thread runner = xceiver.getRunner();
    final String msg = "Waiting for xceiver " + runner + " timed out";
    DataNode.LOG.warn(msg + "\n"
        + (runner != null ? StringUtils.getStackTrace(runner) : "null"));
    throw new IOException(msg);
  }

  private long getTimeToWait(long start, long timeout) throws IOException {
    long timeToWait = timeout - (Time.monotonicNow() - start);
    if (timeToWait <= 0) {
      logAndThrow();
    }
    return timeToWait;
  }

  /**
   * Close peer socket first, then interrupt the task.
   */
  @Override
  public void stop() throws IOException {
    // we do not call DataXceiverServer.closePeer here, DataXceiver will do
    // it when it finds the peer is broken.
    peer.close();
    Future<?> futureLocal;
    synchronized (this) {
      while ((futureLocal = this.future) == null) {
        try {
          // future should be set immediately after we submit the task so we do
          // not introduce a timeout here.
          // See code in DataXceiverServer.run().
          wait();
        } catch (InterruptedException e) {
          throw new InterruptedIOException(
              "Waiting for xceiver is interrupted.");
        }
      }
    }
    futureLocal.cancel(true);
  }

  @Override
  public void waitUntilDone(long timeout) throws IOException {
    try {
      if (!xceiver.waitUntilDone(getTimeToWait(Time.monotonicNow(), timeout))) {
        logAndThrow();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Waiting for xceiver is interrupted.");
    }
  }

}
