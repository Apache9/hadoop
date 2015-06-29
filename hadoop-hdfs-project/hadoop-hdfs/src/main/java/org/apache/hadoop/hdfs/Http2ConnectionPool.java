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
package org.apache.hadoop.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.eclipse.jetty.http2.BufferingFlowControlStrategy;
import org.eclipse.jetty.http2.FlowControlStrategy;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.HTTP2ClientConnectionFactory;
import org.eclipse.jetty.util.FuturePromise;

/**
 * A wrapper of {@link HTTP2Client} which will reuse session to the same server.
 */
@InterfaceAudience.Private
public class Http2ConnectionPool implements Closeable {

  public static final Log LOG = LogFactory.getLog(Http2ConnectionPool.class);

  private HTTP2Client client;

  private Map<InetSocketAddress, SessionAndStreamId> addressToSession;

  private boolean initialized = false;

  private static final int initialWindowSize = 1024 * 1024 * 1024;

  public static class SessionAndStreamId {

    public SessionAndStreamId(Session session) {
      this.session = session;
      this.streamIdGenerator = new AtomicInteger(1);
    }

    private Session session;
    private AtomicInteger streamIdGenerator;

    public Session getSession() {
      return session;
    }

    public AtomicInteger getStreamIdGenerator() {
      return streamIdGenerator;
    }
  }

  public Http2ConnectionPool() {
  }

  public SessionAndStreamId connect(InetSocketAddress address) throws IOException {
    synchronized (this) {
      if (!this.initialized) {
        HTTP2Client c = new HTTP2Client();
        c.setClientConnectionFactory(new HTTP2ClientConnectionFactory() {
          @Override
          protected FlowControlStrategy newFlowControlStrategy() {
            return new BufferingFlowControlStrategy(initialWindowSize, 0.5f);
          }
        });
        try {
          c.start();
          this.addressToSession = new HashMap<InetSocketAddress, SessionAndStreamId>();
        } catch (Exception e) {
          try {
            c.stop();
          } catch (Exception e1) {
            throw new IOException(e1);
          }
          throw new IOException(e);
        }
        this.client = c;
        this.initialized = true;
      }
    }
    synchronized (this.addressToSession) {
      SessionAndStreamId sessionAndStreamId = this.addressToSession.get(address);
      if (sessionAndStreamId == null) {
        FuturePromise<Session> sessionPromise = new FuturePromise<>();
        this.client.connect(address, new Session.Listener.Adapter(), sessionPromise);
        try {
          sessionAndStreamId = new SessionAndStreamId(sessionPromise.get());
          this.addressToSession.put(address, sessionAndStreamId);
        } catch (InterruptedException | ExecutionException e) {
          LOG.warn("connect to " + address + " failed ", e);
          throw new IOException(e);
        }
      }
      return sessionAndStreamId;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      client.stop();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
