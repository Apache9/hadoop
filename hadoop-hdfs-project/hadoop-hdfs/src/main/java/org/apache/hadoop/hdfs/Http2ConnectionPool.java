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

import org.apache.hadoop.classification.InterfaceAudience;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.client.HTTP2Client;

/**
 * A wrapper of {@link HTTP2Client} which will reuse session to the same server.
 */
@InterfaceAudience.Private
public class Http2ConnectionPool implements Closeable {

  private final HTTP2Client client;

  public Http2ConnectionPool() throws IOException {
    HTTP2Client c = new HTTP2Client();
    try {
      c.start();
    } catch (Exception e) {
      try {
        c.stop();
      } catch (Exception e1) {
        throw new IOException(e1);
      }
      throw new IOException(e);
    }
    this.client = c;
  }

  public Session connect(InetSocketAddress address) throws IOException {
    // TODO:
    return null;
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
