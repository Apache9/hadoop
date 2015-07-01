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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.util.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUrlDispatcher;
import org.apache.hadoop.hdfs.web.http2.ClientHttp2ConnectionHandler;
import org.apache.hadoop.hdfs.web.http2.Http2DataReceiver;
import org.apache.hadoop.hdfs.web.http2.Http2StreamBootstrap;
import org.apache.hadoop.hdfs.web.http2.Http2StreamChannel;
import org.eclipse.jetty.http2.client.HTTP2Client;

/**
 * A wrapper of {@link HTTP2Client} which will reuse session to the same server.
 */
@InterfaceAudience.Private
public class Http2ConnectionPool implements Closeable {

  public static final Log LOG = LogFactory.getLog(Http2ConnectionPool.class);

  private EventLoopGroup workerGroup = new NioEventLoopGroup();

  private Map<InetSocketAddress, Channel> addressToChannel =
      new HashMap<InetSocketAddress, Channel>();

  private Configuration conf;

  public Http2ConnectionPool(Configuration conf) {
    this.conf = conf;
  }

  public Http2StreamChannel connect(InetSocketAddress address) throws IOException {

    try {
      Channel channel = null;
      synchronized (this.addressToChannel) {
        channel = this.addressToChannel.get(address);
        if (channel == null) {
          channel =
              new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                  .handler(new ChannelInitializer<Channel>() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                      ch.pipeline().addLast(ClientHttp2ConnectionHandler.create(ch, conf));
                    }

                  }).connect(address).sync().channel();
          this.addressToChannel.put(address, channel);
        }

      }
      return new Http2StreamBootstrap()
          .channel(channel)
          .handler(new ChannelInitializer<Http2StreamChannel>() {

            @Override
            protected void initChannel(Http2StreamChannel ch) throws Exception {
              ch.pipeline().addLast(new Http2DataReceiver());
            }

          })
          .headers(
            new DefaultHttp2Headers()
                .method(new ByteString(HttpMethod.POST.name(), StandardCharsets.UTF_8))
                .path(
                  new ByteString(DtpUrlDispatcher.URL_PREFIX + DtpUrlDispatcher.OP_READ_BLOCK,
                      StandardCharsets.UTF_8))
                .scheme(new ByteString("http", StandardCharsets.UTF_8))
                .authority(
                  new ByteString(address.getHostString() + ":" + address.getPort(),
                      StandardCharsets.UTF_8))).endStream(false).connect().sync().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<InetSocketAddress, Channel> entry : this.addressToChannel.entrySet()) {
      Channel channel = entry.getValue();
      if (channel != null) {
        channel.close();
      }
    }
    workerGroup.shutdownGracefully();
  }
}
