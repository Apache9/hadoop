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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
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
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUrlDispatcher;
import org.apache.hadoop.hdfs.web.http2.ClientHttp2ConnectionHandler;
import org.apache.hadoop.hdfs.web.http2.Http2DataReceiver;
import org.apache.hadoop.hdfs.web.http2.Http2StreamBootstrap;
import org.apache.hadoop.hdfs.web.http2.Http2StreamChannel;
import org.apache.hadoop.net.NetUtils;
import org.eclipse.jetty.http2.client.HTTP2Client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.CodedOutputStream;

/**
 * A wrapper of {@link HTTP2Client} which will reuse session to the same server.
 */
@InterfaceAudience.Private
public class Http2ConnectionPool implements Closeable {

  public static final Log LOG = LogFactory.getLog(Http2ConnectionPool.class);

  private final boolean closeEventLoopGroup;

  private final EventLoopGroup workerGroup;

  private final int maxConnPerServer;

  private static final class PooledConnections {

    public int allocated = 0;

    public final List<Channel> conns = new ArrayList<Channel>();
  }

  private final LoadingCache<String, PooledConnections> cache = CacheBuilder
      .newBuilder().concurrencyLevel(16)
      .build(new CacheLoader<String, PooledConnections>() {

        @Override
        public PooledConnections load(String key) throws Exception {
          return new PooledConnections();
        }
      });

  private final Configuration conf;

  public Http2ConnectionPool(Configuration conf, DfsClientConf clientConf) {
    this(conf, clientConf, null);
  }

  public Http2ConnectionPool(Configuration conf, DfsClientConf clientConf,
      EventLoopGroup workerGroup) {
    this.conf = conf;
    this.maxConnPerServer = clientConf.getMaxHttp2ConnectionPerServer();
    if (workerGroup != null) {
      this.closeEventLoopGroup = false;
      this.workerGroup = workerGroup;
    } else {
      this.closeEventLoopGroup = true;
      this.workerGroup = new NioEventLoopGroup();
    }
  }

  private Channel pickUp(String infoAddr) throws InterruptedException {
    PooledConnections pool = cache.getUnchecked(infoAddr);
    synchronized (pool) {
      if (pool.conns.isEmpty()) {
        pool.allocated++;
      } else {
        Channel least = null;
        int leastNumActiveStreams = Integer.MAX_VALUE;
        for (Channel ch : pool.conns) {
          ClientHttp2ConnectionHandler handler =
              ch.pipeline().get(ClientHttp2ConnectionHandler.class);
          if (handler == null) {
            continue;
          }
          int numActiveStreams = handler.numActiveStreams();
          if (numActiveStreams == 0) {
            return ch;
          }
          if (numActiveStreams < leastNumActiveStreams) {
            least = ch;
            leastNumActiveStreams = numActiveStreams;
          }
        }
        if (least != null
            && pool.conns.size() + pool.allocated >= maxConnPerServer) {
          return least;
        }
        pool.allocated++;
      }
    }
    Channel channel = null;
    try {
      channel =
          new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
              .handler(new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(Channel ch) throws Exception {
                  ch.pipeline().addLast(
                    ClientHttp2ConnectionHandler.create(ch, conf));
                }

              }).connect(NetUtils.createSocketAddr(infoAddr)).sync().channel();
    } finally {
      synchronized (pool) {
        pool.allocated--;
        if (channel != null) {
          pool.conns.add(channel);
        }
      }
    }
    return channel;
  }

  public Http2DataReceiver connect(String infoAddr,
      OpReadBlockRequestProto request) throws IOException {
    try {
      Channel channel = pickUp(infoAddr);
      int serializedSize = request.getSerializedSize();
      ByteBuf data =
          channel.alloc().buffer(
            CodedOutputStream.computeRawVarint32Size(serializedSize)
                + serializedSize);
      request.writeDelimitedTo(new ByteBufOutputStream(data));
      final Http2DataReceiver receiver = new Http2DataReceiver();
      new Http2StreamBootstrap()
          .channel(channel)
          .handler(new ChannelInitializer<Http2StreamChannel>() {

            @Override
            protected void initChannel(Http2StreamChannel ch) throws Exception {
              ch.pipeline().addLast(receiver);
            }

          })
          .headers(
            new DefaultHttp2Headers()
                .method(
                  new ByteString(HttpMethod.POST.name(), StandardCharsets.UTF_8))
                .path(
                  new ByteString(DtpUrlDispatcher.URL_PREFIX
                      + DtpUrlDispatcher.OP_READ_BLOCK, StandardCharsets.UTF_8)))
          .data(data).endStream(true).connect().sync();
      return receiver;
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, PooledConnections> entry : this.cache.asMap()
        .entrySet()) {
      PooledConnections pool = entry.getValue();
      for (Channel ch : pool.conns) {
        ch.close();
      }
    }
    this.cache.invalidateAll();
    if (closeEventLoopGroup) {
      workerGroup.shutdownGracefully();
    }
  }
}
