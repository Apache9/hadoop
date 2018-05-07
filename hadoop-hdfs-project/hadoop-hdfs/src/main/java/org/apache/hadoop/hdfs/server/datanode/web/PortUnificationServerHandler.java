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
package org.apache.hadoop.hdfs.server.datanode.web;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUrlDispatcher;

import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBufUtil;
import com.xiaomi.infra.thirdparty.io.netty.channel.Channel;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInitializer;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpServerCodec;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2CodecUtil;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Exception;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2FrameLogger;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import com.xiaomi.infra.thirdparty.io.netty.handler.logging.LogLevel;
import com.xiaomi.infra.thirdparty.io.netty.handler.stream.ChunkedWriteHandler;

/**
 * A port unification handler to support HTTP/1.1 and HTTP/2 on the same port.
 */
@InterfaceAudience.Private
public class PortUnificationServerHandler extends ByteToMessageDecoder {

  private static final ByteBuf HTTP2_CLIENT_CONNECTION_PREFACE =
      Http2CodecUtil.connectionPrefaceBuf();

  // we only want to support HTTP/1.1 and HTTP/2, so the first 3 bytes is
  // enough. No HTTP/1.1 request could start with "PRI"
  private static final int MAGIC_HEADER_LENGTH = 3;

  private final InetSocketAddress proxyHost;

  private final Configuration conf;

  private final Configuration confForCreate;

  private final DataNode datanode;

  public PortUnificationServerHandler(InetSocketAddress proxyHost,
      Configuration conf, Configuration confForCreate, DataNode datanode) {
    this.proxyHost = proxyHost;
    this.conf = conf;
    this.confForCreate = confForCreate;
    this.datanode = datanode;
  }

  private void configureHttp1(ChannelHandlerContext ctx) {
    ctx.pipeline().addLast(new HttpServerCodec(), new ChunkedWriteHandler(),
        new URLDispatcher(proxyHost, conf, confForCreate));
  }

  private static final ConcurrentMap<LogLevel, Http2FrameLogger> FRAME_LOGGERS =
      new ConcurrentHashMap<>();

  private Http2FrameLogger getFrameLogger() {
    String level = conf.get(DFSConfigKeys.DFS_HTTP2_FRAME_LOG_LEVEL_KEY,
        DFSConfigKeys.DFS_HTTP2_FRAME_LOG_LEVEL_DEFAULT);
    LogLevel logLevel;
    try {
      logLevel = LogLevel.valueOf(level);
    } catch (IllegalArgumentException e) {
      logLevel =
          LogLevel.valueOf(DFSConfigKeys.DFS_HTTP2_FRAME_LOG_LEVEL_DEFAULT);
    }
    Http2FrameLogger logger = FRAME_LOGGERS.get(logLevel);
    if (logger != null) {
      return logger;
    }
    logger = new Http2FrameLogger(logLevel, "HTTP/2 DTP Server");
    Http2FrameLogger oldLogger = FRAME_LOGGERS.putIfAbsent(logLevel, logger);
    return oldLogger == null ? logger : oldLogger;
  }

  private void configureHttp2(ChannelHandlerContext ctx) throws Http2Exception {
    ctx.pipeline().addLast(
        Http2MultiplexCodecBuilder.forServer(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new DtpUrlDispatcher(datanode));
          }
        }).frameLogger(getFrameLogger()).build());
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {
    if (in.readableBytes() < MAGIC_HEADER_LENGTH) {
      return;
    }
    if (ByteBufUtil.equals(in, 0, HTTP2_CLIENT_CONNECTION_PREFACE, 0,
        MAGIC_HEADER_LENGTH)) {
      configureHttp2(ctx);
    } else {
      configureHttp1(ctx);
    }
    ctx.pipeline().remove(this);
  }

}