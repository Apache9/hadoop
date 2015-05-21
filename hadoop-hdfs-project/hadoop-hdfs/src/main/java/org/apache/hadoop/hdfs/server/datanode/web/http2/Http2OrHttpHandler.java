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
package org.apache.hadoop.hdfs.server.datanode.web.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http2.Http2OrHttpChooser.SelectedProtocol;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
import org.apache.hadoop.hdfs.server.datanode.web.URLDispatcher;

@InterfaceAudience.Private
public class Http2OrHttpHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Log LOG = DatanodeHttpServer.LOG;

  private final InetSocketAddress proxyHost;

  private final Configuration conf;

  private final Configuration confForCreate;

  private final ExecutorService executor;

  public Http2OrHttpHandler(InetSocketAddress proxyHost, Configuration conf,
      Configuration confForCreate, ExecutorService executor) {
    this.proxyHost = proxyHost;
    this.conf = conf;
    this.confForCreate = confForCreate;
    this.executor = executor;
  }

  private void configureHttp1(ChannelHandlerContext ctx) {
    ctx.pipeline().addLast(new HttpServerCodec(), new ChunkedWriteHandler(),
      new URLDispatcher(proxyHost, conf, confForCreate));
  }

  private void configureHttp2(ChannelHandlerContext ctx) {
    ctx.pipeline().addLast(
      new DataNodeHttp2Handler(conf, confForCreate, executor),
      new ChunkedWriteHandler());
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
      throws Exception {
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    String protocol = sslHandler.engine().getSession().getProtocol();
    if (!protocol.contains(":")) {
      // this should be a request without alpn, just fall back to http/1.1
      if (LOG.isDebugEnabled()) {
        LOG.info("Client " + ctx.channel()
            + " connected without alpn, fallback to "
            + SelectedProtocol.HTTP_1_1);
      }
      configureHttp1(ctx);
    } else {
      SelectedProtocol selectedProtocol =
          SelectedProtocol.protocol(protocol.split(":")[1]);
      if (LOG.isDebugEnabled()) {
        LOG.info("Client " + ctx.channel() + " selected protocol is "
            + selectedProtocol);
      }
      switch (selectedProtocol) {
      case HTTP_2:
        configureHttp2(ctx);
        break;
      case HTTP_1_0:
      case HTTP_1_1:
        configureHttp1(ctx);
        break;
      default:
        throw new IllegalStateException("Unknown SelectedProtocol");
      }
    }
    ctx.pipeline().remove(this);
    ctx.fireChannelRead(msg.retain());
  }
}
