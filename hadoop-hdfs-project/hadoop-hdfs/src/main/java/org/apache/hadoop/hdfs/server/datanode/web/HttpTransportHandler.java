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

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedStream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.IOUtils;

class HttpTransportHandler extends ChannelInboundHandlerAdapter implements
    DataNodeTransportMessageVisitor {

  private static final Log LOG = DatanodeHttpServer.LOG;

  private final DataNodeApplicationHandler webHdfsHandler;

  private final InetSocketAddress proxyHost;

  private HttpResponse response;

  private OutputStream out;

  HttpTransportHandler(DataNodeApplicationHandler webHdfsHandler,
      InetSocketAddress proxyHost) {
    this.webHdfsHandler = webHdfsHandler;
    this.proxyHost = proxyHost;
  }

  private void handleRequest(final ChannelHandlerContext ctx, HttpRequest req)
      throws Exception {
    String uri = req.getUri();
    if (uri.startsWith(WEBHDFS_PREFIX)) {
      webHdfsHandler.handle(uri, req.getMethod()).accept(ctx, this);
    } else {
      SimpleHttpProxyHandler h = new SimpleHttpProxyHandler(proxyHost);
      ctx.pipeline().replace(this,
          SimpleHttpProxyHandler.class.getSimpleName(), h);
      h.channelRead0(ctx, req);
    }
  }

  private void handleContent(ChannelHandlerContext ctx, HttpContent chunk)
      throws IOException {
    if (out != null) {
      chunk.content().readBytes(out, chunk.content().readableBytes());
      if (chunk instanceof LastHttpContent) {
        out.close();
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
      }
    } else {
      if (chunk != LastHttpContent.EMPTY_LAST_CONTENT) {
        throw new IllegalStateException("output is not ready");
      }
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg instanceof HttpRequest) {
      handleRequest(ctx, (HttpRequest) msg);
    } else if (msg instanceof HttpContent) {
      handleContent(ctx, (HttpContent) msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    IOUtils.cleanup(LOG, out);
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(cause);
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    IOUtils.cleanup(LOG, out);
  }

  private HttpResponse toResponse(HttpResponseStatus status,
      Map<String, Object> headers, ByteBuf content) {
    HttpResponse resp =
        content == null ? new DefaultHttpResponse(HTTP_1_1, status)
            : new DefaultFullHttpResponse(HTTP_1_1, status, content);
    for (Map.Entry<String, Object> header : headers.entrySet()) {
      resp.headers().set(header.getKey(), header.getValue());
    }
    return resp;
  }

  @Override
  public void visit(ChannelHandlerContext ctx, ByteArrayTransportMessage msg) {
    ctx.writeAndFlush(
      toResponse(msg.getStatus(), msg.getHeaders(),
        ctx.alloc().buffer(msg.getBuf().length).writeBytes(msg.getBuf())))
        .addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void visit(ChannelHandlerContext ctx,
      InputStreamTransportMessage msg) {
    ctx.write(toResponse(msg.getStatus(), msg.getHeaders(), null));
    ctx.writeAndFlush(new ChunkedStream(msg.getIn())).addListener(
        ChannelFutureListener.CLOSE);
  }

  @Override
  public void
      visit(ChannelHandlerContext ctx, OutputStreamTransportMessage msg) {
    ctx.writeAndFlush(toResponse(CONTINUE,
        Collections.<String, Object> emptyMap(), Unpooled.EMPTY_BUFFER));
    this.out = msg.getOut();
    this.response = toResponse(msg.getStatus(), msg.getHeaders(), null);
  }

}
