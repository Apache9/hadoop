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
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import com.google.common.base.Throwables;

import com.xiaomi.infra.thirdparty.io.netty.buffer.CompositeByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpResponseStatus;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2DataFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2HeadersFrame;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Http2DataReceiver extends ChannelInboundHandlerAdapter {

  private boolean finished;

  private HttpResponseStatus status;

  private CompositeByteBuf buffer;

  private byte[] data;

  private Throwable error;

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    buffer = new CompositeByteBuf(ctx.alloc(), true, Integer.MAX_VALUE);
  }

  @Override
  public synchronized void channelInactive(ChannelHandlerContext ctx)
      throws Exception {
    if (!finished) {
      error = new IOException("Stream closed");
      finished = true;
      buffer.release();
      notifyAll();
    }
  }

  @Override
  public synchronized void exceptionCaught(ChannelHandlerContext ctx,
      Throwable cause) throws Exception {
    if (!finished) {
      error = cause;
      finished = true;
      buffer.release();
      notifyAll();
    }
  }

  private synchronized void complete() {
    finished = true;
    data = new byte[buffer.readableBytes()];
    buffer.readBytes(data);
    buffer.release();
    if (!HttpResponseStatus.OK.equals(status)) {
      if (HttpResponseStatus.NOT_FOUND.equals(status)) {
        error = new FileNotFoundException("Status: " + status + ", content: " +
            new String(data, StandardCharsets.UTF_8));
      } else {
        error = new IOException("Status: " + status + ", content: " +
            new String(data, StandardCharsets.UTF_8));
      }

    }
    notifyAll();
  }

  private void onHeadersRead(Http2HeadersFrame headers) {
    status = HttpResponseStatus.parseLine(headers.headers().status());
    if (headers.isEndStream()) {
      complete();
    }
  }

  private void onDataRead(Http2DataFrame data) {
    buffer.addComponent(true, data.content().retain());
    if (data.isEndStream()) {
      complete();
    }
    data.release();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg instanceof Http2HeadersFrame) {
      onHeadersRead((Http2HeadersFrame) msg);
    } else if (msg instanceof Http2DataFrame) {
      onDataRead((Http2DataFrame) msg);
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  public synchronized byte[] get() throws IOException, InterruptedException {
    while (!finished) {
      wait();
    }
    if (error != null) {
      Throwables.propagateIfPossible(error, IOException.class);
      throw new IOException(error);
    } else {
      return data;
    }
  }
}
