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
package org.apache.hadoop.hdfs.web.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.util.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUrlDispatcher;

import com.google.protobuf.CodedOutputStream;

/**
 *
 */
public class Http2SmallReadTestingHandler extends ChannelOutboundHandlerAdapter {

  private LocatedBlock block;

  private int len;

  private int remainingCount;

  private long start;

  private long cost = -1L;

  private ChannelHandlerContext ctx;

  private byte[] buf;

  private void nextRequest0() throws IOException {
    OpReadBlockRequestProto request =
        OpReadBlockRequestProto
            .newBuilder()
            .setHeader(
              ClientOperationHeaderProto
                  .newBuilder()
                  .setBaseHeader(
                    DataTransferProtoUtil.buildBaseHeader(block.getBlock(),
                      block.getBlockToken())).setClientName("TestClient"))
            .setOffset(0).setLen(len).setSendChecksums(true).build();
    int serializedSize = request.getSerializedSize();
    ByteBuf data =
        ctx.alloc().buffer(
          CodedOutputStream.computeRawVarint32Size(serializedSize)
              + serializedSize);
    request.writeDelimitedTo(new ByteBufOutputStream(data));
    new Http2StreamBootstrap()
        .channel(ctx.channel())
        .headers(
          new DefaultHttp2Headers().method(
            new ByteString(HttpMethod.POST.name(), StandardCharsets.UTF_8))
              .path(
                new ByteString(DtpUrlDispatcher.URL_PREFIX
                    + DtpUrlDispatcher.OP_READ_BLOCK, StandardCharsets.UTF_8)))
        .data(data).endStream(true)
        .handler(new ChannelInitializer<Http2StreamChannel>() {

          @Override
          protected void initChannel(Http2StreamChannel ch) throws Exception {
            ch.pipeline().addLast(
              new Http2SmallReadReceiveBlockHandler(
                  Http2SmallReadTestingHandler.this));
          }
        }).connect();
  }

  private void nextRequest() {
    ctx.channel().eventLoop().execute(new Runnable() {

      @Override
      public void run() {
        try {
          nextRequest0();
        } catch (Throwable t) {
          ctx.fireExceptionCaught(t);
        }
      }
    });
  }

  void receiveFinished(ByteBuffer data) {
    data.get(buf);
    remainingCount--;
    if (remainingCount == 0) {
      synchronized (this) {
        cost = (System.nanoTime() - start) / 1000000;
        notifyAll();
      }
    } else {
      nextRequest();
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof ReadBlockTestContext) {
      this.ctx = ctx;
      ReadBlockTestContext testCtx = (ReadBlockTestContext) msg;
      remainingCount = testCtx.readCount;
      len = testCtx.len;
      buf = new byte[len];
      this.block = testCtx.block;
      start = System.nanoTime();
      nextRequest();
    } else {
      ctx.write(msg);
    }
  }

  public long getCost() throws InterruptedException {
    synchronized (this) {
      while (cost < 0L) {
        wait();
      }
      return cost;
    }
  }
}
