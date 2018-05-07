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

import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelPipeline;
import com.xiaomi.infra.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpResponseStatus;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Headers;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2HeadersFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;

class Http2HeadersReceiver
    extends SimpleChannelInboundHandler<Http2HeadersFrame> {

  private final Http2BlockDataReceiver receiver;

  public Http2HeadersReceiver(Http2BlockDataReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2HeadersFrame msg)
      throws Exception {
    Http2Headers headers = msg.headers();
    receiver.http2HeaderReceived(headers);
    if (HttpResponseStatus.OK.codeAsText().compareTo(headers.status()) == 0) {
      // if OK, then remove this handler and add handlers to read response
      // proto.
      ChannelPipeline p = ctx.pipeline();
      ProtobufVarint32FrameDecoder frameDecoder =
          new ProtobufVarint32FrameDecoder();
      frameDecoder.setSingleDecode(true);
      p.addLast(Http2DataFrameUnwrapHandler.get(), frameDecoder,
          new ProtobufDecoder(OpReadBlockResponseProto.getDefaultInstance()),
          new Http2ReadBlockResponseReceiver(receiver));
      p.remove(this);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    receiver.channelInactive();
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    receiver.onError(cause);
  }

}
