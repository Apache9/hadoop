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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;

import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelPipeline;
import com.xiaomi.infra.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;

class Http2ReadBlockResponseReceiver
    extends SimpleChannelInboundHandler<OpReadBlockResponseProto> {

  private final Http2BlockDataReceiver receiver;

  public Http2ReadBlockResponseReceiver(Http2BlockDataReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx,
      OpReadBlockResponseProto msg) throws Exception {
    receiver.resonpseReceived(msg);
    if (msg.getStatus() == SUCCESS) {
      ChannelPipeline p = ctx.pipeline();
      ctx.pipeline().addLast(new Http2BlockReaderFrameDecoder(),
          new Http2FrameReceiver(receiver));
      p.remove(this);
      p.remove(ProtobufDecoder.class);
      p.remove(ProtobufVarint32FrameDecoder.class);
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
