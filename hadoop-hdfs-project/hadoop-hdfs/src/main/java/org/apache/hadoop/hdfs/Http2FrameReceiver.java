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

import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;

class Http2FrameReceiver extends ChannelInboundHandlerAdapter {

  private final Http2BlockDataReceiver receiver;

  public Http2FrameReceiver(Http2BlockDataReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    receiver.channelInactive();
    ctx.fireChannelInactive();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg instanceof OpReadBlockFrameHeaderProto) {
      receiver.frameHeaderReceived((OpReadBlockFrameHeaderProto) msg);
    } else if (msg instanceof ByteBuf) {
      receiver.frameDataReceived((ByteBuf) msg);
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    receiver.onError(cause);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
      throws Exception {
    if (evt == Http2DataFrameUnwrapHandler.END_OF_STREAM) {
      receiver.onComplete();
    }
  }

}
