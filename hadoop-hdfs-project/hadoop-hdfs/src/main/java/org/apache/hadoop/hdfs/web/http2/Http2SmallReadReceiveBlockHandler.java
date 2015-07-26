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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.util.DataChecksum;

/**
 *
 */
public class Http2SmallReadReceiveBlockHandler extends
    ChannelInboundHandlerAdapter {

  private final Http2SmallReadTestingHandler handler;

  private CompositeByteBuf cumulation;

  public Http2SmallReadReceiveBlockHandler(Http2SmallReadTestingHandler handler) {
    this.handler = handler;
  }

  private void receiveFinished(ChannelHandlerContext ctx) throws IOException {
    ByteBufInputStream in = new ByteBufInputStream(cumulation);
    OpReadBlockResponseProto resp =
        OpReadBlockResponseProto.parseDelimitedFrom(in);
    DataChecksum checksum =
        DataTransferProtoUtil.fromProto(resp.getReadOpChecksumInfo()
            .getChecksum());
    OpReadBlockFrameHeaderProto header =
        OpReadBlockFrameHeaderProto.parseDelimitedFrom(in);
    ByteBuf data = ctx.alloc().directBuffer(header.getDataLength());
    cumulation.readBytes(data);
    cumulation.release();
    checksum.verifyChunkedSums(data.nioBuffer(), header.getChecksums()
        .asReadOnlyByteBuffer(), "/test", 0);
    handler.receiveFinished(data.nioBuffer());
    data.release();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    cumulation = ctx.alloc().compositeBuffer();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg instanceof Http2Headers) {
      return;
    }
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      cumulation.addComponent(buf);
      cumulation.writerIndex(cumulation.writerIndex() + buf.readableBytes());
      return;
    }
    if (msg == LastHttp2Message.get()) {
      receiveFinished(ctx);
      return;
    }
    ctx.fireChannelRead(msg);
  }

}
