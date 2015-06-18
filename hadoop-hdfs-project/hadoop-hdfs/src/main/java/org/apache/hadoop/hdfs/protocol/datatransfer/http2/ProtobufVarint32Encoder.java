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
package org.apache.hadoop.hdfs.protocol.datatransfer.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

/**
 * An outbound handler that encode a protobuf to ByteBuf using writeDelimitedTo.
 */
@InterfaceAudience.Private
public class ProtobufVarint32Encoder extends StreamOutboundHandlerAdaptor {

  private ByteBuf encode(StreamHandlerContext ctx, Message msg)
      throws IOException {
    int msgSize = msg.getSerializedSize();
    int serializedSize =
        CodedOutputStream.computeRawVarint32Size(msgSize) + msgSize;
    ByteBuf buf = ctx.alloc().buffer(serializedSize);
    msg.writeDelimitedTo(new ByteBufOutputStream(buf));
    return buf;
  }

  @Override
  public void write(StreamHandlerContext ctx, Object msg, boolean endOfStream)
      throws Exception {
    if (msg instanceof Message) {
      ctx.write(encode(ctx, (Message) msg), endOfStream);
    } else {
      ctx.write(msg, endOfStream);
    }
  }

  @Override
  public void writeAndFlush(StreamHandlerContext ctx, Object msg,
      boolean endOfStream) throws Exception {
    if (msg instanceof Message) {
      ctx.writeAndFlush(encode(ctx, (Message) msg), endOfStream);
    } else {
      ctx.writeAndFlush(msg, endOfStream);
    }
  }

}
