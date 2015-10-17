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
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

/**
 * Encoding a protobuf message with writeDelimitedTo.
 */
@InterfaceAudience.Private
public class ProtobufVarint32Encoder extends MessageToMessageEncoder<Message> {

  @Override
  protected void
      encode(ChannelHandlerContext ctx, Message msg, List<Object> out)
          throws Exception {
    int serializedSize = msg.getSerializedSize();
    int size =
        CodedOutputStream.computeRawVarint32Size(serializedSize)
            + serializedSize;
    ByteBuf buf = ctx.alloc().buffer(size);
    msg.writeDelimitedTo(new ByteBufOutputStream(buf));
    out.add(buf);
  }

}
