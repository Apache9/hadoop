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
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * An inbound handler that decode a protobuf varint32 message.
 */
@InterfaceAudience.Private
public class ProtobufVarint32Decoder extends
    SimpleStreamInboundHandler<ByteBuf> {

  private final Parser<? extends Message> parser;

  private ByteBuf cumulation;

  private final byte[] varintBuf = new byte[5];

  public ProtobufVarint32Decoder(Parser<? extends Message> parser) {
    super(false);
    this.parser = parser;
  }

  private Message tryDecode() throws IOException {
    ByteBuf in = cumulation;
    in.markReaderIndex();
    for (int i = 0; i < varintBuf.length; i++) {
      if (!in.isReadable()) {
        in.resetReaderIndex();
        return null;
      }
      varintBuf[i] = in.readByte();
      if (varintBuf[i] >= 0) {
        int length =
            CodedInputStream.newInstance(varintBuf, 0, i + 1).readRawVarint32();
        if (length < 0) {
          throw new InvalidProtocolBufferException("negative length: " + length);
        }

        if (in.readableBytes() < length) {
          in.resetReaderIndex();
          return null;
        } else {
          in.resetReaderIndex();
          return parser.parseDelimitedFrom(new ByteBufInputStream(in));
        }
      }
    }
    // Couldn't find the byte whose MSB is off.
    throw new InvalidProtocolBufferException("length wider than 32-bit");
  }

  private void release() {
    cumulation.release();
    cumulation = null;
  }

  @Override
  protected void streamRead0(StreamHandlerContext ctx, ByteBuf msg,
      boolean endOfStream) throws Exception {
    if (cumulation == null) {
      cumulation = msg.retain();
    } else {
      cumulation =
          ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(),
            cumulation, msg);
    }
    Message protobufMsg = tryDecode();
    if (protobufMsg != null) {
      if (cumulation.isReadable()) {
        ByteBuf remaining = cumulation.readBytes(cumulation.readableBytes());
        release();
        ctx.fireStreamRead(protobufMsg, false);
        ctx.fireStreamRead(remaining, endOfStream);
      } else {
        release();
        ctx.fireStreamRead(protobufMsg, endOfStream);
      }
    }
  }

  @Override
  public void streamClosed(StreamHandlerContext ctx) {
    if (cumulation != null) {
      release();
    }
    ctx.fireStreamClosed();
  }

}
