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

import org.apache.hadoop.classification.InterfaceAudience;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

/**
 * 
 */
@InterfaceAudience.Private
public class LastChunkedInput implements ChunkedInput<Object> {

  private final ChunkedInput<ByteBuf> in;

  public LastChunkedInput(ChunkedInput<ByteBuf> in) {
    this.in = in;
  }

  @Override
  public boolean isEndOfInput() throws Exception {
    return in.isEndOfInput();
  }

  @Override
  public void close() throws Exception {
    in.close();
  }

  @Override
  public Object readChunk(ChannelHandlerContext ctx) throws Exception {
    if (isEndOfInput()) {
      return null;
    }
    ByteBuf chunk = in.readChunk(ctx);
    if (isEndOfInput()) {
      return new LastMessage(chunk);
    } else {
      return chunk;
    }
  }

  @Override
  public long length() {
    return in.length();
  }

  @Override
  public long progress() {
    return in.progress();
  }

}
