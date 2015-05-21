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
package org.apache.hadoop.hdfs.server.datanode.web.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class Http2ChunkedInput implements ChunkedInput<Http2DataFrame> {

  private final int streamId;

  private final ChunkedInput<ByteBuf> input;

  public Http2ChunkedInput(int streamId, ChunkedInput<ByteBuf> input) {
    this.streamId = streamId;
    this.input = input;
  }

  @Override
  public boolean isEndOfInput() throws Exception {
    return input.isEndOfInput();
  }

  @Override
  public void close() throws Exception {
    input.close();
  }

  @Override
  public Http2DataFrame readChunk(ChannelHandlerContext ctx) throws Exception {
    if (isEndOfInput()) {
      return null;
    }
    ByteBuf buf = input.readChunk(ctx);
    return new Http2DataFrame(streamId, buf, 0, isEndOfInput());
  }

  @Override
  public long length() {
    return input.length();
  }

  @Override
  public long progress() {
    return input.progress();
  }

}
