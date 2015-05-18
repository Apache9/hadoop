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

import org.apache.hadoop.classification.InterfaceAudience;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;

@InterfaceAudience.Private
public class Http2FrameWriteHandler extends ChannelOutboundHandlerAdapter {

  private final Http2ConnectionEncoder encoder;

  public Http2FrameWriteHandler(Http2ConnectionEncoder encoder) {
    this.encoder = encoder;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof Http2HeadersFrame) {
      Http2HeadersFrame frame = (Http2HeadersFrame) msg;
      encoder.writeHeaders(ctx, frame.getStreamId(), frame.getHeaders(),
        frame.getPadding(), frame.isEndOfStream(), promise);
    } else if (msg instanceof Http2DataFrame) {
      Http2DataFrame frame = (Http2DataFrame) msg;
      encoder.writeData(ctx, frame.getStreamId(), frame.getData(),
        frame.getPadding(), frame.isEndOfStream(), promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

}
