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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.logging.LogLevel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * HTTP/2 connection handler. Now is only for server use.
 */
@InterfaceAudience.Private
public class DataTransferHttp2ConnectionHandler extends Http2ConnectionHandler {

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, DataTransferHttp2ConnectionHandler.class);

  static final Log LOG = LogFactory
      .getLog(DataTransferHttp2ConnectionHandler.class);

  private DataTransferHttp2ConnectionHandler(Http2Connection connection,
      Http2FrameReader frameReader, Http2FrameWriter frameWriter,
      Http2FrameListener listener) {
    super(connection, frameReader, frameWriter, listener);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof Http2DataFrame) {
      Http2DataFrame frame = (Http2DataFrame) msg;
      encoder().writeData(ctx, frame.getStreamId(), frame.getData(), 0,
        frame.isEndOfStream(), promise);
    } else if (msg instanceof Http2HeaderFrame) {
      Http2HeaderFrame frame = (Http2HeaderFrame) msg;
      encoder().writeHeaders(ctx, frame.getStreamId(), frame.getHeaders(), 0,
        frame.isEndOfStream(), promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    onException(ctx, cause);
  }

  public static DataTransferHttp2ConnectionHandler create(Channel channel,
      StreamHandlerInitializer initializer, boolean verbose) {
    Http2Connection conn = new DefaultHttp2Connection(true);
    DataTransferHttp2EventListener listener =
        new DataTransferHttp2EventListener(channel, conn, initializer);
    conn.addListener(listener);
    Http2FrameReader frameReader;
    Http2FrameWriter frameWriter;
    if (verbose) {
      frameReader =
          new Http2InboundFrameLogger(new DefaultHttp2FrameReader(),
              FRAME_LOGGER);
      frameWriter =
          new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(),
              FRAME_LOGGER);
    } else {
      frameReader = new DefaultHttp2FrameReader();
      frameWriter = new DefaultHttp2FrameWriter();
    }
    return new DataTransferHttp2ConnectionHandler(conn, frameReader,
        frameWriter, listener);
  }
}
