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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Connection.PropertyKey;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.logging.LogLevel;

/**
 *
 */
public class ClientHttp2ConnectionHandler extends Http2ConnectionHandler {
  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, ClientHttp2ConnectionHandler.class);

  private int nextStreamId = 3;

  private final PropertyKey subChannelPropKey;

  private ClientHttp2ConnectionHandler(Http2Connection connection,
      Http2FrameReader frameReader, Http2FrameWriter frameWriter,
      ClientHttp2EventListener listener) {
    super(connection, frameReader, frameWriter, listener);
    subChannelPropKey = listener.getSubChannelPropKey();
  }

  private int nextStreamId() {
    int streamId = nextStreamId;
    nextStreamId += 2;
    return streamId;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    boolean endStream;
    if (msg instanceof LastMessage) {
      msg = ((LastMessage) msg).get();
      endStream = true;
    } else {
      endStream = false;
    }
    if (msg instanceof Http2HeadersAndPromise) {
      final Http2HeadersAndPromise headersAndPromise =
          (Http2HeadersAndPromise) msg;
      final int streamId = nextStreamId();
      Http2ConnectionEncoder encoder = encoder();
      encoder.writeHeaders(ctx, streamId, headersAndPromise.headers, 0,
        endStream, promise).addListener(new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            headersAndPromise.promise.setSuccess(connection().stream(streamId)
                .<Http2StreamChannel> getProperty(subChannelPropKey));
          } else {
            headersAndPromise.promise.setFailure(future.cause());
          }
        }
      });
    } else {
      throw new UnsupportedMessageTypeException(msg, Http2Headers.class);
    }
  }

  public static ClientHttp2ConnectionHandler create(Channel channel,
      boolean verbose) {
    Http2Connection conn = new DefaultHttp2Connection(false);
    ClientHttp2EventListener listener =
        new ClientHttp2EventListener(channel, conn);
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
    return new ClientHttp2ConnectionHandler(conn, frameReader, frameWriter,
        listener);
  }
}
