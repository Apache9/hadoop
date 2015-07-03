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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Connection.PropertyKey;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.logging.LogLevel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class ClientHttp2ConnectionHandler extends Http2ConnectionHandler {

  private static final Log LOG = LogFactory
      .getLog(ClientHttp2ConnectionHandler.class);

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, ClientHttp2ConnectionHandler.class);

  private int nextStreamId = 3;

  private final PropertyKey subChannelPropKey;

  private ClientHttp2ConnectionHandler(Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder) {
    super(decoder, encoder);
    this.subChannelPropKey =
        ((ClientHttp2EventListener) decoder.listener()).subChannelPropKey;
  }

  private int nextStreamId() {
    int streamId = nextStreamId;
    nextStreamId += 2;
    return streamId;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof Http2HeadersAndPromise) {
      final Http2HeadersAndPromise headersAndPromise =
          (Http2HeadersAndPromise) msg;
      final int streamId = nextStreamId();
      Http2ConnectionEncoder encoder = encoder();
      encoder.writeHeaders(ctx, streamId, headersAndPromise.headers, 0,
        headersAndPromise.endStream, promise).addListener(
        new ChannelFutureListener() {

          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              headersAndPromise.promise.setSuccess(connection()
                  .stream(streamId).<Http2StreamChannel> getProperty(
                    subChannelPropKey));
            } else {
              headersAndPromise.promise.setFailure(future.cause());
            }
          }
        });
    } else {
      throw new UnsupportedMessageTypeException(msg,
          Http2HeadersAndPromise.class);
    }
  }

  private static final Http2Util.Http2ConnectionHandlerFactory<ClientHttp2ConnectionHandler> FACTORY =
      new Http2Util.Http2ConnectionHandlerFactory<ClientHttp2ConnectionHandler>() {

        @Override
        public ClientHttp2ConnectionHandler create(
            Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder) {
          return new ClientHttp2ConnectionHandler(decoder, encoder);
        }
      };

  public static ClientHttp2ConnectionHandler create(Channel channel,
      Configuration conf) throws Http2Exception {
    Http2Connection conn = new DefaultHttp2Connection(false);
    ClientHttp2EventListener listener =
        new ClientHttp2EventListener(channel, conn);
    return Http2Util.create(conf, conn, listener, FACTORY,
      LOG.isDebugEnabled() ? FRAME_LOGGER : null);
  }
}
