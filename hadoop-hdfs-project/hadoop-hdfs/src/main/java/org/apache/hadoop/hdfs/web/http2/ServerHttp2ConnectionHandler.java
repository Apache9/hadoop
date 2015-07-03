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
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionDecoder;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionEncoder;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.logging.LogLevel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 *
 */
@InterfaceAudience.Private
public class ServerHttp2ConnectionHandler extends Http2ConnectionHandler {

  private static final Log LOG = LogFactory
      .getLog(ServerHttp2ConnectionHandler.class);

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, ServerHttp2ConnectionHandler.class);

  private ServerHttp2ConnectionHandler(Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder) {
    super(decoder, encoder);
  }

  public static ServerHttp2ConnectionHandler create(Channel channel,
      ChannelInitializer<Http2StreamChannel> initializer, Configuration conf)
      throws Http2Exception {
    Http2Connection conn = new DefaultHttp2Connection(true);
    ServerHttp2EventListener listener =
        new ServerHttp2EventListener(channel, conn, initializer);
    conn.addListener(listener);
    DefaultHttp2FrameReader rawFrameReader = new DefaultHttp2FrameReader();
    DefaultHttp2FrameWriter rawFrameWriter = new DefaultHttp2FrameWriter();
    rawFrameWriter.maxFrameSize(Http2CodecUtil.MAX_FRAME_SIZE_UPPER_BOUND);
    Http2FrameReader frameReader;
    Http2FrameWriter frameWriter;
    if (LOG.isDebugEnabled()) {
      frameReader = new Http2InboundFrameLogger(rawFrameReader, FRAME_LOGGER);
      frameWriter = new Http2OutboundFrameLogger(rawFrameWriter, FRAME_LOGGER);
    } else {
      frameReader = rawFrameReader;
      frameWriter = rawFrameWriter;
    }
    DefaultHttp2LocalFlowController localFlowController =
        new DefaultHttp2LocalFlowController(conn, frameWriter, conf.getFloat(
          DFSConfigKeys.DFS_HTTP2_WINDOW_UPDATE_RATIO,
          DFSConfigKeys.DFS_HTTP2_WINDOW_UPDATE_RATIO_DEFAULT));
    localFlowController.initialWindowSize(conf.getInt(
      DFSConfigKeys.DFS_HTTP2_INITIAL_WINDOW_SIZE,
      DFSConfigKeys.DFS_HTTP2_INITIAL_WINDOW_SIZE_DEFAULT));
    conn.local().flowController(localFlowController);

    DefaultHttp2ConnectionEncoder encoder =
        new DefaultHttp2ConnectionEncoder(conn, frameWriter);
    DefaultHttp2ConnectionDecoder decoder =
        new DefaultHttp2ConnectionDecoder(conn, encoder, frameReader, listener);
    return new ServerHttp2ConnectionHandler(decoder, encoder);
  }
}
