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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Connection.PropertyKey;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2EventAdapter;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.HttpUtil;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Base class of HTTP/2 event listener.
 */
@InterfaceAudience.Private
public class DataTransferHttp2EventListener extends Http2EventAdapter {

  private final Channel channel;

  private final StreamHandlerInitializer initializer;

  private final Http2Connection conn;

  private final PropertyKey embeddedStreamPropKey;

  public DataTransferHttp2EventListener(Channel channel, Http2Connection conn,
      StreamHandlerInitializer initializer) {
    this.channel = channel;
    this.conn = conn;
    this.initializer = initializer;
    this.embeddedStreamPropKey = conn.newKey();
  }

  @Override
  public void onStreamActive(Http2Stream stream) {
    EmbeddedStream embeddedStream = new EmbeddedStream(channel, stream.id());
    initializer.initialize(embeddedStream);
    stream.setProperty(embeddedStreamPropKey, embeddedStream);
  }

  @Override
  public final void onStreamClosed(Http2Stream stream) {
    EmbeddedStream embeddedStream =
        stream.removeProperty(embeddedStreamPropKey);
    if (embeddedStream != null) {
      embeddedStream.pipeline().fireStreamClosed();
    }
  }

  private EmbeddedStream getEmbeddedStream(int streamId) throws Http2Exception {
    EmbeddedStream embeddedStream =
        conn.stream(streamId).getProperty(embeddedStreamPropKey);
    if (embeddedStream == null) {
      throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR,
        "No embedded stream found");
    }
    return embeddedStream;
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int padding, boolean endStream)
      throws Http2Exception {
    FullHttpRequest request = HttpUtil.toHttpRequest(streamId, headers, true);
    getEmbeddedStream(streamId).pipeline().fireStreamRead(request, endStream);
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int streamDependency, short weight,
      boolean exclusive, int padding, boolean endStream) throws Http2Exception {
    onHeadersRead(ctx, streamId, headers, padding, endStream);
  }

  @Override
  public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data,
      int padding, boolean endOfStream) throws Http2Exception {
    int processed = data.readableBytes() + padding;
    getEmbeddedStream(streamId).pipeline().fireStreamRead(data, endOfStream);
    return processed;
  }

}
