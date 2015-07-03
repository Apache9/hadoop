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

import static io.netty.handler.codec.http2.Http2CodecUtil.CONNECTION_STREAM_ID;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Connection.PropertyKey;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2EventAdapter;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.Http2StreamVisitor;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 *
 */
@InterfaceAudience.Private
public abstract class AbstractHttp2EventListener extends Http2EventAdapter {

  protected final Channel parentChannel;

  protected final Http2Connection conn;

  protected final PropertyKey subChannelPropKey;

  protected AbstractHttp2EventListener(Channel parentChannel,
      Http2Connection conn) {
    this.parentChannel = parentChannel;
    this.conn = conn;
    this.subChannelPropKey = conn.newKey();
  }

  protected abstract void initChannelOnStreamActive(
      Http2StreamChannel subChannel);

  @Override
  public void onStreamActive(final Http2Stream stream) {
    Http2StreamChannel subChannel =
        new Http2StreamChannel(parentChannel, stream);
    stream.setProperty(subChannelPropKey, subChannel);
    initChannelOnStreamActive(subChannel);
  }

  @Override
  public void onStreamClosed(Http2Stream stream) {
    Http2StreamChannel subChannel = stream.removeProperty(subChannelPropKey);
    if (subChannel != null && subChannel.isRegistered()) {
      subChannel.close();
    }
  }

  private Http2StreamChannel getSubChannel(int streamId) throws Http2Exception {
    Http2StreamChannel subChannel =
        conn.stream(streamId).getProperty(subChannelPropKey);
    if (subChannel == null) {
      throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR,
        "No sub channel found");
    }
    return subChannel;
  }

  private boolean writeInbound(int streamId, Object msg, boolean endOfStream,
      int pendingBytes) throws Http2Exception {
    Http2StreamChannel subChannel = getSubChannel(streamId);
    subChannel.writeInbound(msg);
    if (endOfStream) {
      subChannel.writeInbound(LastHttp2Message.get());
    }
    if (subChannel.config().isAutoRead()) {
      subChannel.read();
      return true;
    } else {
      subChannel.incrementPendingInboundBytes(pendingBytes);
      return false;
    }
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int padding, boolean endOfStream)
      throws Http2Exception {
    writeInbound(streamId, headers, endOfStream, 0);
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int streamDependency, short weight,
      boolean exclusive, int padding, boolean endOfStream)
      throws Http2Exception {
    onHeadersRead(ctx, streamId, headers, padding, endOfStream);
  }

  @Override
  public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data,
      int padding, boolean endOfStream) throws Http2Exception {
    int pendingBytes = data.readableBytes() + padding;
    if (writeInbound(streamId, data.retain(), endOfStream, pendingBytes)) {
      return pendingBytes;
    } else {
      return 0;
    }
  }

  @Override
  public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId,
      int windowSizeIncrement) throws Http2Exception {
    if (streamId == CONNECTION_STREAM_ID) {
      conn.forEachActiveStream(new Http2StreamVisitor() {

        @Override
        public boolean visit(Http2Stream stream) throws Http2Exception {
          Http2StreamChannel subChannel = stream.getProperty(subChannelPropKey);
          if (subChannel != null) {
            subChannel.tryWrite();
          }
          return true;
        }
      });
    } else {
      Http2Stream stream = conn.stream(streamId);
      if (stream != null) {
        Http2StreamChannel subChannel = stream.getProperty(subChannelPropKey);
        if (subChannel != null) {
          subChannel.tryWrite();
        }
      }
    }
  }
}
