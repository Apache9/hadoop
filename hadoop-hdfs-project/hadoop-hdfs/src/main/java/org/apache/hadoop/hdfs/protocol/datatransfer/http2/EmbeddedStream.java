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
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpUtil;
import io.netty.handler.stream.ChunkedInput;

import java.net.SocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A simple work around before https://github.com/netty/netty/issues/3667
 * finished.
 */
@InterfaceAudience.Private
public class EmbeddedStream {

  private final Channel channel;

  private final int streamId;

  private final StreamPipeline pipeline;

  public EmbeddedStream(Channel channel, int streamId) {
    this.channel = channel;
    this.streamId = streamId;
    this.pipeline = new StreamPipeline(this);
  }

  public StreamPipeline pipeline() {
    return pipeline;
  }

  void writeToParentChannel(Object msg, boolean endOfStream, boolean flush) {
    if (msg instanceof FullHttpResponse) {
      FullHttpResponse resp = (FullHttpResponse) msg;
      Http2Headers headers;
      try {
        headers = HttpUtil.toHttp2Headers(resp);
      } catch (Exception e) {
        // should not happen
        throw new AssertionError(e);
      }
      Http2HeaderFrame headerFrame =
          new Http2HeaderFrame(streamId, headers, 0, false);
      channel.write(headerFrame);
      Http2DataFrame dataFrame =
          new Http2DataFrame(streamId, resp.content(), 0, endOfStream);
      if (flush) {
        channel.writeAndFlush(dataFrame).addListener(
          new ResetOnFailureListener(streamId));
      } else {
        channel.write(dataFrame);
      }
    } else if (msg instanceof HttpResponse) {
      HttpResponse resp = (HttpResponse) msg;
      DefaultFullHttpResponse fullResp =
          new DefaultFullHttpResponse(resp.protocolVersion(), resp.status());
      fullResp.headers().add(resp.headers());
      Http2Headers headers;
      try {
        headers = HttpUtil.toHttp2Headers(fullResp);
      } catch (Exception e) {
        // should not happen
        throw new AssertionError(e);
      }
      Http2HeaderFrame frame =
          new Http2HeaderFrame(streamId, headers, 0, endOfStream);
      if (flush) {
        channel.writeAndFlush(frame).addListener(
          new ResetOnFailureListener(streamId));
      } else {
        channel.write(frame);
      }
    } else if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      Http2DataFrame frame = new Http2DataFrame(streamId, buf, 0, endOfStream);
      if (flush) {
        channel.writeAndFlush(frame).addListener(
          new ResetOnFailureListener(streamId));
      } else {
        channel.write(frame);
      }
    } else if (msg instanceof ChunkedInput) {
      @SuppressWarnings("unchecked")
      Http2ChunkedInput chunkedInput =
          new Http2ChunkedInput(streamId, (ChunkedInput<ByteBuf>) msg,
              endOfStream);
      if (flush) {
        channel.writeAndFlush(chunkedInput).addListener(
          new ResetOnFailureListener(streamId));
      } else {
        channel.write(chunkedInput);
      }
    } else {
      if (flush) {
        channel.writeAndFlush(msg).addListener(
          new ResetOnFailureListener(streamId));
      } else {
        channel.write(msg);
      }
    }
  }

  Channel channel() {
    return channel;
  }

  public SocketAddress remoteAddress() {
    return channel.remoteAddress();
  }
}
