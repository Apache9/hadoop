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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A simple work around before https://github.com/netty/netty/issues/3667
 * finished.
 */
@InterfaceAudience.Private
public class StreamHandlerContext {

  StreamHandlerContext prev;

  StreamHandlerContext next;

  private final StreamHandler handler;

  private final boolean inbound;

  private final boolean outbound;

  private final EmbeddedStream stream;

  public StreamHandlerContext(StreamHandlerContext prev,
      StreamHandlerContext next, StreamHandler handler, EmbeddedStream stream) {
    this.prev = prev;
    this.next = next;
    this.handler = handler;
    this.stream = stream;
    this.inbound = handler instanceof StreamInboundHandler;
    this.outbound = handler instanceof StreamOutboundHandler;
  }

  private StreamHandlerContext nextInbound() {
    for (StreamHandlerContext ctx = next; ctx != null;) {
      if (ctx.inbound) {
        return ctx;
      }
    }
    return null;
  }

  private StreamHandlerContext nextOutbound() {
    for (StreamHandlerContext ctx = prev; ctx != null;) {
      if (ctx.outbound) {
        return ctx;
      }
    }
    return null;
  }

  /**
   * Should only be called inside event loop.
   */
  public void fireStreamRead(Object msg, boolean endOfStream) {
    StreamHandlerContext ctx = nextInbound();
    try {
      ((StreamInboundHandler) ctx.handler).streamRead(ctx, msg, endOfStream);
    } catch (Throwable t) {
      ctx.handler.exceptionCaught(ctx, t);
    }
  }

  /**
   * Should only be called inside event loop.
   */
  public void fireStreamClosed() {
    next.handler.streamClosed(next);
  }

  public void fireExceptionCaught(final Throwable cause) {
    EventLoop executor = stream.channel().eventLoop();
    if (executor.inEventLoop()) {
      next.handler.exceptionCaught(next, cause);
    } else {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          next.handler.exceptionCaught(next, cause);
        }
      });
    }
  }

  private void write0(Object msg, boolean endOfStream) {
    StreamHandlerContext ctx = nextOutbound();
    try {
      ((StreamOutboundHandler) ctx.handler).write(ctx, msg, endOfStream);
    } catch (Throwable t) {
      ctx.handler.exceptionCaught(ctx, t);
    }
  }

  public void write(final Object msg, final boolean endOfStream) {
    EventLoop executor = stream.channel().eventLoop();
    if (executor.inEventLoop()) {
      write0(msg, endOfStream);
    } else {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          write0(msg, endOfStream);
        }
      });
    }
  }

  private void writeAndFlush0(Object msg, boolean endOfStream) {
    StreamHandlerContext ctx = nextOutbound();
    try {
      ((StreamOutboundHandler) ctx.handler)
          .writeAndFlush(ctx, msg, endOfStream);
    } catch (Throwable t) {
      ctx.handler.exceptionCaught(ctx, t);
    }
  }

  public void writeAndFlush(final Object msg, final boolean endOfStream) {
    EventLoop executor = stream.channel().eventLoop();
    if (executor.inEventLoop()) {
      writeAndFlush0(msg, endOfStream);
    } else {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          writeAndFlush0(msg, endOfStream);
        }
      });
    }
  }

  public EmbeddedStream stream() {
    return stream;
  }

  public ByteBufAllocator alloc() {
    return stream.channel().alloc();
  }
}
