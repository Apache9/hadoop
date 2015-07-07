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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.timeout.ReadTimeoutException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 *
 */
public class Http2DataReceiver extends ChannelInboundHandlerAdapter {

  private static final ByteBuf END_OF_STREAM = Unpooled
      .wrappedBuffer(new byte[1]);

  private static final EOFException EOF = new EOFException();

  private final Deque<ByteBuf> queue = new ArrayDeque<ByteBuf>();

  private Channel channel;

  private Throwable error;

  private Http2Headers headers;

  private final ByteBufferReadableInputStream contentInput =
      new ByteBufferReadableInputStream() {

        @Override
        public int read() throws IOException {
          ByteBuf buf = peekUntilAvailable();
          if (buf == END_OF_STREAM) {
            return -1;
          }
          int b = buf.readByte() & 0xFF;
          if (!buf.isReadable()) {
            removeHead();
          }
          return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          ByteBuf buf = peekUntilAvailable();
          if (buf == END_OF_STREAM) {
            return -1;
          }
          int bufReadableBytes = buf.readableBytes();
          if (len >= bufReadableBytes) {
            buf.readBytes(b, off, bufReadableBytes);
            removeHead();
            return bufReadableBytes;
          } else {
            buf.readBytes(b, off, len);
            return len;
          }
        }

        @Override
        public long skip(long n) throws IOException {
          ByteBuf buf = peekUntilAvailable();
          if (buf == END_OF_STREAM) {
            return 0;
          }
          int bufReadableBytes = buf.readableBytes();
          if (n >= bufReadableBytes) {
            removeHead();
            return bufReadableBytes;
          } else {
            buf.skipBytes((int) n);
            return n;
          }
        }

        @Override
        public int read(ByteBuffer bb) throws IOException {
          ByteBuf buf = peekUntilAvailable();
          if (buf == END_OF_STREAM) {
            return -1;
          }
          int bbRemaining = bb.remaining();
          int bufReadableBytes = buf.readableBytes();
          if (bbRemaining >= bufReadableBytes) {
            int toRestoredLimit = bb.limit();
            bb.limit(bb.position() + bufReadableBytes);
            buf.readBytes(bb);
            bb.limit(toRestoredLimit);
            removeHead();
            return bufReadableBytes;
          } else {
            buf.readBytes(bb);
            return bbRemaining;
          }
        }

        private boolean closed = false;

        @Override
        public void close() throws IOException {
          if (closed) {
            return;
          }
          synchronized (queue) {
            if (error == null) {
              error = EOF;
            }
          }
          channel.close().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future)
                throws Exception {
              synchronized (queue) {
                for (ByteBuf buf; (buf = queue.peek()) != null;) {
                  if (buf == END_OF_STREAM) {
                    return;
                  }
                  buf.release();
                  queue.remove();
                }
              }
            }
          });
        }

      };

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg == LastHttp2Message.get()) {
      enqueue(END_OF_STREAM);
    } else if (msg instanceof Http2Headers) {
      synchronized (queue) {
        headers = (Http2Headers) msg;
        queue.notifyAll();
      }
    } else if (msg instanceof ByteBuf) {
      enqueue((ByteBuf) msg);
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    synchronized (queue) {
      if (error == null) {
        error = cause;
        queue.notifyAll();
      }
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    channel = ctx.channel();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    synchronized (queue) {
      if (error != null) {
        return;
      }
      ByteBuf lastBuf = queue.peekLast();
      if (lastBuf == END_OF_STREAM) {
        return;
      }
      error = EOF;
      notifyAll();
    }
  }

  private void enqueue(ByteBuf buf) {
    if (buf.isReadable()) {
      synchronized (queue) {
        queue.add(buf);
        queue.notifyAll();
      }
    } else {
      buf.release();
    }
  }

  private ByteBuf peekUntilAvailable() throws IOException {
    Throwable cause;
    synchronized (queue) {
      for (;;) {
        if (!queue.isEmpty()) {
          return queue.peek();
        }
        if (error != null) {
          cause = error;
          break;
        }
        try {
          queue.wait();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
    }
    throw toIOE(cause);
  }

  private void removeHead() {
    ByteBuf buf;
    synchronized (queue) {
      buf = queue.remove();
    }
    buf.release();
  }

  private IOException toIOE(Throwable cause) {
    if (cause == ReadTimeoutException.INSTANCE) {
      return new IOException("Read timeout");
    } else if (cause == EOF) {
      return new IOException("Connection reset by peer: "
          + channel.remoteAddress());
    } else {
      return new IOException(cause);
    }
  }

  public Http2Headers waitForResponse() throws IOException {
    Throwable cause;
    synchronized (queue) {
      for (;;) {
        if (error != null) {
          cause = error;
          break;
        }
        if (headers != null) {
          return headers;
        }
        try {
          queue.wait();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
    }
    throw toIOE(cause);
  }

  /**
   * The returned stream is not thread safe.
   */
  public ByteBufferReadableInputStream content() {
    return contentInput;
  }

}
