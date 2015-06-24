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
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.handler.codec.http2.Http2RemoteFlowController;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.internal.InternalThreadLocalMap;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.collect.ImmutableSet;

/**
 *
 */
@InterfaceAudience.Private
public class Http2StreamChannel extends AbstractChannel {

  private static final ChannelMetadata METADATA = new ChannelMetadata(false);

  private static final int MAX_READER_STACK_DEPTH = 8;

  private final ChannelHandlerContext http2ConnHandlerCtx;

  private final Http2Stream connStream;

  private final Http2Stream stream;

  private final Http2LocalFlowController localFlowController;

  private final Http2RemoteFlowController remoteFlowController;

  private final Http2ConnectionEncoder encoder;

  private final DefaultChannelConfig config;

  private final Queue<Object> inboundMessageQueue = new ArrayDeque<>();

  private int pendingInboundBytes;

  private boolean writePending = false;

  private enum State {
    OPEN, HALF_CLOSED_LOCAL, HALF_CLOSED_REMOTE, PRE_CLOSED, CLOSED
  }

  private volatile State state = State.OPEN;

  public Http2StreamChannel(Channel parent, Http2Stream stream) {
    super(parent);
    this.http2ConnHandlerCtx =
        parent.pipeline().context(Http2ConnectionHandler.class);
    Http2ConnectionHandler connHandler =
        (Http2ConnectionHandler) http2ConnHandlerCtx.handler();
    this.connStream = connHandler.connection().connectionStream();
    this.stream = stream;
    this.localFlowController =
        connHandler.connection().local().flowController();
    this.remoteFlowController =
        connHandler.connection().remote().flowController();
    this.encoder = connHandler.encoder();
    this.config = new DefaultChannelConfig(this);
  }

  public Http2Stream stream() {
    return stream;
  }

  @Override
  public ChannelConfig config() {
    return config;
  }

  @Override
  public boolean isOpen() {
    return state != State.CLOSED;
  }

  @Override
  public boolean isActive() {
    return isOpen();
  }

  @Override
  public ChannelMetadata metadata() {
    return METADATA;
  }

  private final class Http2Unsafe extends AbstractUnsafe {

    @Override
    public void connect(SocketAddress remoteAddress,
        SocketAddress localAddress, ChannelPromise promise) {
      throw new UnsupportedOperationException();
    }

    public void forceFlush() {
      super.flush0();
    }
  }

  @Override
  protected AbstractUnsafe newUnsafe() {
    return new Http2Unsafe();
  }

  @Override
  protected boolean isCompatible(EventLoop loop) {
    return true;
  }

  @Override
  protected SocketAddress localAddress0() {
    return parent().localAddress();
  }

  @Override
  protected SocketAddress remoteAddress0() {
    return parent().remoteAddress();
  }

  @Override
  protected void doBind(SocketAddress localAddress) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doDisconnect() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doClose() throws Exception {
    if (stream.state() != Http2Stream.State.CLOSED) {
      encoder.writeRstStream(http2ConnHandlerCtx, stream.id(),
        Http2Error.INTERNAL_ERROR.code(), http2ConnHandlerCtx.newPromise());
    }
    state = State.CLOSED;
  }

  private final Runnable readTask = new Runnable() {
    @Override
    public void run() {
      ChannelPipeline pipeline = pipeline();
      int maxMessagesPerRead = config().getMaxMessagesPerRead();
      for (int i = 0; i < maxMessagesPerRead; i++) {
        Object m = inboundMessageQueue.poll();
        if (m == null) {
          break;
        }
        if (m == LastHttp2Message.get()) {
          state =
              state == State.HALF_CLOSED_LOCAL ? State.PRE_CLOSED
                  : State.HALF_CLOSED_REMOTE;
        }
        pipeline.fireChannelRead(m);
      }
      pipeline.fireChannelReadComplete();
    }
  };

  @Override
  protected void doBeginRead() throws Exception {
    State currentState = this.state;
    if (currentState == State.CLOSED) {
      throw new ClosedChannelException();
    }
    if (pendingInboundBytes > 0) {
      localFlowController.consumeBytes(http2ConnHandlerCtx, stream,
        pendingInboundBytes);
      pendingInboundBytes = 0;
    }
    if (inboundMessageQueue.isEmpty()) {
      return;
    }
    final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
    final Integer stackDepth = threadLocals.localChannelReaderStackDepth();
    if (stackDepth < MAX_READER_STACK_DEPTH) {
      threadLocals.setLocalChannelReaderStackDepth(stackDepth + 1);
      try {
        readTask.run();
      } finally {
        threadLocals.setLocalChannelReaderStackDepth(stackDepth);
      }
    } else {
      eventLoop().execute(readTask);
    }
  }

  private boolean canWrite() {
    return remoteFlowController.windowSize(stream) > 0
        && remoteFlowController.windowSize(connStream) > 0;
  }

  @Override
  protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    State currentState = this.state;
    if (currentState == State.CLOSED) {
      throw new ClosedChannelException();
    }
    if (!canWrite()) {
      writePending = true;
      return;
    }
    boolean flush = false;
    for (;;) {
      Object msg = in.current();
      if (msg == null) {
        break;
      }
      if (msg == LastHttp2Message.get()) {
        this.state =
            currentState == State.HALF_CLOSED_REMOTE ? State.PRE_CLOSED
                : State.HALF_CLOSED_LOCAL;
        encoder.writeData(http2ConnHandlerCtx, stream.id(), http2ConnHandlerCtx
            .alloc().buffer(0), 0, true, http2ConnHandlerCtx.newPromise());
      } else if (msg instanceof Http2Headers) {
        encoder.writeHeaders(http2ConnHandlerCtx, stream.id(),
          (Http2Headers) msg, 0, false, http2ConnHandlerCtx.newPromise());
      } else if (msg instanceof ByteBuf) {
        ByteBuf data = (ByteBuf) msg;
        encoder.writeData(http2ConnHandlerCtx, stream.id(), data.retain(), 0,
          false, http2ConnHandlerCtx.newPromise());
      } else {
        throw new UnsupportedMessageTypeException(msg, Http2Headers.class,
            ByteBuf.class);
      }
      in.remove();
      flush = true;
    }
    if (flush) {
      http2ConnHandlerCtx.channel().flush();
    }
  }

  public void writeInbound(Object msg) {
    inboundMessageQueue.add(msg);
  }

  public void incrementPendingInboundBytes(int bytes) {
    pendingInboundBytes += bytes;
  }

  public void tryWrite() {
    if (writePending) {
      writePending = false;
      ((Http2Unsafe) unsafe()).forceFlush();
    }
  }

  private static final ImmutableSet<State> REMOTE_SIDE_CLOSED_STATES =
      ImmutableSet.of(State.HALF_CLOSED_REMOTE, State.PRE_CLOSED, State.CLOSED);

  public boolean remoteSideClosed() {
    return REMOTE_SIDE_CLOSED_STATES.contains(state);
  }

  private static final ImmutableSet<State> LOCAL_SIDE_CLOSED_STATES =
      ImmutableSet.of(State.HALF_CLOSED_LOCAL, State.PRE_CLOSED, State.CLOSED);

  public boolean localSideClosed() {
    return LOCAL_SIDE_CLOSED_STATES.contains(state);
  }
}
