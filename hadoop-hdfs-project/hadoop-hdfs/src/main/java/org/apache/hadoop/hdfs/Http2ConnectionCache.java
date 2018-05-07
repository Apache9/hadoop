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
package org.apache.hadoop.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.net.NetUtils;

import com.xiaomi.infra.thirdparty.io.netty.bootstrap.Bootstrap;
import com.xiaomi.infra.thirdparty.io.netty.channel.Channel;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelFuture;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelFutureListener;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandler.Sharable;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInitializer;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelOption;
import com.xiaomi.infra.thirdparty.io.netty.channel.EventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.epoll.EpollSocketChannel;
import com.xiaomi.infra.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.oio.OioEventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import com.xiaomi.infra.thirdparty.io.netty.channel.socket.oio.OioSocketChannel;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Connection;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2ConnectionHandler;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2FrameLogger;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2LocalFlowController;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Settings;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Stream;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.Future;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.FutureListener;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.Promise;

/**
 * TODO: close idle connections.
 */
class Http2ConnectionCache implements Closeable {

  private static final Log LOG = LogFactory.getLog(Http2ConnectionCache.class);

  // The number of streams that can be created on a HTTP/2 connection is limited
  // as the stream id is an int, and we can only use the odd ones at client
  // side, so here we set a limit to close the connection and create a new one
  // before overflow.
  private static final int MAX_STREAM_CREATED_PER_CONN = 1 << 24;

  @Sharable
  public static final class DelayedCloseHandler
      extends ChannelInboundHandlerAdapter {

    // no need to be volatile, only accessed under lock
    private int numberOfStreamsCreated;

    private final AtomicInteger numberOfActiveStreams = new AtomicInteger(0);

    private final Channel channel;

    // 0 open
    // 1 no new streams, i.e., prepare for closing
    // 2 closed
    private final AtomicInteger closingState = new AtomicInteger(0);

    public DelayedCloseHandler(Channel channel) {
      this.channel = channel;
    }

    void newStream() {
      newStream(1);
    }

    void newStream(int n) {
      numberOfActiveStreams.addAndGet(n);
      numberOfStreamsCreated += n;
    }

    boolean exceededMaxStreamCreated() {
      return numberOfActiveStreams() > MAX_STREAM_CREATED_PER_CONN;
    }

    int numberOfActiveStreams() {
      return numberOfActiveStreams.get();
    }

    private void closeConn() {
      if (!closingState.compareAndSet(1, 2)) {
        return;
      }
      LOG.info(
          "Going to close HTTP/2 connection to " + channel.remoteAddress() +
              ", " + numberOfStreamsCreated + " streams created");
      channel.close();
    }

    void noNewStreams() {
      closingState.set(1);
      if (numberOfActiveStreams.get() == 0) {
        closeConn();
      }
    }

    private void closeStream() {
      int count = numberOfActiveStreams.decrementAndGet();
      if (count == 0 && closingState.get() == 1) {
        closeConn();
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      closeStream();
      ctx.fireChannelInactive();
    }
  }

  @Sharable
  private static final class ConnectionWindowSizeHandler
      extends ChannelInboundHandlerAdapter {

    private final int initialWindowSize;

    public ConnectionWindowSizeHandler(int initialWindowSize) {
      this.initialWindowSize = initialWindowSize;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelActive();
      Http2Connection conn =
          ctx.pipeline().get(Http2ConnectionHandler.class).connection();
      Http2LocalFlowController flowController = conn.local().flowController();
      Http2Stream connStream = conn.connectionStream();
      int currentSize = flowController.windowSize(connStream);
      int delta = initialWindowSize - currentSize;
      flowController.incrementWindowSize(connStream, delta);
      ctx.pipeline().remove(this);
    }
  }

  private static final class Pool {

    public Promise<Pair<Channel, DelayedCloseHandler>> underConstruction;

    public int waitingOnConstruction;

    public final List<Pair<Channel, DelayedCloseHandler>> values =
        new ArrayList<>();
  }

  private final ConcurrentMap<String, Pool> cache = new ConcurrentHashMap<>();

  private final EventLoopGroup workerGroup;

  private final Class<? extends Channel> channelClass;

  private final int capacityPerDn;

  private final int initialWindowSize;

  private final Http2FrameLogger frameLogger;

  private final ConnectionWindowSizeHandler connWindowSizeHandler;

  public Http2ConnectionCache(String eventLoopType, int capacityPerDn,
      int initialWindowSize, Http2FrameLogger frameLogger) {
    ThreadFactory threadFactory = new DefaultThreadFactory(
        Http2ConnectionCache.class, true, Thread.MAX_PRIORITY);
    if ("EPOLL".equalsIgnoreCase(eventLoopType)) {
      workerGroup = new EpollEventLoopGroup(0, threadFactory);
      channelClass = EpollSocketChannel.class;
    } else if ("OIO".equalsIgnoreCase(eventLoopType)) {
      workerGroup = new OioEventLoopGroup(0, threadFactory);
      channelClass = OioSocketChannel.class;
    } else {
      // default to nio
      workerGroup = new NioEventLoopGroup(0, threadFactory);
      channelClass = NioSocketChannel.class;
    }
    this.capacityPerDn = capacityPerDn;
    this.initialWindowSize = initialWindowSize;
    this.frameLogger = frameLogger;
    if (initialWindowSize > 0) {
      this.connWindowSizeHandler =
          new ConnectionWindowSizeHandler(initialWindowSize);
    } else {
      this.connWindowSizeHandler = null;
    }
  }

  private Pool getPool(String infoAddr) {
    Pool pool = cache.get(infoAddr);
    if (pool != null) {
      return pool;
    }
    pool = new Pool();
    Pool oldPool = cache.putIfAbsent(infoAddr, pool);
    return oldPool == null ? pool : oldPool;
  }

  private ChannelFuture connect(String infoAddr) {
    return new Bootstrap().group(workerGroup).channel(channelClass)
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            Http2MultiplexCodecBuilder builder = Http2MultiplexCodecBuilder
                .forClient(new ChannelInitializer<Channel>() {

                  @Override
                  protected void initChannel(Channel ch) throws Exception {
                    throw new UnsupportedOperationException(
                        "Stream created from server is not allowed");
                  }
                }).frameLogger(frameLogger);
            if (initialWindowSize > 0) {
              builder.initialSettings(
                  new Http2Settings().initialWindowSize(initialWindowSize));
              ch.pipeline().addLast(connWindowSizeHandler);
            }
            ch.pipeline().addLast(builder.build());
          }
        }).connect(NetUtils.createSocketAddr(infoAddr));
  }

  private void createNewConn(final Pool pool, String infoAddr) {
    LOG.info("Create new HTTP/2 connection to " + infoAddr +
        ", current pool size is " + pool.values.size());
    ChannelFuture f = connect(infoAddr);
    pool.underConstruction = f.channel().eventLoop().newPromise();
    f.addListener(new ChannelFutureListener() {

      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          Pair<Channel, DelayedCloseHandler> channelAndHandler = Pair
              .of(future.channel(), new DelayedCloseHandler(future.channel()));
          Promise<Pair<Channel, DelayedCloseHandler>> promise;
          synchronized (pool) {
            channelAndHandler.getRight().newStream(pool.waitingOnConstruction);
            pool.waitingOnConstruction = 0;
            promise = pool.underConstruction;
            pool.underConstruction = null;
            boolean replaced = false;
            for (int i = 0, n = pool.values.size(); i < n; i++) {
              Pair<Channel, DelayedCloseHandler> oldChannelAndHandler =
                  pool.values.get(i);
              DelayedCloseHandler oldHandler = oldChannelAndHandler.getRight();
              if (oldHandler.exceededMaxStreamCreated()) {
                pool.values.set(i, channelAndHandler);
                oldHandler.noNewStreams();
                replaced = true;
                break;
              }
            }
            if (!replaced) {
              pool.values.add(channelAndHandler);
            }
          }
          promise.trySuccess(channelAndHandler);
        } else {
          Promise<?> promise;
          synchronized (pool) {
            promise = pool.underConstruction;
            pool.underConstruction = null;
            pool.waitingOnConstruction = 0;
          }
          promise.tryFailure(future.cause());
        }
      }
    });
  }

  private Future<Pair<Channel, DelayedCloseHandler>> getFromPool(
      final Pool pool, String infoAddr) {
    if (pool.values.isEmpty()) {
      if (pool.underConstruction == null) {
        createNewConn(pool, infoAddr);
      }
      pool.waitingOnConstruction++;
      return pool.underConstruction;
    }
    // select connections with least active streams
    int minActive = Integer.MAX_VALUE;
    Pair<Channel, DelayedCloseHandler> selected = null;
    boolean exceededMaxStreamCreated = false;
    for (Pair<Channel, DelayedCloseHandler> channelAndHandler : pool.values) {
      int active = channelAndHandler.getRight().numberOfActiveStreams();
      if (active < minActive) {
        selected = channelAndHandler;
        minActive = active;
      }
      exceededMaxStreamCreated |=
          channelAndHandler.getRight().exceededMaxStreamCreated();
    }
    // see if we need to create new connection
    if (pool.underConstruction == null &&
        ((minActive > 0 && pool.values.size() < capacityPerDn) ||
            exceededMaxStreamCreated)) {
      createNewConn(pool, infoAddr);
    }
    selected.getRight().newStream();
    return selected.getLeft().eventLoop().newSucceededFuture(selected);
  }

  public Pair<Channel, DelayedCloseHandler> get(DatanodeID dnId)
      throws IOException {
    String infoAddr = dnId.getInfoAddr();
    Pool pool = getPool(infoAddr);
    Future<Pair<Channel, DelayedCloseHandler>> future;
    synchronized (pool) {
      future = getFromPool(pool, infoAddr);
    }
    return future.syncUninterruptibly().getNow();
  }

  @Override
  public void close() throws IOException {
    for (Pool pool : cache.values()) {
      synchronized (pool) {
        for (Pair<Channel, ?> value : pool.values) {
          value.getLeft().close();
        }
        pool.values.clear();
        if (pool.underConstruction != null) {
          pool.underConstruction
              .addListener(new FutureListener<Pair<Channel, ?>>() {

                @Override
                public void operationComplete(Future<Pair<Channel, ?>> future)
                    throws Exception {
                  if (future.isSuccess()) {
                    future.getNow().getLeft().close();
                  }
                }
              });
          pool.underConstruction = null;
        }
      }
    }
    cache.clear();
  }
}
