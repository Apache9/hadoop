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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http.MetaData.Response;
import org.eclipse.jetty.http2.ErrorCode;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.PriorityFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Test Http2Server with jetty HTTP2Client.
 */
public class TestHttp2ServerMultiThread {

  private EventLoopGroup bossGroup = new NioEventLoopGroup(1);

  private EventLoopGroup workerGroup = new NioEventLoopGroup();

  private Channel server;

  private HTTP2Client client = new HTTP2Client();

  private Session session;

  private int concurrency = 20;

  private ExecutorService executor = Executors.newFixedThreadPool(concurrency,
    new ThreadFactoryBuilder().setNameFormat("Echo-Client-%d").setDaemon(true)
        .build());

  private int requestCount = 10000;

  @Before
  public void setUp() throws Exception {
    ServerBootstrap b =
        new ServerBootstrap().group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(
                  new DataTransferHttp2ConnectionHandler(ch,
                      new StreamHandlerInitializer() {

                        @Override
                        public void initialize(EmbeddedStream stream) {
                          stream.pipeline().addHandlerFirst(
                            new DispatcherHandler());
                        }
                      }));
              }

            });
    server = b.bind(0).syncUninterruptibly().channel();
    client.start();
    int port = ((InetSocketAddress) server.localAddress()).getPort();
    FuturePromise<Session> sessionPromise = new FuturePromise<>();
    client.connect(new InetSocketAddress("127.0.0.1", port),
      new Session.Listener.Adapter(), sessionPromise);
    session = sessionPromise.get();
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
    if (session != null) {
      session.close(ErrorCode.NO_ERROR.code, "", new Callback.Adapter());
    }
    if (server != null) {
      server.close();
    }
    client.stop();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  private final AtomicInteger handlerClosedCount = new AtomicInteger(0);

  private final class DispatcherHandler extends StreamInboundHandlerAdaptor {

    @Override
    public void streamRead(StreamHandlerContext ctx, Object msg,
        boolean endOfStream) throws Exception {
      if (msg instanceof HttpRequest) {
        StreamPipeline pipeline = ctx.stream().pipeline();
        pipeline.removeHandler(this);
        pipeline.addHandlerFirst(new EchoHandler());
        ctx.writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK), endOfStream);
      } else {
        super.streamRead(ctx, msg, endOfStream);
      }
    }

  }

  private final class EchoHandler extends StreamInboundHandlerAdaptor {

    @Override
    public void streamRead(StreamHandlerContext ctx, Object msg,
        boolean endOfStream) throws Exception {
      if (msg instanceof ByteBuf) {
        ByteBuf data = (ByteBuf) msg;
        ctx.writeAndFlush(data.retain(), endOfStream);
      } else {
        super.streamRead(ctx, msg, endOfStream);
      }
    }

    @Override
    public void streamClosed(StreamHandlerContext ctx) {
      handlerClosedCount.incrementAndGet();
    }

  }

  private static final class StreamListener extends Stream.Listener.Adapter {

    private boolean finish = false;

    private byte[] buf = new byte[0];

    private int status = -1;

    private boolean reset;

    @Override
    public void onData(Stream stream, DataFrame frame, Callback callback) {
      synchronized (this) {
        if (reset) {
          callback.failed(new IllegalStateException("Stream already closed"));
        }
        if (status == -1) {
          callback.failed(new IllegalStateException(
              "Haven't received header yet"));
        }
        int bufLen = buf.length;
        int newBufLen = bufLen + frame.getData().remaining();
        buf = Arrays.copyOf(buf, newBufLen);
        frame.getData().get(buf, bufLen, frame.getData().remaining());
        if (frame.isEndStream()) {
          finish = true;
        }
        notifyAll();
        callback.succeeded();
      }
    }

    @Override
    public void onHeaders(Stream stream, HeadersFrame frame) {
      synchronized (this) {
        if (reset) {
          throw new IllegalStateException("Stream already closed");
        }
        if (status != -1) {
          throw new IllegalStateException("Header already received");
        }
        MetaData meta = frame.getMetaData();
        if (!meta.isResponse()) {
          throw new IllegalStateException("Received non-response header");
        }
        status = ((Response) meta).getStatus();
        if (frame.isEndStream()) {
          finish = true;
          notifyAll();
        }
      }
    }

    @Override
    public void onReset(Stream stream, ResetFrame frame) {
      synchronized (this) {
        reset = true;
        finish = true;
        notifyAll();
      }
    }

    public int getStatus() throws InterruptedException, IOException {
      synchronized (this) {
        while (!finish) {
          wait();
        }
        if (reset) {
          throw new IOException("Stream reset");
        }
        return status;
      }
    }

    public byte[] getData() throws InterruptedException, IOException {
      synchronized (this) {
        while (!finish) {
          wait();
        }
        if (reset) {
          throw new IOException("Stream reset");
        }
        return buf;
      }
    }
  }

  private void testEcho() throws InterruptedException, ExecutionException,
      IOException {
    HttpFields fields = new HttpFields();
    fields.put(HttpHeader.C_METHOD, HttpMethod.GET.asString());
    fields.put(HttpHeader.C_PATH, "/");
    FuturePromise<Stream> streamPromise = new FuturePromise<>();
    StreamListener listener = new StreamListener();
    session.newStream(new HeadersFrame(1, new MetaData(
        org.eclipse.jetty.http.HttpVersion.HTTP_2, fields), new PriorityFrame(
        1, 0, 1, false), false), streamPromise, listener);
    Stream stream = streamPromise.get();
    if (ThreadLocalRandom.current().nextInt(5) < 1) { // 20%
      stream.reset(new ResetFrame(stream.getId(), ErrorCode.NO_ERROR.code),
        new Callback.Adapter());
    } else {
      byte[] msg = new byte[ThreadLocalRandom.current().nextInt(10, 100)];
      ThreadLocalRandom.current().nextBytes(msg);
      stream.data(new DataFrame(stream.getId(), ByteBuffer.wrap(msg), true),
        new Callback.Adapter());
      assertEquals(HttpStatus.OK_200, listener.getStatus());
      assertArrayEquals(msg, listener.getData());
    }
  }

  @Test
  public void test() throws InterruptedException {
    final AtomicBoolean succ = new AtomicBoolean(true);
    for (int i = 0; i < requestCount; i++) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            testEcho();
          } catch (Throwable t) {
            t.printStackTrace();
            succ.set(false);
          }
        }
      });
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.MINUTES));
    assertTrue(succ.get());
    Thread.sleep(1000);
    assertEquals(requestCount, handlerClosedCount.get());
  }
}
