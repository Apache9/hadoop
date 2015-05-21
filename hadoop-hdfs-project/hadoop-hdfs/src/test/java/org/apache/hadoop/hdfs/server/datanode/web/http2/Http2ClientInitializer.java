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
package org.apache.hadoop.hdfs.server.datanode.web.http2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OrHttpChooser.SelectedProtocol;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslHandler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLEngine;

/**
 *
 */
public class Http2ClientInitializer extends ChannelInitializer<Channel> {

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, Http2ClientInitializer.class);

  private final SSLEngine engine;

  private final Http2ResponseHandler responseHandler =
      new Http2ResponseHandler();

  private Http2SettingsHandler settingsHandler;

  public Http2ClientInitializer(SSLEngine engine) {
    this.engine = engine;
  }

  public void awaitUpgrade(long timeout, TimeUnit unit) throws TimeoutException {
    settingsHandler.awaitSettings(timeout, unit);
  }

  public Http2ResponseHandler getResponseHandler() {
    return responseHandler;
  }

  /**
   * A handler that triggers the cleartext upgrade to HTTP/2 by sending an
   * initial HTTP request.
   */
  private final class UpgradeRequestHandler extends
      ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      DefaultFullHttpRequest upgradeRequest =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
      ctx.writeAndFlush(upgradeRequest);

      ctx.fireChannelActive();

      // Done with this handler, remove it from the pipeline.
      ctx.pipeline().remove(this).addLast(settingsHandler, responseHandler);
    }
  }

  private void setupClearText(Channel ch,
      Http2ConnectionHandler connectionHandler) {
    HttpClientCodec sourceCodec = new HttpClientCodec();
    Http2ClientUpgradeCodec upgradeCodec =
        new Http2ClientUpgradeCodec(connectionHandler);
    HttpClientUpgradeHandler upgradeHandler =
        new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, 65536);

    ch.pipeline().addLast(sourceCodec, upgradeHandler,
      new UpgradeRequestHandler());
  }

  private SSLEngine wrapSSLEngine(SSLEngine engine) {
    JdkAlpnApplicationProtocolNegotiator apn =
        new JdkAlpnApplicationProtocolNegotiator(false,
            SelectedProtocol.HTTP_2.protocolName());
    return apn.wrapperFactory().wrapSslEngine(engine, apn, false);
  }

  private void setupSSl(Channel ch, Http2ConnectionHandler connectionHandler) {
    ch.pipeline().addLast(new SslHandler(wrapSSLEngine(engine)),
      connectionHandler, settingsHandler, responseHandler);
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    settingsHandler = new Http2SettingsHandler(ch.newPromise());
    Http2Connection connection = new DefaultHttp2Connection(false);
    Http2ConnectionHandler connectionHandler =
        new HttpToHttp2ConnectionHandler(connection, frameReader(),
            frameWriter(), new DelegatingDecompressorFrameListener(connection,
                new InboundHttp2ToHttpAdapter.Builder(connection)
                    .maxContentLength(Integer.MAX_VALUE)
                    .propagateSettings(true).build()));

    if (engine == null) {
      setupClearText(ch, connectionHandler);
    } else {
      setupSSl(ch, connectionHandler);
    }
  }

  private static Http2FrameReader frameReader() {
    return new Http2InboundFrameLogger(new DefaultHttp2FrameReader(),
        FRAME_LOGGER);
  }

  private static Http2FrameWriter frameWriter() {
    return new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(),
        FRAME_LOGGER);
  }
}
