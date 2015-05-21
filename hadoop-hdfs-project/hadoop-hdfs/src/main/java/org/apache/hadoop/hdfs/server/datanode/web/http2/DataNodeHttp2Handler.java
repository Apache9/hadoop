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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.URLDispatcher;

@InterfaceAudience.Private
public class DataNodeHttp2Handler extends Http2ConnectionHandler {

  private static final String UPGRADE_RESPONSE_HEADER = "Http-To-Http2-Upgrade";

  public DataNodeHttp2Handler(Configuration conf, Configuration confForCreate,
      ExecutorService executor) {
    super(true, new DataNodeHttp2FrameListener(conf, confForCreate, executor));
    ((DataNodeHttp2FrameListener) decoder().listener()).encoder(encoder());
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
      throws Exception {
    if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
      ctx.pipeline().remove(URLDispatcher.class);
      // Write an HTTP/2 response to the upgrade request
      Http2Headers headers =
          new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText())
              .set(new AsciiString(UPGRADE_RESPONSE_HEADER),
                new AsciiString("true"));
      encoder().writeHeaders(ctx, 1, headers, 0, true, ctx.newPromise());
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof Http2HeadersFrame) {
      Http2HeadersFrame frame = (Http2HeadersFrame) msg;
      encoder().writeHeaders(ctx, frame.getStreamId(), frame.getHeaders(),
        frame.getPadding(), frame.isEndOfStream(), promise);
    } else if (msg instanceof Http2DataFrame) {
      Http2DataFrame frame = (Http2DataFrame) msg;
      encoder().writeData(ctx, frame.getStreamId(), frame.getData(),
        frame.getPadding(), frame.isEndOfStream(), promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

}
