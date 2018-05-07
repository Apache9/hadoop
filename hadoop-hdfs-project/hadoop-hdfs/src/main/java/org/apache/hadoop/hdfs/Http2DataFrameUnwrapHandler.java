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

import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandler.Sharable;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2DataFrame;

@Sharable
public final class Http2DataFrameUnwrapHandler
    extends SimpleChannelInboundHandler<Http2DataFrame> {

  static final Object END_OF_STREAM = new Object();

  private static final Http2DataFrameUnwrapHandler HANDLER =
      new Http2DataFrameUnwrapHandler();

  private Http2DataFrameUnwrapHandler() {
    super(Http2DataFrame.class, false);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2DataFrame msg)
      throws Exception {
    ctx.fireChannelRead(msg.content());
    if (msg.isEndStream()) {
      ctx.fireUserEventTriggered(END_OF_STREAM);
    }
  }

  public static Http2DataFrameUnwrapHandler get() {
    return HANDLER;
  }
}
