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
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpUtil;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.LastMessage;
import org.apache.hadoop.hdfs.server.datanode.web.ExceptionHandler;

/**
 *
 */
@InterfaceAudience.Private
public class Http2ExceptionHandler {

  public static void
      exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
          throws Exception {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(cause);
    Http2Headers headers = HttpUtil.toHttp2Headers(resp);
    if (resp.content().isReadable()) {
      ctx.write(headers);
      ctx.writeAndFlush(new LastMessage(resp.content()));
    } else {
      ctx.writeAndFlush(new LastMessage(headers));
    }
  }
}
