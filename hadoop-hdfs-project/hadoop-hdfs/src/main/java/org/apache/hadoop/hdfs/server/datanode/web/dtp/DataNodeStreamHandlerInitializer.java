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

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hdfs.protocol.datatransfer.http2.EmbeddedStream;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamHandlerContext;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamHandlerInitializer;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamInboundHandlerAdaptor;

/**
 *
 */
public class DataNodeStreamHandlerInitializer implements
    StreamHandlerInitializer {

  @Override
  public void initialize(EmbeddedStream stream) {
    stream.pipeline().addHandlerFirst(new StreamInboundHandlerAdaptor() {

      @Override
      public void streamRead(StreamHandlerContext ctx, Object msg,
          boolean endOfStream) {
        ctx.writeAndFlush(
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
              HttpResponseStatus.OK, ctx.alloc().buffer()
                  .writeBytes("HTTP/2 DTP".getBytes(StandardCharsets.UTF_8))),
          true);
      }
    });
  }

}
