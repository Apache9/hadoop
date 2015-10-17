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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ByteString;

import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.web.http2.ProtobufVarint32Encoder;

@InterfaceAudience.Private
public class DtpUrlDispatcher extends SimpleChannelInboundHandler<Http2Headers> {

  static final Log LOG = LogFactory.getLog(DtpUrlDispatcher.class);

  public static final int VERSION = 1;

  public static final String URL_PREFIX = "/dtp/v" + VERSION;

  public static final String OP_READ_BLOCK = "/read_block";

  private final DataNode datanode;

  private final ExecutorService executor;

  public DtpUrlDispatcher(DataNode datanode, ExecutorService executor) {
    this.datanode = datanode;
    this.executor = executor;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2Headers headers)
      throws Exception {
    ByteString method = headers.method();
    if (method == null || !method.toString().equals(HttpMethod.POST.name())) {
      throw new IllegalArgumentException("Request method " + method
          + " is not supported");
    }
    ByteString path = headers.path();
    if (path == null || !path.toString().startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("No mapping found for uri " + path);
    }
    String uri = path.toString();
    if (uri.endsWith(OP_READ_BLOCK)) {
      ChannelPipeline pipeline = ctx.pipeline();
      pipeline.remove(this).addLast(new ChunkedWriteHandler(),
        new ProtobufVarint32Encoder(), new ProtobufVarint32FrameDecoder(),
        new ProtobufDecoder(OpReadBlockRequestProto.getDefaultInstance()),
        new ReadBlockHandler(datanode, executor));
    } else {
      throw new IllegalArgumentException("No mapping found for uri " + uri);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    Http2ExceptionHandler.exceptionCaught(ctx, cause);
  }
}
