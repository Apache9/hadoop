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

import static org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUtil.OP_READ_BLOCK;
import static org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUtil.URL_PREFIX;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.stream.ChunkedWriteHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

@InterfaceAudience.Private
public class DtpUrlDispatcher
    extends SimpleChannelInboundHandler<Http2HeadersFrame> {

  static final Log LOG = LogFactory.getLog(DtpUrlDispatcher.class);

  private final DataNode datanode;

  public DtpUrlDispatcher(DataNode datanode) {
    this.datanode = datanode;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx,
      Http2HeadersFrame headers) throws Exception {
    CharSequence method = headers.headers().method();
    if (method == null || HttpMethod.POST.asciiName().compareTo(method) != 0) {
      throw new IllegalArgumentException(
          "Request method " + method + " is not supported");
    }
    CharSequence path = headers.headers().path();
    if (path == null) {
      throw new IllegalArgumentException("No mapping found for uri " + path);
    }
    String pathStr = path.toString();
    if (!pathStr.startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("No mapping found for uri " + path);
    }

    if (pathStr.endsWith(OP_READ_BLOCK)) {
      ctx.pipeline().remove(this).addLast(new ChunkedWriteHandler(),
          new SimpleChannelInboundHandler<Http2DataFrame>() {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx,
                Http2DataFrame msg) throws Exception {
              ctx.fireChannelRead(msg.content().retain());
            }
          }, new ProtobufVarint32FrameDecoder(),
          new ProtobufDecoder(OpReadBlockRequestProto.getDefaultInstance()),
          new ReadBlockHandler(datanode));
    } else {
      throw new IllegalArgumentException("No mapping found for uri " + path);
    }
  }

}
