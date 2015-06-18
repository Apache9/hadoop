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

import static org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpStreamHandlerInitializer.OP_READ_BLOCK;
import static org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpStreamHandlerInitializer.URL_PREFIX;
import static org.apache.hadoop.hdfs.server.datanode.web.dtp.HandlerNames.*;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.ProtobufVarint32Decoder;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.ProtobufVarint32Encoder;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.SimpleStreamInboundHandler;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamHandlerContext;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamPipeline;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.web.ExceptionHandler;

/**
 *
 */
@InterfaceAudience.Private
class DtpUrlDispatcher extends SimpleStreamInboundHandler<HttpRequest> {

  private final DataNode datanode;

  public DtpUrlDispatcher(DataNode datanode) {
    this.datanode = datanode;
  }

  @Override
  protected void streamRead0(StreamHandlerContext ctx, HttpRequest req,
      boolean endOfStream) throws Exception {
    if (req.method() != HttpMethod.POST) {
      throw new IllegalArgumentException("Only accept " + HttpMethod.POST
          + " requests");
    }
    if (!req.uri().startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("No mapping found for uri "
          + req.uri());
    }
    if (req.uri().endsWith(OP_READ_BLOCK)) {
      StreamPipeline pipeline = ctx.stream().pipeline();
      pipeline.remove(this);
      pipeline
          .addLast(RESPONSE_PROTO_ENCODER_HANDLER_NAME,
            new ProtobufVarint32Encoder())
          .addLast(REQUEST_PROTO_DECODER_HANDLER_NAME,
            new ProtobufVarint32Decoder(OpReadBlockRequestProto.PARSER))
          .addLast(READ_BLOCK_HANDLER_NAME, new ReadBlockHandler(datanode));
    } else {
      throw new IllegalArgumentException("No mapping found for uri "
          + req.uri());
    }
  }

  @Override
  public void exceptionCaught(StreamHandlerContext ctx, Throwable cause) {
    ctx.writeAndFlush(ExceptionHandler.exceptionCaught(cause), true);
  }

}
