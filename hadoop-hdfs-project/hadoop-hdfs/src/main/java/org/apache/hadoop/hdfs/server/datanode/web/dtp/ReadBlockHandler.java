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
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

@InterfaceAudience.Private
class ReadBlockHandler extends
    SimpleChannelInboundHandler<OpReadBlockRequestProto> {

  private final DataNode datanode;

  private final ExecutorService executor;

  public ReadBlockHandler(DataNode datanode, ExecutorService executor) {
    this.datanode = datanode;
    this.executor = executor;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    Http2ExceptionHandler.exceptionCaught(ctx, cause);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final OpReadBlockRequestProto msg) {
    executor.execute(new ReadBlockTask(datanode, ctx, msg));
  }
}
