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
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeEvent;
import io.netty.handler.codec.http2.Http2ConnectionHandler;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.URLDispatcher;

@InterfaceAudience.Private
public class DataNodeHttp2Handler extends Http2ConnectionHandler {

  public DataNodeHttp2Handler(Configuration conf, Configuration confForCreate,
      ExecutorService executor) {
    super(true, new DataNodeHttp2FrameListener(conf, confForCreate, executor));
    ((DataNodeHttp2FrameListener) decoder().listener()).encoder(encoder());
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
      throws Exception {
    if (evt instanceof UpgradeEvent) {
      ctx.pipeline().remove(URLDispatcher.class);
      ctx.pipeline().addLast(new Http2FrameWriteHandler(encoder()));
    }
    super.userEventTriggered(ctx, evt);
  }

}
