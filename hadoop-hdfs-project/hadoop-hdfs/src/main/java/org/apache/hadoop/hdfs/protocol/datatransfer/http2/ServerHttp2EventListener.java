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
package org.apache.hadoop.hdfs.protocol.datatransfer.http2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 *
 */
@InterfaceAudience.Private
public class ServerHttp2EventListener extends AbstractHttp2EventListener {

  private final ChannelInitializer<Http2StreamChannel> subChannelInitializer;

  public ServerHttp2EventListener(Channel parentChannel, Http2Connection conn,
      ChannelInitializer<Http2StreamChannel> subChannelInitializer) {
    super(parentChannel, conn);
    this.subChannelInitializer = subChannelInitializer;
  }

  @Override
  protected void initChannelOnStreamActive(final Http2StreamChannel subChannel) {
    subChannel.pipeline().addFirst(subChannelInitializer);
    parentChannel.eventLoop().register(subChannel)
        .addListener(new FutureListener<Void>() {

          @Override
          public void operationComplete(Future<Void> future) throws Exception {
            if (!future.isSuccess()) {
              subChannel.stream().removeProperty(subChannelPropKey);
            }
          }

        });
  }

}
