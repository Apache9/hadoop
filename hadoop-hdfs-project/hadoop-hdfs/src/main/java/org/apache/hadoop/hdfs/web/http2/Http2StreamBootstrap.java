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
package org.apache.hadoop.hdfs.web.http2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

/**
 *
 */
@InterfaceAudience.Private
public class Http2StreamBootstrap {
  private Channel channel;

  private Http2Headers headers;

  private boolean endStream;

  private ChannelHandler handler;

  public Http2StreamBootstrap channel(Channel channel) {
    this.channel = channel;
    return this;
  }

  public Http2StreamBootstrap headers(Http2Headers headers) {
    this.headers = headers;
    return this;
  }

  public Http2StreamBootstrap endStream(boolean endStream) {
    this.endStream = endStream;
    return this;
  }

  public Http2StreamBootstrap handler(ChannelHandler handler) {
    this.handler = handler;
    return this;
  }

  public Promise<Http2StreamChannel> connect() {
    Preconditions.checkNotNull(headers);
    Preconditions.checkNotNull(handler);
    final Promise<Http2StreamChannel> registeredPromise =
        channel.eventLoop().<Http2StreamChannel> newPromise();

    Http2HeadersAndPromise headersAndPromise =
        new Http2HeadersAndPromise(headers, endStream, channel.eventLoop()
            .<Http2StreamChannel> newPromise()
            .addListener(new FutureListener<Http2StreamChannel>() {

              @Override
              public void operationComplete(Future<Http2StreamChannel> future)
                  throws Exception {
                if (future.isSuccess()) {
                  final Http2StreamChannel subChannel = future.get();
                  subChannel.pipeline().addFirst(handler);
                  channel.eventLoop().register(subChannel)
                      .addListener(new ChannelFutureListener() {

                        @Override
                        public void operationComplete(ChannelFuture future)
                            throws Exception {
                          if (future.isSuccess()) {
                            subChannel.config().setAutoRead(true);
                            registeredPromise.setSuccess(subChannel);
                          } else {
                            registeredPromise.setFailure(future.cause());
                          }
                        }
                      });
                } else {
                  registeredPromise.setFailure(future.cause());
                }
              }

            }));
    channel.writeAndFlush(headersAndPromise);
    return registeredPromise;
  }
}
