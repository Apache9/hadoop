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
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.Http2Settings;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Http2SettingsHandler extends
    SimpleChannelInboundHandler<Http2Settings> {

  private ChannelPromise promise;

  /**
   * Create new instance
   * @param promise Promise object used to notify when first settings are
   *          received
   */
  public Http2SettingsHandler(ChannelPromise promise) {
    this.promise = promise;
  }

  /**
   * Wait for this handler to be added after the upgrade to HTTP/2, and for
   * initial preface handshake to complete.
   * @param timeout Time to wait
   * @param unit {@link java.util.concurrent.TimeUnit} for {@code timeout}
   * @throws TimeoutException if timeout or other failure occurs
   */
  public void awaitSettings(long timeout, TimeUnit unit)
      throws TimeoutException {
    if (!promise.awaitUninterruptibly(timeout, unit)) {
      throw new TimeoutException("Timed out waiting for settings");
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2Settings msg)
      throws Exception {
    promise.setSuccess();

    // Only care about the first settings message
    ctx.pipeline().remove(this);
  }
}
