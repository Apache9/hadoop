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

import org.apache.hadoop.classification.InterfaceAudience;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;

/**
 * Reset HTTP/2 stream if operation failed.
 */
@InterfaceAudience.Private
public class ResetOnFailureListener implements ChannelFutureListener {

  private final int streamId;

  public ResetOnFailureListener(int streamId) {
    this.streamId = streamId;
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception {
    if (!future.isSuccess()) {
      future
          .channel()
          .pipeline()
          .fireExceptionCaught(
            Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR,
              future.cause(), ""));
    }
  }

}
