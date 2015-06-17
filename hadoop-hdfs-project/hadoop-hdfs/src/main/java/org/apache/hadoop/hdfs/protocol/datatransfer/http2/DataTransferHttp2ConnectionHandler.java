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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2ConnectionHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * HTTP/2 connection handler. Now is only for server use.
 */
@InterfaceAudience.Private
public class DataTransferHttp2ConnectionHandler extends Http2ConnectionHandler {

  static final Log LOG = LogFactory
      .getLog(DataTransferHttp2ConnectionHandler.class);

  public DataTransferHttp2ConnectionHandler(Channel channel,
      StreamHandlerInitializer initializer) {
    super(true, new DataTransferHttp2EventListener(channel, initializer));
    ((DataTransferHttp2EventListener) decoder().listener())
        .connection(connection());
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof Http2DataFrame) {
      Http2DataFrame frame = (Http2DataFrame) msg;
      encoder().writeData(ctx, frame.getStreamId(), frame.getData(), 0,
        frame.isEndOfStream(), promise);
    } else if (msg instanceof Http2HeaderFrame) {
      Http2HeaderFrame frame = (Http2HeaderFrame) msg;
      encoder().writeHeaders(ctx, frame.getStreamId(), frame.getHeaders(), 0,
        frame.isEndOfStream(), promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    onException(ctx, cause);
  }
}
