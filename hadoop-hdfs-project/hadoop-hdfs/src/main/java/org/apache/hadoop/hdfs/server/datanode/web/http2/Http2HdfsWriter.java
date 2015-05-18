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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
import org.apache.hadoop.io.IOUtils;

@InterfaceAudience.Private
public class Http2HdfsWriter implements Http2StreamDataFrameReadHandler {

  private static final Log LOG = DatanodeHttpServer.LOG;

  private final int streamId;

  private final DFSClient client;

  private final OutputStream out;

  private final Http2Headers responseHeaders;

  public Http2HdfsWriter(int streamId, DFSClient client, OutputStream out,
      Http2Headers responseHeaders) {
    this.streamId = streamId;
    this.client = client;
    this.out = out;
    this.responseHeaders = responseHeaders;
  }

  @Override
  public int onDataRead(ChannelHandlerContext ctx, ByteBuf data, int padding,
      boolean endOfStream) throws Http2Exception {
    int processed = data.readableBytes() + padding;
    try {
      data.readBytes(out, data.readableBytes());
    } catch (Exception e) {
      throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR, e,
        "");
    }
    if (endOfStream) {
      try {
        out.close();
      } catch (Exception e) {
        throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR,
          e, "");
      } finally {
        IOUtils.cleanup(LOG, client);
      }
      ctx.writeAndFlush(new Http2HeadersFrame(processed, responseHeaders, 0,
          true));
    }
    return processed;
  }

  @Override
  public void onRstStreamRead(ChannelHandlerContext ctx, long errorCode) {
    IOUtils.cleanup(LOG, out, client);
  }

}
