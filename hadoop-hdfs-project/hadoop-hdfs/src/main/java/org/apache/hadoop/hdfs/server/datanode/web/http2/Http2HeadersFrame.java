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

import io.netty.handler.codec.http2.Http2Headers;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class Http2HeadersFrame {

  private final int streamId;

  private final Http2Headers headers;

  private final int padding;

  private final boolean endOfStream;

  public Http2HeadersFrame(int streamId, Http2Headers headers, int padding,
      boolean endOfStream) {
    this.streamId = streamId;
    this.headers = headers;
    this.padding = padding;
    this.endOfStream = endOfStream;
  }

  public int getStreamId() {
    return streamId;
  }

  public Http2Headers getHeaders() {
    return headers;
  }

  public int getPadding() {
    return padding;
  }

  public boolean isEndOfStream() {
    return endOfStream;
  }

}
