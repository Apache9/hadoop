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

import io.netty.buffer.ByteBuf;

/**
 * Represent an HTTP/2 data frame.
 */
@InterfaceAudience.Private
public class Http2DataFrame {

  private final int streamId;

  private final ByteBuf data;

  private final int padding;

  private final boolean endOfStream;

  public Http2DataFrame(int streamId, ByteBuf data, int padding,
      boolean endOfStream) {
    this.streamId = streamId;
    this.data = data;
    this.padding = padding;
    this.endOfStream = endOfStream;
  }

  public int getStreamId() {
    return streamId;
  }

  public ByteBuf getData() {
    return data;
  }

  public int getPadding() {
    return padding;
  }

  public boolean isEndOfStream() {
    return endOfStream;
  }

  @Override
  public String toString() {
    return "Http2DataFrame [streamId=" + streamId + ", data=" + data
        + ", padding=" + padding + ", endOfStream=" + endOfStream + "]";
  }

}
