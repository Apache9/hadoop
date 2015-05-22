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
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A transport message with an InputStream. Transport handler will read from the
 * InputStream and write data back to client.
 */
@InterfaceAudience.Private
public class InputStreamTransportMessage extends
    AbstractDataNodeTransportMessage {

  private final InputStream in;

  public InputStreamTransportMessage(HttpResponseStatus status,
      Map<String, Object> headers, InputStream in) {
    super(status, headers);
    this.in = in;
  }

  public InputStream getIn() {
    return in;
  }

  @Override
  void accept(ChannelHandlerContext ctx,
      DataNodeTransportMessageVisitor visitor) {
    visitor.visit(ctx, this);
  }

}
