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

import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A transport message with an OutputStream. Transport handler will feed the
 * OutputStream with data received from client and close the OutputStream at the
 * end.
 */
@InterfaceAudience.Private
public class OutputStreamTransportMessage extends
    AbstractDataNodeTransportMessage {

  private final OutputStream out;

  public OutputStreamTransportMessage(HttpResponseStatus status,
      Map<String, Object> headers, OutputStream out) {
    super(status, headers);
    this.out = out;
  }

  public OutputStream getOut() {
    return out;
  }

  @Override
  void accept(ChannelHandlerContext ctx,
      DataNodeTransportMessageVisitor visitor) {
    visitor.visit(ctx, this);
  }

}
