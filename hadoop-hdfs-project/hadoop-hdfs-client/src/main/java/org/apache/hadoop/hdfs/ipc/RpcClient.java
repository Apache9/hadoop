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
package org.apache.hadoop.hdfs.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The protobuf based rpc client.
 */
@InterfaceAudience.Private
public class RpcClient implements Closeable {

  final byte[] clientId;

  final EventLoopGroup group = new NioEventLoopGroup();

  final Class<? extends Channel> channelClass = NioSocketChannel.class;

  private final AtomicInteger callIdCnt = new AtomicInteger(0);

  private final ConcurrentMap<ConnectionId, RpcConnection> connections =
      new ConcurrentHashMap<>();

  public RpcClient() {
    this.clientId = ClientId.getClientId();
  }

  private int nextCallId() {
    int id, next;
    do {
      id = callIdCnt.get();
      next = id < Integer.MAX_VALUE ? id + 1 : 0;
    } while (!callIdCnt.compareAndSet(id, next));
    return id;
  }

  private void onCallFinished(Call call, HdfsRpcController hrc,
      InetSocketAddress addr, RpcCallback<Message> callback) {
    if (call.error != null) {
      if (call.error instanceof RemoteException) {
        call.error.fillInStackTrace();
        hrc.setException(call.error);
      } else {
        hrc.setException(call.error);
      }
      callback.run(null);
    } else {
      callback.run(call.response);
    }
  }

  private void callMethod(String protocolName, long protocolVersion,
      Descriptors.MethodDescriptor md, HdfsRpcController hrc, Message param,
      Message returnType, UserGroupInformation ugi, InetSocketAddress addr,
      RpcCallback<Message> callback) {
    Call call =
        new Call(nextCallId(), protocolName, protocolVersion, md.getName(),
            param, returnType, c -> onCallFinished(c, hrc, addr, callback));
    ConnectionId remoteId = new ConnectionId(ugi, protocolName, addr);
    connections
        .computeIfAbsent(remoteId,
            k -> new RpcConnection(this, k, AuthProtocol.NONE))
        .sendRequest(call);
  }

  public RpcChannel createRpcChannel(Class<?> protocol, InetSocketAddress addr,
      UserGroupInformation ugi) {
    String protocolName = RPC.getProtocolName(protocol);
    long protocolVersion = RPC.getProtocolVersion(protocol);
    return (method, controller, request, responsePrototype, done) -> callMethod(
        protocolName, protocolVersion, method, (HdfsRpcController) controller,
        request, responsePrototype, ugi, addr, done);
  }

  @Override
  public void close() throws IOException {
    connections.values().forEach(c -> c.shutdown());
    connections.clear();
  }
}
