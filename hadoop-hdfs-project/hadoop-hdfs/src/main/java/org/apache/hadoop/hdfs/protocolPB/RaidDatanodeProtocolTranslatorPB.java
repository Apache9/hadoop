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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.protocol.RaidDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.GetRaidReplicaStateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.GetRaidReplicaStateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.DeleteReplicaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.DeleteReplicaResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link RaidDatanodeProtocol} interfaces to the RPC server implementing
 * {@link RaidDatanodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RaidDatanodeProtocolTranslatorPB implements
    ProtocolMetaInterface, RaidDatanodeProtocol,
    ProtocolTranslator, Closeable {
  public static final Log LOG = LogFactory
      .getLog(RaidDatanodeProtocolTranslatorPB.class);
  
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final RaidDatanodeProtocolPB rpcProxy;

  public RaidDatanodeProtocolTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf, SocketFactory factory)
      throws IOException {
    rpcProxy = createRaidDatanodeProtocolProxy(addr, ticket, conf, factory, 0);
  }
  
  
  static RaidDatanodeProtocolPB createRaidDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int socketTimeout) throws IOException {
    RPC.setProtocolEngine(conf, RaidDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProxy(RaidDatanodeProtocolPB.class,
        RPC.getProtocolVersion(RaidDatanodeProtocolPB.class), addr, ticket,
        conf, factory, socketTimeout);
  }

  public static RaidDatanodeProtocol createRaidDatanodeProtocolProxy(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf) throws IOException {
    return new RaidDatanodeProtocolTranslatorPB(addr, ticket, conf, NetUtils.getDefaultSocketFactory(conf));
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public String getRaidReplicaState(ExtendedBlock b) throws IOException {
    GetRaidReplicaStateRequestProto req = GetRaidReplicaStateRequestProto
        .newBuilder().setBlock(PBHelper.convert(b)).build();
    try {
      return rpcProxy.getRaidReplicaState(NULL_CONTROLLER, req).getState();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteReplica(ExtendedBlock b) throws IOException {
    DeleteReplicaRequestProto req = DeleteReplicaRequestProto
        .newBuilder().setBlock(PBHelper.convert(b)).build();

    try {
      rpcProxy.deleteReplica(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, RaidDatanodeProtocolPB.class,
      RPC.RpcKind.RPC_PROTOCOL_BUFFER, RPC.getProtocolVersion(RaidDatanodeProtocolPB.class),
      methodName);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}
