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
import java.util.List;
import java.util.Map;

import javax.net.SocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FederationClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RaidDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.FederationClientDatanodeProtocolProtos.AddBlocksToNewPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.FederationClientDatanodeProtocolProtos.AddBlocksToNewPoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
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
 * {@link FederationClientDatanodeProtocol} interfaces to the RPC server implementing
 * {@link FederationClientDatanodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class FederationClientDatanodeProtocolTranslatorPB implements
    ProtocolMetaInterface, FederationClientDatanodeProtocol,
    ProtocolTranslator, Closeable {
  public static final Log LOG = LogFactory
      .getLog(FederationClientDatanodeProtocolTranslatorPB.class);
  
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final FederationClientDatanodeProtocolPB rpcProxy;

  public FederationClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, boolean connectToDnViaHostname,
      LocatedBlock locatedBlock) throws IOException {
    rpcProxy = createFederationClientDatanodeProtocolProxy(datanodeid, conf, 
                  socketTimeout, connectToDnViaHostname, locatedBlock);
  }
  
  public FederationClientDatanodeProtocolTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf, SocketFactory factory)
      throws IOException {
    rpcProxy = createFederationClientDatanodeProtocolProxy(addr, ticket, conf, factory, 0);
  }
  
  /**
   * Constructor.
   * @param datanodeid Datanode to connect to.
   * @param conf Configuration.
   * @param socketTimeout Socket timeout to use.
   * @param connectToDnViaHostname connect to the Datanode using its hostname
   * @throws IOException
   */
  public FederationClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, boolean connectToDnViaHostname)
      throws IOException {
    final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
    InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr + " addr=" + addr);
    }
    rpcProxy = createFederationClientDatanodeProtocolProxy(addr,
        UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  static FederationClientDatanodeProtocolPB createFederationClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname, LocatedBlock locatedBlock) throws IOException {
    final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
    InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr + " addr=" + addr);
    }
    
    // Since we're creating a new UserGroupInformation here, we know that no
    // future RPC proxies will be able to re-use the same connection. And
    // usages of this proxy tend to be one-off calls.
    //
    // This is a temporary fix: callers should really achieve this by using
    // RPC.stopProxy() on the resulting object, but this is currently not
    // working in trunk. See the discussion on HDFS-1965.
    Configuration confWithNoIpcIdle = new Configuration(conf);
    confWithNoIpcIdle.setInt(CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 0);

    UserGroupInformation ticket = UserGroupInformation
        .createRemoteUser(locatedBlock.getBlock().getLocalBlock().toString());
    ticket.addToken(locatedBlock.getBlockToken());
    return createFederationClientDatanodeProtocolProxy(addr, ticket, confWithNoIpcIdle,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }
  
  static FederationClientDatanodeProtocolPB createFederationClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int socketTimeout) throws IOException {
    RPC.setProtocolEngine(conf, FederationClientDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProxy(FederationClientDatanodeProtocolPB.class,
        RPC.getProtocolVersion(FederationClientDatanodeProtocolPB.class), addr, ticket,
        conf, factory, socketTimeout);
  }

  public static FederationClientDatanodeProtocol createFederationClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf)
      throws IOException {
    return new FederationClientDatanodeProtocolTranslatorPB(addr, ticket, conf,
        NetUtils.getDefaultSocketFactory(conf));
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Block[] addBlocksToNewPool(String srcPool, String dstPool, Block[] blks) throws IOException {
    AddBlocksToNewPoolRequestProto.Builder builder = AddBlocksToNewPoolRequestProto 
        .newBuilder().setSrcPool(srcPool).setDstPool(dstPool);
    AddBlocksToNewPoolResponseProto res;
    for (int i = 0; i < blks.length; i++) {
      builder.addBlocks(PBHelper.convert(blks[i]));
    }
    try {
      res = rpcProxy.addBlocksToNewPool(NULL_CONTROLLER, builder.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    List<BlockProto> lblks = res.getBlocksList();
    Block[] resblks = new Block[lblks.size()];
    for (int i = 0; i < lblks.size(); i++) {
      resblks[i] = PBHelper.convert(lblks.get(i));
    }
    return resblks;
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        FederationClientDatanodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(FederationClientDatanodeProtocolPB.class), methodName);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

}
