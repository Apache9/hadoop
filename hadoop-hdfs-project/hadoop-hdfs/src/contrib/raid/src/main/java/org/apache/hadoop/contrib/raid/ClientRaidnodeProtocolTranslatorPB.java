/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.contrib.raid;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.GetPolicyInfosRequestProto;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.GetPolicyInfosResponseProto;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.PolicyInfoProto;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.PolicyInfosProto;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ClientRaidnodeProtocolTranslatorPB implements ProtocolMetaInterface,
    ClientRaidnodeProtocol, ProtocolTranslator, Closeable {

  private final static RpcController NULL_CONTROLLER = null;
  private final ClientRaidnodeProtocolPB rpcProxy;

  public ClientRaidnodeProtocolTranslatorPB(InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf) throws IOException {
    rpcProxy = createClientRaidnodeProtocolProxy(addr, ticket, conf, 0);
  }

  static ClientRaidnodeProtocolPB createClientRaidnodeProtocolProxy(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf, int socketTimeout) throws IOException {
    RPC.setProtocolEngine(conf, ClientRaidnodeProtocolPB.class, ProtobufRpcEngine.class);
    return RPC.getProxy(ClientRaidnodeProtocolPB.class,
      RPC.getProtocolVersion(ClientRaidnodeProtocolPB.class), addr, ticket, conf,
      NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  public static ClientRaidnodeProtocol createClientRaidnodeProtocolProxy(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf) throws IOException {
    return new ClientRaidnodeProtocolTranslatorPB(addr, ticket, conf);
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  private Policy convertFromProto(PolicyInfosProto infos) {
    List<PolicyInfoProto> pis;
    if (infos != null) {
      pis = infos.getPolicyinfosList();
    } else {
      pis = new LinkedList<PolicyInfoProto>();
    }
    Policy policy = new Policy(null);
    for (PolicyInfoProto pi : pis) {
      policy.addNewPolicy(pi.getPath(), pi.getInterval());
    }
    policy.setCookie(infos.getCookie());
    return policy;
  }

  @Override
  public Policy getPolicyInfos(String cookie) throws IOException {
    GetPolicyInfosRequestProto req = GetPolicyInfosRequestProto.newBuilder()
        .setCookie((cookie == null) ? ("") : cookie).build();
    try {
      GetPolicyInfosResponseProto res = null;
      res = rpcProxy.getPolicyInfos(NULL_CONTROLLER, req);
      return convertFromProto(res.getPolicies());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, ClientRaidnodeProtocolPB.class,
      RPC.RpcKind.RPC_PROTOCOL_BUFFER, RPC.getProtocolVersion(ClientRaidnodeProtocolPB.class),
      methodName);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}
