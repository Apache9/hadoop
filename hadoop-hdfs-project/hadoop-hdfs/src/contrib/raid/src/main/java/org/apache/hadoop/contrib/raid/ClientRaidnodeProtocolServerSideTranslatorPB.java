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

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.GetPolicyInfosRequestProto;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.GetPolicyInfosResponseProto;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.PolicyInfosProto;
import org.apache.hadoop.contrib.raid.ClientRaidnodeProtocolProtos.PolicyInfoProto;
import org.apache.hadoop.contrib.raid.Policy.PolicyEntry;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ClientRaidnodeProtocolServerSideTranslatorPB implements ClientRaidnodeProtocolPB {

  private final ClientRaidnodeProtocol impl;

  public ClientRaidnodeProtocolServerSideTranslatorPB(ClientRaidnodeProtocol impl) {
    this.impl = impl;
  }

  private PolicyInfosProto convertToProto(Policy policy) {
    if (policy == null) return null;
    List<PolicyInfoProto> pips = new LinkedList<PolicyInfoProto>();
    for (PolicyEntry pe : policy.getPolicyEntries()) {
      PolicyInfoProto pip = PolicyInfoProto.newBuilder().setPath(pe.getPathStr())
          .setInterval(pe.getInterval()).build();
      pips.add(pip);
    }
    String cookie = policy.getCookie();
    cookie = (cookie == null) ? ("") : cookie;
    return PolicyInfosProto.newBuilder().addAllPolicyinfos(pips).setCookie(cookie).build();
  }

  @Override
  public GetPolicyInfosResponseProto getPolicyInfos(RpcController unused,
      GetPolicyInfosRequestProto request) throws ServiceException {
    Policy policy = null;
    try {
      policy = impl.getPolicyInfos(request.getCookie());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetPolicyInfosResponseProto.newBuilder().setPolicies(convertToProto(policy)).build();
  }
}
