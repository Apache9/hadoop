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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.RaidDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.GetRaidReplicaStateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.GetRaidReplicaStateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.DeleteReplicaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RaidDatanodeProtocolProtos.DeleteReplicaResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.VersionInfo;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
public class RaidDatanodeProtocolServerSideTranslatorPB implements
    RaidDatanodeProtocolPB {
  
  private final static DeleteReplicaResponseProto DELETE_REPLICA_RESP =
      DeleteReplicaResponseProto.newBuilder().build();

  private final RaidDatanodeProtocol impl;

  public RaidDatanodeProtocolServerSideTranslatorPB(
      RaidDatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetRaidReplicaStateResponseProto getRaidReplicaState(
      RpcController unused, GetRaidReplicaStateRequestProto request)
      throws ServiceException {
    String state;
    try {
      state = impl.getRaidReplicaState(PBHelper.convert(request.getBlock()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetRaidReplicaStateResponseProto.newBuilder().setState(state)
        .build();
  }


  @Override
  public DeleteReplicaResponseProto deleteReplica (RpcController unused,
    DeleteReplicaRequestProto request) throws ServiceException {
    try {
      impl.deleteReplica(PBHelper.convert(request.getBlock()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return DELETE_REPLICA_RESP;
  }
}
