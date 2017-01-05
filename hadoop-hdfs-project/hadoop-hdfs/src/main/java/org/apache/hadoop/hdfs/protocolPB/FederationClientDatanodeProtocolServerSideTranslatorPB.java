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
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FederationClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.proto.FederationClientDatanodeProtocolProtos.AddBlocksToNewPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.FederationClientDatanodeProtocolProtos.AddBlocksToNewPoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link ClientDatanodeProtocolPB} to the
 * {@link ClientDatanodeProtocol} server implementation.
 */
@InterfaceAudience.Private
public class FederationClientDatanodeProtocolServerSideTranslatorPB implements
    FederationClientDatanodeProtocolPB {
  
  private final FederationClientDatanodeProtocol impl;

  public FederationClientDatanodeProtocolServerSideTranslatorPB(
      FederationClientDatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public AddBlocksToNewPoolResponseProto addBlocksToNewPool(
      RpcController unused, AddBlocksToNewPoolRequestProto request)
      throws ServiceException {
    Block[] res;
    try {
      List<BlockProto> blklist = request.getBlocksList();
      Block[] blks = new Block[blklist.size()];
      for (int i = 0; i < blks.length; i++) {
        blks[i] = PBHelper.convert(blklist.get(i));
      }
      res = impl.addBlocksToNewPool(request.getSrcPool(), request.getDstPool(),
        blks);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    AddBlocksToNewPoolResponseProto.Builder builder = AddBlocksToNewPoolResponseProto.newBuilder();
    for (int i = 0; i < res.length; i++) {
      builder.addBlocks(PBHelper.convert(res[i]));
    }
    return builder.build();
  }

}
