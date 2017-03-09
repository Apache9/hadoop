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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.protocol.FederationClientProtocol;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.protocol.proto.FederationClientNamenodeProtocolProtos.*;


import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link ClientNamenodeProtocolPB}.
 * This class translates the PB data types
 * to the native data types used inside the NN as specified in the generic
 * ClientProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class FederationClientNamenodeProtocolServerSideTranslatorPB implements
    FederationClientNamenodeProtocolPB {
  final private FederationClientProtocol server;

  /**
   * Constructor
   * 
   * @param server - the NN server
   * @throws IOException
   */
  public FederationClientNamenodeProtocolServerSideTranslatorPB(FederationClientProtocol server)
      throws IOException {
    this.server = server;
  }

  @Override
  public FederationRenameSrcPhase1ResponseProto renameSrcPhase1(
      RpcController controller, FederationRenameSrcPhase1RequestProto req)
      throws ServiceException {
    DirectorySubTree subTree;
    try {
      subTree =
          server.renameSrcPhase1(req.getSrc(), req.getSrcId(), req.getDst(), req.getDstId());
      return FederationRenameSrcPhase1ResponseProto.newBuilder()
          .setSubTree(PBHelper.convert(subTree)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FederationRenameSrcPhase2ResponseProto renameSrcPhase2(
      RpcController controller, FederationRenameSrcPhase2RequestProto req)
      throws ServiceException {
    boolean res;
    try {
      res = server.renameSrcPhase2(req.getRenameId(), req.getToCancel());
      return FederationRenameSrcPhase2ResponseProto.newBuilder()
          .setSrcPhase2Res(res).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FederationRenameDestPhase1ResponseProto renameDestPhase1(
      RpcController controller, FederationRenameDestPhase1RequestProto req)
      throws ServiceException {
    BlocksToDup res;
    try {
      res =
          server.renameDestPhase1(req.getSrc(), req.getSrcId(), req.getDst(), req.getDstId(),
              PBHelper.convert(req.getSubTree()));
      return FederationRenameDestPhase1ResponseProto.newBuilder()
          .setDestPhase1Res(PBHelper.convert(res)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FederationRenameDestPhase2ResponseProto renameDestPhase2(
      RpcController controller, FederationRenameDestPhase2RequestProto req)
      throws ServiceException {
    boolean res;
    try {
      res = server.renameDestPhase2(req.getRenameId(), req.getSrcId());
      return FederationRenameDestPhase2ResponseProto.newBuilder()
          .setDestPhase2Res(res).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FederationRenameRecordExistResponseProto renameRecordExist(
      RpcController controller, FederationRenameRecordExistRequestProto req)
      throws ServiceException {
    boolean res;
    try {
      res = server.renameRecordExist(req.getRenameId(), req.getSrcId(), req.getDstId(),
           req.getIsSource());
      return FederationRenameRecordExistResponseProto.newBuilder()
          .setExist(res).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetRenameDestSubTreeResponseProto getRenameDestSubTree(
      RpcController controller, GetRenameDestSubTreeRequestProto req)
      throws ServiceException {
    DirectorySubTree res;
    try {
      res = server.getRenameDestSubTree(req.getDst());
      return GetRenameDestSubTreeResponseProto.newBuilder()
          .setSubTree(PBHelper.convert(res)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetPoolIdResponseProto getPoolId(
      RpcController controller, GetPoolIdRequestProto req)
      throws ServiceException {
    String res;
    try {
      res = server.getPoolId();
      return GetPoolIdResponseProto.newBuilder().setPoolId(res).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

}
