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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.FederationClientProtocol;
import org.apache.hadoop.hdfs.protocol.proto.FederationClientNamenodeProtocolProtos.*;
import org.apache.hadoop.hdfs.protocol.proto.FederationProtos.*;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.security.AccessControlException;

import com.xiaomi.infra.hadoop.HdfsPerfCounter;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

/**
 * This class forwards NN's FederationClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in FederationClientProtocol to the
 * new PB types.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class FederationClientNamenodeProtocolTranslatorPB implements
    ProtocolMetaInterface, FederationClientProtocol, Closeable, ProtocolTranslator {
  final private FederationClientNamenodeProtocolPB rpcProxy;

  public FederationClientNamenodeProtocolTranslatorPB(FederationClientNamenodeProtocolPB proxy) {
    rpcProxy = proxy;
  }
  
  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public boolean rename(String src, String dst, String dstId) throws UnresolvedLinkException,
      IOException {
    FederationRenameRequestProto req = FederationRenameRequestProto.newBuilder()
        .setSrc(src)
        .setDst(dst).build();
    long startTime = System.currentTimeMillis();
    try {
      return rpcProxy.rename(null, req).getResult();
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("rename", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter
          .count("rename", 1, System.currentTimeMillis() - startTime);
    }
  }
  

  @Override
  public void rename2(String src, String dst, String dstId, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    boolean overwrite = false;
    if (options != null) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    FederationRename2RequestProto req = FederationRename2RequestProto.newBuilder().
        setSrc(src).
        setDst(dst).setOverwriteDest(overwrite).
        build();
    long startTime = System.currentTimeMillis();
    try {
      rpcProxy.rename2(null, req);
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("rename2", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter
          .count("rename2", 1, System.currentTimeMillis() - startTime);
    }
  }

  @Override
  public DirectorySubTree renameSrcPhase1(String src, String srcId, String dst, String dstId) 
      throws IOException {
    FederationRenameSrcPhase1RequestProto req =
        FederationRenameSrcPhase1RequestProto.newBuilder().setSrc(src)
        .setSrcId(srcId).setDst(dst).setDstId(dstId).build();
    FederationRenameSrcPhase1ResponseProto res;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.renameSrcPhase1(null, req);
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("renameSrcPhase1", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter
          .count("renameSrcPhase1", 1, System.currentTimeMillis() - startTime);
    }
    return PBHelper.convert(res.getSubTree());
  }

  @Override
  public boolean renameSrcPhase2(long renameId, boolean toCancel) throws IOException {
    FederationRenameSrcPhase2RequestProto req =
        FederationRenameSrcPhase2RequestProto.newBuilder()
        .setRenameId(renameId).setToCancel(toCancel).build();
    boolean res;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.renameSrcPhase2(null, req).getSrcPhase2Res();
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("renameSrcPhase1", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter.count("renameSrcPhase1", 1, System.currentTimeMillis()
          - startTime);
    }
    return res;
  }

  @Override
  public BlocksToDup renameDestPhase1(String src, String srcId, String dst, String dstId,
      DirectorySubTree subTree) throws IOException {
    FederationRenameDestPhase1RequestProto req =
        FederationRenameDestPhase1RequestProto.newBuilder().setSrc(src)
        .setSrcId(srcId).setDst(dst).setDstId(dstId)
        .setSubTree(PBHelper.convert(subTree)).build();
    BlocksToDupProto res = null;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.renameDestPhase1(null, req).getDestPhase1Res();
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("renameSrcPhase1", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter
          .count("renameSrcPhase1", 1, System.currentTimeMillis() - startTime);
    }
    return PBHelper.convert(res);
  }

  @Override
  public boolean renameDestPhase2(long renameId, String srcId)
      throws IOException {
    FederationRenameDestPhase2RequestProto req =
        FederationRenameDestPhase2RequestProto.newBuilder()
        .setRenameId(renameId).setSrcId(srcId).build();
    boolean res;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.renameDestPhase2(null, req).getDestPhase2Res();
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("renameSrcPhase1", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter.count("renameSrcPhase1", 1, System.currentTimeMillis()
          - startTime);
    }
    return res;
  }

  @Override
  public boolean renameRecordExist(long renameId, String srcId, String dstId, boolean isSource)
      throws IOException {
    FederationRenameRecordExistRequestProto req =
        FederationRenameRecordExistRequestProto.newBuilder()
        .setRenameId(renameId).setSrcId(srcId).setDstId(dstId)
        .setIsSource(isSource).build();
    boolean res;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.renameRecordExist(null, req).getExist();
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("renameRecordExist", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter.count("renameRecordExist", 1, System.currentTimeMillis()
          - startTime);
    }
    return res;
  }

  @Override
  public String getPoolId() throws IOException {
    GetPoolIdRequestProto req = GetPoolIdRequestProto.newBuilder().build();
    String res = null;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.getPoolId(null, req).getPoolId();
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("getPoolId", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter.count("getPoolId", 1, System.currentTimeMillis()
          - startTime);
    }
    return res;
  }

  @Override
  public DirectorySubTree getRenameDestSubTree(String dst) throws IOException {
    GetRenameDestSubTreeRequestProto req = GetRenameDestSubTreeRequestProto.newBuilder()
        .setDst(dst).build();
    GetRenameDestSubTreeResponseProto res = null;
    long startTime = System.currentTimeMillis();
    try {
      res = rpcProxy.getRenameDestSubTree(null, req);
    } catch (ServiceException e) {
      HdfsPerfCounter.countFail("getRenameDestSubTree", 1);
      throw ProtobufHelper.getRemoteException(e);
    } finally {
      HdfsPerfCounter.count("getRenameDestSubTree", 1, System.currentTimeMillis()
          - startTime);
    }
    return PBHelper.convert(res.getSubTree());
  }


  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        FederationClientNamenodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(FederationClientNamenodeProtocolPB.class), methodName);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}
