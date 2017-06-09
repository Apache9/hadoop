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
package org.apache.hadoop.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.ipc.HdfsRpcController;
import org.apache.hadoop.hdfs.ipc.RpcClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 */
@InterfaceAudience.Private
public class AsyncDFSClient implements Closeable {

  private final RpcClient rpcClient;

  private final ClientNamenodeProtocol.Interface stub;

  private final DfsClientConf conf;

  public AsyncDFSClient(Configuration conf, InetSocketAddress addr) throws IOException {
    this.conf = new DfsClientConf(conf);
    rpcClient = new RpcClient();
    stub = ClientNamenodeProtocol.newStub(rpcClient.createRpcChannel(ClientNamenodeProtocolPB.class,
      addr, UserGroupInformation.getCurrentUser()));
  }

  private FsPermission applyUMaskDir(FsPermission permission) {
    if (permission == null) {
      permission = FsPermission.getDirDefault();
    }
    return FsCreateModes.applyUMask(permission, conf.getUMask());
  }

  public CompletableFuture<Boolean> mkdirs(String src, FsPermission permission,
      boolean createParent) {
    FsPermission masked = applyUMaskDir(permission);
    HdfsRpcController controller = new HdfsRpcController();
    MkdirsRequestProto.Builder builder = MkdirsRequestProto.newBuilder().setSrc(src)
        .setMasked(PBHelperClient.convert(masked)).setCreateParent(createParent);
    FsPermission unmasked = masked.getUnmasked();
    if (unmasked != null) {
      builder.setUnmasked(PBHelperClient.convert(unmasked));
    }
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    stub.mkdirs(controller, builder.build(), resp -> {
      if (controller.failed()) {
        future.completeExceptionally(controller.getException());
      } else {
        future.complete(resp.getResult());
      }
    });
    return future;
  }

  public CompletableFuture<Optional<HdfsFileStatus>> getFileInfo(String src) {
    HdfsRpcController controller = new HdfsRpcController();
    CompletableFuture<Optional<HdfsFileStatus>> future = new CompletableFuture<>();
    stub.getFileInfo(controller, GetFileInfoRequestProto.newBuilder().setSrc(src).build(), resp -> {
      if (controller.failed()) {
        future.completeExceptionally(controller.getException());
      } else {
        future.complete(
          resp.hasFs() ? Optional.of(PBHelperClient.convert(resp.getFs())) : Optional.empty());
      }
    });
    return future;
  }

  @Override
  public void close() throws IOException {
    rpcClient.close();
  }
}
