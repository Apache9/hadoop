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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.retry.AsyncCallHandler;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 *
 */
@Unstable
public class AsyncDistributedFileSystem {

  private final DistributedFileSystem dfs;

  AsyncDistributedFileSystem(final DistributedFileSystem dfs) {
    this.dfs = dfs;
  }

  private <R> CompletableFuture<R> runAsync(CompletableFuture<?> future, Callable<?> c) {
    boolean isAsync = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
    try {
      c.call();
    } catch (Exception e) {
      future.completeExceptionally(e);
    } finally {
      Client.setAsynchronousMode(isAsync);
    }
    return AsyncCallHandler.getAsyncReturn();
  }

  private void addBlock(CompletableFuture<AsyncDFSOutput> future, String src, int bufferSize,
      Progressable progress, ChecksumOpt checksumOpt, HdfsFileStatus stat) {
    CompletableFuture<LocatedBlock> blockFuture = runAsync(future, () -> dfs.dfs.namenode
        .addBlock(src, dfs.dfs.clientName, null, null, stat.getFileId(), null, null));
    
  }

  private void create(CompletableFuture<AsyncDFSOutput> future, String src, FsPermission masked,
      EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize,
      MutableInt createRetryCount, final int bufferSize, final Progressable progress,
      final ChecksumOpt checksumOpt) {
    CompletableFuture<HdfsFileStatus> statFuture = runAsync(future,
      () -> dfs.dfs.namenode.create(src, masked, dfs.dfs.clientName, flag, createParent,
        replication, blockSize, CryptoProtocolVersion.supported()));
    statFuture.whenComplete((r, e) -> {
      if (e == null) {
        addBlock(future, src, bufferSize, progress, checksumOpt, r);
      } else {
        if (e instanceof RemoteException) {
          IOException ioe = ((RemoteException) e).unwrapRemoteException(
            AccessControlException.class, DSQuotaExceededException.class,
            QuotaByStorageTypeExceededException.class, FileAlreadyExistsException.class,
            FileNotFoundException.class, ParentNotDirectoryException.class,
            NSQuotaExceededException.class, RetryStartFileException.class, SafeModeException.class,
            UnresolvedPathException.class, SnapshotAccessControlException.class,
            UnknownCryptoProtocolVersionException.class);
          if (ioe instanceof RetryStartFileException) {
            if (createRetryCount.intValue() > 0) {
              createRetryCount.decrement();
              create(future, src, masked, flag, createParent, replication, blockSize,
                createRetryCount, bufferSize, progress, checksumOpt);
            } else {
              future.completeExceptionally(new IOException(
                  "Too many retries because of encryption" + " zone operations", ioe));
            }
          }
        } else {
          future.completeExceptionally(e);
        }
      }
    });
  }

  public CompletableFuture<AsyncDFSOutput> create(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> cflags, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress, final ChecksumOpt checksumOpt) {
    dfs.getFsStatistics().incrementWriteOps(1);
    dfs.getDFSOpsCountStatistics().incrementOpCounter(OpType.CREATE);
    Path absF = dfs.fixRelativePart(f);
    CompletableFuture<AsyncDFSOutput> future = new CompletableFuture<>();
    create(future, dfs.getPathName(absF), dfs.dfs.applyUMask(permission),
      new EnumSetWritable<>(cflags), true, replication, blockSize,
      new MutableInt(DFSOutputStream.CREATE_RETRY_COUNT), bufferSize, progress, checksumOpt);
    return future;
  }
}
