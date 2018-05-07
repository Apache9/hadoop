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
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import static com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM;
import static com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import com.google.protobuf.CodedOutputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.web.ExceptionHandler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBufOutputStream;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelFuture;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelFutureListener;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.DefaultFullHttpResponse;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2Headers;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2DataFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Headers;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2HeadersFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.HttpConversionUtil;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.EventExecutor;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.Future;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.FutureListener;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.Promise;

@InterfaceAudience.Private
public class ReadBlockHandler
    extends SimpleChannelInboundHandler<OpReadBlockRequestProto> {
  private static final Log LOG = LogFactory.getLog(ReadBlockHandler.class);

  private final DataNode datanode;

  public ReadBlockHandler(DataNode datanode) {
    this.datanode = datanode;
  }

  private static Replica getReplica(ExtendedBlock block,
      FsDatasetSpi<? extends FsVolumeSpi> data)
      throws ReplicaNotFoundException {
    @SuppressWarnings("deprecation")
    Replica replica =
        data.getReplica(block.getBlockPoolId(), block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    return replica;
  }

  private static void waitForMinLength(final Promise<ReplicaHolder> promise,
      final EventExecutor executor, final ReplicaBeingWritten rbw,
      final long replicaVisibleLength, final long expectedMinLength,
      final int numChecks) {
    long bytesOnDisk = rbw.getBytesOnDisk();
    if (bytesOnDisk >= expectedMinLength) {
      promise.trySuccess(new ReplicaHolder(rbw, replicaVisibleLength,
          rbw.getLastChecksumAndDataLen()));
      return;
    }
    if (numChecks >= 30) {
      promise.tryFailure(new IOException(
          String.format("Need %d bytes, but only %d bytes available",
              expectedMinLength, bytesOnDisk)));
      return;
    }
    executor.schedule(new Runnable() {

      @Override
      public void run() {
        waitForMinLength(promise, executor, rbw, replicaVisibleLength,
            expectedMinLength, numChecks + 1);
      }
    }, 100, TimeUnit.MILLISECONDS);
  }

  private static final class ReplicaHolder {

    public final Replica replica;

    public final long replicaVisibleLength;

    public final ChunkChecksum lastChunkChecksum;

    ReplicaHolder(Replica replica, long replicaVisibleLength) {
      this.replica = replica;
      this.replicaVisibleLength = replicaVisibleLength;
      this.lastChunkChecksum = null;
    }

    ReplicaHolder(ReplicaBeingWritten rbw, long replicaVisibleLength,
        ChunkChecksum lastChunkChecksum) {
      this.replica = rbw;
      this.replicaVisibleLength = replicaVisibleLength;
      this.lastChunkChecksum = lastChunkChecksum;
    }

  }

  private Future<ReplicaHolder> getReplica(ChannelHandlerContext ctx,
      ExtendedBlock block, FsDatasetSpi<? extends FsVolumeSpi> data,
      OpReadBlockRequestProto request) throws IOException {
    Replica replica;
    long replicaVisibleLength;
    synchronized (data) {
      replica = getReplica(block, data);
      replicaVisibleLength = replica.getVisibleLength();
    }
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException("Replica genstamp < block genstamp, block=" +
          block + ", replica=" + replica);
    } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
      if (LOG.isDebugEnabled()) {
        DataNode.LOG.debug(
            "Bumping up the client provided" + " block's genstamp to latest " +
                replica.getGenerationStamp() + " for block " + block);
      }
      block.setGenerationStamp(replica.getGenerationStamp());
    }
    if (replicaVisibleLength < 0) {
      throw new IOException(
          "Replica is not readable, block=" + block + ", replica=" + replica);
    }
    final EventExecutor executor = ctx.executor();
    if (!(replica instanceof ReplicaBeingWritten)) {
      return executor
          .newSucceededFuture(new ReplicaHolder(replica, replicaVisibleLength));
    }
    // there is write in progress
    ReplicaBeingWritten rbw = (ReplicaBeingWritten) replica;
    long expectedMinLength = request.getLen() < 0 ? replicaVisibleLength
        : request.getOffset() + request.getLen();
    if (rbw.getBytesOnDisk() >= expectedMinLength) {
      return executor.newSucceededFuture(new ReplicaHolder(rbw,
          replicaVisibleLength, rbw.getLastChecksumAndDataLen()));
    }
    Promise<ReplicaHolder> promise = executor.newPromise();
    // wait for 3 seconds to see if we can reach the expected minimum length,
    // fail otherwise.
    waitForMinLength(promise, executor, rbw, replicaVisibleLength,
        expectedMinLength, 0);
    return promise;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(cause);
    Http2Headers headers = HttpConversionUtil.toHttp2Headers(resp, false);
    if (resp.content().isReadable()) {
      ctx.write(new DefaultHttp2HeadersFrame(headers, false));
      ctx.writeAndFlush(new DefaultHttp2DataFrame(resp.content(), true));
    } else {
      ctx.writeAndFlush(new DefaultHttp2HeadersFrame(headers, true));
    }
  }

  private void readBlock(ChannelHandlerContext ctx,
      OpReadBlockRequestProto request, FsDatasetSpi<? extends FsVolumeSpi> data,
      final ExtendedBlock block, Replica replica, long replicaVisibleLength,
      ChunkChecksum lastChunkChecksum) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", replica=" + replica);
    }
    FsVolumeSpi volume = data.getVolume(block);
    if (volume == null) {
      throw new ReplicaNotFoundException(block);
    }
    DataInputStream checksumInput = null;
    InputStream blockInput = null;
    boolean success = false;
    try {
      DataChecksum checksum = null;
      if (request.getSendChecksums()) {
        boolean closeInput = true;
        LengthInputStream metadataInput = data.getMetaDataInputStream(block);
        try {
          if (metadataInput == null) {
            throw new FileNotFoundException("Meta-data not found for " + block);
          }
          if (metadataInput.getLength() > BlockMetadataHeader.getHeaderSize()) {
            checksumInput =
                new DataInputStream(new BufferedInputStream(metadataInput));
            checksum =
                BlockMetadataHeader.readDataChecksum(checksumInput, block);
            closeInput = false;
          }
        } finally {
          if (closeInput) {
            IOUtils.cleanup(LOG, metadataInput);
          }
        }
      }
      if (checksum == null) {
        checksum = DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 512);
      }
      int chunkSize = checksum.getBytesPerChecksum();
      if (chunkSize > 10 * 1024 * 1024 && chunkSize > replicaVisibleLength) {
        checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
            Math.max((int) replicaVisibleLength, 10 * 1024 * 1024));
        chunkSize = checksum.getBytesPerChecksum();
      }
      long blockLength = lastChunkChecksum == null ? replica.getBytesOnDisk()
          : lastChunkChecksum.getDataLength();
      long offset = request.getOffset();
      long len = request.getLen() < 0 ? replicaVisibleLength - offset
          : request.getLen();
      if (offset < 0 || offset > blockLength || (offset + len) > blockLength) {
        String errorMsg = " Offset " + offset + " and length " + len +
            " don't match block " + block + " ( blockLen " + blockLength + " )";
        LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
            ":sendBlock() : " + errorMsg);
        throw new IOException(errorMsg);
      }
      long startOffset = offset - (offset % chunkSize);
      long endOffset = offset + len;
      if (endOffset % chunkSize != 0) {
        endOffset = endOffset - endOffset % chunkSize + chunkSize;
      }
      if (endOffset >= blockLength) {
        endOffset = blockLength;
      } else {
        // do not need to read last chunk
        lastChunkChecksum = null;
      }
      if (checksumInput != null && startOffset > 0) {
        IOUtils.skipFully(checksumInput,
            startOffset / chunkSize * checksum.getChecksumSize());
      }
      blockInput = data.getBlockInputStream(block, startOffset);
      long length = endOffset - startOffset;
      OpReadBlockResponseProto resp =
          OpReadBlockResponseProto.newBuilder().setStatus(SUCCESS)
              .setReadOpChecksumInfo(ReadOpChecksumInfoProto.newBuilder()
                  .setChecksum(DataTransferProtoUtil.toProto(checksum))
                  .setChunkOffset(startOffset))
              .build();
      int respSize = resp.getSerializedSize();
      int respSizeWithPrefix =
          respSize + CodedOutputStream.computeRawVarint32Size(respSize);
      ByteBuf respBuf = ctx.alloc().buffer(respSizeWithPrefix);
      resp.writeDelimitedTo(new ByteBufOutputStream(respBuf));
      Http2DataFrame respFrame = new DefaultHttp2DataFrame(respBuf, false);
      Http2HeadersFrame headers = new DefaultHttp2HeadersFrame(
          new DefaultHttp2Headers().status(OK.codeAsText())
              .set(CONTENT_TYPE, APPLICATION_OCTET_STREAM),
          false);
      ChunkedBlockInput input = new ChunkedBlockInput(blockInput, checksumInput,
          lastChunkChecksum == null ? null : lastChunkChecksum.getChecksum(),
          checksum,
          Math.max(1,
              ChunkedBlockInput.numberOfBlockChunks(
                  DFSUtil.getIoFileBufferSize(datanode.getConf()),
                  checksum.getBytesPerChecksum())),
          length);
      ctx.write(headers);
      ctx.write(respFrame);
      ctx.writeAndFlush(input)
          .addListener(new ChannelFutureListener() {
            
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (!future.isSuccess()) {
                LOG.warn("read block + " + block + " failed", future.cause());
                future.channel().close();
              }
            }
          });
      success = true;
    } finally {
      if (!success) {
        IOUtils.cleanup(LOG, blockInput, checksumInput);
      }
    }
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final OpReadBlockRequestProto request) throws Exception {
    final ExtendedBlock block =
        PBHelper.convert(request.getHeader().getBaseHeader().getBlock());
    Token<BlockTokenIdentifier> token =
        PBHelper.convert(request.getHeader().getBaseHeader().getToken());
    DtpUtil.checkAccess(datanode, block, token, Op.READ_BLOCK,
        BlockTokenSecretManager.AccessMode.READ, ctx.channel().remoteAddress());

    final FsDatasetSpi<? extends FsVolumeSpi> data = datanode.getFSDataset();
    getReplica(ctx, block, data, request)
        .addListener(new FutureListener<ReplicaHolder>() {

          @Override
          public void operationComplete(Future<ReplicaHolder> future) {
            if (future.isSuccess()) {
              ReplicaHolder holder = future.getNow();
              try {
                readBlock(ctx, request, data, block, holder.replica,
                    holder.replicaVisibleLength, holder.lastChunkChecksum);
              } catch (Throwable t) {
                exceptionCaught(ctx, t);
              }
            } else {
              exceptionCaught(ctx, future.cause());
            }
          }
        });
  }

}
