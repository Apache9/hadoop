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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
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
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.web.http2.LastHttp2Message;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

/**
 *
 */
@InterfaceAudience.Private
class ReadBlockHandler extends
    SimpleChannelInboundHandler<OpReadBlockRequestProto> {
  private static final Log LOG = DtpUrlDispatcher.LOG;

  private final DataNode datanode;

  private final ExecutorService executor;

  public ReadBlockHandler(DataNode datanode, ExecutorService executor) {
    this.datanode = datanode;
    this.executor = executor;
  }

  private static Replica getReplica(ExtendedBlock block,
      FsDatasetSpi<? extends FsVolumeSpi> data) throws ReplicaNotFoundException {
    @SuppressWarnings("deprecation")
    Replica replica =
        data.getReplica(block.getBlockPoolId(), block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    return replica;
  }

  /**
   * Wait for rbw replica to reach the length.
   * @param rbw replica that is being written to
   * @param len minimum length to reach
   * @throws IOException on failing to reach the len in given wait time
   */
  private static void waitForMinLength(ReplicaBeingWritten rbw, long len)
      throws IOException {
    // Wait for 3 seconds for rbw replica to reach the minimum length
    for (int i = 0; i < 30 && rbw.getBytesOnDisk() < len; i++) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    long bytesOnDisk = rbw.getBytesOnDisk();
    if (bytesOnDisk < len) {
      throw new IOException(String.format(
        "Need %d bytes, but only %d bytes available", len, bytesOnDisk));
    }
  }

  private void handleReadBlock(final ChannelHandlerContext ctx,
      OpReadBlockRequestProto request) throws IOException {
    ExtendedBlock block =
        PBHelper.convert(request.getHeader().getBaseHeader().getBlock());
    Token<BlockTokenIdentifier> token =
        PBHelper.convert(request.getHeader().getBaseHeader().getToken());
    DtpUtil.checkAccess(datanode, block, token, Op.READ_BLOCK,
      BlockTokenIdentifier.AccessMode.READ, ctx.channel().remoteAddress());

    FsDatasetSpi<? extends FsVolumeSpi> data = datanode.getFSDataset();
    Replica replica;
    long replicaVisibleLength;
    synchronized (data) {
      replica = getReplica(block, data);
      replicaVisibleLength = replica.getVisibleLength();
    }
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException("Replica genstamp < block genstamp, block=" + block
          + ", replica=" + replica);
    } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
      if (LOG.isDebugEnabled()) {
        DataNode.LOG.debug("Bumping up the client provided"
            + " block's genstamp to latest " + replica.getGenerationStamp()
            + " for block " + block);
      }
      block.setGenerationStamp(replica.getGenerationStamp());
    }
    if (replicaVisibleLength < 0) {
      throw new IOException("Replica is not readable, block=" + block
          + ", replica=" + replica);
    }
    long offset = request.getOffset();
    long len = request.getLen();
    if (len < 0) {
      len = replicaVisibleLength - offset;
    }
    // if there is a write in progress
    ChunkChecksum lastChunkChecksum = null;
    if (replica instanceof ReplicaBeingWritten) {
      final ReplicaBeingWritten rbw = (ReplicaBeingWritten) replica;
      waitForMinLength(rbw, offset + len);
      lastChunkChecksum = rbw.getLastChecksumAndDataLen();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", replica=" + replica);
    }
    FsVolumeSpi volume = data.getVolume(block);
    if (volume == null) {
      throw new ReplicaNotFoundException(block);
    }
    FsVolumeReference volumeRef = volume.obtainReference();
    DataInputStream checksumInput = null;
    InputStream blockInput = null;
    boolean success = false;
    try {
      int ioBufferSize = DFSUtil.getIoFileBufferSize(datanode.getConf());
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
                new DataInputStream(new BufferedInputStream(metadataInput,
                    ioBufferSize));
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
        checksum =
            DataChecksum.newDataChecksum(checksum.getChecksumType(),
              Math.max((int) replicaVisibleLength, 10 * 1024 * 1024));
        chunkSize = checksum.getBytesPerChecksum();
      }
      long blockLength =
          lastChunkChecksum == null ? replica.getBytesOnDisk()
              : lastChunkChecksum.getDataLength();
      if (offset < 0 || offset > blockLength || (offset + len) > blockLength) {
        String errorMsg =
            " Offset " + offset + " and length " + len + " don't match block "
                + block + " ( blockLen " + blockLength + " )";
        LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId())
            + ":sendBlock() : " + errorMsg);
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
      final Http2Headers headers =
          new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText());
      final OpReadBlockResponseProto resp =
          OpReadBlockResponseProto
              .newBuilder()
              .setStatus(SUCCESS)
              .setReadOpChecksumInfo(
                ReadOpChecksumInfoProto.newBuilder()
                    .setChecksum(DataTransferProtoUtil.toProto(checksum))
                    .setChunkOffset(startOffset)).build();
      final ChunkedBlockInput input =
          new ChunkedBlockInput(volumeRef, blockInput, checksumInput,
              lastChunkChecksum == null ? null
                  : lastChunkChecksum.getChecksum(), checksum, Math.max(
                1,
                ChunkedBlockInput.numberOfBlockChunks(
                  DFSUtil.getIoFileBufferSize(datanode.getConf()),
                  checksum.getBytesPerChecksum())), length);
      ctx.write(headers);
      ctx.write(resp);
      ctx.write(input).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      ctx.writeAndFlush(LastHttp2Message.get());
//      ctx.channel().eventLoop().execute(new Runnable() {
//
//        @Override
//        public void run() {
//          ctx.write(headers);
//          ctx.write(resp);
//          ctx.write(input).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
//          ctx.writeAndFlush(LastHttp2Message.get());
//        }
//      });

      success = true;
    } finally {
      if (!success) {
        IOUtils.cleanup(LOG, blockInput, checksumInput, volumeRef);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    Http2ExceptionHandler.exceptionCaught(ctx, cause);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final OpReadBlockRequestProto msg) throws Exception {
    handleReadBlock(ctx, msg);
//    executor.execute(new Runnable() {
//
//      @Override
//      public void run() {
//        try {
//          handleReadBlock(ctx, msg);
//        } catch (Throwable t) {
//          ctx.channel().pipeline().fireExceptionCaught(t);
//        }
//      }
//    });
  }

}
