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

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

@InterfaceAudience.Private
class ChunkedBlockInput implements ChunkedInput<ByteBuf> {

  private static final Log LOG = DtpUrlDispatcher.LOG;

  private static final long LONG_READ_THRESHOLD_BYTES = 256 * 1024;
  private static long CACHE_DROP_INTERVAL_BYTES = 1024 * 1024; // 1MB

  private final DataNode datanode;

  private final FsVolumeReference volumeRef;

  private final ExtendedBlock block;

  private final InputStream blockInput;

  private final FileChannel blockInputChannel;

  private final FileDescriptor blockInputFd;

  private final InputStream checksumInput;

  private final byte[] lastChunkChecksum;

  private final DataChecksum checksum;

  private final long initialOffset;

  private final long length;

  private final int chunkSize;

  private final byte[] blockReadBuffer;

  private long progress;

  // Cache-management related fields
  private final boolean isLongRead;

  private final long readaheadLength;

  private ReadaheadRequest curReadahead;

  private final boolean alwaysReadahead;

  private final boolean dropCacheBehindLargeReads;

  private final boolean dropCacheBehindAllReads;

  private long lastCacheDropOffset;

  public ChunkedBlockInput(DataNode datanode, CachingStrategy cachingStrategy,
      FsVolumeReference volumeRef, ExtendedBlock block, InputStream blockInput,
      InputStream checksumInput, byte[] lastChunkChecksum,
      DataChecksum checksum, int chunksPerFrame, long initialOffset, long length)
      throws IOException {
    this.datanode = datanode;
    this.volumeRef = volumeRef;
    this.block = block;
    this.blockInput = blockInput;
    this.checksumInput = checksumInput;
    this.lastChunkChecksum = lastChunkChecksum;
    this.checksum = checksum;
    this.initialOffset = initialOffset;
    this.length = length;
    this.chunkSize = checksum.getBytesPerChecksum() * chunksPerFrame;
    if (blockInput instanceof FileInputStream) {
      FileInputStream fileInput = (FileInputStream) blockInput;
      this.blockInputChannel = fileInput.getChannel();
      this.blockInputFd = fileInput.getFD();
      this.blockReadBuffer =
          new byte[checksum.getChecksumSize() * chunksPerFrame];
    } else {
      this.blockInputChannel = null;
      this.blockInputFd = null;
      this.blockReadBuffer = new byte[chunkSize];
    }
    /*
     * If the client asked for the cache to be dropped behind all reads, we
     * honor that. Otherwise, we use the DataNode defaults. When using DataNode
     * defaults, we use a heuristic where we only drop the cache for large
     * reads.
     */
    if (cachingStrategy.getDropBehind() == null) {
      this.dropCacheBehindAllReads = false;
      this.dropCacheBehindLargeReads =
          datanode.getDnConf().isDropCacheBehindReads();
    } else {
      this.dropCacheBehindAllReads =
          this.dropCacheBehindLargeReads =
              cachingStrategy.getDropBehind().booleanValue();
    }
    /*
     * Similarly, if readahead was explicitly requested, we always do it.
     * Otherwise, we read ahead based on the DataNode settings, and only when
     * the reads are large.
     */
    if (cachingStrategy.getReadahead() == null) {
      this.alwaysReadahead = false;
      this.readaheadLength = datanode.getDnConf().getReadaheadLength();
    } else {
      this.alwaysReadahead = true;
      this.readaheadLength = cachingStrategy.getReadahead().longValue();
    }

    this.isLongRead = length > LONG_READ_THRESHOLD_BYTES;
  }

  @Override
  public boolean isEndOfInput() {
    return progress == length;
  }

  @Override
  public void close() {
    if (blockInputFd != null
        && ((dropCacheBehindAllReads) || (dropCacheBehindLargeReads && isLongRead))) {
      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
          block.getBlockName(), blockInputFd, lastCacheDropOffset,
          initialOffset + length - lastCacheDropOffset, POSIX_FADV_DONTNEED);
      } catch (Exception e) {
        LOG.warn("Unable to drop cache on file close", e);
      }
    }
    IOUtils.cleanup(LOG, blockInput, checksumInput, volumeRef);
  }

  static int numberOfBlockChunks(long length, int blockChunkSize) {
    return (int) ((length + blockChunkSize - 1) / blockChunkSize);
  }

  private void readChecksum(byte[] buf, int off, int len) throws IOException {
    IOUtils.readFully(checksumInput, buf, off, len);
  }

  private void manageOsCache(ChannelHandlerContext ctx)
      throws NativeIOException {
    // We can't manage the cache for this block if we don't have a file
    // descriptor to work with.
    if (blockInputFd == null) return;

    long offset = initialOffset + progress;
    // Perform readahead if necessary
    if ((readaheadLength > 0) && (datanode.getReadaheadPool() != null)
        && (alwaysReadahead || isLongRead)) {
      curReadahead =
          datanode.getReadaheadPool()
              .readaheadStream(
                ctx.channel().remoteAddress() + "-" + block.getBlockId(),
                blockInputFd, offset, readaheadLength, Long.MAX_VALUE,
                curReadahead);
    }

    // Drop what we've just read from cache, since we aren't
    // likely to need it again
    if (dropCacheBehindAllReads || (dropCacheBehindLargeReads && isLongRead)) {
      long nextCacheDropOffset =
          lastCacheDropOffset + CACHE_DROP_INTERVAL_BYTES;
      if (offset >= nextCacheDropOffset) {
        long dropLength = offset - lastCacheDropOffset;
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
          block.getBlockName(), blockInputFd, lastCacheDropOffset, dropLength,
          POSIX_FADV_DONTNEED);
        lastCacheDropOffset = offset;
      }
    }
  }

  @Override
  public ByteBuf readChunk(ChannelHandlerContext ctx) throws IOException {
    int blockBytesToRead = (int) Math.min(chunkSize, length - progress);
    progress += blockBytesToRead;
    int numBlockChunks =
        numberOfBlockChunks(blockBytesToRead, checksum.getBytesPerChecksum());
    ByteString checksums;
    if (checksumInput != null) {
      int checksumLength = numBlockChunks * checksum.getChecksumSize();
      if (isEndOfInput() && lastChunkChecksum != null) {
        readChecksum(blockReadBuffer, 0,
          checksumLength - checksum.getBytesPerChecksum());
        System.arraycopy(lastChunkChecksum, 0, blockReadBuffer, checksumLength
            - checksum.getBytesPerChecksum(), checksum.getBytesPerChecksum());
      } else {
        readChecksum(blockReadBuffer, 0, checksumLength);
      }
      checksums = ByteString.copyFrom(blockReadBuffer, 0, checksumLength);
    } else {
      checksums = ByteString.EMPTY;
    }
    OpReadBlockFrameHeaderProto header =
        OpReadBlockFrameHeaderProto
            .newBuilder()
            .setNumChunks(
              numberOfBlockChunks(blockBytesToRead,
                checksum.getBytesPerChecksum())).setChecksums(checksums)
            .setDataLength(blockBytesToRead).build();
    ByteBuf data =
        ctx.alloc().buffer(
          CodedOutputStream.computeRawVarint32Size(header.getSerializedSize())
              + header.getSerializedSize() + blockBytesToRead);
    header.writeDelimitedTo(new ByteBufOutputStream(data));
    manageOsCache(ctx);
    if (blockInputChannel != null) {
      while (blockBytesToRead > 0) {
        int read = data.writeBytes(blockInputChannel, blockBytesToRead);
        if (read < 0) {
          throw new EOFException();
        }
        blockBytesToRead -= read;
      }
    } else {
      IOUtils.readFully(blockInput, blockReadBuffer, 0, blockBytesToRead);
      data.writeBytes(blockReadBuffer, 0, blockBytesToRead);
    }
    return data;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public long progress() {
    return progress;
  }
}
