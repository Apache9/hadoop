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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

/**
 * 
 */
@InterfaceAudience.Private
class ChunkedBlockInput implements ChunkedInput<ByteBuf> {

  private static final Log LOG = DtpStreamHandlerInitializer.LOG;

  private final FsVolumeReference volumeRef;

  private final InputStream blockInput;

  private final InputStream checksumInput;

  private final byte[] lastChunkChecksum;

  private final DataChecksum checksum;

  private final long length;

  private final byte[] blockReadBuffer;

  private long progress;

  public ChunkedBlockInput(FsVolumeReference volumeRef, InputStream blockInput,
      InputStream checksumInput, byte[] lastChunkChecksum,
      DataChecksum checksum, int chunksPerFrame, long length) {
    this.volumeRef = volumeRef;
    this.blockInput = blockInput;
    this.checksumInput = checksumInput;
    this.lastChunkChecksum = lastChunkChecksum;
    this.checksum = checksum;
    this.length = length;
    this.blockReadBuffer =
        new byte[checksum.getBytesPerChecksum() * chunksPerFrame];
  }

  @Override
  public boolean isEndOfInput() {
    return progress == length;
  }

  @Override
  public void close() {
    IOUtils.cleanup(LOG, blockInput, checksumInput, volumeRef);
  }

  static int numberOfBlockChunks(long length, int blockChunkSize) {
    return (int) ((length + blockChunkSize - 1) / blockChunkSize);
  }

  private void readChecksum(byte[] buf, int off, int len) throws IOException {
    IOUtils.readFully(checksumInput, buf, off, len);
  }

  @Override
  public ByteBuf readChunk(ChannelHandlerContext ctx) throws IOException {
    int blockBytesToRead =
        (int) Math.min(blockReadBuffer.length, length - progress);
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
    // TODO: use transfer to
    IOUtils.readFully(blockInput, blockReadBuffer, 0, blockBytesToRead);

    data.writeBytes(blockReadBuffer, 0, blockBytesToRead);
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
