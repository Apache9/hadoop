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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.web.http2.ByteBufferReadableInputStream;
import org.apache.hadoop.hdfs.web.http2.Http2DataReceiver;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

/**
 * A block reader that reads data over HTTP/2.
 */
@InterfaceAudience.Private
public class Http2BlockReader implements BlockReader {

  public static final Log LOG = LogFactory.getLog(Http2BlockReader.class);

  private static final ByteBuffer EMPTY_BB = ByteBuffer.allocate(0);

  private final String file;

  private final ByteBufferReadableInputStream in;

  private final int firstFrameSkipBytes;

  private final DataChecksum checksum;

  private final boolean verifyChecksum;

  private final boolean isLocal;

  private final long endOffsetInBlock;

  public Http2BlockReader(String file, ByteBufferReadableInputStream in,
      long offsetInBlock, int firstFrameSkipBytes, long endOffsetInBlock,
      DataChecksum checksum, boolean verifyChecksum, boolean isLocal) {
    this.file = file;
    this.in = in;
    this.offsetInBlock = offsetInBlock;
    this.firstFrameSkipBytes = firstFrameSkipBytes;
    this.endOffsetInBlock = endOffsetInBlock;
    this.checksum = checksum;
    this.verifyChecksum = verifyChecksum;
    this.isLocal = isLocal;
  }

  private long offsetInBlock;

  private ByteBuffer data = EMPTY_BB;

  private boolean firstFrame = true;

  private void receiveNextFrame() throws IOException {
    OpReadBlockFrameHeaderProto header =
        OpReadBlockFrameHeaderProto.parseDelimitedFrom(in);
    if (data.capacity() < header.getDataLength()) {
      data = ByteBuffer.allocate(header.getDataLength());
    } else {
      data.clear();
      data.limit(header.getDataLength());
    }
    while (data.hasRemaining()) {
      if (in.read(data) == -1) {
        throw new EOFException();
      }
    }
    data.flip();
    if (verifyChecksum) {
      checksum.verifyChunkedSums(data, header.getChecksums()
          .asReadOnlyByteBuffer(), file, offsetInBlock);
    }
    offsetInBlock += header.getDataLength();
    if (firstFrame) {
      data.position(firstFrameSkipBytes);
      firstFrame = false;
    }
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (!data.hasRemaining()) {
      if (offsetInBlock >= endOffsetInBlock) {
        return -1;
      }
      receiveNextFrame();
    }
    int bufRemaining = buf.remaining();
    int dataRemaining = data.remaining();
    if (bufRemaining < dataRemaining) {
      int oldLimit = data.limit();
      data.limit(data.position() + bufRemaining);
      buf.put(data);
      data.limit(oldLimit);
      return bufRemaining;
    } else {
      return dataRemaining;
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if (!data.hasRemaining()) {
      if (offsetInBlock >= endOffsetInBlock) {
        return -1;
      }
      receiveNextFrame();
    }
    int toRead = Math.min(len, data.remaining());
    data.get(buf, off, toRead);
    return toRead;
  }

  @Override
  public long skip(long n) throws IOException {
    if (!data.hasRemaining()) {
      if (offsetInBlock >= endOffsetInBlock) {
        return 0L;
      }
      receiveNextFrame();
    }
    int toSkip = (int) Math.min(n, data.remaining());
    data.position(data.position() + toSkip);
    return toSkip;
  }

  @Override
  public int available() throws IOException {
    int blockAvailable = (int) Math.max(0, endOffsetInBlock - offsetInBlock);
    return Math.min(blockAvailable, DFSClient.TCP_WINDOW_SIZE);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public void readFully(byte[] buf, int readOffset, int amtToRead)
      throws IOException {
    BlockReaderUtil.readFully(this, buf, readOffset, amtToRead);

  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return BlockReaderUtil.readAll(this, buf, offset, len);
  }

  @Override
  public boolean isLocal() {
    return isLocal;
  }

  @Override
  public boolean isShortCircuit() {
    return false;
  }

  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    return null;
  }

  public static Http2BlockReader
      newBlockReader(Http2ConnectionPool connPool, String file,
          ExtendedBlock block, Token<BlockTokenIdentifier> blockToken,
          long startOffsetInBlock, long len, boolean verifyChecksum,
          String clientName, DatanodeID datanodeID,
          CachingStrategy cachingStrategy) throws IOException {
    OpReadBlockRequestProto req =
        OpReadBlockRequestProto
            .newBuilder()
            .setHeader(
              ClientOperationHeaderProto
                  .newBuilder()
                  .setBaseHeader(
                    DataTransferProtoUtil.buildBaseHeader(block, blockToken))
                  .setClientName(clientName)).setOffset(startOffsetInBlock)
            .setLen(len).setSendChecksums(verifyChecksum).build();
    Http2DataReceiver receiver =
        connPool.connect(new InetSocketAddress(datanodeID.getIpAddr(),
            datanodeID.getInfoPort()), req);
    ByteBufferReadableInputStream in = receiver.content();
    boolean succ = false;
    try {
      Http2Headers headers = receiver.waitForResponse();
      if (!HttpResponseStatus.OK.codeAsText().equals(headers.status())) {
        // TODO: receive json error message
        throw new IOException("Unexpected http status code: "
            + headers.status());
      }
      OpReadBlockResponseProto resp =
          OpReadBlockResponseProto.parseDelimitedFrom(in);
      if (resp.getStatus() != Status.SUCCESS) {
        throw new IOException("Unexpected status: " + resp.getStatus());
      }
      long chunkOffset = resp.getReadOpChecksumInfo().getChunkOffset();
      int firstFrameSkipBytes = (int) (startOffsetInBlock - chunkOffset);
      DataChecksum checksum =
          DataTransferProtoUtil.fromProto(resp.getReadOpChecksumInfo()
              .getChecksum());
      succ = true;
      return new Http2BlockReader(file, in, chunkOffset, firstFrameSkipBytes,
          startOffsetInBlock + len, checksum, verifyChecksum,
          DFSClient.isLocalAddress(NetUtils.createSocketAddr(datanodeID
              .getXferAddr())));
    } finally {
      if (!succ) {
        in.close();
      }
    }
  }
}
