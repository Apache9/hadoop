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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.ChunkInputStream;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.web.http2.Http2DataReceiver;
import org.apache.hadoop.hdfs.web.http2.Http2StreamChannel;
import org.apache.hadoop.hdfs.web.http2.LastHttp2Message;
import org.apache.hadoop.util.DataChecksum;

/**
 * A block reader that reads data over HTTP/2.
 */
@InterfaceAudience.Private
public class Http2BlockReader implements BlockReader {

  public static final Log LOG = LogFactory.getLog(Http2BlockReader.class);

  private String fileName;

  private ExtendedBlock block;

  private long startOffsetInBlock;

  private boolean verifyChecksum;

  private String clientName;

  private long length;

  private CachingStrategy strategy;

  private boolean initialReadBlock = false;

  private ChunkInputStream chunkInputStream = null;

  private Http2StreamChannel http2StreamChannel;

  public Http2BlockReader(Http2StreamChannel http2StreamChannel, String fileName,
      ExtendedBlock block, long startOffsetInBlock, boolean verifyChecksum, String clientName,
      long length, CachingStrategy strategy) {
    this.http2StreamChannel = http2StreamChannel;
    this.fileName = fileName;
    this.block = block;
    this.startOffsetInBlock = startOffsetInBlock;
    this.verifyChecksum = verifyChecksum;
    this.clientName = clientName;
    this.length = length;
    this.strategy = strategy;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    sendReadBlockRequest();
    int nRead = buf.remaining();
    int retRead = this.read(buf.array(), buf.position(), nRead);
    if (retRead >= 0) {
      buf.position(buf.position() + retRead);
    }
    return retRead;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    sendReadBlockRequest();
    return this.chunkInputStream.read(buf, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return this.chunkInputStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return this.chunkInputStream.available();
  }

  @Override
  public void close() throws IOException {
    if (this.chunkInputStream != null) {
      this.chunkInputStream.close();
    }
  }

  @Override
  public void readFully(byte[] buf, int readOffset, int amtToRead) throws IOException {
    BlockReaderUtil.readFully(this, buf, readOffset, amtToRead);

  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return BlockReaderUtil.readAll(this, buf, offset, len);
  }

  private void sendReadBlockRequest() throws IOException {

    if (!initialReadBlock) {
      OpReadBlockRequestProto proto =
          OpReadBlockRequestProto
              .newBuilder()
              .setHeader(
                ClientOperationHeaderProto.newBuilder()
                    .setBaseHeader(BaseHeaderProto.newBuilder().setBlock(PBHelper.convert(block)))
                    .setClientName(this.clientName)).setOffset(this.startOffsetInBlock)
              .setLen(this.length).setSendChecksums(this.verifyChecksum).build();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      proto.writeDelimitedTo(bos);
      http2StreamChannel.write(http2StreamChannel.alloc().buffer().writeBytes(bos.toByteArray()));
      http2StreamChannel.writeAndFlush(LastHttp2Message.get());
      Http2DataReceiver receiver = http2StreamChannel.pipeline().get(Http2DataReceiver.class);
      String status = receiver.waitForResponse().status().toString();
      if (!status.equals(HttpResponseStatus.OK.codeAsText().toString())) {
        String message = "write data failed, http status code is " + status;
        LOG.warn(message);
        throw new IOException(message);
      } else {
        ChunkInputStream chunkInputStream = new ChunkInputStream(receiver.content());
        OpReadBlockResponseProto respProto =
            OpReadBlockResponseProto.parseDelimitedFrom(chunkInputStream);
        if (respProto.getStatus() == Status.SUCCESS) {
          long chunkOffset = respProto.getReadOpChecksumInfo().getChunkOffset();
          DataChecksum dataChecksum =
              DataTransferProtoUtil.fromProto(respProto.getReadOpChecksumInfo().getChecksum());
          if (chunkOffset < 0 || chunkOffset > this.startOffsetInBlock
              || chunkOffset <= (this.startOffsetInBlock - dataChecksum.getBytesPerChecksum())) {
            throw new IOException("BlockReader: error in first chunk offset (" + chunkOffset
                + ") startOffset is " + startOffsetInBlock + " for file " + this.fileName);
          }
          this.chunkInputStream.setDataChecksum(dataChecksum);
          this.chunkInputStream.setFileName(this.fileName);
          this.chunkInputStream.setChunkOffset(chunkOffset);
          this.chunkInputStream.setSkipBytes(this.startOffsetInBlock - chunkOffset);
          this.initialReadBlock = true;
        } else {
          String message = "resp status is " + respProto.getStatus();
          LOG.warn(message);
          throw new IOException(message);
        }
      }
    }
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public boolean isShortCircuit() {
    return false;
  }

  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    return null;
  }

}
