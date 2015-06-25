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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.Http2ConnectionPool.SessionAndStreamId;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.ChunkInputStream;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.ContinuousStreamListener;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUrlDispatcher;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.util.DataChecksum;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.PriorityFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;

/**
 * A block reader that reads data over HTTP/2.
 */
@InterfaceAudience.Private
public class Http2BlockReader implements BlockReader {

  public static final Log LOG = LogFactory.getLog(Http2BlockReader.class);

  private Session session;

  private AtomicInteger streamIdGenerator;

  private String fileName;

  private ExtendedBlock block;

  private long startOffsetInBlock;

  private boolean verifyChecksum;

  private String clientName;

  private long length;

  private CachingStrategy strategy;

  private boolean newStream = false;

  private Stream stream = null;

  private ChunkInputStream chunkInputStream = null;

  private ContinuousStreamListener listener = null;

  public Http2BlockReader(SessionAndStreamId sessionAndSessionId, String fileName,
      ExtendedBlock block, long startOffsetInBlock, boolean verifyChecksum, String clientName,
      long length, CachingStrategy strategy) {
    this.session = sessionAndSessionId.getSession();
    this.streamIdGenerator = sessionAndSessionId.getStreamIdGenerator();
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
    buildFrameInputStream();
    int nRead = buf.remaining();
    int retRead = this.read(buf.array(), buf.position(), nRead);
    if (retRead >= 0) {
      buf.position(buf.position() + retRead);
    }
    return retRead;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    buildFrameInputStream();
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
    this.chunkInputStream.close();
  }

  @Override
  public void readFully(byte[] buf, int readOffset, int amtToRead) throws IOException {
    BlockReaderUtil.readFully(this, buf, readOffset, amtToRead);

  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return BlockReaderUtil.readAll(this, buf, offset, len);
  }

  private void buildFrameInputStream() throws IOException {
    if (!this.newStream) {
      HttpFields fields = new HttpFields();
      fields.put(HttpHeader.C_METHOD, HttpMethod.POST.asString());
      fields.put(HttpHeader.C_PATH, DtpUrlDispatcher.URL_PREFIX + DtpUrlDispatcher.OP_READ_BLOCK);
      FuturePromise<Stream> streamPromise = null;
      int streamId = streamIdGenerator.getAndAdd(2);
      streamPromise = new FuturePromise<>();
      this.listener = new ContinuousStreamListener();
      //this.listener = new ContinuousStreamListener();
      session.newStream(new HeadersFrame(streamId, new MetaData(
          org.eclipse.jetty.http.HttpVersion.HTTP_2, fields), new PriorityFrame(streamId, 0, 1,
          false), false), streamPromise, this.listener);
      try {
        this.stream = streamPromise.get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.warn("new stream failed ", e);
        throw new IOException(e);
      }
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
      stream.data(new DataFrame(stream.getId(), ByteBuffer.wrap(bos.toByteArray()), true),
        new Callback.Adapter());
      try {
        if (listener.getStatus() == HttpStatus.OK_200) {
          this.chunkInputStream = new ChunkInputStream(listener);
          OpReadBlockResponseProto respProto =
              OpReadBlockResponseProto.parseDelimitedFrom(this.chunkInputStream
                  .getFrameInputStream());
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
            this.newStream = true;
          } else {
            String message = "resp status is " + respProto.getStatus();
            LOG.warn(message);
            throw new IOException(message);
          }
        } else {
          String message = "stream data failed, http status code is " + listener.getStatus();
          LOG.warn(message);
          throw new IOException(message);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
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
