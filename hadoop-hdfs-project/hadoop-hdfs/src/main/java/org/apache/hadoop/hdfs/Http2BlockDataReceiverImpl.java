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

import static com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import com.google.protobuf.ByteString;

import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.channel.Channel;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2Headers;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.NativeCodeLoader;

class Http2BlockDataReceiverImpl implements Http2BlockDataReceiver, Closeable {

  private final Channel channel;

  private final String file;

  private final boolean verifyChecksum;

  private final long startOffsetInBlock;

  private final long maxBufferedDataSize;

  private long offsetInBlock;

  private int frameSkipBytes;

  private DataChecksum checksum;

  private boolean finished;

  private OpReadBlockFrameHeaderProto frameHeader;

  private final Queue<ByteBuf> frameDatas = new ArrayDeque<ByteBuf>();

  private long bufferedDataSize = 0L;

  private Throwable error;

  public Http2BlockDataReceiverImpl(Channel channel, String file,
      boolean verifyChecksum, long startOffsetInBlock,
      long maxBufferedDataSize) {
    this.channel = channel;
    this.file = file;
    this.verifyChecksum = verifyChecksum;
    this.startOffsetInBlock = startOffsetInBlock;
    this.maxBufferedDataSize = maxBufferedDataSize;
  }

  private void finishAndClose() {
    finished = true;
    channel.close();
  }

  @Override
  public synchronized void http2HeaderReceived(Http2Headers headers) {
    if (finished) {
      return;
    }
    if (OK.codeAsText().compareTo(headers.status()) != 0) {
      this.error =
          new IOException("Unexpected http status: " + headers.status());
      finishAndClose();
      notifyAll();
    }
  }

  @Override
  public void resonpseReceived(OpReadBlockResponseProto resp) {
    synchronized (this) {
      if (finished) {
        return;
      }
      if (resp.getStatus() != SUCCESS) {
        this.error = new IOException("Unexpected status: " + resp.getStatus());
        finishAndClose();
        notifyAll();
      }
    }
    long chunkOffset = resp.getReadOpChecksumInfo().getChunkOffset();
    offsetInBlock = chunkOffset;
    frameSkipBytes = (int) (startOffsetInBlock - chunkOffset);
    checksum = DataTransferProtoUtil
        .fromProto(resp.getReadOpChecksumInfo().getChecksum());
  }

  @Override
  public void frameHeaderReceived(OpReadBlockFrameHeaderProto header) {
    this.frameHeader = header;
  }

  private void verifyChecksum(ByteBuf data) throws ChecksumException {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      ByteString checksums = frameHeader.getChecksums();
      ByteBuf headerBuf = channel.alloc().directBuffer(checksums.size());
      try {
        ByteBuffer bb = headerBuf.nioBuffer(0, headerBuf.capacity());
        checksums.copyTo(bb);
        bb.flip();
        checksum.verifyChunkedSums(data.nioBuffer(), bb, file, offsetInBlock);
      } finally {
        headerBuf.release();
      }
    } else {
      checksum.verifyChunkedSums(data.nioBuffer(),
          frameHeader.getChecksums().asReadOnlyByteBuffer(), file,
          offsetInBlock);
    }
    offsetInBlock += data.readableBytes();
  }

  @Override
  public void frameDataReceived(ByteBuf data) {
    synchronized (this) {
      if (finished) {
        data.release();
        return;
      }
    }
    if (verifyChecksum) {
      try {
        verifyChecksum(data);
      } catch (ChecksumException e) {
        data.release();
        synchronized (this) {
          if (finished) {
            return;
          }
          this.error = e;
          this.frameHeader = null;
          finishAndClose();
        }
        return;
      }
    }
    if (frameSkipBytes > 0) {
      data.skipBytes(frameSkipBytes);
      frameSkipBytes = 0;
    }
    synchronized (this) {
      this.frameHeader = null;
      frameDatas.add(data);
      bufferedDataSize += data.readableBytes();
      if (bufferedDataSize >= maxBufferedDataSize &&
          channel.config().isAutoRead()) {
        channel.config().setAutoRead(false);
      }
      notifyAll();
    }

  }

  @Override
  public synchronized void onComplete() {
    if (finished) {
      return;
    }
    finishAndClose();
    notifyAll();
  }

  @Override
  public synchronized void channelInactive() {
    if (finished) {
      return;
    }
    error = new IOException("Stream closed");
    finishAndClose();
    notifyAll();
  }

  @Override
  public synchronized void onError(Throwable error) {
    if (finished) {
      return;
    }
    this.error = error;
    finishAndClose();
    notifyAll();
  }

  private void await() throws IOException {
    try {
      wait();
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException().initCause(e);
    }
  }

  public synchronized void drainTo(Collection<ByteBuf> c) throws IOException {
    while (!finished && frameDatas.isEmpty()) {
      await();
    }
    if (frameDatas.isEmpty()) {
      if (error != null) {
        throw new IOException(error);
      } else {
        return;
      }
    }
    for (ByteBuf b; (b = frameDatas.poll()) != null;) {
      c.add(b);
    }
    bufferedDataSize = 0L;
    if (!channel.config().isAutoRead()) {
      channel.config().setAutoRead(true);
    }
    return;
  }

  @Override
  public synchronized void close() {
    finishAndClose();
    for (ByteBuf data : frameDatas) {
      data.release();
    }
    frameDatas.clear();
  }
}
