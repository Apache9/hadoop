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

import com.google.protobuf.CodedOutputStream;

import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBufOutputStream;
import com.xiaomi.infra.thirdparty.io.netty.channel.Channel;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInitializer;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpMethod;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2Headers;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2DataFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2HeadersFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.Http2ConnectionCache.DelayedCloseHandler;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpUtil;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

class Http2BlockReader implements BlockReader {

  private final Http2BlockDataReceiverImpl receiver;

  private final boolean isLocal;

  private final long endOffsetInBlock;

  private long offsetInBlock;

  private final Queue<ByteBuf> queue = new ArrayDeque<>();

  public Http2BlockReader(Http2BlockDataReceiverImpl receiver,
      long offsetInBlock, long endOffsetInBlock, boolean isLocal) {
    this.receiver = receiver;
    this.offsetInBlock = offsetInBlock;
    this.endOffsetInBlock = endOffsetInBlock;
    this.isLocal = isLocal;
  }

  // return whether we should stop
  private boolean pollIfNeeded() throws IOException {
    if (!queue.isEmpty()) {
      return true;
    }
    if (offsetInBlock >= endOffsetInBlock) {
      return false;
    }
    receiver.drainTo(queue);
    return true;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (!pollIfNeeded()) {
      return -1;
    }
    ByteBuf data = queue.peek();
    int bufRemaining = buf.remaining();
    int dataRemaining = data.readableBytes();
    if (bufRemaining < dataRemaining) {
      buf.mark();
      data.readBytes(buf);
      buf.reset();
      offsetInBlock += bufRemaining;
      return bufRemaining;
    } else {
      buf.mark();
      int oldLimit = buf.limit();
      buf.limit(buf.position() + dataRemaining);
      data.readBytes(buf);
      queue.remove();
      data.release();
      buf.reset();
      buf.limit(oldLimit);
      offsetInBlock += dataRemaining;
      return dataRemaining;
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if (!pollIfNeeded()) {
      return -1;
    }
    ByteBuf data = queue.peek();
    int toRead = Math.min(len, data.readableBytes());
    data.readBytes(buf, off, toRead);
    if (!data.isReadable()) {
      queue.remove();
      data.release();
    }
    offsetInBlock += toRead;
    return toRead;
  }

  @Override
  public long skip(long n) throws IOException {
    for (long remaining = n;;) {
      if (!pollIfNeeded()) {
        return n - remaining;
      }
      ByteBuf data = queue.peek();
      int dataRemaining = data.readableBytes();
      if (remaining > dataRemaining) {
        remaining -= dataRemaining;
        queue.remove();
        data.release();
        offsetInBlock += dataRemaining;
      } else {
        if (remaining == dataRemaining) {
          queue.remove();
          data.release();
        } else {
          data.skipBytes((int) remaining);
        }
        offsetInBlock += remaining;
        return n;
      }
    }

  }

  @Override
  public int available() throws IOException {
    return DFSClient.TCP_WINDOW_SIZE;
  }

  @Override
  public void close() {
    receiver.close();
    for (ByteBuf buf; (buf = queue.poll()) != null;) {
      buf.release();
    }
  }

  @Override
  public void readFully(byte[] buf, int off, int len) throws IOException {
    BlockReaderUtil.readFully(this, buf, off, len);
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

  public static Http2BlockReader newBlockReader(final String file,
      ExtendedBlock block, Token<BlockTokenIdentifier> blockToken,
      final long startOffsetInBlock, long len, final boolean verifyChecksum,
      String clientName, DatanodeID datanodeID, Http2ConnectionCache connCache,
      CachingStrategy cachingStrategy, final long maxBufferedDataSize)
      throws IOException {
    Pair<Channel, DelayedCloseHandler> channelAndHandler =
        connCache.get(datanodeID);
    Channel channel = channelAndHandler.getLeft();
    final DelayedCloseHandler delayedCloseHandler =
        channelAndHandler.getRight();
    final Promise<Http2BlockDataReceiverImpl> promise =
        channel.eventLoop().newPromise();
    final Channel stream;
    try {
      stream = new Http2StreamChannelBootstrap(channel)
          .handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
              Http2BlockDataReceiverImpl receiver =
                  new Http2BlockDataReceiverImpl(ch, file, verifyChecksum,
                      startOffsetInBlock, maxBufferedDataSize);
              ch.pipeline().addLast(delayedCloseHandler,
                  new Http2HeadersReceiver(receiver));
              promise.trySuccess(receiver);
            }
          }).open().syncUninterruptibly().getNow();
    } catch (IllegalArgumentException e) {
      // If the HTTP/2 connection has already been closed then a IAE will be
      // thrown so here we convert it to an IOException.
      throw new IOException(e);
    }
    final Http2HeadersFrame reqHeaders = new DefaultHttp2HeadersFrame(
        new DefaultHttp2Headers().method(HttpMethod.POST.name())
            .path(DtpUtil.URL_PREFIX + DtpUtil.OP_READ_BLOCK),
        false);
    OpReadBlockRequestProto req = OpReadBlockRequestProto.newBuilder()
        .setHeader(ClientOperationHeaderProto.newBuilder()
            .setBaseHeader(
                DataTransferProtoUtil.buildBaseHeader(block, blockToken))
            .setClientName(clientName))
        .setOffset(startOffsetInBlock).setLen(len)
        .setSendChecksums(verifyChecksum).build();
    int serializedSize = req.getSerializedSize();
    ByteBuf reqBuf = channel.alloc()
        .buffer(CodedOutputStream.computeRawVarint32Size(serializedSize) +
            serializedSize);
    req.writeDelimitedTo(new ByteBufOutputStream(reqBuf));
    final Http2DataFrame reqData = new DefaultHttp2DataFrame(reqBuf, true);
    stream.eventLoop().execute(new Runnable() {

      @Override
      public void run() {
        stream.write(reqHeaders);
        stream.writeAndFlush(reqData);
      }
    });
    Http2BlockDataReceiverImpl receiver =
        promise.syncUninterruptibly().getNow();
    return new Http2BlockReader(receiver, startOffsetInBlock,
        startOffsetInBlock + len, DFSClient.isLocalAddress(
            NetUtils.createSocketAddr(datanodeID.getXferAddr())));
  }
}
