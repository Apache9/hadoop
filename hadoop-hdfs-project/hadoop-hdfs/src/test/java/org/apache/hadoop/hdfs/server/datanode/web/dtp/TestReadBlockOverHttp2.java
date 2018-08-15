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

import static org.junit.Assert.assertEquals;

import com.google.common.primitives.Ints;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.util.DataChecksum;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.xiaomi.infra.thirdparty.io.netty.bootstrap.Bootstrap;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBufOutputStream;
import com.xiaomi.infra.thirdparty.io.netty.channel.Channel;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelInitializer;
import com.xiaomi.infra.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http.HttpMethod;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2Headers;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2FrameLogger;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2StreamChannel;
import com.xiaomi.infra.thirdparty.io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import com.xiaomi.infra.thirdparty.io.netty.handler.logging.LogLevel;

public class TestReadBlockOverHttp2 {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static NioEventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  @BeforeClass
  public static void setUp() throws Exception {
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    int port = CLUSTER.getDataNodes().get(0).getInfoPort();

    CHANNEL =
        new Bootstrap().group(WORKER_GROUP).channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(Http2MultiplexCodecBuilder
                    .forClient(new ChannelInitializer<Http2StreamChannel>() {

                      @Override
                      protected void initChannel(Http2StreamChannel ch)
                          throws Exception {
                        throw new UnsupportedOperationException(
                            "Stream created from server is not allowed");
                      }
                    }).frameLogger(new Http2FrameLogger(LogLevel.INFO, "HTTP/2 DTP Client")).build());
              }

            }).connect(new InetSocketAddress("127.0.0.1", port)).sync()
            .channel();
  }

  @After
  public void tearDown()
      throws FileNotFoundException, IllegalArgumentException, IOException {
    for (RemoteIterator<LocatedFileStatus> iter =
        CLUSTER.getFileSystem().listFiles(new Path("/"), false); iter
            .hasNext();) {
      CLUSTER.getFileSystem().delete(iter.next().getPath(), true);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (CHANNEL != null) {
      CHANNEL.close();
    }
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  @Test
  public void test()
      throws IOException, InterruptedException, ExecutionException {
    FSDataOutputStream out =
      CLUSTER.getFileSystem().create(new Path("/test"));
    out.write(1);
    out.close();
    Channel stream = new Http2StreamChannelBootstrap(CHANNEL)
        .handler(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new Http2DataReceiver());
          }
        }).open().sync().getNow();
    stream.write(new DefaultHttp2HeadersFrame(
        new DefaultHttp2Headers().method(HttpMethod.POST.name())
            .path(DtpUtil.URL_PREFIX + DtpUtil.OP_READ_BLOCK),
        false));
    ExtendedBlock block = CLUSTER.getFileSystem().getClient()
        .getLocatedBlocks("/test", 0).get(0).getBlock();
    OpReadBlockRequestProto proto =
        OpReadBlockRequestProto.newBuilder()
            .setHeader(ClientOperationHeaderProto.newBuilder()
                .setBaseHeader(BaseHeaderProto.newBuilder()
                    .setBlock(PBHelper.convert(block)))
                .setClientName("Test"))
            .setOffset(0).setLen(1).setSendChecksums(true).build();
    ByteBuf buf = CHANNEL.alloc().buffer();
    proto.writeDelimitedTo(new ByteBufOutputStream(buf));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    proto.writeDelimitedTo(bos);
    stream.writeAndFlush(new DefaultHttp2DataFrame(buf, true));
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    byte[] data = receiver.get();
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    OpReadBlockResponseProto respProto =
        OpReadBlockResponseProto.parseDelimitedFrom(input);
    assertEquals(Status.SUCCESS, respProto.getStatus());
    assertEquals(0L, respProto.getReadOpChecksumInfo().getChunkOffset());
    OpReadBlockFrameHeaderProto frameHeaderProto =
        OpReadBlockFrameHeaderProto.parseDelimitedFrom(input);
    assertEquals(1, frameHeaderProto.getNumChunks());
    assertEquals(4, frameHeaderProto.getChecksums().size());
    assertEquals(1, frameHeaderProto.getDataLength());
    assertEquals(1, input.read());
    assertEquals(-1, input.read());
    DataChecksum checksum = DataTransferProtoUtil
        .fromProto(respProto.getReadOpChecksumInfo().getChecksum());
    checksum.reset();
    checksum.update(1);
    assertEquals((int) checksum.getValue(),
        Ints.fromByteArray(frameHeaderProto.getChecksums().toByteArray()));
  }

  @Ignore
  @Test(expected = FileNotFoundException.class)
  public void testBlockNotExists()
      throws IOException, InterruptedException, ExecutionException {
    FSDataOutputStream out = null;
    try {
       out = CLUSTER.getFileSystem().create(new Path("/test"));
      out.write(2);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    Channel stream = new Http2StreamChannelBootstrap(CHANNEL)
        .handler(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new Http2DataReceiver());
          }
        }).open().sync().getNow();
    stream.write(new DefaultHttp2HeadersFrame(
        new DefaultHttp2Headers().method(HttpMethod.POST.name())
            .path(DtpUtil.URL_PREFIX + DtpUtil.OP_READ_BLOCK),
        false));
    ExtendedBlock block = CLUSTER.getFileSystem().getClient()
        .getLocatedBlocks("/test", 0).get(0).getBlock();
    block.setBlockId(block.getBlockId() + 1);
    OpReadBlockRequestProto proto =
        OpReadBlockRequestProto.newBuilder()
            .setHeader(ClientOperationHeaderProto.newBuilder()
                .setBaseHeader(BaseHeaderProto.newBuilder()
                    .setBlock(PBHelper.convert(block)))
                .setClientName("Test"))
            .setOffset(0).setLen(1).setSendChecksums(true).build();
    ByteBuf buf = CHANNEL.alloc().buffer();
    proto.writeDelimitedTo(new ByteBufOutputStream(buf));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    proto.writeDelimitedTo(bos);
    stream.writeAndFlush(new DefaultHttp2DataFrame(buf, true));
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    receiver.get();
  }
}
