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
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.util.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.http2.ClientHttp2ConnectionHandler;
import org.apache.hadoop.hdfs.web.http2.Http2DataReceiver;
import org.apache.hadoop.hdfs.web.http2.Http2StreamBootstrap;
import org.apache.hadoop.hdfs.web.http2.Http2StreamChannel;
import org.apache.hadoop.hdfs.web.http2.LastHttp2Message;
import org.apache.hadoop.util.DataChecksum;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;

/**
 *
 */
public class TestReadBlock {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static NioEventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setBoolean(DFSConfigKeys.DFS_HTTP2_VERBOSE_KEY, true);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    int port = CLUSTER.getDataNodes().get(0).getInfoPort();

    CHANNEL =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(
                  ClientHttp2ConnectionHandler.create(ch, CONF));
              }

            }).connect(new InetSocketAddress("127.0.0.1", port)).sync()
            .channel();
  }

  @After
  public void tearDown() throws FileNotFoundException,
      IllegalArgumentException, IOException {
    for (RemoteIterator<LocatedFileStatus> iter =
        CLUSTER.getFileSystem().listFiles(new Path("/"), false); iter.hasNext();) {
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
  public void test() throws IOException, InterruptedException,
      ExecutionException {
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path("/test"));
    out.write(1);
    out.close();
    Http2StreamChannel stream =
        new Http2StreamBootstrap()
            .channel(CHANNEL)
            .handler(new ChannelInitializer<Http2StreamChannel>() {

              @Override
              protected void initChannel(Http2StreamChannel ch)
                  throws Exception {
                ch.pipeline().addLast(new Http2DataReceiver());
              }

            })
            .headers(
              new DefaultHttp2Headers().method(
                new ByteString(HttpMethod.POST.name(), StandardCharsets.UTF_8))
                  .path(
                    new ByteString(DtpUrlDispatcher.URL_PREFIX
                        + DtpUrlDispatcher.OP_READ_BLOCK,
                        StandardCharsets.UTF_8))).endStream(false).connect()
            .sync().get();
    ExtendedBlock block =
        CLUSTER.getFileSystem().getClient().getLocatedBlocks("/test", 0).get(0)
            .getBlock();
    OpReadBlockRequestProto proto =
        OpReadBlockRequestProto
            .newBuilder()
            .setHeader(
              ClientOperationHeaderProto
                  .newBuilder()
                  .setBaseHeader(
                    BaseHeaderProto.newBuilder().setBlock(
                      PBHelper.convert(block))).setClientName("Test"))
            .setOffset(0).setLen(1).setSendChecksums(true).build();
    ByteBuf buf = CHANNEL.alloc().buffer();
    proto.writeDelimitedTo(new ByteBufOutputStream(buf));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    proto.writeDelimitedTo(bos);
    stream.write(buf);
    stream.writeAndFlush(LastHttp2Message.get());
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    assertEquals(HttpResponseStatus.OK.codeAsText(), receiver.waitForResponse()
        .status());
    InputStream input = receiver.content();
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
    DataChecksum checksum =
        DataTransferProtoUtil.fromProto(respProto.getReadOpChecksumInfo()
            .getChecksum());
    checksum.reset();
    checksum.update(1);
    assertEquals((int) checksum.getValue(),
      Ints.fromByteArray(frameHeaderProto.getChecksums().toByteArray()));
  }

  @Test
  public void testBlockNotExists() throws IOException, InterruptedException,
      ExecutionException {
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path("/test"));
    out.write(2);
    out.close();
    Http2StreamChannel stream =
        new Http2StreamBootstrap()
            .channel(CHANNEL)
            .handler(new ChannelInitializer<Http2StreamChannel>() {

              @Override
              protected void initChannel(Http2StreamChannel ch)
                  throws Exception {
                ch.pipeline().addLast(new Http2DataReceiver());
              }

            })
            .headers(
              new DefaultHttp2Headers().method(
                new ByteString(HttpMethod.POST.name(), StandardCharsets.UTF_8))
                  .path(
                    new ByteString(DtpUrlDispatcher.URL_PREFIX
                        + DtpUrlDispatcher.OP_READ_BLOCK,
                        StandardCharsets.UTF_8))).endStream(false).connect()
            .sync().get();
    ExtendedBlock block =
        CLUSTER.getFileSystem().getClient().getLocatedBlocks("/test", 0).get(0)
            .getBlock();
    block.setBlockId(block.getBlockId() + 1);
    OpReadBlockRequestProto proto =
        OpReadBlockRequestProto
            .newBuilder()
            .setHeader(
              ClientOperationHeaderProto
                  .newBuilder()
                  .setBaseHeader(
                    BaseHeaderProto.newBuilder().setBlock(
                      PBHelper.convert(block))).setClientName("Test"))
            .setOffset(0).setLen(1).setSendChecksums(true).build();
    ByteBuf buf = CHANNEL.alloc().buffer();
    proto.writeDelimitedTo(new ByteBufOutputStream(buf));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    proto.writeDelimitedTo(bos);
    stream.write(buf);
    stream.writeAndFlush(LastHttp2Message.get());
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    assertEquals(HttpResponseStatus.NOT_FOUND.codeAsText(), receiver
        .waitForResponse().status());
    JsonNode jsonNode =
        new ObjectMapper().readTree(new String(ByteStreams.toByteArray(receiver
            .content()), StandardCharsets.UTF_8));
    assertEquals(ReplicaNotFoundException.class.getSimpleName(),
      jsonNode.get("RemoteException").get("exception").asText());
  }
}
