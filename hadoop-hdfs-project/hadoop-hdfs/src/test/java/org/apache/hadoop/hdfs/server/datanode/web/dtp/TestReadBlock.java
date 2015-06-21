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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamListener;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.util.DataChecksum;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.ErrorCode;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.PriorityFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.primitives.Ints;

/**
 *
 */
public class TestReadBlock {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static HTTP2Client CLIENT = new HTTP2Client();

  private static Session SESSION;

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setBoolean(DFSConfigKeys.DFS_HTTP2_VERBOSE_KEY, true);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    CLIENT.start();
    int port = CLUSTER.getDataNodes().get(0).getInfoPort();
    FuturePromise<Session> sessionPromise = new FuturePromise<>();
    CLIENT.connect(new InetSocketAddress("127.0.0.1", port),
      new Session.Listener.Adapter(), sessionPromise);
    SESSION = sessionPromise.get();
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
    if (SESSION != null) {
      SESSION.close(ErrorCode.NO_ERROR.code, "", new Callback.Adapter());
    }
    CLIENT.stop();
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
    HttpFields fields = new HttpFields();
    fields.put(HttpHeader.C_METHOD, HttpMethod.POST.asString());
    fields.put(HttpHeader.C_PATH, DtpUrlDispatcher.URL_PREFIX
        + DtpUrlDispatcher.OP_READ_BLOCK);
    FuturePromise<Stream> streamPromise = new FuturePromise<>();
    StreamListener listener = new StreamListener();
    SESSION.newStream(new HeadersFrame(1, new MetaData(HttpVersion.HTTP_2,
        fields), new PriorityFrame(1, 0, 1, false), false), streamPromise,
      listener);
    Stream stream = streamPromise.get();
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
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    proto.writeDelimitedTo(bos);
    stream.data(
      new DataFrame(stream.getId(), ByteBuffer.wrap(bos.toByteArray()), true),
      new Callback.Adapter());
    assertEquals(HttpStatus.OK_200, listener.getStatus());
    ByteArrayInputStream input = new ByteArrayInputStream(listener.getData());
    OpReadBlockResponseProto respProto =
        OpReadBlockResponseProto.parseDelimitedFrom(input);
    assertEquals(Status.SUCCESS, respProto.getStatus());
    assertEquals(0L, respProto.getReadOpChecksumInfo().getChunkOffset());
    OpReadBlockFrameHeaderProto frameHeaderProto =
        OpReadBlockFrameHeaderProto.parseDelimitedFrom(input);
    assertEquals(1, frameHeaderProto.getNumChunks());
    assertEquals(4, frameHeaderProto.getChecksums().size());
    assertEquals(1, frameHeaderProto.getDataLength());
    assertEquals(1, input.available());
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
    HttpFields fields = new HttpFields();
    fields.put(HttpHeader.C_METHOD, HttpMethod.POST.asString());
    fields.put(HttpHeader.C_PATH, DtpUrlDispatcher.URL_PREFIX
        + DtpUrlDispatcher.OP_READ_BLOCK);
    FuturePromise<Stream> streamPromise = new FuturePromise<>();
    StreamListener listener = new StreamListener();
    SESSION.newStream(new HeadersFrame(1, new MetaData(HttpVersion.HTTP_2,
        fields), new PriorityFrame(1, 0, 1, false), false), streamPromise,
      listener);
    Stream stream = streamPromise.get();
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
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    proto.writeDelimitedTo(bos);
    stream.data(
      new DataFrame(stream.getId(), ByteBuffer.wrap(bos.toByteArray()), true),
      new Callback.Adapter());
    assertEquals(HttpStatus.NOT_FOUND_404, listener.getStatus());
    JsonNode jsonNode =
        new ObjectMapper().readTree(new String(listener.getData(),
            StandardCharsets.UTF_8));
    assertEquals(ReplicaNotFoundException.class.getSimpleName(),
      jsonNode.get("RemoteException").get("exception").asText());
  }
}
