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
package org.apache.hadoop.hdfs.server.datanode.web.http2;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpUtil;
import io.netty.util.concurrent.Promise;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Type;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class DataNodeHttp2TestHelper {

  private static final Log LOG = LogFactory
      .getLog(DataNodeHttp2TestHelper.class);

  public static String getRedirectUri(WebHdfsFileSystem fs,
      final URLConnectionFactory connFactory, final HttpOpParam.Op op,
      Path path, Param<?, ?>... parameters) throws IOException,
      InterruptedException {
    final URL url = fs.toUrl(op, path, parameters);
    return UserGroupInformation.getCurrentUser().doAs(
      new PrivilegedExceptionAction<String>() {

        @Override
        public String run() throws Exception {
          HttpURLConnection conn =
              (HttpURLConnection) connFactory.openConnection(url);
          conn.setRequestMethod(op.getType().toString());
          conn.setInstanceFollowRedirects(false);
          if (op.getType() == Type.PUT || op.getType() == Type.POST) {
            conn.setDoOutput(true);
            if (!op.getDoOutput()) {
              conn.getOutputStream().close();
            } else {
              conn.setRequestProperty("Content-Type",
                MediaType.APPLICATION_OCTET_STREAM);
              conn.setChunkedStreamingMode(32 << 10);
            }
          } else {
            conn.setDoOutput(op.getDoOutput());
          }
          conn.connect();
          try {
            LOG.info("connect to " + url + ", code=" + conn.getResponseCode());
            URL redirectUrl = new URL(conn.getHeaderField("Location"));
            return redirectUrl.getPath() + "?" + redirectUrl.getQuery();
          } finally {
            conn.disconnect();
          }
        }
      });
  }

  public static void test(WebHdfsFileSystem fs,
      URLConnectionFactory connFactory, Channel channel,
      Http2ResponseHandler responseHandler) throws IOException,
      InterruptedException, ExecutionException {
    int streamId = 11;

    String createUri =
        getRedirectUri(fs, connFactory, PutOpParam.Op.CREATE,
          new Path("/test"), new PermissionParam(new FsPermission("644")),
          new OverwriteParam(true), new BufferSizeParam(4096),
          new ReplicationParam((short) 1),
          new BlockSizeParam(fs.getDefaultBlockSize()));
    FullHttpRequest createRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
            createUri, channel.alloc().buffer(1).writeByte(1));
    createRequest.headers().add(HttpUtil.ExtensionHeaderNames.STREAM_ID.text(),
      streamId);
    Promise<FullHttpResponse> createPromise = channel.eventLoop().newPromise();
    synchronized (responseHandler) {
      channel.writeAndFlush(createRequest);
      responseHandler.put(streamId, createPromise);
    }
    assertEquals(HttpResponseStatus.CREATED, createPromise.get().status());

    streamId += 2;
    String appendUri =
        getRedirectUri(fs, connFactory, PostOpParam.Op.APPEND,
          new Path("/test"), new BufferSizeParam(4096));
    FullHttpRequest appendRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
            appendUri, channel.alloc().buffer(1).writeByte(2));
    createRequest.headers().add(HttpUtil.ExtensionHeaderNames.STREAM_ID.text(),
      streamId);
    Promise<FullHttpResponse> appendPromise = channel.eventLoop().newPromise();
    synchronized (responseHandler) {
      channel.writeAndFlush(appendRequest);
      responseHandler.put(streamId, appendPromise);
    }
    assertEquals(HttpResponseStatus.OK, appendPromise.get().status());

    streamId += 2;
    String openUri =
        getRedirectUri(fs, connFactory, GetOpParam.Op.OPEN, new Path("/test"),
          new OffsetParam(1L));
    FullHttpRequest openRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
            openUri);
    openRequest.headers().add(HttpUtil.ExtensionHeaderNames.STREAM_ID.text(),
      streamId);
    Promise<FullHttpResponse> openPromise = channel.eventLoop().newPromise();
    synchronized (responseHandler) {
      channel.writeAndFlush(openRequest);
      responseHandler.put(streamId, openPromise);
    }
    assertEquals(HttpResponseStatus.OK, openPromise.get().status());
    ByteBuf openContent = openPromise.get().content();
    assertEquals(1, openContent.readableBytes());
    assertEquals(2, openContent.readByte() & 0xFF);

    streamId += 2;
    String getFileChecksumUri =
        getRedirectUri(fs, connFactory, GetOpParam.Op.GETFILECHECKSUM,
          new Path("/test"));
    FullHttpRequest getFileChecksumRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
            getFileChecksumUri);
    getFileChecksumRequest.headers().add(
      HttpUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
    Promise<FullHttpResponse> getFileChecksumPromise =
        channel.eventLoop().newPromise();
    synchronized (responseHandler) {
      channel.writeAndFlush(getFileChecksumRequest);
      responseHandler.put(streamId, getFileChecksumPromise);
    }
    assertEquals(HttpResponseStatus.OK, getFileChecksumPromise.get().status());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteBuf getFileChecksumContent = getFileChecksumPromise.get().content();
    getFileChecksumContent.readBytes(bos,
      getFileChecksumContent.readableBytes());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonChecksum =
        mapper.readTree(bos.toString()).get(FileChecksum.class.getSimpleName());
    MD5MD5CRC32FileChecksum expectedChecksum =
        fs.getFileChecksum(new Path("/test"));
    assertEquals(expectedChecksum.getAlgorithmName(),
      jsonChecksum.get("algorithm").asText());
    assertEquals(expectedChecksum.getLength(), jsonChecksum.get("length")
        .asInt());
    assertEquals(StringUtils.byteToHexString(expectedChecksum.getBytes()),
      jsonChecksum.get("bytes").asText());
  }
}
