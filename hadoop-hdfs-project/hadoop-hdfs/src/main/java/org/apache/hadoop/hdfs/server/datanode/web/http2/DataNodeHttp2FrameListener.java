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

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.AsciiString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.DataNodeUGIProvider;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.ParameterParser;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LimitInputStream;

@InterfaceAudience.Private
public class DataNodeHttp2FrameListener extends Http2FrameAdapter {

  private static final Log LOG = DatanodeHttpServer.LOG;

  public static final AsciiString APPLICATION_JSON_UTF8 = new AsciiString(
      WebHdfsHandler.APPLICATION_JSON_UTF8);

  private final Configuration conf;

  private final Configuration confForCreate;

  private final ExecutorService executor;

  private final SortedMap<Integer, Http2StreamDataFrameReadHandler> streamId2Handler =
      new TreeMap<>();

  private Http2ConnectionEncoder encoder;

  public DataNodeHttp2FrameListener(Configuration conf,
      Configuration confForCreate, ExecutorService executor) {
    this.conf = conf;
    this.confForCreate = confForCreate;
    this.executor = executor;
  }

  public void encoder(Http2ConnectionEncoder encoder) {
    this.encoder = encoder;
  }

  private DFSClient newDFSClient(String nnId, Configuration conf)
      throws IOException {
    return new DFSClient(URI.create(HdfsConstants.HDFS_URI_SCHEME + "://"
        + nnId), conf);
  }

  private void onGetFileChecksum(ChannelHandlerContext ctx, int streamId,
      ParameterParser params) {
    Thread.currentThread().setName("WebHdfs-GetFileChecksum-" + params.path());
    DFSClient client = null;
    try {
      client = newDFSClient(params.namenodeId(), conf);
      MD5MD5CRC32FileChecksum checksum =
          client.getFileChecksum(params.path(), Long.MAX_VALUE);
      byte[] js =
          JsonUtil.toJsonString(checksum).getBytes(StandardCharsets.UTF_8);
      ctx.write(new Http2HeadersFrame(streamId, new DefaultHttp2Headers()
          .status(HttpResponseStatus.OK.codeAsText()), 0, false));
      ctx.write(new Http2DataFrame(streamId, ctx.alloc().buffer(js.length)
          .writeBytes(js), 0, true));
    } catch (Exception e) {
      Http2ExceptionHandler.exceptionCaught(ctx, streamId, e);
    } finally {
      IOUtils.cleanup(LOG, client);
    }
  }

  private static final AsciiString ACCESS_CONTROL_ALLOW_METHODS_VALUE =
      new AsciiString(GET.name());

  private static final AsciiString ACCESS_CONTROL_ALLOW_ORIGIN_VALUE =
      new AsciiString("*");

  private void onOpen(final ChannelHandlerContext ctx, final int streamId,
      final ParameterParser params) {
    Thread.currentThread().setName("WebHdfs-Open-" + params.path());
    DFSClient client = null;
    try {
      client = newDFSClient(params.namenodeId(), conf);
      HdfsDataInputStream in =
          client.createWrappedInputStream(client.open(params.path(),
            params.bufferSize(), true));
      in.seek(params.offset());
      long contentLength = in.getVisibleLength() - params.offset();
      if (params.length() >= 0) {
        contentLength = Math.min(contentLength, params.length());
      }
      InputStream data;
      Http2Headers headers =
          new DefaultHttp2Headers()
              .add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS,
                ACCESS_CONTROL_ALLOW_METHODS_VALUE)
              .add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN,
                ACCESS_CONTROL_ALLOW_ORIGIN_VALUE)
              .add(HttpHeaderNames.CONTENT_TYPE,
                HttpHeaderValues.APPLICATION_OCTET_STREAM);
      if (contentLength >= 0) {
        headers.addLong(HttpHeaderNames.CONTENT_LENGTH, contentLength);
        data = new LimitInputStream(in, contentLength);
      } else {
        data = in;
      }
      ctx.write(new Http2HeadersFrame(streamId, headers, 0, false));
      final DFSClient deferredClient = client;
      // TODO: this could still block event loop, should move the real
      // DFSInputStream.read to a thread pool.
      ctx.writeAndFlush(
        new Http2ChunkedInput(streamId, new ChunkedStream(data) {

          @Override
          public void close() throws Exception {
            try {
              super.close();
            } finally {
              deferredClient.close();
            }
          }

        })).addListener(new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()
              && Http2CodecUtil.getEmbeddedHttp2Exception(future.cause()) == null) {
            encoder.writeRstStream(ctx, streamId,
              Http2Error.INTERNAL_ERROR.code(), ctx.newPromise());
          }
        }
      });
    } catch (Exception e) {
      Http2ExceptionHandler.exceptionCaught(ctx, streamId, e);
    } finally {
      IOUtils.cleanup(LOG, client);
    }
  }

  // TODO: execute in thread pool
  private void onCreate(final ChannelHandlerContext ctx, final int streamId,
      final ParameterParser params) throws IOException, URISyntaxException {
    Http2Headers headers =
        new DefaultHttp2Headers()
            .status(HttpResponseStatus.CREATED.codeAsText())
            .add(
              HttpHeaderNames.LOCATION,
              new AsciiString(new URI(HDFS_URI_SCHEME, params.namenodeId(),
                  params.path(), null, null).toString()))
            .addInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    EnumSet<CreateFlag> flags =
        params.overwrite() ? EnumSet
            .of(CreateFlag.CREATE, CreateFlag.OVERWRITE) : EnumSet
            .of(CreateFlag.CREATE);
    DFSClient client = newDFSClient(params.namenodeId(), confForCreate);
    try {
      OutputStream out =
          client.createWrappedOutputStream(
            client.create(params.path(), params.permission(), flags,
              params.replication(), params.blockSize(), null,
              params.bufferSize(), null), null);
      streamId2Handler.put(streamId, new Http2HdfsWriter(streamId, client, out,
          headers));
      client = null;
    } finally {
      IOUtils.cleanup(LOG, client);
    }

  }

  // TODO: execute in thread pool
  private void onAppend(final ChannelHandlerContext ctx, final int streamId,
      final ParameterParser params) throws IOException {
    Http2Headers headers =
        new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText())
            .addInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    DFSClient client = newDFSClient(params.namenodeId(), confForCreate);
    try {
      OutputStream out =
          client.append(params.path(), params.bufferSize(),
            EnumSet.of(CreateFlag.APPEND), null, null);
      streamId2Handler.put(streamId, new Http2HdfsWriter(streamId, client, out,
          headers));
      client = null;
    } finally {
      IOUtils.cleanup(LOG, client);
    }
  }

  private void handleWebHdfs(final ChannelHandlerContext ctx,
      final int streamId, String uri, final String method) throws Exception {
    QueryStringDecoder queryString = new QueryStringDecoder(uri);
    final ParameterParser params = new ParameterParser(queryString, conf);
    DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(params);
    UserGroupInformation ugi = ugiProvider.ugi();
    if (UserGroupInformation.isSecurityEnabled()) {
      Token<DelegationTokenIdentifier> token = params.delegationToken();
      token.setKind(HDFS_DELEGATION_KIND);
      ugi.addToken(token);
    }
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        String op = params.op();
        if (PutOpParam.Op.CREATE.name().equalsIgnoreCase(op)
            && PUT.name().equals(method)) {
          onCreate(ctx, streamId, params);
        } else if (PostOpParam.Op.APPEND.name().equalsIgnoreCase(op)
            && POST.name().equals(method)) {
          onAppend(ctx, streamId, params);
        } else if (GetOpParam.Op.OPEN.name().equalsIgnoreCase(op)
            && GET.name().equals(method)) {
          executor.execute(new NamePreservingRunnable(new Runnable() {

            @Override
            public void run() {
              onOpen(ctx, streamId, params);
            }

          }));
        } else if (GetOpParam.Op.GETFILECHECKSUM.name().equalsIgnoreCase(op)
            && GET.name().equals(method)) {
          executor.execute(new NamePreservingRunnable(new Runnable() {

            @Override
            public void run() {
              onGetFileChecksum(ctx, streamId, params);
            }

          }));
        } else {
          throw new IllegalArgumentException("Invalid operation " + op);
        }
        return null;
      }
    });
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int padding, boolean endStream)
      throws Http2Exception {
    String uri = headers.path() != null ? headers.path().toString() : "";
    if (uri.startsWith(WEBHDFS_PREFIX)) {
      String method =
          headers.method() != null ? headers.method().toString() : "";
      try {
        handleWebHdfs(ctx, streamId, uri, method);
      } catch (Exception e) {
        throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR,
          e, "");
      }
    } else {
      // just use http.
      throw Http2Exception.streamError(streamId, Http2Error.HTTP_1_1_REQUIRED,
        "Use Http/1.1 to get proxy result");
    }
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int streamDependency, short weight,
      boolean exclusive, int padding, boolean endStream) throws Http2Exception {
    onHeadersRead(ctx, streamId, headers, padding, endStream);
  }

  @Override
  public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data,
      int padding, boolean endOfStream) throws Http2Exception {
    Http2StreamDataFrameReadHandler handler =
        endOfStream ? streamId2Handler.remove(streamId) : streamId2Handler
            .get(streamId);
    if (handler != null) {
      try {
        return handler.onDataRead(ctx, data, padding, endOfStream);
      } catch (Http2Exception e) {
        if (!endOfStream) {
          streamId2Handler.remove(streamId);
        }
        throw e;
      }
    } else {
      throw Http2Exception.streamError(streamId, Http2Error.REFUSED_STREAM,
        "No handler for stream %d", streamId);
    }
  }

  @Override
  public void onRstStreamRead(ChannelHandlerContext ctx, int streamId,
      long errorCode) throws Http2Exception {
    Http2StreamDataFrameReadHandler handler = streamId2Handler.remove(streamId);
    if (handler != null) {
      handler.onRstStreamRead(ctx, errorCode);
    }

  }

  @Override
  public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId,
      long errorCode, ByteBuf debugData) throws Http2Exception {
    for (Iterator<Http2StreamDataFrameReadHandler> iter =
        streamId2Handler.tailMap(lastStreamId + 1).values().iterator(); iter
        .hasNext();) {
      iter.next().onRstStreamRead(ctx, errorCode);
      iter.remove();
    }
  }

}