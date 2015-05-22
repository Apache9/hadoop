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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.web.AbstractDataNodeTransportMessage;
import org.apache.hadoop.hdfs.server.datanode.web.ByteArrayTransportMessage;
import org.apache.hadoop.hdfs.server.datanode.web.DataNodeApplicationHandler;
import org.apache.hadoop.hdfs.server.datanode.web.InputStreamTransportMessage;
import org.apache.hadoop.hdfs.server.datanode.web.OutputStreamTransportMessage;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LimitInputStream;

import com.google.common.collect.ImmutableMap;

/**
 * Handle web hdfs requests.
 */
@InterfaceAudience.Private
public class WebHdfsHandler implements DataNodeApplicationHandler {

  static final Log LOG = LogFactory.getLog(WebHdfsHandler.class);

  public static final String APPLICATION_OCTET_STREAM =
      "application/octet-stream";

  public static final String APPLICATION_JSON_UTF8 =
      "application/json; charset=utf-8";
  public static final String WEBHDFS_PREFIX = WebHdfsFileSystem.PATH_PREFIX;

  public static final int WEBHDFS_PREFIX_LENGTH = WEBHDFS_PREFIX.length();

  private final Configuration conf;

  private final Configuration confForCreate;

  public WebHdfsHandler(Configuration conf, Configuration confForCreate) {
    this.conf = conf;
    this.confForCreate = confForCreate;
  }

  private String path;
  private ParameterParser params;
  private UserGroupInformation ugi;

  @Override
  public AbstractDataNodeTransportMessage handle(String uri,
      final HttpMethod method) throws IOException, InterruptedException {
    QueryStringDecoder queryString = new QueryStringDecoder(uri);
    params = new ParameterParser(queryString, conf);
    DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(params);
    ugi = ugiProvider.ugi();
    path = params.path();

    injectToken();
    return ugi.doAs(
        new PrivilegedExceptionAction<AbstractDataNodeTransportMessage>() {
          @Override
          public AbstractDataNodeTransportMessage run() throws Exception {
            return handle(method);
          }
        });
  }

  private AbstractDataNodeTransportMessage handle(HttpMethod method)
      throws IOException, URISyntaxException {
    String op = params.op();
    if (PutOpParam.Op.CREATE.name().equalsIgnoreCase(op) && method == PUT) {
      return onCreate();
    } else if (PostOpParam.Op.APPEND.name().equalsIgnoreCase(op)
        && method == POST) {
      return onAppend();
    } else if (GetOpParam.Op.OPEN.name().equalsIgnoreCase(op)
        && method == GET) {
      return onOpen();
    } else if (GetOpParam.Op.GETFILECHECKSUM.name().equalsIgnoreCase(op)
        && method == GET) {
      return onGetFileChecksum();
    } else {
      throw new IllegalArgumentException("Invalid operation " + op);
    }
  }

  private AbstractDataNodeTransportMessage onCreate() throws IOException,
      URISyntaxException {
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final short replication = params.replication();
    final long blockSize = params.blockSize();
    final FsPermission permission = params.permission();

    EnumSet<CreateFlag> flags =
        params.overwrite() ? EnumSet
            .of(CreateFlag.CREATE, CreateFlag.OVERWRITE) : EnumSet
            .of(CreateFlag.CREATE);

    DFSClient dfsClient = newDfsClient(nnId, confForCreate);
    try {
      OutputStream out =
          dfsClient.createWrappedOutputStream(dfsClient.create(path,
            permission, flags, replication, blockSize, null, bufferSize, null),
            null);

      final URI uri = new URI(HDFS_URI_SCHEME, nnId, path, null, null);
      Map<String, Object> headers =
          ImmutableMap.<String, Object> of(LOCATION, uri.toString(),
            CONTENT_LENGTH, 0, CONNECTION, CLOSE);
      dfsClient = null;
      return new OutputStreamTransportMessage(CREATED, headers,
          new OutputStreamWithExternalCloseables(out, dfsClient));
    } finally {
      IOUtils.cleanup(LOG, dfsClient);
    }
  }

  private AbstractDataNodeTransportMessage onAppend() throws IOException {
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();

    DFSClient dfsClient = newDfsClient(nnId, conf);
    try {
      OutputStream out =
          dfsClient.append(path, bufferSize, EnumSet.of(CreateFlag.APPEND),
            null, null);
      Map<String, Object> headers =
          ImmutableMap
              .<String, Object> of(CONTENT_LENGTH, 0, CONNECTION, CLOSE);
      dfsClient = null;
      return new OutputStreamTransportMessage(OK, headers,
          new OutputStreamWithExternalCloseables(out, dfsClient));
    } finally {
      IOUtils.cleanup(LOG, dfsClient);
    }
  }

  private AbstractDataNodeTransportMessage onOpen() throws IOException {
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final long offset = params.offset();
    final long length = params.length();

    ImmutableMap.Builder<String, Object> headersBuilder =
        ImmutableMap
            .<String, Object> builder()
            // Allow the UI to access the file
            .put(ACCESS_CONTROL_ALLOW_METHODS, GET)
            .put(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .put(CONTENT_TYPE, APPLICATION_OCTET_STREAM).put(CONNECTION, CLOSE);

    DFSClient dfsClient = newDfsClient(nnId, conf);
    try {
      HdfsDataInputStream in =
          dfsClient.createWrappedInputStream(dfsClient.open(path, bufferSize,
            true));
      in.seek(offset);

      long contentLength = in.getVisibleLength() - offset;
      if (length >= 0) {
        contentLength = Math.min(contentLength, length);
      }
      final InputStream data;
      if (contentLength >= 0) {
        headersBuilder.put(CONTENT_LENGTH, contentLength);
        data = new LimitInputStream(in, contentLength);
      } else {
        data = in;
      }
      dfsClient = null;
      return new InputStreamTransportMessage(OK, headersBuilder.build(),
          new InputStreamWithExternalCloseables(data, dfsClient));
    } finally {
      IOUtils.cleanup(LOG, dfsClient);
    }
  }

  private AbstractDataNodeTransportMessage onGetFileChecksum()
      throws IOException {
    MD5MD5CRC32FileChecksum checksum = null;
    final String nnId = params.namenodeId();
    DFSClient dfsclient = newDfsClient(nnId, conf);
    try {
      checksum = dfsclient.getFileChecksum(path, Long.MAX_VALUE);
    } finally {
      IOUtils.cleanup(LOG, dfsclient);
    }
    final byte[] js = JsonUtil.toJsonString(checksum).getBytes(Charsets.UTF_8);
    Map<String, Object> headers =
        ImmutableMap.<String, Object> of(CONTENT_TYPE, APPLICATION_JSON_UTF8,
          CONTENT_LENGTH, js.length, CONNECTION, CLOSE);
    return new ByteArrayTransportMessage(OK, headers, js);
  }

  private static DFSClient newDfsClient(String nnId, Configuration conf)
      throws IOException {
    URI uri = URI.create(HDFS_URI_SCHEME + "://" + nnId);
    return new DFSClient(uri, conf);
  }

  private void injectToken() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      Token<DelegationTokenIdentifier> token = params.delegationToken();
      token.setKind(HDFS_DELEGATION_KIND);
      ugi.addToken(token);
    }
  }
}
