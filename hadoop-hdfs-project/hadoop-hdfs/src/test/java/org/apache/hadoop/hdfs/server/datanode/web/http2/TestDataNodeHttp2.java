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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataNodeHttp2 {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static URLConnectionFactory CONN_FACTORY;

  private static WebHdfsFileSystem FS;

  private static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  private static Http2ResponseHandler RESPONSE_HANDLER;

  @BeforeClass
  public static void setUp() throws IOException, URISyntaxException,
      TimeoutException {
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    CONN_FACTORY = URLConnectionFactory.newDefaultURLConnectionFactory(CONF);

    FS =
        WebHdfsTestUtil.getWebHdfsFileSystem(CONF,
          WebHdfsConstants.WEBHDFS_SCHEME);

    Http2ClientInitializer initializer = new Http2ClientInitializer(null);
    Bootstrap bootstrap =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .remoteAddress("127.0.0.1",
              CLUSTER.getDataNodes().get(0).getInfoPort()).handler(initializer);
    CHANNEL = bootstrap.connect().syncUninterruptibly().channel();
    initializer.awaitUpgrade(10, TimeUnit.SECONDS);
    RESPONSE_HANDLER = initializer.getResponseHandler();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (CHANNEL != null) {
      CHANNEL.close().syncUninterruptibly();
    }
    WORKER_GROUP.shutdownGracefully();
    if (FS != null) {
      FS.close();
    }
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  @Test
  public void test() throws IOException, InterruptedException,
      ExecutionException {
    DataNodeHttp2TestHelper.test(FS, CONN_FACTORY, CHANNEL, RESPONSE_HANDLER);
  }
}
