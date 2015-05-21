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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataNodeHttp2Ssl {

  private static final String BASEDIR = System.getProperty("test.build.dir",
    "target/test-dir") + "/" + TestDataNodeHttp2Ssl.class.getSimpleName();

  private static MiniDFSCluster CLUSTER;

  private static Configuration CONF;

  private static String KEYSTORE_DIR;

  private static String SSL_CONF_DIR;

  private static URLConnectionFactory CONN_FACTORY;

  private static WebHdfsFileSystem FS;

  private static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  private static Http2ResponseHandler RESPONSE_HANDLER;

  private static SSLFactory SSL_FACTORY;

  @BeforeClass
  public static void setUp() throws Exception {
    CONF = new Configuration();
    CONF.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY,
      HttpConfig.Policy.HTTPS_ONLY.name());
    CONF.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    CONF.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    KEYSTORE_DIR = new File(BASEDIR).getAbsolutePath();
    SSL_CONF_DIR = KeyStoreTestUtil.getClasspathDir(TestDataNodeHttp2Ssl.class);

    KeyStoreTestUtil.setupSSLConfig(KEYSTORE_DIR, SSL_CONF_DIR, CONF, false);

    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    InetSocketAddress addr = CLUSTER.getNameNode().getHttpsAddress();
    CONF.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
      NetUtils.getHostPortString(addr));

    CONN_FACTORY = URLConnectionFactory.newDefaultURLConnectionFactory(CONF);

    FS =
        WebHdfsTestUtil.getWebHdfsFileSystem(CONF,
          WebHdfsConstants.SWEBHDFS_SCHEME);

    SSL_FACTORY = new SSLFactory(SSLFactory.Mode.CLIENT, CONF);
    SSL_FACTORY.init();

    Http2ClientInitializer initializer =
        new Http2ClientInitializer(SSL_FACTORY.createSSLEngine());
    Bootstrap bootstrap =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .remoteAddress("127.0.0.1",
              CLUSTER.getDataNodes().get(0).getInfoSecurePort())
            .handler(initializer);
    CHANNEL = bootstrap.connect().syncUninterruptibly().channel();
    initializer.awaitUpgrade(10, TimeUnit.SECONDS);
    RESPONSE_HANDLER = initializer.getResponseHandler();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CHANNEL != null) {
      CHANNEL.close().syncUninterruptibly();
    }
    WORKER_GROUP.shutdownGracefully();
    if (SSL_FACTORY != null) {
      SSL_FACTORY.destroy();
    }
    if (FS != null) {
      FS.close();
    }
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(KEYSTORE_DIR, SSL_CONF_DIR);
  }

  @Test
  public void test() throws IOException, InterruptedException,
      ExecutionException {
    DataNodeHttp2TestHelper.test(FS, CONN_FACTORY, CHANNEL, RESPONSE_HANDLER);
  }
}
