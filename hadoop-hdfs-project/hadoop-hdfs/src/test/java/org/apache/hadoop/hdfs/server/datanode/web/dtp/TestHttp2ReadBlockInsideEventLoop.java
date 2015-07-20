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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.http2.ClientHttp2ConnectionHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestHttp2ReadBlockInsideEventLoop {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static Path FILE = new Path("/test");

  private static int LEN = 2048;

  private static Channel CHANNEL;

  private static NioEventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static int READ_COUNT = 100000;

  private static TestHttp2SmallReadHandler HANDLER =
      new TestHttp2SmallReadHandler();

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setBoolean(DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY, false);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
    try (FSDataOutputStream out = CLUSTER.getFileSystem().create(FILE)) {
      byte[] b = new byte[LEN];
      ThreadLocalRandom.current().nextBytes(b);
      out.write(b);
    }
    int port = CLUSTER.getDataNodes().get(0).getInfoPort();
    CHANNEL =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(
                  ClientHttp2ConnectionHandler.create(ch, CONF), HANDLER);
              }

            }).connect(new InetSocketAddress("127.0.0.1", port)).sync()
            .channel();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CHANNEL != null) {
      CHANNEL.close();
    }
    if (WORKER_GROUP != null) {
      WORKER_GROUP.shutdownGracefully();
    }
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  @Test
  public void testHttp2() throws IOException, InterruptedException {
    LocatedBlock block =
        CLUSTER.getFileSystem().getClient()
            .getLocatedBlocks(FILE.toString(), 0).get(0);
    CHANNEL.writeAndFlush(new ReadBlockTestContext(LEN, READ_COUNT, block));
    long cost = HANDLER.getCost();
    System.err.println("******* time based on http2 " + cost + "ms");
  }

  @Test
  public void testTcp() throws IOException {
    long cost;
    try (FSDataInputStream in = CLUSTER.getFileSystem().open(FILE)) {
      long start = System.nanoTime();
      byte[] buf = new byte[LEN];
      for (int i = 0; i < READ_COUNT; i++) {
        in.seek(0);
        in.readFully(buf);
      }
      cost = (System.nanoTime() - start) / 1000000;
    }
    System.err.println("******* time based on tcp " + +cost + "ms");
  }
}
