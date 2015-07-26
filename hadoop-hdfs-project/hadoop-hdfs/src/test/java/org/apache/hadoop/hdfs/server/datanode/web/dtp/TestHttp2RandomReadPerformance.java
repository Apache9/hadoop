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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.Http2BlockReader;
import org.apache.hadoop.hdfs.Http2ConnectionPool;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.PeerCache;
import org.apache.hadoop.hdfs.RemoteBlockReader2;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHttp2RandomReadPerformance {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static Path FILE = new Path("/test");

  private static int LEN = 2048;

  private static int CONNECTION_COUNT = 20;

  private boolean http2;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  public TestHttp2RandomReadPerformance(boolean http2) {
    this.http2 = http2;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
      2 * CONNECTION_COUNT);
    CONF.setBoolean(DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY, false);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
    try (FSDataOutputStream out = CLUSTER.getFileSystem().create(FILE)) {
      byte[] b = new byte[LEN];
      ThreadLocalRandom.current().nextBytes(b);
      out.write(b);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  @Test
  public void test() throws IOException {
    LocatedBlock block =
        CLUSTER.getFileSystem().getClient()
            .getLocatedBlocks(FILE.toUri().toString(), 0).get(0);
    EventLoopGroup workerGroup = null;
    PeerCache peerCache = null;
    List<Http2ConnectionPool> connPoolList =
        new ArrayList<Http2ConnectionPool>();
    byte[] buf = new byte[LEN];
    try (FileSystem fs = FileSystem.get(CONF)) {
      DFSClient client = ((DistributedFileSystem) fs).getClient();
      if (http2) {
        final DfsClientConf clientConf = new DfsClientConf(CONF);
        workerGroup = new NioEventLoopGroup();
        for (int i = 0; i < CONNECTION_COUNT; i++) {
          Http2ConnectionPool connPool =
              new Http2ConnectionPool(CONF, clientConf, workerGroup);
          connPoolList.add(connPool);
          Http2BlockReader reader =
              Http2BlockReader
                  .newBlockReader(connPool, FILE.toUri().toString(),
                    block.getBlock(), block.getBlockToken(), 0, LEN, true,
                    "testClient", block.getLocations()[0],
                    client.getDefaultReadCachingStrategy());
          reader.close();
        }
      } else {
        peerCache =
            new PeerCache(CONNECTION_COUNT * 2, TimeUnit.DAYS.toMillis(1));
        for (int i = 0; i < CONNECTION_COUNT; i++) {
          InetSocketAddress addr =
              NetUtils.createSocketAddr(block.getLocations()[0].getXferAddr());
          Peer peer =
              client.newConnectedPeer(addr, block.getBlockToken(),
                block.getLocations()[0]);
          BlockReader reader =
              RemoteBlockReader2.newBlockReader(FILE.toUri().toString(),
                block.getBlock(), block.getBlockToken(), 0, LEN, true,
                "testClient", peer, block.getLocations()[0], peerCache,
                client.getDefaultReadCachingStrategy());
          reader.readFully(buf, 0, LEN);
          reader.close();
        }
      }
      long start = System.currentTimeMillis();
      if (http2) {
        for (int i = 0; i < CONNECTION_COUNT; i++) {
          Http2BlockReader reader =
              Http2BlockReader.newBlockReader(connPoolList.get(i), FILE.toUri()
                  .toString(), block.getBlock(), block.getBlockToken(), 0, LEN,
                true, "testClient", block.getLocations()[0], client
                    .getDefaultReadCachingStrategy());
          reader.readFully(buf, 0, LEN);
          reader.close();
        }
      } else {
        for (int i = 0; i < CONNECTION_COUNT; i++) {
          Peer peer = peerCache.get(block.getLocations()[0], false);
          BlockReader reader =
              RemoteBlockReader2.newBlockReader(FILE.toUri().toString(),
                block.getBlock(), block.getBlockToken(), 0, LEN, true,
                "testClient", peer, block.getLocations()[0], peerCache,
                client.getDefaultReadCachingStrategy());
          reader.readFully(buf, 0, LEN);
          reader.close();
        }
      }
      long cost = System.currentTimeMillis() - start;
      System.err.println("******* time based on " + (http2 ? "http2 " : "tcp ")
          + cost + "ms");
    } finally {
      if (http2) {
        for (Http2ConnectionPool connPool : connPoolList) {
          connPool.close();
        }
        if (workerGroup != null) {
          workerGroup.shutdownGracefully();
        }
      } else {
        if (peerCache != null) {
          peerCache.close();
        }
      }
    }

  }
}
