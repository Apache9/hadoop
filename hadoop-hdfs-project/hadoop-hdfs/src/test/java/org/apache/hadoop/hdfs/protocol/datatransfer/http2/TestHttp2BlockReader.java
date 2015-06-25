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
package org.apache.hadoop.hdfs.protocol.datatransfer.http2;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.Http2BlockReader;
import org.apache.hadoop.hdfs.Http2ConnectionPool;
import org.apache.hadoop.hdfs.Http2ConnectionPool.SessionAndStreamId;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestHttp2BlockReader {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setBoolean(DFSConfigKeys.DFS_HTTP2_VERBOSE_KEY, true);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  @After
  public void tearDown() throws Exception {
    for (RemoteIterator<LocatedFileStatus> iter =
        CLUSTER.getFileSystem().listFiles(new Path("/"), false); iter.hasNext();) {
      CLUSTER.getFileSystem().delete(iter.next().getPath(), true);
    }
  }

  @Test
  public void test() throws IllegalArgumentException, IOException {
    String fileName = "/test2";
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path(fileName));
    int len = 4096 * 100;
    byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();
    ExtendedBlock block =
        CLUSTER.getFileSystem().getClient().getLocatedBlocks(fileName, 0).get(0).getBlock();
    Http2ConnectionPool http2ConnPool = new Http2ConnectionPool();
    SessionAndStreamId sessionAndStreamId =
        http2ConnPool.connect(new InetSocketAddress("127.0.0.1", CLUSTER.getDataNodes().get(0)
            .getInfoPort()));
    int offset = 0;
    int length = len - offset;
    BlockReader blockReader =
        new Http2BlockReader(sessionAndStreamId, block.toString(), block, offset, true,
            "clientName", length, null);
    byte[] result = new byte[length];
    blockReader.readFully(result, 0, length);
    byte[] expected = new byte[length];
    for (int i = 0; i < length; ++i) {
      expected[i] = b[i + offset];
    }
    Arrays.equals(result, expected);
    http2ConnPool.close();
  }

  @Test
  public void testPerformance() throws IllegalArgumentException, IOException, InterruptedException {
    String fileName = "/test";
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path(fileName));
    final int len = 6 * 1024 * 1024 - 10;
    final byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();

    final ExtendedBlock block =
        CLUSTER.getFileSystem().getClient().getLocatedBlocks(fileName, 0).get(0).getBlock();

    final Http2ConnectionPool http2ConnectionPool = new Http2ConnectionPool();

    int concurrency = 50;

    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency,
          new ThreadFactoryBuilder().setNameFormat("Http2-BlockReader-%d").setDaemon(true).build());
    for (int i = 0; i < concurrency; ++i) {
      final int index = i;
      executor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            SessionAndStreamId sessionAndStreamId =
                http2ConnectionPool.connect(new InetSocketAddress("127.0.0.1", CLUSTER
                    .getDataNodes().get(0).getInfoPort()));
            int offset = ThreadLocalRandom.current().nextInt(0, len);
            int length = len - offset;
            BlockReader blockReader =
                new Http2BlockReader(sessionAndStreamId, block.toString(), block, offset, true,
                    "clientName" + index, length, null);
            byte[] expected = new byte[length];
            for (int j = 0; j < length; ++j) {
              expected[j] = b[j + offset];
            }
            byte[] result = new byte[length];
            blockReader.readFully(result, 0, length);
            Arrays.equals(expected, result);
            System.out.println("succ");
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

      });
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.MINUTES));
    http2ConnectionPool.close();

  }

}
