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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.Http2BlockReader;
import org.apache.hadoop.hdfs.Http2ConnectionPool;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestHttp2BlockReader {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static Http2ConnectionPool CONN_POOL;

  @BeforeClass
  public static void setUp() throws Exception {
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
    CONN_POOL = new Http2ConnectionPool(CONF, new NioEventLoopGroup());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (CONN_POOL != null) {
      CONN_POOL.close();
    }
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
    int len = 6 * 1024 * 1024 - 10;
    byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();
    LocatedBlock block =
        CLUSTER.getFileSystem().getClient().getLocatedBlocks(fileName, 0)
            .get(0);
    int offset = 1;
    int length = len - offset;
    byte[] result = new byte[length];
    Http2BlockReader blockReader =
        Http2BlockReader.newBlockReader(CONN_POOL, fileName, block.getBlock(),
          block.getBlockToken(), offset, length, true, "clientName",
          block.getLocations()[0], null);
    try {
      blockReader.readFully(result, 0, length);
    } finally {
      blockReader.close();
    }
    assertArrayEquals(Arrays.copyOfRange(b, offset, len), result);
  }

  @Test
  public void testConcurrency() throws IllegalArgumentException, IOException,
      InterruptedException {
    final String fileName = "/test";
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path(fileName));
    final int len = 1024 * 20 - 10;
    final byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();

    final LocatedBlock block =
        CLUSTER.getFileSystem().getClient().getLocatedBlocks(fileName, 0)
            .get(0);
    int concurrency = 100;
    final AtomicBoolean succ = new AtomicBoolean(true);
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency, new ThreadFactoryBuilder()
            .setNameFormat("Http2-BlockReader-%d").setDaemon(true).build());
    for (int i = 0; i < concurrency; ++i) {
      final int index = i;
      executor.execute(new Runnable() {

        @Override
        public void run() {
          for (int i = 0; i < 100; i++) {
            int offset = 0;
            Http2BlockReader blockReader = null;
            try {
              offset = ThreadLocalRandom.current().nextInt(0, len);
              int length = len - offset;
              blockReader =
                  Http2BlockReader.newBlockReader(CONN_POOL, fileName,
                    block.getBlock(), block.getBlockToken(), offset, length,
                    true, "clientName" + index, block.getLocations()[0], null);
              byte[] result = new byte[length];
              blockReader.readFully(result, 0, length);
              assertArrayEquals(Arrays.copyOfRange(b, offset, len), result);
            } catch (Throwable t) {
              t.printStackTrace();
              succ.set(false);
              break;
            } finally {
              if (blockReader != null) {
                try {
                  blockReader.close();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            }
          }
        }
      });
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.MINUTES));
  }

}
