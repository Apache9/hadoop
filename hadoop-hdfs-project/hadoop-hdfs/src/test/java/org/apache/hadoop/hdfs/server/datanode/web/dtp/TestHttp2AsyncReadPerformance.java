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

import static org.junit.Assert.assertTrue;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestHttp2AsyncReadPerformance {
  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static Path FILE = new Path("/test");

  private static int FILE_LEN = 1024 * 1024;

  private static byte[] BUF = new byte[FILE_LEN];

  private static int LEN = 2048;

  private static int CONNECTION_COUNT = 20;

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
      2 * CONNECTION_COUNT);
    CONF.setBoolean(DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY, false);
    CONF.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
    ThreadLocalRandom.current().nextBytes(BUF);
    try (FSDataOutputStream out = CLUSTER.getFileSystem().create(FILE)) {
      out.write(BUF);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  private void read(final AtomicBoolean succ, final CountDownLatch latch,
      final Semaphore concurrencyControl, final DFSInputStream in,
      final int position, final byte[] b, final int off, final int len)
      throws IOException {
    in.asyncRead(position, b, off, len).addListener(
      new FutureListener<Integer>() {

        @Override
        public void operationComplete(Future<Integer> future) throws Exception {
          if (future.isSuccess()) {
            int read = future.get().intValue();
            for (int i = 0; i < read; i++) {
              if (BUF[position + i] != b[off + i]) {
                succ.set(false);
                concurrencyControl.release();
                latch.countDown();
              }
            }
            if (len == read) {
              concurrencyControl.release();
              latch.countDown();
            } else {
              read(succ, latch, concurrencyControl, in, position + read, b, off
                  + read, len - read);
            }
          } else {
            future.cause().printStackTrace();
            concurrencyControl.release();
            succ.set(false);
            latch.countDown();
          }
        }
      });
  }

  @Test
  public void test() throws IOException, InterruptedException {
    int readCount = 10000;
    AtomicBoolean succ = new AtomicBoolean(true);
    CountDownLatch latch = new CountDownLatch(readCount);
    Semaphore concurrencyControl = new Semaphore(1000);
    int seekBound = FILE_LEN - LEN;
    try (DFSInputStream in =
        CLUSTER.getFileSystem().getClient().open(FILE.toString())) {
      for (int i = 0; i < readCount; i++) {
        concurrencyControl.acquire();
        read(succ, latch, concurrencyControl, in, ThreadLocalRandom.current()
            .nextInt(seekBound), new byte[LEN], 0, 2048);
      }
      latch.await();
    }
    assertTrue(succ.get());
  }
}
