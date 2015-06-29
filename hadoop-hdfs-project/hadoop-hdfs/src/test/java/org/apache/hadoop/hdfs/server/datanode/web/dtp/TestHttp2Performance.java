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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.log4j.lf5.util.StreamUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestHttp2Performance {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static final Configuration CONF2 = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static MiniDFSCluster CLUSTER2;

  @BeforeClass
  public static void setUp() throws Exception {
    CONF2.setBoolean(DFSConfigKeys.DFS_HTTP2_VERBOSE_KEY, true);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    CONF2.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, CONF.get(MiniDFSCluster.HDFS_MINIDFS_BASEDIR)
        + "_2");
    CONF2.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);

    CLUSTER2 = new MiniDFSCluster.Builder(CONF2).numDataNodes(1).build();
    CLUSTER2.waitActive();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }

    if (CLUSTER2 != null) {
      CLUSTER2.shutdown();
    }
  }

  @Test
  public void testTcp() throws IllegalArgumentException, IOException, InterruptedException {
    final String fileName = "/test";
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path(fileName));
    final int len = 6 * 1024 * 1024 - 10;
    byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();
    int concurrency = 50;
    long start = System.currentTimeMillis();
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency,
          new ThreadFactoryBuilder().setNameFormat("TCP-DFSClient-%d").setDaemon(true).build());

    for (int i = 0; i < concurrency; ++i) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            read(fileName, len, false);
          } catch (IllegalArgumentException | IOException e) {
            assertTrue(false);
          }
        }
      });
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.MINUTES));
    System.err.println("******* time based on tcp " + (System.currentTimeMillis() - start));
  }

  @Test
  public void testHttp2() throws IllegalArgumentException, IOException, InterruptedException {
    final String fileName = "/test2";
    FSDataOutputStream out = FileSystem.get(CONF2).create(new Path(fileName));
    final int len = 6 * 1024 * 1024 - 10;
    byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();
    int concurrency = 50;
    long start = System.currentTimeMillis();
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency,
          new ThreadFactoryBuilder().setNameFormat("Http2-DFSClient-%d").setDaemon(true).build());

    for (int i = 0; i < concurrency; ++i) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            read(fileName, len, true);
          } catch (IllegalArgumentException | IOException e) {
            assertTrue(false);
          }
        }
      });
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.MINUTES));
    System.err.println("******* time based on http2 " + (System.currentTimeMillis() - start));
  }

  private void read(String fileName, int len, boolean useHttp2) throws IllegalArgumentException,
      IOException {
    FSDataInputStream inputStream = null;
    if (useHttp2) {
      inputStream = FileSystem.get(CONF2).open(new Path(fileName));
    } else {
      inputStream = FileSystem.get(CONF).open(new Path(fileName));
    }
    assertEquals(len, StreamUtils.getBytes(inputStream).length);
    inputStream.close();
  }

}
