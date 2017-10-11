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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestReadFileOverHttp2 {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static Path FILE = new Path("/test");

  private static Random random = new Random();

  private static byte[] CONTENT;

  @Parameter
  public String eventLoopType;

  @Parameters(name = "EventLoopType = {0}")
  public static List<Object[]> getParams() {
    return Arrays.asList(new Object[] {"NIO"}, new Object[] {"OIO"},
        new Object[] {"EPOLL"});
  }

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setLong(DFSConfigKeys.DFS_CLIENT_HTTP2_MAX_READ_LENGTH_KEY,
        1024 * 1024 * 1024);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
    CONTENT = new byte[4 * 1024 * 1024];
    random.nextBytes(CONTENT);
    FSDataOutputStream out = null;
    try {
      out = CLUSTER.getFileSystem().create(FILE, true,
        8 * 1024, (short) 3, 1024 * 1024);
      out.write(CONTENT);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  private FileSystem getFileSystem() throws IOException {
    Configuration conf = new Configuration(CLUSTER.getNameNodeInfos()[0].conf);
    conf.set(DFSConfigKeys.DFS_CLIENT_HTTP2_EVENT_LOOP_TYPE_KEY, eventLoopType);
    return FileSystem.get(CLUSTER.getURI(0), conf);
  }

  @Test
  public void test() throws IOException {
    byte[] data = new byte[CONTENT.length];
    FileSystem fs = getFileSystem();
    FSDataInputStream in = fs.open(FILE);
    in.readFully(data);
    fs.close();
    in.close();
    assertArrayEquals(CONTENT, data);
  }

  private void pread(FSDataInputStream in) throws IOException {
    int offset = random.nextInt(CONTENT.length);
    int length = random.nextInt(Math.min(64 * 1024, CONTENT.length - offset)) + 1;
    byte[] data = new byte[length];
    for (int totalRead = 0; totalRead < length;) {
      int read =
          in.read(offset + totalRead, data, totalRead, length - totalRead);
      if (read < 0) {
        throw new EOFException();
      }
      totalRead += read;
    }
    for (int i = 0; i < length; i++) {
      assertEquals("offset " + offset + ", position " + i, CONTENT[offset + i],
          data[i]);
    }
  }

  @Test
  public void testPreadMultiThreaded()
      throws IOException, InterruptedException {
    int numThreads = 10;
    final AtomicReferenceArray<Throwable> error =
        new AtomicReferenceArray<Throwable>(numThreads);
    FileSystem fs = null;
    try {
      fs = getFileSystem();
      final FSDataInputStream in = fs.open(FILE);
      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        final int index = i;
        threads[i] = new Thread("test-" + i) {

          @Override
          public void run() {
            try {
              for (int i = 0; i < 100; i++) {
                pread(in);
              }
            } catch (Throwable t) {
              error.set(index, t);
            }
          }
        };
      }
      for (Thread t : threads) {
        t.start();
      }
      for (Thread t : threads) {
        t.join();
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
    for (int i = 0; i < numThreads; i++) {
      assertNull(error.get(i));
    }
  }
}
