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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHttp2LargeReadPerformance {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static Path FILE = new Path("/test");

  private static int LEN = 512 * 1024 * 1024;

  private boolean http2;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  public TestHttp2LargeReadPerformance(boolean http2) {
    this.http2 = http2;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setBoolean(DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY, false);
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();
    try (FSDataOutputStream out =
        CLUSTER.getFileSystem().create(FILE, false, 4096, (short) 1, LEN)) {
      byte[] b = new byte[4096];
      for (int remaining = LEN; remaining > 0;) {
        ThreadLocalRandom.current().nextBytes(b);
        int toWrite = Math.min(remaining, b.length);
        out.write(b, 0, toWrite);
        remaining -= toWrite;
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  private void consume(FSDataInputStream in, int len, byte[] buf)
      throws IOException {
    for (int remaining = len; remaining > 0;) {
      int read = in.read(buf, 0, Math.min(remaining, buf.length));
      if (read < 0) {
        throw new EOFException("Unexpected EOF got, should still have "
            + remaining + " bytes remaining");
      }
      remaining -= read;
    }
  }

  @Test
  public void test() throws IllegalArgumentException, IOException,
      InterruptedException {
    Configuration conf = new Configuration(CONF);
    if (http2) {
      conf.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    }
    try (final FileSystem fs = FileSystem.newInstance(conf)) {
      byte[] buf = new byte[4096];
      // warm up
      try (FSDataInputStream in = fs.open(FILE)) {
        in.read(buf);
      }
      long cost;
      try (FSDataInputStream in = fs.open(FILE)) {
        long start = System.currentTimeMillis();
        consume(in, LEN, buf);
        cost = System.currentTimeMillis() - start;
      }
      System.err.println("******* time based on " + (http2 ? "http2 " : "tcp ")
          + cost + "ms");
    }
  }
}
