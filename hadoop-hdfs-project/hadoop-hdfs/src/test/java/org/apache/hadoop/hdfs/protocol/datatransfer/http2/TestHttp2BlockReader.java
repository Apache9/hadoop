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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.BlockReader;
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

public class TestHttp2BlockReader {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  @BeforeClass
  public static void setUp() throws Exception {
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
    String fileName = "/test";
    FSDataOutputStream out = CLUSTER.getFileSystem().create(new Path(fileName));
    int len = 6 * 1024 * 1024 - 10;
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
    BlockReader blockReader =
        new Http2BlockReader(sessionAndStreamId, block.toString(), block, 1, true, "clientName",
            len - 1, null);
    byte[] result = new byte[len];
    blockReader.read(result, 0, len);
    byte[] expected = new byte[b.length - 1];
    for (int i = 0; i < expected.length; ++i) {
      expected[i] = b[i + 1];
    }
    Arrays.equals(result, expected);
  }

}
