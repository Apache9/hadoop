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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestDFSOutputStream {
  static MiniDFSCluster cluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();
  }

  /**
   * The close() method of DFSOutputStream should never throw the same exception
   * twice. See HDFS-5335 for details.
   */
  @Test
  public void testCloseTwice() throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path("/test"));
    DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
        "wrappedStream");
    @SuppressWarnings("unchecked")
    AtomicReference<IOException> ex = (AtomicReference<IOException>) Whitebox
        .getInternalState(dos, "lastException");
    Assert.assertEquals(null, ex.get());

    dos.close();

    IOException dummy = new IOException("dummy");
    ex.set(dummy);
    try {
      dos.close();
    } catch (IOException e) {
      Assert.assertEquals(e, dummy);
    }
    Assert.assertEquals(null, ex.get());
    dos.close();
  }

  /**
   * If dfs.client.recover-on-close-exception.enable is set and exception
   * happens in close, the local lease should be closed and lease in namenode
   * should be recovered.
   */
  @Test
  public void testExceptionInClose() throws IOException {
    String testStr = "Test exception in close";
    DistributedFileSystem fs = cluster.getFileSystem();
    Path testFile = new Path("/closeexception");
    fs.getConf().setBoolean(
        DFSConfigKeys.DFS_CLIENT_RECOVER_ON_CLOSE_EXCEPTION, true);
    FSDataOutputStream os = fs.create(testFile);
    DFSOutputStream dos =
        (DFSOutputStream) Whitebox.getInternalState(os, "wrappedStream");
    dos.setExceptionInClose(true);
    os.write(testStr.getBytes());
    try {
      dos.close();
      // There should be exception
      Assert.assertTrue(false);
    } catch (IOException ioe) {
      // Sleep a while for file recovery
      try {
        Thread.sleep(5000);
      } catch (Exception e) {
        // Ignore
      }
      Assert.assertTrue(fs.isFileClosed(testFile));
    }
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }
}
