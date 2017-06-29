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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.htrace.Sampler;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertTrue;


public class TestHdfsHtrace {
  private static TemporarySocketDirectory sockDir;
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 10*1024;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }

  @Before
  public void before() {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
  }

  // creates a file but does not close it
  static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
      fileSys.getConf().getInt("io.file.buffer.size", 4096),
      (short)repl, blockSize);
    return stm;
  }

  @Test
  public void testWriteFlush() throws Exception {
    Configuration conf = new Configuration();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      // check that / exists
      Path path = new Path("/");
      assertTrue("/ should be a directory", fs.getFileStatus(path)
        .isDirectory() == true);

      byte[] bigData = AppendTestUtil.randomBytes(seed, 2*blockSize);
      byte[] smallData = AppendTestUtil.randomBytes(seed, blockSize/10);
      Path file1 = fs.makeQualified(new Path("fileread.dat"));

      TraceScope createScope = Trace.startSpan("HdfsHtraceLocal.WriteFlush", Sampler.ALWAYS);
      FSDataOutputStream stm = createFile(fs, file1, 1);
      stm.write(bigData);
      stm.hflush();
      stm.write(smallData);
      stm.hflush();
      stm.write(smallData);
      stm.hflush();
      stm.close();
      createScope.close();
      String str = createScope.getSpan().toJson();
      Assert.assertTrue(!str.isEmpty());

      List<TimelineAnnotation> listAnnotation = createScope.getSpan().getTimelineAnnotations();
      Assert.assertTrue(listAnnotation.get(0).getMessage().contains("created file"));
      int writtenSize = 0;
      String strContent = "HDFS: flush done. block offset=" + writtenSize;
      Assert.assertTrue(listAnnotation.get(1).getMessage().contains(strContent));
      writtenSize += blockSize/10;
      strContent = "HDFS: flush done. block offset=" + writtenSize;
      Assert.assertTrue(listAnnotation.get(2).getMessage().contains(strContent));
      writtenSize += blockSize/10;
      strContent = "HDFS: flush done. block offset=" + writtenSize;
      Assert.assertTrue(listAnnotation.get(3).getMessage().contains(strContent));
      Assert.assertTrue(listAnnotation.get(4).getMessage().contains("closed file"));

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testReadFromLocal() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
      true);
    // Set a random client context name so that we don't share a cache with
    // other invocations of this function.
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT,
      UUID.randomUUID().toString());
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(),
        "TestShortCircuitLocalRead._PORT.sock").getAbsolutePath());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    FileSystem fs = cluster.getFileSystem();
    try {
      // check that / exists
      Path path = new Path("/");
      assertTrue("/ should be a directory", fs.getFileStatus(path)
        .isDirectory() == true);

      byte[] fileData = AppendTestUtil.randomBytes(seed, 3*blockSize+100);
      Path file1 = fs.makeQualified(new Path("filelocal.dat"));

      FSDataOutputStream stm = createFile(fs, file1, 1);
      stm.write(fileData);
      stm.close();

      TraceScope scope = Trace.startSpan("HdfsHtraceLocal.WriteRead.position", Sampler.ALWAYS);
      FSDataInputStream ism = fs.open(file1);
      byte[] readBuf = new byte[blockSize];
      //ism.readFully(0, actual);
      ism.read(readBuf);
      stm.close();
      scope.close();

      String str = scope.getSpan().toJson();
      Assert.assertTrue(!str.isEmpty());
      List<TimelineAnnotation> listAnnotation = scope.getSpan().getTimelineAnnotations();
      Assert.assertTrue(listAnnotation.get(0).getMessage().contains("HDFS: created Reader"));
      Assert.assertTrue(listAnnotation.get(1).getMessage().contains("HDFS: fill data buffer done"));
      Assert.assertEquals(listAnnotation.get(2).getMessage(),
        "HDFS: read done, offset=0 length=" + blockSize + " read length=" + blockSize);

      TraceScope scopePosition = Trace.startSpan("HdfsHtraceLocal.WriteRead.position", Sampler.ALWAYS);
      //ism.readFully(0, actual);
      ism.read(0, readBuf, 0, blockSize);
      stm.close();
      scopePosition.close();

      str = scopePosition.getSpan().toJson();
      Assert.assertTrue(!str.isEmpty());
      listAnnotation = scopePosition.getSpan().getTimelineAnnotations();
      Assert.assertTrue(listAnnotation.get(0).getMessage().contains("HDFS: created Reader"));
      Assert.assertTrue(listAnnotation.get(1).getMessage().contains("HDFS: fill data buffer done"));
      Assert.assertEquals(listAnnotation.get(2).getMessage(),
        "HDFS: read done, offset=0 length=" + blockSize + " read length=" + blockSize);

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

}
