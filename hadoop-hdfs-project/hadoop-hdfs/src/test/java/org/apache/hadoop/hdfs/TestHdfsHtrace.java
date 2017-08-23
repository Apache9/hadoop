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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.htrace.*;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertTrue;

class BufferAppender extends org.apache.log4j.AppenderSkeleton {
  private static ArrayList<String> msgs = new ArrayList<String>();

  @Override
  protected void append(LoggingEvent evt) {
    String str = (String)evt.getMessage();
    msgs.add(str);
  }

  ArrayList<String> getMessage() {
    return msgs;
  }

  public boolean requiresLayout() {
    return true;
  }

  public void close() {
    msgs.clear();
  }
}

public class TestHdfsHtrace {
  private static TemporarySocketDirectory sockDir;
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 10*1024;

  @BeforeClass
  public static void setUp() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    sockDir.close();
  }

  @Before
  public void before() {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
  }

  // creates a file but does not close it
  static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl, int bsize)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
      fileSys.getConf().getInt("io.file.buffer.size", 4096),
      (short)repl, bsize);
    return stm;
  }

  @Test
  public void testWriteFlush() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try {
      byte[] bigData = AppendTestUtil.randomBytes(seed, 2*blockSize);
      byte[] smallData = AppendTestUtil.randomBytes(seed, blockSize/10);
      Path file1 = fs.makeQualified(new Path("fileread.dat"));

      TraceScope createScope = Trace.startSpan("HdfsHtraceLocal.WriteFlush", Sampler.ALWAYS);
      FSDataOutputStream stm = createFile(fs, file1, 1, blockSize);
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
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        "TestShortCircuitLocalRead._PORT.sock").getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      byte[] fileData = AppendTestUtil.randomBytes(seed, 3*blockSize+100);
      Path file1 = fs.makeQualified(new Path("filelocal.dat"));

      FSDataOutputStream stm = createFile(fs, file1, 1, blockSize);
      stm.write(fileData);
      stm.close();

      TraceScope scope = Trace.startSpan("HdfsHtraceLocal.WriteRead.position", Sampler.ALWAYS);
      FSDataInputStream ism = fs.open(file1);
      byte[] readBuf = new byte[blockSize];
      ism.read(readBuf);
      stm.close();
      scope.close();

      String str = scope.getSpan().toJson();
      Assert.assertTrue(!str.isEmpty());
      List<TimelineAnnotation> listAnnotation = scope.getSpan().getTimelineAnnotations();
      Assert.assertTrue(listAnnotation.get(0).getMessage().contains("HDFS: created Reader"));
      Assert.assertEquals(listAnnotation.get(1).getMessage(),
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
      Assert.assertEquals(listAnnotation.get(1).getMessage(),
        "HDFS: read done, offset=0 length=" + blockSize + " read length=" + blockSize);

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeTraceWriteRead() throws Exception {
    BufferAppender bufferAppender;
    bufferAppender = new BufferAppender();

    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 0);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try {
      int bSize = 2*64*1024;
      byte[] data = AppendTestUtil.randomBytes(seed, bSize);
      Path file1 = fs.makeQualified(new Path("tracer.dat"));
      FSDataOutputStream os = createFile(fs, file1, 3, bSize);
      os.write(data);
      os.hflush();
      os.close();

      ArrayList<String> msgs = bufferAppender.getMessage();
      int length = msgs.size();
      Assert.assertTrue(length > 0);
      for (int i=0; i < length; i++) {
        Assert.assertTrue(msgs.get(i).contains("blockID="));
        Assert.assertTrue(msgs.get(i).contains("TimelineAnnotations"));
      }
      String str = "offsetInBlock=0, seqno=0, dataLen=";
      Assert.assertTrue(msgs.get(2).contains(str));
      Assert.assertTrue(msgs.get(2).contains("Write data to disk done"));
      msgs.clear();

      FSDataInputStream is = fs.open(file1);
      int rSize = 1024;
      byte[] readBuf = new byte[rSize];
      //ism.readFully(0, actual);
      for (int i=0; i < 1; i++) {
        is.read(readBuf);
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          // Ignore
        }
      }
      is.close();

      msgs = bufferAppender.getMessage();
      length = msgs.size();
      Assert.assertTrue(length > 0);
      for (int i=0; i < length; i++) {
        Assert.assertTrue(msgs.get(i).contains("op=Send Packet"));
        Assert.assertTrue(msgs.get(i).contains("blockID="));
        Assert.assertTrue(msgs.get(i).contains("TimelineAnnotations"));
      }
      str = "offset=0 dataLen=";
      Assert.assertTrue(msgs.get(0).contains(str));

    } finally {
      fs.close();
      cluster.shutdown();
      Logger.getLogger(TracerLog.class.getName()).removeAppender(bufferAppender);
    }
  }

  @Test
  public void testDatanodeTraceWarnTime() throws Exception {
    BufferAppender bufferAppender;
    bufferAppender = new BufferAppender();
    bufferAppender.close();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 1000);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try {
      byte[] data = AppendTestUtil.randomBytes(seed, blockSize);
      Path file1 = fs.makeQualified(new Path("tracer_no.dat"));

      FSDataOutputStream os = createFile(fs, file1, 1, blockSize);
      os.write(data);
      os.hflush();
      os.close();

      ArrayList<String> msgs = bufferAppender.getMessage();
      int length = msgs.size();
      Assert.assertEquals(0, length);

    } finally {
      fs.close();
      cluster.shutdown();
      Logger.getLogger(TracerLog.class.getName()).removeAppender(bufferAppender);
    }
  }

  @Test
  public void testClientAndDatanode() throws Exception {
    BufferAppender bufferAppender;
    bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    int sizePackage = 64*1024;
    int sizeBlock = 5*sizePackage;

    try {
      TraceScope createScope = Trace.startSpan("clien_namenode.tracer", Sampler.ALWAYS);
      byte[] data = AppendTestUtil.randomBytes(seed, sizePackage/8);
      Path file1 = fs.makeQualified(new Path("tracer.dat"));
      FSDataOutputStream os = createFile(fs, file1, 3 , sizeBlock);
      os.write(data);
      os.hflush();
      os.close();
      FSDataInputStream is = fs.open(file1);
      byte[] readBuf = new byte[sizePackage];
      is.read(readBuf);
      is.close();

      ArrayList<String> msgs = bufferAppender.getMessage();
      int length = msgs.size();
      Assert.assertTrue(length > 0);
      for (int i=0; i < length; i++) {
        Assert.assertTrue(msgs.get(i).contains("blockID="));
        Assert.assertTrue(msgs.get(i).contains("TimelineAnnotations"));
      }
      createScope.close();
      String strClient = createScope.getSpan().toJson();
      Assert.assertTrue(!strClient.isEmpty());

    } finally {
      fs.close();
      cluster.shutdown();
      Logger.getLogger(TracerLog.class.getName()).removeAppender(bufferAppender);
    }
  }

  @Test
  public void testFlushSync() throws Exception {
    BufferAppender bufferAppender;
    bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try {
      int dataSize = 64*1024;
      byte[] count = AppendTestUtil.randomBytes(seed, dataSize);
      Path file1 = fs.makeQualified(new Path("testFlushSync.dat"));
      FSDataOutputStream fos = createFile(fs, file1, 1, blockSize);
      int cnt = 10;
      while(cnt-- > 0) {
        TracerLog.startScope("testFlushSync", "NO:"+cnt);
        fos.write(count);
        fos.write(count);
        fos.write(count);
        fos.write(count);
        fos.write(count);
        fos.hflush();
        TracerLog.closeScope();

      }
      ArrayList<String> msgs = bufferAppender.getMessage();
      Assert.assertTrue(msgs.size() > 0);
      fos.close();

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testDisableClientTracer() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, false);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
      .format(true).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      int dataSize = 64*1024;
      byte[] data = AppendTestUtil.randomBytes(seed, dataSize);

      Path file1 = fs.makeQualified(new Path("testFlushSync.dat"));
      FSDataOutputStream fos = createFile(fs, file1, 1, blockSize);
      TraceScope traceScope = Trace.startSpan("disableTracer.dat", Sampler.ALWAYS);
      fos.write(data);
      fos.hflush();
      fos.close();
      List<TimelineAnnotation> listAnnotation = traceScope.getSpan().getTimelineAnnotations();
      Assert.assertTrue(listAnnotation.isEmpty());
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

}
