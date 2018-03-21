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
import org.apache.hadoop.net.unix.TemporarySocketDirectory;

import org.htrace.Sampler;
import org.htrace.TimelineAnnotation;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestHtraceClient {
  private static TemporarySocketDirectory sockDir;
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 2*64*1024;
  private static byte[] smallData = AppendTestUtil.randomBytes(seed, 1024);

  private static Configuration conf = new HdfsConfiguration();
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws IOException{
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACE_LOG, true);
    final String[] INITIAL_RACKS = {"/RACK0", "/RACK1", "/RACK2"};
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).racks(INITIAL_RACKS).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  // creates a file but does not close it
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl, int bsize)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,4096, (short)repl, bsize);
    return stm;
  }

  @Test
  public void testClientCreate() throws IOException {
    TraceScope readScope = Trace.startSpan("/tracer.testClientCreate", Sampler.ALWAYS);
    String fileName = "testClientCreate.dat";
    Path file = fs.makeQualified(new Path(fileName));
    FSDataOutputStream fos = createFile(fs, file, 1, blockSize);

    List<TimelineAnnotation> listAnnotation = readScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.size() > 0);
    Assert.assertTrue(listAnnotation.get(0).getMessage().contains("HDFS: create file done."));
    Assert.assertTrue(listAnnotation.get(0).getMessage().contains(fileName));
    fos.close();
    fs.delete(file, true);
  }

  @Test
  public void testClientClose() throws IOException {
    String fileName = "testClientCreate.dat";
    Path file = fs.makeQualified(new Path(fileName));
    FSDataOutputStream fos = createFile(fs, file, 1, blockSize);
    fos.write(smallData);

    TraceScope readScope = Trace.startSpan("/tracer.testClientClose", Sampler.ALWAYS);
    fos.close();

    List<TimelineAnnotation> listAnnotation = readScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.size() > 0);
    Assert.assertTrue(listAnnotation.get(0).getMessage().equals("HDFS: flushBuffer done."));
    Assert.assertTrue(listAnnotation.get(1).getMessage().equals("HDFS: waitAndQueueCurrentPacket done."));
    Assert.assertTrue(listAnnotation.get(listAnnotation.size()-1).getMessage().
      contains("HDFS: complete file done"));
    Assert.assertTrue(listAnnotation.get(listAnnotation.size()-1).getMessage().
      contains(fileName));
    fs.delete(file, true);
  }

  @Test
  public void testClientWrite() throws IOException {
    String fileName = "testClientWrite.dat";
    Path file = fs.makeQualified(new Path(fileName));
    FSDataOutputStream fos = createFile(fs, file, 1, blockSize);
    int wSize = blockSize/2;
    byte[] wBuf = new byte[wSize];

    TraceScope readScope = Trace.startSpan("tracer.ClientWrite", Sampler.ALWAYS);

    fos.write(wBuf);
    fos.write(wBuf);

    List<TimelineAnnotation> listAnnotation = readScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.size() > 0);
    String str =
      "HDFS: packet is full, put it to queue. seqno=0 offset=0 len=65024 packetSize=65532 chunksPerPacket=127";
    Assert.assertTrue(listAnnotation.get(0).getMessage().equals(str));
    Assert.assertTrue(listAnnotation.get(1).getMessage().equals("HDFS: put packet to queue done."));
    fos.close();
    fs.delete(file, true);
  }

  @Test
  public void testClientWriteFlush() throws IOException {
    String fileName = "/testClientWriteFlush.dat";
    Path file = fs.makeQualified(new Path(fileName));
    FSDataOutputStream fos = createFile(fs, file, 3, blockSize);
    fos.write(smallData);

    TraceScope flushScope = Trace.startSpan("HdfsHtraceLocal.WriteFlush", Sampler.ALWAYS);
    fos.hflush();
    flushScope.close();
    List<TimelineAnnotation> listAnnotation = flushScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.get(0).getMessage().equals("HDFS: waiting for seqno=0"));
    Assert.assertTrue(listAnnotation.get(1).getMessage().contains("HDFS: target datanodes"));
    Assert.assertTrue(listAnnotation.get(2).getMessage().contains("HDFS: created block on datanode."));
    Assert.assertTrue(listAnnotation.get(3).getMessage().
      contains("send a packet done. seqno=0 offset=0 len=1024 block="));
    Assert.assertTrue(listAnnotation.get(4).getMessage().
      contains("HDFS: received an ack. seqno=0 offset=0 len=1024 block="));
    Assert.assertTrue(listAnnotation.get(5).getMessage().contains("HDFS: fsync done."));
    Assert.assertTrue(listAnnotation.get(5).getMessage().contains(fileName));
    Assert.assertTrue(listAnnotation.get(6).getMessage().
      contains("HDFS: flush done. lastFlushOffset=1024 lastAckedSeqno=0 src="));
    Assert.assertTrue(listAnnotation.get(6).getMessage().contains(fileName));

    fos.close();
    fs.delete(file, true);
  }

  @Test
  public void testClientRead() throws IOException {
    Path file = fs.makeQualified(new Path("/ClientRead.dat"));
    DFSTestUtil.createFile(fs, file, blockSize, (short)1, seed);

    FSDataInputStream is = fs.open(file);
    int rSize = 1024;
    byte[] readBuf = new byte[rSize];

    TraceScope readScope = Trace.startSpan("tracer.testClientRead", Sampler.ALWAYS);
    int len = is.read(readBuf);

    readScope.close();
    List<TimelineAnnotation> listAnnotation = readScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.size() > 0);
    Assert.assertTrue(listAnnotation.get(0).getMessage().
      contains("HDFS: chosen datanode "));
    Assert.assertTrue(listAnnotation.get(1).getMessage().
      contains("HDFS: created new peer"));
    Assert.assertTrue(listAnnotation.get(2).getMessage().
      contains("HDFS: created reader in blockSeekTo()."));
    Assert.assertTrue(listAnnotation.get(3).getMessage().
      equals("HDFS: received a packet. offset=0 len=65536 seqno=0"));
    String str = "HDFS: read done. offset=0 len=1024 nread="+len;
    Assert.assertTrue(listAnnotation.get(4).getMessage().equals(str));
    fs.delete(file, true);
    is.close();
  }

  @Test
  public void testIsClientTracing() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACE_LOG, false);
    TracerMgr.initClient(conf);

    TraceScope maxScope = Trace.startSpan("tracer.testIsClientTracing", Sampler.ALWAYS);
    boolean ret = TracerMgr.isClientTracing();
    Assert.assertFalse(ret);

    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACE_LOG, true);
    TracerMgr.initClient(conf);
    ret = TracerMgr.isClientTracing();
    Assert.assertTrue(ret);

    for(int i=0; i < TracerMgr.MAX_ANNOTATION_COUNT; i++) {
      ret = TracerMgr.isClientTracing();
      Assert.assertTrue(ret);
      if (ret) {
        Trace.addTimelineAnnotation("step " + i);
      }
    }
    ret = TracerMgr.isClientTracing();
    Assert.assertFalse(ret);

    maxScope.close();

    List<TimelineAnnotation> listAnnotation = maxScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.get(TracerMgr.MAX_ANNOTATION_COUNT).
      getMessage().contains("HDFS: " + TracerMgr.TAG_MORE_LOGS));
  }
}
