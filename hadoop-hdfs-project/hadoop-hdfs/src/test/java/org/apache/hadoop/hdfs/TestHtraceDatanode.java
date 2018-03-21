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
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class TestHtraceDatanode {
  private static final int blockSize = 2*1024;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static SpanReceiverHost spanReceiverHost;
  TraceScope traceScope;
  static BufferAppender bufferAppender;

  @BeforeClass
  public static void setUp() throws Exception{
    Configuration conf = new HdfsConfiguration();
    conf.setInt("hadoop." + DFSConfigKeys.TRACE_WARN_TIME_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACE_LOG, false);
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
      TracerLog.class.getName());

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
      TracerLog.class.getName());
    spanReceiverHost = SpanReceiverHost.getInstance(conf);
    bufferAppender = new BufferAppender();
    Logger.getLogger("HTrace").addAppender(bufferAppender);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  public static class BufferAppender extends org.apache.log4j.AppenderSkeleton {
    private ArrayList<String> msgs = new ArrayList<String>();

    @Override
    protected void append(LoggingEvent evt) {
      String str = (String)evt.getMessage();
      msgs.add(str);
    }

    ArrayList<String> getMessage(int count) {
      while (msgs.size() < count) {
        Thread.yield();
      }
      return msgs;
    }

    public boolean requiresLayout() {
      return true;
    }

    public void close() {}

    public void clear() {
      msgs.clear();
    }
  }

  @Before
  public void setup() {
    traceScope = Trace.startSpan("testSpan", Sampler.ALWAYS);
  }

  @After
  public void clear() {
    traceScope.close();
    bufferAppender.close();
  }

  @Test
  public void testDatanodeWrite() throws Exception {
    Path file = fs.makeQualified(new Path("/testDatanodeWrite.dat"));
    FSDataOutputStream fos =
      fs.create(file, true,4096, (short)1, blockSize);
    int wSize = blockSize/2;
    byte[] wBuf = new byte[wSize];
    bufferAppender.clear();
    fos.write(wBuf);
    fos.close();

    ArrayList<String> msgs = bufferAppender.getMessage(1);
    Assert.assertTrue(msgs.get(5).contains("write to disk done"));
    fs.delete(file, true);
  }

  @Test
  public void testDatanodeRead() throws Exception {
    Path file = fs.makeQualified(new Path("/testDatanodeRead.dat"));
    DFSTestUtil.createFile(fs, file,  blockSize, (short)1, 0);

    FSDataInputStream is = fs.open(file);
    int rSize = blockSize/2;;
    byte[] readBuf = new byte[rSize];

    bufferAppender.clear();
    is.read(readBuf);
    is.close();

    ArrayList<String> msgs = bufferAppender.getMessage(1);
    Assert.assertTrue(msgs.get(0).contains("created BlockSender done"));
    Assert.assertTrue(msgs.get(0).contains("readBlock: done"));
    fs.delete(file, true);
  }
}
