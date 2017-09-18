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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.htrace.*;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_KEY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;


public class TestHdfsHtrace {
  private static TemporarySocketDirectory sockDir;
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 2*64*1024;
  private static byte[] smallData = AppendTestUtil.randomBytes(seed, 1024);

  private static Configuration  conf = new HdfsConfiguration();
  private static MiniDFSCluster cluster;
  private static FileSystem  fs;

  @BeforeClass
  public static void setUp() throws Exception{
    conf.setLong(DFS_TRACER_WARN_TIME_NORMAL_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWBLOCK_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);
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

  class BufferAppender extends org.apache.log4j.AppenderSkeleton {
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

    public void close() {
      msgs.clear();
    }
  }

  private static void mwait(long mills) {
    try {
      Thread.sleep(mills);
    } catch (InterruptedException ie) {
      // Ignore
    }
  }

  // creates a file but does not close it
  private static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl, int bsize)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,4096, (short)repl, bsize);
    return stm;
  }

  private static String getValue(String str, String key) {
    String a[] = str.split(" ");
    for (String s : a) {
      if (s.length() >= key.length() &&
        s.substring(0,key.length()).equals(key)) {
        String b[] = s.split("=");
        if (b.length == 2) {
          return b[1].trim();
        }
      }
    }
    return null;
  }

  @Test
  public void testClientCreate() throws Exception {
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
  public void testClientClose() throws Exception {
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
  public void testClientWrite() throws Exception {
    String fileName = "testClientWrite.dat";
    Path file = fs.makeQualified(new Path(fileName));
    FSDataOutputStream fos = createFile(fs, file, 1, blockSize);
    int wSize = blockSize/2;
    byte[] wBuf = new byte[wSize];

    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    TraceScope readScope = Trace.startSpan("tracer.ClientWrite", Sampler.ALWAYS);

    fos.write(wBuf);
    fos.write(wBuf);

    List<TimelineAnnotation> listAnnotation = readScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.size() > 0);
    String str =
      "HDFS: packet is full, put it to queue. seqno=0 offset=0 len=65024 packetSize=65532 chunksPerPacket=127";
    Assert.assertTrue(listAnnotation.get(0).getMessage().equals(str));
    Assert.assertTrue(listAnnotation.get(1).getMessage().equals("HDFS: put packet to queue done."));
    Assert.assertTrue(listAnnotation.get(8).getMessage().equals("HDFS: block boundary, sent an empty packet."));
    fos.close();
    fs.delete(file, true);
  }

  @Test
  public void testClientWriteFlush() throws Exception {
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
  public void testClientRead() throws Exception {
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
  public void testDatanodeWrite() throws Exception {

    Path file = fs.makeQualified(new Path("/testDatanodeWrite.dat"));
    FSDataOutputStream fos = createFile(fs, file, 1, blockSize);
    int wSize = blockSize/2;
    byte[] wBuf = new byte[wSize];

    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    fos.write(wBuf);

    ArrayList<String> msgs = bufferAppender.getMessage(1);
    Assert.assertTrue(msgs.get(0).contains("Receive Packet"));
    String str = "offset=0 seqno=0 len=65024";
    Assert.assertTrue(msgs.get(0).contains(str));
    Assert.assertTrue(msgs.get(0).contains("write data to disk done"));
    fs.delete(file, true);
  }

  @Test
  public void testDatanodeRead() throws Exception {
    byte[] buf = AppendTestUtil.randomBytes(seed, blockSize);
    Path file = fs.makeQualified(new Path("/testDatanodeRead.dat"));
    DFSTestUtil.createFile(fs, file,  blockSize, (short)1, seed);

    FSDataInputStream is = fs.open(file);
    int rSize = 1024;
    byte[] readBuf = new byte[rSize];

    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    int len = is.read(readBuf);
    is.close();

    ArrayList<String> msgs = bufferAppender.getMessage(1);
    Assert.assertTrue(msgs.size() > 0);
    String str = "offset=0 len=65536 seqno=0";
    Assert.assertTrue(msgs.get(0).contains("Send Packet"));
    Assert.assertTrue(msgs.get(0).contains(str));
    Assert.assertTrue(msgs.get(0).contains("write header done"));
    Assert.assertTrue(msgs.get(0).contains("waitForWritable done"));
    Assert.assertTrue(msgs.get(0).contains("transferToFully done"));
    fs.delete(file, true);
  }

  /* Copy a block from sourceProxy to destination. If the block becomes
  * over-replicated, preferably remove it from source.
  *
  * Return true if a block is successfully copied; otherwise false.
  */
  private boolean replaceBlock( ExtendedBlock block, DatanodeInfo source,
                                DatanodeInfo sourceProxy, DatanodeInfo destination) throws IOException {
    Socket sock = new Socket();
    sock.connect(NetUtils.createSocketAddr(
      destination.getXferAddr()), HdfsServerConstants.READ_TIMEOUT);
    sock.setKeepAlive(true);
    // sendRequest
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());
    new Sender(out).replaceBlock(block, BlockTokenSecretManager.DUMMY_TOKEN,
      source.getDatanodeUuid(), sourceProxy);
    out.flush();
    // receiveResponse
    DataInputStream reply = new DataInputStream(sock.getInputStream());

    DataTransferProtos.BlockOpResponseProto proto =
      DataTransferProtos.BlockOpResponseProto.parseDelimitedFrom(reply);
    return proto.getStatus() == DataTransferProtos.Status.SUCCESS;
  }

  @Test
  public void testDatanodeReplaceBlock() throws Exception {
    String strFile = "/testDatanodeReplaceBlock.dat";
    Path file = fs.makeQualified(new Path(strFile));

    int bSize = 1024;
    // create a file with one block
    DFSTestUtil.createFile(fs, file,  bSize, (short)1, seed);
    DFSTestUtil.waitReplication(fs, file, (short)1);

    InetSocketAddress addr = new InetSocketAddress("localhost", cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    List<LocatedBlock> locatedBlocks = client.getNamenode().
      getBlockLocations(strFile, 0, bSize).getLocatedBlocks();

    LocatedBlock block = locatedBlocks.get(0);
    DatanodeInfo[]  oldNodes = block.getLocations();
    assertEquals(oldNodes.length, 1);
    ExtendedBlock b = block.getBlock();

    DatanodeInfo[] datanodes = client.datanodeReport(HdfsConstants.DatanodeReportType.ALL);
    // find out the new node
    DatanodeInfo newNode=null;
    for(DatanodeInfo node : datanodes) {
      if(!node.equals(oldNodes[0])) {
        newNode = node;
        break;
      }
    }
    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);

    replaceBlock(b, oldNodes[0], oldNodes[0], newNode);

    ArrayList<String> msgs = bufferAppender.getMessage(6);
    Assert.assertTrue(msgs.size() > 0);

    String str = b.getLocalBlock().toString();
    Assert.assertTrue(msgs.get(4).contains("Copy Block"));
    Assert.assertTrue(msgs.get(4).contains(b.getLocalBlock().toString()));
    Assert.assertTrue(msgs.get(5).contains("Replace Block"));
    Assert.assertTrue(msgs.get(5).contains(b.getLocalBlock().toString()));
    fs.delete(file, true);
  }

  @Test
  public void testClientDatanodeRead() throws Exception {
    int bSize = 10*64*1024;
    byte[] buf = AppendTestUtil.randomBytes(seed, bSize);
    Path file = fs.makeQualified(new Path("/testClientDatanodeRead.dat"));
    FSDataOutputStream stm = createFile(fs, file, 1, bSize);
    stm.write(buf);
    stm.write(buf);
    stm.close();

    FSDataInputStream is = fs.open(file);
    int rSize = 1024;
    byte[] readBuf = new byte[rSize];
    int len = 0;

    BufferAppender bufferAppender;
    bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
    //TracerLog.init(conf);
    TraceScope readScope = Trace.startSpan("tracer.ClientDatanodeRead", Sampler.ALWAYS);

    int pos = 2*64*1024;
    //len = is.read(readBuf);
    is.seek(pos);
    len = is.read(readBuf);

    is.close();
    readScope.close();
    List<TimelineAnnotation> listAnnotation = readScope.getSpan().getTimelineAnnotations();
    String clientBlockID = getValue(listAnnotation.get(2).getMessage(), "block");
    String ClientOffsetInBlock = getValue(listAnnotation.get(3).getMessage(), "offset");

    ArrayList<String> msgs = bufferAppender.getMessage(1);
    String dnBlockID = getValue(msgs.get(0), "block");
    String dnOffsetInBlock = getValue(msgs.get(0), "offset");

    Assert.assertEquals(clientBlockID, dnBlockID);
    Assert.assertEquals(ClientOffsetInBlock, dnOffsetInBlock);
    fs.delete(file, true);
  }

  @Test
  public void testClientDatanodeWriteFlush() throws Exception {
    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);

    //byte[] data = AppendTestUtil.randomBytes(seed, blockSize);
    Path file = fs.makeQualified(new Path("/testClientDatanodeWriteFlush.dat"));
    FSDataOutputStream fos = createFile(fs, file, 3, blockSize);

    TraceScope writeFlushScope = Trace.startSpan("tracer.testClientDatanodeWriteFlush", Sampler.ALWAYS);
    fos.write(smallData);
    fos.hflush();

    writeFlushScope.close();
    List<TimelineAnnotation> listAnnotation = writeFlushScope.getSpan().getTimelineAnnotations();

    String clientBlockId = getValue(listAnnotation.get(2).getMessage(), "block");
    String clientDN1 = "/" + getValue(listAnnotation.get(2).getMessage(), "datanode");

    ArrayList<String> msgs = bufferAppender.getMessage(3);
    String ndtBlockIdOnMirror = getValue(msgs.get(0), "block");
    String ndtBlockIdOnReceive = getValue(msgs.get(4), "block");
    String dn1Mirror = getValue(msgs.get(1), "my");
    String dn1Receive = getValue(msgs.get(2), "my");
    Assert.assertEquals(ndtBlockIdOnMirror, ndtBlockIdOnReceive);
    Assert.assertEquals(dn1Mirror, dn1Receive);

    Assert.assertEquals(clientBlockId, ndtBlockIdOnReceive);
    Assert.assertEquals(clientDN1, dn1Receive);

    fos.close();
    fs.delete(file, true);
  }

  @Test
  public void testPressureFlushSync() throws Exception {
    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);

    int dataSize = blockSize/2;
    byte[] data = AppendTestUtil.randomBytes(seed, dataSize);
    Path file = fs.makeQualified(new Path("/testPressureFlushSync.dat"));
    FSDataOutputStream fos = createFile(fs, file, 3, blockSize);
    //int cnt = 1000;
    int cnt = 1;
    while(cnt-- > 0) {
      TracerLog.startScope("PressureFlushSyn", "NO:"+cnt);
      fos.write(data);
      fos.hflush();
      TracerLog.closeScopeThreadLocal();
    }
    ArrayList<String> msgs = bufferAppender.getMessage(1);
    Assert.assertTrue(msgs.size() > 0);
    fos.close();
    fs.delete(file, true);
  }

  @Test
  public void testDatanodeTraceWarnTime() throws Exception {
    conf.setLong(DFS_TRACER_WARN_TIME_NORMAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 1000);
    TracerLog.initServer(conf);

    BufferAppender bufferAppender = new BufferAppender();
    Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);

    byte[] data = AppendTestUtil.randomBytes(seed, blockSize);
    Path file = fs.makeQualified(new Path("/testDatanodeTraceWarnTime.dat"));

    FSDataOutputStream os = createFile(fs, file, 1, blockSize);
    os.write(data);
    os.hflush();
    os.close();

    ArrayList<String> msgs = bufferAppender.getMessage(0);
    int length = msgs.size();
    Assert.assertEquals(0, length);

    conf.setLong(DFS_TRACER_WARN_TIME_NORMAL_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 0);
    TracerLog.initServer(conf);
    fs.delete(file, true);
  }


  @Test
  public void testIsClientTracing() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, false);
    TracerLog.initClient(conf);

    TraceScope maxScope = Trace.startSpan("tracer.testIsClientTracing", Sampler.ALWAYS);
    boolean ret = TracerLog.isClientTracing();
    Assert.assertFalse(ret);

    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);
    TracerLog.initClient(conf);
    ret = TracerLog.isClientTracing();
    Assert.assertTrue(ret);

    for(int i=0; i < TracerLog.MAX_ANNOTATION_COUNT; i++) {
      ret = TracerLog.isClientTracing();
      Assert.assertTrue(ret);
      if (ret) {
        Trace.addTimelineAnnotation("step " + i);
      }
    }
    ret = TracerLog.isClientTracing();
    Assert.assertFalse(ret);

    maxScope.close();

    List<TimelineAnnotation> listAnnotation = maxScope.getSpan().getTimelineAnnotations();
    Assert.assertTrue(listAnnotation.get(TracerLog.MAX_ANNOTATION_COUNT).
      getMessage().contains("HDFS: " + TracerLog.TAG_MORE_LOGS));
  }

  @Test
  public void testClientReadFromLocal() throws Exception {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));

    Configuration confLocal = new Configuration();
    confLocal.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    confLocal.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, true);
    confLocal.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
    confLocal.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
      "TestShortCircuitLocalRead._PORT.sock").getAbsolutePath());
    confLocal.setLong(DFS_TRACER_WARN_TIME_NORMAL_KEY, 0);
    confLocal.setLong(DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY, 0);
    confLocal.setBoolean(DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG, true);
    MiniDFSCluster clusterLocal = new MiniDFSCluster.Builder(confLocal).numDataNodes(1)
      .format(true).build();
    clusterLocal.waitActive();
    FileSystem fsLocal = clusterLocal.getFileSystem();
    try {
      byte[] fileData = AppendTestUtil.randomBytes(seed, blockSize+100);
      Path file = fsLocal.makeQualified(new Path("/testClientReadFromLocal.dat"));
      FSDataOutputStream fos = createFile(fsLocal, file, 1, blockSize);
      fos.write(fileData);
      fos.close();
      FSDataInputStream ism = fsLocal.open(file);
      int rSize = 64*1024;
      byte[] readBuf = new byte[rSize];

      BufferAppender bufferAppender = new BufferAppender();
      Logger.getLogger(TracerLog.class.getName()).addAppender(bufferAppender);
      TraceScope scope = Trace.startSpan("testClientReadFromLocal", Sampler.ALWAYS);

      ism.read(readBuf);
      scope.close();

      List<TimelineAnnotation> listAnnotation = scope.getSpan().getTimelineAnnotations();
      Assert.assertTrue(listAnnotation.get(0).getMessage().contains("HDFS: chosen datanode"));
      Assert.assertTrue(listAnnotation.get(1).getMessage().equals("HDFS: created new block reader local"));
      Assert.assertTrue(listAnnotation.get(2).getMessage().
        contains("HDFS: created reader in blockSeekTo()."));
      String str = "HDFS: read done. offset=0 len=" + rSize + " nread=" + rSize;
      Assert.assertTrue(listAnnotation.get(3).getMessage().equals(str));

      ArrayList<String> msgs = bufferAppender.getMessage(2);
      Assert.assertTrue(msgs.size() > 0);
      Assert.assertTrue(msgs.get(0).contains("RequestShortCircuitShm"));
      Assert.assertTrue(msgs.get(0).contains("request done"));

      Assert.assertTrue(msgs.get(1).contains("RequestShortCircuitFds"));
      Assert.assertTrue(msgs.get(1).contains("request done"));

      String dnBlockID = getValue(msgs.get(1), "block");
      String clientBlockID = getValue(listAnnotation.get(2).getMessage(), "block");
      Assert.assertEquals(dnBlockID, clientBlockID);

      ism.close();
    } finally {
      fsLocal.close();
      clusterLocal.shutdown();
      sockDir.close();
    }
  }

}
