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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.ThreadUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * These tests make sure that DFSClient excludes writing data to
 * a DN properly in case of errors.
 */
public class TestDFSClientDetectDeadNodes {

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setUp() {
    cluster = null;
    conf = new HdfsConfiguration();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000000)
  public void testNodeBecomeDeadAndActive() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    // Remove two DNs,
    DataNodeProperties one = cluster.stopDataNode(0);
    DataNodeProperties two = cluster.stopDataNode(0);
    DataNodeProperties three = cluster.stopDataNode(0);

    while(cluster.numDataNodes > 0) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    FSDataInputStream in = fs.open(filePath);
    try {
      in.read();
    } catch (BlockMissingException e) {

    }

    DFSInputStream din = (DFSInputStream)in.getWrappedStream();

    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 3);

    cluster.restartDataNode(one, true);
    while(din.getDfsClient().getDeadNodes(din).size() != 2) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    in.close();
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 2);

    in = fs.open(filePath);
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 2);
    try {
      in.read();
    } catch (Exception e) {
      assertTrue(false);
    }
    din = (DFSInputStream)in.getWrappedStream();
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 2);
  }

  @Test(timeout=60000000)
  public void testDetectDeadNodeInBackground() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    // Remove two DNs,
    DataNodeProperties one = cluster.stopDataNode(0);
    DataNodeProperties two = cluster.stopDataNode(0);
    DataNodeProperties three = cluster.stopDataNode(0);

    while(cluster.numDataNodes > 0) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    FSDataInputStream in = fs.open(filePath);
    try {
      in.read();
    } catch (BlockMissingException e) {

    }

    DFSInputStream din = (DFSInputStream)in.getWrappedStream();

    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 3);

    cluster.restartDataNode(one, true);
    while(din.getDfsClient().getDeadNodes(din).size() != 2) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

  }

  @Test(timeout=60000000)
  public void testDetectliveNodeInBackground() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    // Remove two DNs,
    DataNodeProperties one = cluster.stopDataNode(0);


    FSDataInputStream in = fs.open(filePath);
    try {
      // to get the detecting node list via read
      in.read();
    } catch (BlockMissingException e) {
      assertTrue(false);
    }

    DFSInputStream din = (DFSInputStream)in.getWrappedStream();

    while(din.getDfsClient().getDeadNodes(din).size() != 1) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }


    cluster.restartDataNode(one, true);


    while(din.getDfsClient().getDeadNodes(din).size() != 0) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    cluster.stopDataNode(0);

    while(din.getDfsClient().getDeadNodes(din).size() != 1) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

  }

  @Test(timeout=60000000)
  public void testDetectDeadNodeInBackgroundOnly() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, false);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    FSDataInputStream in = fs.open(filePath);
    try {
      // to get the detecting node list via read
      in.read();
    } catch (BlockMissingException e) {
      assertTrue(false);
    }

    DFSInputStream din = (DFSInputStream)in.getWrappedStream();

    while(din.getDfsClient().getDeadNodes(din).size() != 0) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 0);

    DataNodeProperties one = cluster.stopDataNode(0);

    ThreadUtil.sleepAtLeastIgnoreInterrupts(15 * 1000L);


    assertTrue(din.getDfsClient().getLiveNodes().size() == 3);

  }

  @Test(timeout=60000000)
  public void stressTest() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(10).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[4096];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    while (true) {
      FSDataInputStream in = fs.open(filePath);
      try {
        // to get the detecting node list via read
        in.read();
      } catch (BlockMissingException e) {
        assertTrue(false);
      }

      DFSInputStream din = (DFSInputStream) in.getWrappedStream();

      ArrayList<DataNodeProperties> datanodes = new ArrayList<DataNodeProperties>();
      for (int i = 0; i < 5; i++) {
        datanodes.add(cluster.stopDataNode(0));

      }
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
      }

      for (DataNodeProperties dataNodeProperties : datanodes) {
        cluster.restartDataNode(dataNodeProperties, true);
      }
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
      }
    }

  }


  @Test(timeout=60000000)
  public void testLiveNodeMultipleDFSInputStream() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    FSDataInputStream in1 = fs.open(filePath);
    try {
      // to get the detecting node list via read
      in1.read();
    } catch (BlockMissingException e) {
      assertTrue(false);
    }

    FSDataInputStream in2 = fs.open(filePath);

    DFSInputStream din = (DFSInputStream)in1.getWrappedStream();
    assertTrue(din.getDfsClient().getLiveNodes().size() == 1);

    in2.close();
    assertTrue(din.getDfsClient().getLiveNodes().size() == 1);

    in1.close();
    assertTrue(din.getDfsClient().getLiveNodes().size() == 0);

  }

  @Test(timeout=60000000)
  public void testDeadNodeMultipleDFSInputStream() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index=0; index<bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    FSDataInputStream in1 = fs.open(filePath);
    DFSInputStream din = (DFSInputStream)in1.getWrappedStream();
    cluster.stopDataNode(0);

    while(din.getDfsClient().getDeadNodes(din).size() != 1) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    try {
      in1.read();
    } catch (BlockMissingException e) {
    }

    FSDataInputStream in2 = fs.open(filePath);


    assertTrue(din.getDfsClient().getLiveNodes().size() == 0);
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 1);

    in2.close();
    assertTrue(din.getDfsClient().getLiveNodes().size() == 0);
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 1);

    in1.close();
    assertTrue(din.getDfsClient().getLiveNodes().size() == 0);
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 1);


    while(din.getDfsClient().getDeadNodes(din).size() != 0) {


      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }


    assertTrue(din.getDfsClient().getLiveNodes().size() == 0);
    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 0);

  }


  @Test(timeout=60000000)
  public void testDetectLocalDeadNodeOnly() throws IOException {
    // Forgive nodes in under 2.5s for this test case.
    conf.setBoolean(
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLE_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        5000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY, 3);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY, true);
    conf.setInt(
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        1);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    List<DataNodeProperties> props = cluster.dataNodes;
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index = 0; index < bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 1, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    FSDataInputStream in1 = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in1.getWrappedStream();
    FSDataInputStream in2 = fs.open(filePath);
    DFSInputStream din2 = (DFSInputStream) in2.getWrappedStream();

    cluster.stopDataNode(0);

    try {
      in1.read();
    } catch (BlockMissingException e) {
    }

    assertTrue(din.getLocalDeadNodes().size() == 1);
    assertTrue(din2.getLocalDeadNodes().size() == 0);


    while (din.getDfsClient().getLiveNodes().size() != 0) {
      ThreadUtil.sleepAtLeastIgnoreInterrupts(5 * 1000L);
    }

    assertTrue(din.getDfsClient().getDeadNodes(din).size() == 1);
    assertTrue(din2.getDfsClient().getDeadNodes(din2).size() == 1);
    assertTrue(din.getLocalDeadNodes().size() == 1);
    assertTrue(din2.getLocalDeadNodes().size() == 0);

  }
}
