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

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockOutputStream {

  private static Configuration conf;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem dfs;
  private static DFSClient dfsClient;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    dfsCluster.waitActive();
    dfs = dfsCluster.getFileSystem();

    Assert.assertTrue(dfs instanceof DistributedFileSystem);
    dfsClient = ((DistributedFileSystem)dfs).getClient();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Test
  public void testGetAvailableNodes() throws Exception {
    DatanodeInfo[] allNodes = dfsClient.datanodeReport(DatanodeReportType.ALL);
    Assert.assertEquals(4, allNodes.length);

    DatanodeInfo[] availableNodes = BlockOutputStream.getAvailableNodes(
        dfsClient, allNodes);
    Assert.assertNotNull(availableNodes);
    Assert.assertEquals(0, availableNodes.length);

    DatanodeInfo[] excludedNodes = {allNodes[0]};
    availableNodes = BlockOutputStream.getAvailableNodes(
        dfsClient, excludedNodes);
    Assert.assertNotNull(availableNodes);
    Assert.assertEquals(3, availableNodes.length);
  }

  @Test
  public void testWriteCompleteBlock() throws Exception {
    Path file = new Path("/test.txt");
    long blockSize = (1<<20);
    long fileLen = blockSize * 3 - 10;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short)1,
        System.currentTimeMillis());
    byte[] dataContent = DFSTestUtil.readFileBuffer(dfs, file);

    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(),
        blockSize, blockSize);
    List<LocatedBlock> blockList = blocks.getLocatedBlocks();
    Assert.assertEquals(1, blockList.size());
    LocatedBlock block = blockList.get(0);

    dfsClient.reportBadBlocks(blockList.toArray(new LocatedBlock[blockList.size()]));

    byte[] buffer = new byte[(int)blockSize];
    new Random().nextBytes(buffer);
    BlockOutputStream out = BlockOutputStream.createStream(dfsClient, block,
        block.getLocations());
    int packetSize = (1<<16);
    for (int i = 0; i < blockSize / packetSize; ++i) {
      out.write(buffer, i * packetSize, packetSize);
    }
    out.close();

    byte[] newDataContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertArrayEquals(Arrays.copyOfRange(dataContent, 0, (int)blockSize),
        Arrays.copyOfRange(newDataContent, 0, (int)blockSize));
    Assert.assertArrayEquals(buffer, Arrays.copyOfRange(newDataContent,
        (int)blockSize, (int)blockSize * 2));
    Assert.assertArrayEquals(
        Arrays.copyOfRange(dataContent, (int)blockSize * 2, (int)fileLen),
        Arrays.copyOfRange(newDataContent, (int)blockSize * 2, (int)fileLen));
  }

  @Test
  public void testWriteInCompleteBlock() throws Exception {
    Path file = new Path("/test.txt");
    long blockSize = (1<<20);
    long fileLen = blockSize * 3 - 10;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short)1,
        System.currentTimeMillis());
    byte[] dataContent = DFSTestUtil.readFileBuffer(dfs, file);

    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(),
        blockSize * 2, fileLen - blockSize * 2);
    List<LocatedBlock> blockList = blocks.getLocatedBlocks();
    Assert.assertEquals(1, blockList.size());
    LocatedBlock block = blockList.get(0);

    dfsClient.reportBadBlocks(blockList.toArray(new LocatedBlock[blockList.size()]));

    byte[] buffer = new byte[(int)(fileLen - 2 * blockSize)];
    new Random().nextBytes(buffer);
    BlockOutputStream out = BlockOutputStream.createStream(dfsClient, block,
        block.getLocations());
    int packetSize = (1<<16);
    int writtenBytes = 0;
    for (int i = 0; i < (fileLen - 2 *blockSize) / packetSize; ++i) {
      out.write(buffer, i * packetSize, packetSize);
      writtenBytes += packetSize;
    }
    out.write(buffer, writtenBytes, buffer.length - writtenBytes);
    out.close();

    byte[] newDataContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertArrayEquals(Arrays.copyOfRange(dataContent, 0, (int)blockSize),
        Arrays.copyOfRange(newDataContent, 0, (int)blockSize));
    Assert.assertArrayEquals(
        Arrays.copyOfRange(dataContent, (int)blockSize, (int)blockSize * 2),
        Arrays.copyOfRange(newDataContent, (int)blockSize, (int)blockSize * 2));
    Assert.assertArrayEquals(buffer, Arrays.copyOfRange(newDataContent,
        (int)blockSize * 2, (int)fileLen));
  }
}
