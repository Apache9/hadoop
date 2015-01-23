/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.contrib.raid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockCodec {

  private static Configuration conf;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem dfs;
  private static DFSClient dfsClient;

  private static final long BLOCK_SIZE = 1048576;
  private static final int DATA_BLOCK_NUM = 6;
  private static final int CODING_BLOCK_NUM = 3;

  @BeforeClass
  public static void setUpClass() throws IOException {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    dfsCluster.waitActive();
    dfs = dfsCluster.getFileSystem();
    if (dfs instanceof DistributedFileSystem) {
      dfsClient = ((DistributedFileSystem) dfs).getClient();
    } else {
      throw new IOException("Non-distributed filesystem not supported");
    }
  }

  @AfterClass
  public static void tearDownClass() {
    dfsCluster.shutdown();
  }

  @Test
  public void testIsFileEncodable() throws Exception {
    dfs.getConf().setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 4000);
    BlockCodec codec = new BlockCodec(dfs.getConf());

    Path path = new Path("/test");
    dfs.mkdirs(path);
    Assert.assertFalse(codec.isFileEncodable(dfs.getFileStatus(path)));
    Assert.assertFalse(codec.isFileEncodable(path));

    path = new Path("/test.txt");
    dfs.create(path).close();
    Assert.assertFalse(codec.isFileEncodable(dfs.getFileStatus(path)));
    Assert.assertFalse(codec.isFileEncodable(path));

    Thread.sleep(5000);
    Assert.assertTrue(codec.isFileEncodable(dfs.getFileStatus(path)));
    Assert.assertTrue(codec.isFileEncodable(path));
  }

  @Test(expected = IOException.class)
  public void testEstimateSavingDir() throws Exception {
    dfs.getConf().setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    BlockCodec codec = new BlockCodec(dfs.getConf());

    Path path = new Path("/test");
    dfs.mkdirs(path);
    codec.estimateSaving(path);
  }

  @Test(expected = IOException.class)
  public void testEstimateSavingNewFile() throws Exception {
    dfs.getConf().setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 4000);
    BlockCodec codec = new BlockCodec(dfs.getConf());

    Path path = new Path("/test.txt");
    dfs.create(path).close();
    codec.estimateSaving(path);
  }

  @Test
  public void testEstimateSaving() throws Exception {
    Path file = new Path("/text.txt");
    int blockSize = 1048576;
    int fileLen = blockSize * 10 - 100;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short) 3,
      System.currentTimeMillis());
    Thread.sleep(4000);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);
    long expected = 12582612;
    long result = codec.estimateSaving(file);
    Assert.assertTrue(expected == result);
  }

  @Test
  public void testGetErasures() throws Exception {
    Map<Integer, OutputStream> dataErasures = new HashMap<Integer, OutputStream>();
    dataErasures.put(1, new ByteArrayOutputStream(10));
    dataErasures.put(2, new ByteArrayOutputStream(10));
    dataErasures.put(3, new ByteArrayOutputStream(10));

    Map<Integer, OutputStream> codingErasures = new HashMap<Integer, OutputStream>();
    codingErasures.put(4, new ByteArrayOutputStream(10));
    codingErasures.put(5, new ByteArrayOutputStream(10));
    codingErasures.put(6, new ByteArrayOutputStream(10));

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    BlockCodec codec = new BlockCodec(dfs.getConf());
    int[] erasures = codec.getErasures(dataErasures, codingErasures);
    Assert.assertEquals(6, erasures.length);

    Arrays.sort(erasures);
    for (Map.Entry<Integer, OutputStream> entry : dataErasures.entrySet()) {
      Assert.assertTrue(Arrays.binarySearch(erasures, entry.getKey() % 6) >= 0);
    }

    for (Map.Entry<Integer, OutputStream> entry : codingErasures.entrySet()) {
      Assert.assertTrue(Arrays.binarySearch(erasures, (entry.getKey() % 3 + 6)) >= 0);
    }
  }

  @Test
  public void testPartitionErasureBlocks() throws Exception {
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    BlockCodec codec = new BlockCodec(conf);

    int totalBlockNum = 22;
    int[] corruptedBlocks = { 0, 7, 10, 21, 25, 33 };
    Map<Integer, OutputStream>[] dataBlocksGroup = new Map[4];
    Map<Integer, OutputStream>[] codingBlocksGroup = new Map[4];

    codec
        .partitionErasureBlocks(corruptedBlocks, totalBlockNum, dataBlocksGroup, codingBlocksGroup);
    for (int i = 0; i < corruptedBlocks.length; ++i) {
      if (corruptedBlocks[i] < totalBlockNum) {
        // data blocks
        int groupId = corruptedBlocks[i] / 6;
        Assert.assertNotNull(dataBlocksGroup[groupId]);
        Assert.assertNotNull(dataBlocksGroup[groupId].get(corruptedBlocks[i]));
      } else {
        // coding blocks
        int groupId = (corruptedBlocks[i] - totalBlockNum) / 3;
        Assert.assertNotNull(codingBlocksGroup[groupId]);
        Assert.assertNotNull(codingBlocksGroup[groupId].get(corruptedBlocks[i] - totalBlockNum));
      }
    }
  }

  @Test
  public void testConstructBlockInputStreams() throws Exception {
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    BlockCodec codec = new BlockCodec(conf);

    Path file = new Path("/test.txt");
    dfs.create(file).close();
    Path codingFile = BlockCodec.getCodingFile(file);
    dfs.create(codingFile).close();
    int groupNo = 2;

    Map<Integer, OutputStream> dataErasures = new HashMap<Integer, OutputStream>();
    dataErasures.put(12, new ByteArrayOutputStream(10));
    dataErasures.put(13, new ByteArrayOutputStream(10));
    FSDataInputStream[] dataIns = new FSDataInputStream[6];

    Map<Integer, OutputStream> codingErasures = new HashMap<Integer, OutputStream>();
    codingErasures.put(6, new ByteArrayOutputStream(10));
    FSDataInputStream[] codingIns = new FSDataInputStream[3];

    codec.constructBlockInputStreams(file, groupNo, dataErasures, codingErasures, dataIns,
      codingIns);

    for (Map.Entry<Integer, OutputStream> entry : dataErasures.entrySet()) {
      int blockIdx = entry.getKey();
      int index = blockIdx % 6;
      Assert.assertNull(dataIns[index]);
    }

    for (Map.Entry<Integer, OutputStream> entry : codingErasures.entrySet()) {
      int blockIdx = entry.getKey();
      int index = blockIdx % 3;
      Assert.assertNull(codingIns[index]);
    }
  }

  @Test
  public void testEncodeAndDecodeBlocksThroughStream() throws Exception {
    Path file = new Path("/text.txt");
    int blockSize = (int) (BLOCK_SIZE);
    int fileLen = blockSize * 10 - 100;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short) 1,
      System.currentTimeMillis());
    FileStatus fileStatus = dfs.getFileStatus(file);
    Thread.sleep(1000);

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, DATA_BLOCK_NUM);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, CODING_BLOCK_NUM);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);

    // Encode the file
    encodeAndCheckFile(codec, file, fileLen);

    // Decode and check
    int totalBlockNum = (int) ((fileStatus.getLen() - 1) / BLOCK_SIZE + 1);
    // Case 0
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 0, new int[] { 1 }, new int[] {});

    // Case 1
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 0, new int[] { 1 }, new int[] { 1 });

    // Case 2
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 1, new int[] { 9 }, new int[] { 3 });

    // Case 3
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 0, new int[] { 1, 2 },
      new int[] { 1 });

    // Case 4
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 0, new int[] { 1 }, new int[] { 1,
        2 });

    // Case 5
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 0, new int[] { 1, 2, 3 },
      new int[] {});

    // Case 6
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 1, new int[] { 8, 9 },
      new int[] { 3 });

    // Case 7
    decodeAndCheckBlock(codec, file, fileStatus, totalBlockNum, 1, new int[] { 7, 8, 9 },
      new int[] {});
  }

  void encodeAndCheckFile(BlockCodec codec, Path file, long fileLen) throws Exception {
    codec.encode(file);
    Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(codingFile));
    FileStatus codingFileStatus = dfs.getFileStatus(codingFile);
    int codingBlockNum = (int) ((fileLen - 1) / BLOCK_SIZE / DATA_BLOCK_NUM + 1);
    Assert.assertEquals(codingBlockNum * CODING_BLOCK_NUM * BLOCK_SIZE, codingFileStatus.getLen());
  }

  void decodeAndCheckBlock(BlockCodec codec, Path file, FileStatus fileStatus, int totalBlockNum,
      int groupNo, int[] corruptedData, int[] corruptedCoding) throws Exception {
    Assert.assertTrue(corruptedData.length + corruptedCoding.length <= CODING_BLOCK_NUM);

    Map<Integer, OutputStream> dataErasures = new HashMap<Integer, OutputStream>();
    for (int i = 0; i < corruptedData.length; ++i) {
      dataErasures.put(corruptedData[i], new ByteArrayOutputStream((int) BLOCK_SIZE));
    }

    Map<Integer, OutputStream> codingErasures = new HashMap<Integer, OutputStream>();
    for (int i = 0; i < corruptedCoding.length; ++i) {
      codingErasures.put(corruptedCoding[i], new ByteArrayOutputStream((int) BLOCK_SIZE));
    }

    codec.decodeBlocks(file, fileStatus, totalBlockNum, groupNo, BLOCK_SIZE, dataErasures,
      codingErasures);

    // Check the decoded data
    for (int i = 0; i < corruptedData.length; ++i) {
      int dataLen = (int) BLOCK_SIZE;
      if (corruptedData[i] == totalBlockNum - 1) {
        dataLen = (int) ((fileStatus.getLen() - 1) % BLOCK_SIZE + 1);
      }

      byte[] buffer = new byte[dataLen];
      FSDataInputStream dataIn = dfs.open(file);
      dataIn.read(BLOCK_SIZE * corruptedData[i], buffer, 0, buffer.length);
      byte[] result = ((ByteArrayOutputStream) (dataErasures.get(corruptedData[i]))).toByteArray();
      Assert.assertArrayEquals(buffer, Arrays.copyOfRange(result, 0, dataLen));
      dataIn.close();
    }

    // check the decoded coding
    for (int i = 0; i < corruptedCoding.length; ++i) {
      byte[] buffer = new byte[(int) BLOCK_SIZE];
      FSDataInputStream codingIn = dfs.open(BlockCodec.getCodingFile(file));
      codingIn.read(BLOCK_SIZE * corruptedCoding[i], buffer, 0, buffer.length);
      byte[] result = ((ByteArrayOutputStream) (codingErasures.get(corruptedCoding[i])))
          .toByteArray();
      Assert.assertArrayEquals(buffer, result);
      codingIn.close();
    }
  }

  @Test
  public void testEncodeAndDecodeBlocksThroughFile() throws Exception {
    Path file = new Path("/text2.txt");
    int blockSize = 1048576;
    int fileLen = blockSize * 10 - 100;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short) 3,
      System.currentTimeMillis());
    Thread.sleep(1000);

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);

    FileStatus status = dfs.getFileStatus(file);
    Assert.assertEquals((short) 3, status.getReplication());

    // Encode the file
    codec.encode(file);
    Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(codingFile));
    FileStatus codingFileStatus = dfs.getFileStatus(codingFile);
    Assert.assertEquals(codingFileStatus.getBlockSize() * 2 * 3, codingFileStatus.getLen());

    status = dfs.getFileStatus(file);
    Assert.assertEquals((short) 1, status.getReplication());

    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertEquals(fileLen, fileContent.length);
    byte[] codingContent = DFSTestUtil.readFileBuffer(dfs, codingFile);
    Assert.assertEquals(blockSize * 3 * 2, codingContent.length);

    // Corrupt 1 data block and 1 coding block
    LocatedBlock block = getBlock(file, 1, blockSize);
    dfsClient.reportBadBlocks(new LocatedBlock[] { block });
    block = getBlock(codingFile, 1, blockSize);
    dfsClient.reportBadBlocks(new LocatedBlock[] { block });

    // Decode and check
    int[] corruptedBlocks = { 1, 10 + 1 };
    codec.decode(file, corruptedBlocks);

    byte[] decodedFileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertEquals(fileLen, decodedFileContent.length);
    byte[] decodedCodingFileContent = DFSTestUtil.readFileBuffer(dfs, codingFile);
    Assert.assertEquals(blockSize * 3 * 2, decodedCodingFileContent.length);

    // Verify the data
    Assert.assertArrayEquals(fileContent, decodedFileContent);
    Assert.assertArrayEquals(codingContent, decodedCodingFileContent);
  }

  private LocatedBlock getBlock(Path file, int idx, long blockSize) throws Exception {
    NameNode nn = dfsCluster.getNameNode();
    DFSClient dfsClient = new DFSClient(nn.getNameNodeAddress(), conf);
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(), blockSize * idx, blockSize);
    Assert.assertEquals(1, blocks.getLocatedBlocks().size());
    return blocks.getLocatedBlocks().get(0);
  }

  private void deleteBlockFile(LocatedBlock block) throws Exception {
    long blockId = block.getBlock().getBlockId();
    String bpId = block.getBlock().getBlockPoolId();
    String dataDir = dfsCluster.getDataDirectory();
    int dirIdx = 1;

    String blockFile = null;
    FileSystem fs = FileSystem.getLocal(new Configuration());
    do {
      blockFile = dataDir + "/data" + (dirIdx++) + "/current/" + bpId + "/current/finalized/blk_"
          + blockId;
      if (fs.exists(new Path(blockFile))) {
        break;
      }
    } while (dirIdx > 10);
    Assert.assertNotNull(blockFile);

    fs.delete(new Path(blockFile), false);
  }

  @Test
  public void testDecodePartialBlock() throws Exception {
    Path file = new Path("/text3.txt");
    int blockSize = 1048576;
    int fileLen = blockSize * 10 - 100;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short) 3,
      System.currentTimeMillis());
    Thread.sleep(1000);

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);

    FileStatus status = dfs.getFileStatus(file);
    Assert.assertEquals((short) 3, status.getReplication());

    // Encode the file
    codec.encode(file);
    Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(codingFile));
    FileStatus codingFileStatus = dfs.getFileStatus(codingFile);
    Assert.assertEquals(codingFileStatus.getBlockSize() * 2 * 3, codingFileStatus.getLen());

    status = dfs.getFileStatus(file);
    Assert.assertEquals((short) 1, status.getReplication());

    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertEquals(fileLen, fileContent.length);
    byte[] codingContent = DFSTestUtil.readFileBuffer(dfs, codingFile);
    Assert.assertEquals(blockSize * 3 * 2, codingContent.length);

    // Corrupt 1 data block
    LocatedBlock block = getBlock(file, 1, blockSize);
    dfsClient.reportBadBlocks(new LocatedBlock[] { block });

    // Try to read data of the corrupted block
    long offset = blockSize + 10;
    int length = 101;
    byte[] data = codec.decode(file, offset, length);
    Assert.assertNotNull(data);
    Assert.assertArrayEquals(
      Arrays.copyOfRange(fileContent, (int) offset, (int) (offset + length)), data);
  }

  @Test
  public void testAdjustLength() {
    int eps = BlockCodec.getStripeSize();
    Assert.assertEquals(0, BlockCodec.adjustLength(0, eps));
    Assert.assertEquals(eps, BlockCodec.adjustLength(eps, eps));
    Assert.assertEquals(eps, BlockCodec.adjustLength(1, eps));
    Assert.assertEquals(2 * eps, BlockCodec.adjustLength(eps + 1, eps));
    Assert.assertEquals(2 * eps, BlockCodec.adjustLength(2 * eps, eps));
  }

  @Test
  public void testAdjustOffset() {
    int eps = BlockCodec.getStripeSize();
    Assert.assertEquals(0, BlockCodec.adjustOffset(0, eps));
    Assert.assertEquals(0, BlockCodec.adjustOffset(1, eps));
    Assert.assertEquals(0, BlockCodec.adjustOffset(eps - 1, eps));
    Assert.assertEquals(eps, BlockCodec.adjustOffset(eps, eps));
    Assert.assertEquals(eps, BlockCodec.adjustOffset(eps + 1, eps));
  }
}
