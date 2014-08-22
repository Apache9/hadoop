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
    dfs.getConf().setInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 4000);
    BlockCodec codec = new BlockCodec(dfs.getConf());

    Path path = new Path("/test");
    dfs.mkdirs(path);
    Assert.assertFalse(codec.isFileEncodable(dfs.getFileStatus(path)));

    path = new Path("/test.txt");
    dfs.create(path).close();
    Assert.assertFalse(codec.isFileEncodable(dfs.getFileStatus(path)));

    Thread.sleep(5000);
    Assert.assertTrue(codec.isFileEncodable(dfs.getFileStatus(path)));
  }

  @Test
  public void testGetErasures() throws Exception {
    Map<Integer, OutputStream> dataErasures =
        new HashMap<Integer, OutputStream>();
    dataErasures.put(1, new ByteArrayOutputStream(10));
    dataErasures.put(2, new ByteArrayOutputStream(10));
    dataErasures.put(3, new ByteArrayOutputStream(10));

    Map<Integer, OutputStream> codingErasures =
        new HashMap<Integer, OutputStream>();
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
      Assert.assertTrue(Arrays.binarySearch(erasures, (entry.getKey() % 3 + 6))
          >= 0);
    }
  }

  @Test
  public void testPartitionErasureBlocks() throws Exception {
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    BlockCodec codec = new BlockCodec(conf);

    int totalBlockNum = 22;
    int[] corruptedBlocks = {0, 7, 10, 21, 25, 33};
    Map<Integer, OutputStream>[] dataBlocksGroup = new Map[4];
    Map<Integer, OutputStream>[] codingBlocksGroup = new Map[4];

    codec.partitionErasureBlocks(corruptedBlocks, totalBlockNum,
        dataBlocksGroup, codingBlocksGroup);
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
        Assert.assertNotNull(codingBlocksGroup[groupId].get(corruptedBlocks[i]
            - totalBlockNum));
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

    Map<Integer, OutputStream> dataErasures =
        new HashMap<Integer, OutputStream>();
    dataErasures.put(12, new ByteArrayOutputStream(10));
    dataErasures.put(13, new ByteArrayOutputStream(10));
    FSDataInputStream[] dataIns = new FSDataInputStream[6];

    Map<Integer, OutputStream> codingErasures =
        new HashMap<Integer, OutputStream>();
    codingErasures.put(6, new ByteArrayOutputStream(10));
    FSDataInputStream[] codingIns = new FSDataInputStream[3];

    codec.constructBlockInputStreams(file, groupNo, dataErasures,
        codingErasures, dataIns, codingIns);

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
  public void testWriteDecodedData() throws Exception {
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    BlockCodec codec = new BlockCodec(conf);

    int groupNo = 1;
    Map<Integer, OutputStream> dataErasures =
        new HashMap<Integer, OutputStream>();
    dataErasures.put(6, new ByteArrayOutputStream(128));
    dataErasures.put(7, new ByteArrayOutputStream(128));
    byte[][] data = new byte[6][100];
    for (int i = 0; i < data.length; ++i) {
      int blockIdx = 6 * groupNo + i;
      if (dataErasures.get(blockIdx) != null) {
        Arrays.fill(data[i], 0, data[i].length, (byte)1);
      } else {
        Arrays.fill(data[i], 0, data[i].length, (byte)2);
      }
    }

    Map<Integer, OutputStream> codingErasures =
        new HashMap<Integer, OutputStream>();
    codingErasures.put(3, new ByteArrayOutputStream(128));
    byte[][] coding = new byte[3][100];
    for (int i = 0; i < coding.length; ++i) {
      int blockIdx = 3 * groupNo + i;
      if (codingErasures.get(blockIdx) != null) {
        Arrays.fill(data[i], 0, data[i].length, (byte)3);
      } else {
        Arrays.fill(data[i], 0, data[i].length, (byte)4);
      }
    }

    codec.writeDecodedData(groupNo, dataErasures, codingErasures, data, coding);

    for (Map.Entry<Integer, OutputStream> entry : dataErasures.entrySet()) {
      ByteArrayOutputStream out = (ByteArrayOutputStream)entry.getValue();
      byte[] buffer = out.toByteArray();
      int index = entry.getKey().intValue() % 6;
      Assert.assertArrayEquals(data[index], buffer);
    }

    for (Map.Entry<Integer, OutputStream> entry : codingErasures.entrySet()) {
      ByteArrayOutputStream out = (ByteArrayOutputStream)entry.getValue();
      byte[] buffer = out.toByteArray();
      int index = entry.getKey().intValue() % 3;
      Assert.assertArrayEquals(coding[index], buffer);
    }
  }

  @Test
  public void testEncodeAndDecodeBlocksThroughStream() throws Exception {
    Path file = new Path("/text.txt");
    int blockSize = 1048576;
    int fileLen = blockSize * 10 - 100;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short)1,
        System.currentTimeMillis());
    Thread.sleep(1000);

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);

    // Encode the file
    codec.encode(file);
    Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(codingFile));
    FileStatus codingFileStatus = dfs.getFileStatus(codingFile);
    Assert.assertEquals(codingFileStatus.getBlockSize() * 2 * 3,
        codingFileStatus.getLen());

    // Decode one data block and one coding block to memory buffer and check
    // the decoded result
    Map<Integer, OutputStream> dataErasures =
        new HashMap<Integer, OutputStream>();
    dataErasures.put(1, new ByteArrayOutputStream(blockSize));
    Map<Integer, OutputStream> codingErasures =
        new HashMap<Integer, OutputStream>();
    codingErasures.put(1, new ByteArrayOutputStream(blockSize));
    codec.decodeBlocks(file, 0, blockSize, dataErasures, codingErasures);

    // Check the decoded data
    byte[] buffer = new byte[blockSize];
    FSDataInputStream dataIn = dfs.open(file);
    dataIn.read(blockSize, buffer, 0, buffer.length);
    Assert.assertArrayEquals(buffer,
        ((ByteArrayOutputStream)(dataErasures.get(1))).toByteArray());
    dataIn.close();

    // check the decoded coding
    FSDataInputStream codingIn = dfs.open(codingFile);
    codingIn.read(blockSize, buffer, 0, buffer.length);
    Assert.assertArrayEquals(buffer,
        ((ByteArrayOutputStream)(codingErasures.get(1))).toByteArray());
    codingIn.close();
  }

  @Test
  public void testEncodeAndDecodeBlocksThroughFile() throws Exception {
    Path file = new Path("/text2.txt");
    int blockSize = 1048576;
    int fileLen = blockSize * 10 - 100;
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short)3,
        System.currentTimeMillis());
    Thread.sleep(1000);

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);

    FileStatus status = dfs.getFileStatus(file);
    Assert.assertEquals((short)3, status.getReplication());

    // Encode the file
    codec.encode(file);
    Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(codingFile));
    FileStatus codingFileStatus = dfs.getFileStatus(codingFile);
    Assert.assertEquals(codingFileStatus.getBlockSize() * 2 * 3,
        codingFileStatus.getLen());

    status = dfs.getFileStatus(file);
    Assert.assertEquals((short)1, status.getReplication());

    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertEquals(fileLen, fileContent.length);
    byte[] codingContent = DFSTestUtil.readFileBuffer(dfs, codingFile);
    Assert.assertEquals(blockSize * 3 * 2, codingContent.length);

    // Corrupt 1 data block and 1 coding block
    LocatedBlock block = getBlock(file, 1, blockSize);
    dfsClient.reportBadBlocks(new LocatedBlock[]{block});
    block = getBlock(codingFile, 1, blockSize);
    dfsClient.reportBadBlocks(new LocatedBlock[]{block});

    // Decode and check
    int[] corruptedBlocks = {1, 10 + 1};
    codec.decode(file, corruptedBlocks);

    byte[] decodedFileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertEquals(fileLen, decodedFileContent.length);
    byte[] decodedCodingFileContent = DFSTestUtil.readFileBuffer(
        dfs, codingFile);
    Assert.assertEquals(blockSize * 3 * 2, decodedCodingFileContent.length);

    // Verify the data
    Assert.assertArrayEquals(fileContent, decodedFileContent);
    Assert.assertArrayEquals(codingContent, decodedCodingFileContent);
  }

  private LocatedBlock getBlock(Path file, int idx, long blockSize)
      throws Exception {
    NameNode nn = dfsCluster.getNameNode();
    DFSClient dfsClient = new DFSClient(nn.getNameNodeAddress(), conf);
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(),
        blockSize * idx, blockSize);
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
      blockFile = dataDir + "/data" + (dirIdx++) + "/current/" + bpId +
          "/current/finalized/blk_" + blockId;
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
    DFSTestUtil.createFile(dfs, file, 1024, fileLen, blockSize, (short)3,
        System.currentTimeMillis());
    Thread.sleep(1000);

    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY, 6);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY, 3);
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    BlockCodec codec = new BlockCodec(conf);

    FileStatus status = dfs.getFileStatus(file);
    Assert.assertEquals((short)3, status.getReplication());

    // Encode the file
    codec.encode(file);
    Path codingFile = BlockCodec.getCodingFile(file);
    Assert.assertTrue(dfs.exists(codingFile));
    FileStatus codingFileStatus = dfs.getFileStatus(codingFile);
    Assert.assertEquals(codingFileStatus.getBlockSize() * 2 * 3,
        codingFileStatus.getLen());

    status = dfs.getFileStatus(file);
    Assert.assertEquals((short)1, status.getReplication());

    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Assert.assertEquals(fileLen, fileContent.length);
    byte[] codingContent = DFSTestUtil.readFileBuffer(dfs, codingFile);
    Assert.assertEquals(blockSize * 3 * 2, codingContent.length);

    // Corrupt 1 data block
    LocatedBlock block = getBlock(file, 1, blockSize);
    dfsClient.reportBadBlocks(new LocatedBlock[]{block});

    // Try to read data of the corrupted block
    long offset = blockSize + 10;
    int length = 101;
    byte[] data = codec.decode(file, offset, length);
    Assert.assertNotNull(data);
    Assert.assertArrayEquals(
        Arrays.copyOfRange(fileContent, (int)offset, (int)(offset + length)),
        data);
  }
}
