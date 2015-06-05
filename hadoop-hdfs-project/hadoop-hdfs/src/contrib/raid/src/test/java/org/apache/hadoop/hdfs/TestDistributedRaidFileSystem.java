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
package org.apache.hadoop.hdfs;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.BlockCodec;
import org.apache.hadoop.contrib.raid.HdfsRaidConfigKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDistributedRaidFileSystem {

  private static Configuration conf;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem dfs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();

    dfs = new DistributedRaidFileSystem();
    dfs.initialize(dfsCluster.getURI(), conf);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Test
  public void testBasicReadWrite() throws Exception {
    String data = "Hello, this is some test data";
    Path file = new Path("/test.txt");

    FSDataOutputStream out = dfs.create(file);
    out.write(data.getBytes());
    out.close();

    byte[] buffer = new byte[data.length()];
    FSDataInputStream in = dfs.open(file);
    in.read(buffer);
    in.close();

    Assert.assertArrayEquals(data.getBytes(), buffer);
  }

  public void testAppend() throws Exception {
    String data = "Hello, this is some test data";
    String data1 = " Hello, this is some appended data";
    Path file = new Path("/test.txt");

    FSDataOutputStream out = dfs.create(file);
    out.write(data.getBytes());
    out.close();
    out = dfs.append(file, 4096, null);
    out.write(data1.getBytes());
    out.close();

    byte[] buffer = new byte[data.length() + data1.length()];
    FSDataInputStream in = dfs.open(file);
    in.read(buffer);
    in.close();
    Assert.assertArrayEquals((data + data1).getBytes(), buffer);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAppendEncodedFile() throws Exception {
    String data = "Hello, this is some test data";
    Path file = new Path("/test.txt");
    Path encodedFile = BlockCodec.getCodingFile(file);

    FSDataOutputStream out = dfs.create(file);
    out.write(data.getBytes());
    out.close();
    FSDataOutputStream encodedOut = dfs.create(encodedFile);
    encodedOut.close();
    out = dfs.append(file, 4096, null);
    out.write(data.getBytes());
    out.close();
  }

  @Test
  public void testReadFully() throws Exception {
    String data = "Hello, this is some test data";
    Path file = new Path("/test.txt");

    FSDataOutputStream out = dfs.create(file);
    out.write(data.getBytes());
    out.close();

    FSDataInputStream in = dfs.open(file);
    byte[] buf = new byte[data.getBytes().length];
    long origPos = in.getPos();
    in.readFully(0, buf);
    long posAfterRf = in.getPos();
    in.close();

    Assert.assertEquals(origPos, posAfterRf);
  }

  @Test
  public void testBasicReadWriteAfterEncoding() throws Exception {
    Path file = new Path("/test2.txt");
    DFSTestUtil.createFile(dfs, file, 4096, 10 << 20, 1 << 20, (short) 3,
      System.currentTimeMillis());
    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Thread.sleep(1000);

    conf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    BlockCodec codec = new BlockCodec(conf);
    codec.encode(file);
    FileStatus fileStatus = dfs.getFileStatus(file);
    Assert.assertEquals(1, fileStatus.getReplication());

    byte[] data = new byte[fileContent.length];
    FSDataInputStream in = dfs.open(file);
    int totalReadLen = 0;
    while (totalReadLen < data.length) {
      int readLen = in.read(data, totalReadLen, data.length - totalReadLen);
      totalReadLen += readLen;
    }
    Assert.assertArrayEquals(fileContent, data);
  }

  @Test
  public void testReadWithMissingBlock() throws Exception {
    Path file = new Path("/test3.txt");
    DFSTestUtil.createFile(dfs, file, 4096, 10 << 20, 1 << 20, (short) 3,
      System.currentTimeMillis());
    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Thread.sleep(1000);

    conf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    BlockCodec codec = new BlockCodec(conf);
    codec.encode(file);
    FileStatus fileStatus = dfs.getFileStatus(file);
    Assert.assertEquals(1, fileStatus.getReplication());

    DFSClient dfsClient = new DFSClient(dfsCluster.getURI(), conf);
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(), 4 << 20, 1 << 20);
    Assert.assertEquals(1, blocks.getLocatedBlocks().size());
    LocatedBlock block = blocks.get(0);
    deleteBlockFile(block);

    runReadTest(file, fileContent, 4);
  }

  @Test
  public void testReadWithChecksumCorrupted() throws Exception {
    Path file = new Path("/test4.txt");
    DFSTestUtil.createFile(dfs, file, 4096, 10 << 20, 1 << 20, (short) 3,
      System.currentTimeMillis());
    byte[] fileContent = DFSTestUtil.readFileBuffer(dfs, file);
    Thread.sleep(1000);

    conf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 1000);
    BlockCodec codec = new BlockCodec(conf);
    codec.encode(file);
    FileStatus fileStatus = dfs.getFileStatus(file);
    Assert.assertEquals(1, fileStatus.getReplication());

    DFSClient dfsClient = new DFSClient(dfsCluster.getURI(), conf);
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(), 4 << 20, 1 << 20);
    Assert.assertEquals(1, blocks.getLocatedBlocks().size());
    LocatedBlock block = blocks.get(0);
    corruptChecksumFile(block);

    runReadTest(file, fileContent, 4);
  }

  void runReadTest(Path file, byte[] fileContent, int corruptedBlockIdx) throws Exception {
    FileStatus fileStatus = dfs.getFileStatus(file);

    // Read the whole file and verify the data
    byte[] data = new byte[fileContent.length];
    FSDataInputStream in = dfs.open(file);
    int totalReadLen = 0;
    while (totalReadLen < data.length) {
      int readLen = in.read(data, totalReadLen, data.length - totalReadLen);
      totalReadLen += readLen;
    }
    Assert.assertArrayEquals(fileContent, data);
    in.close();

    // Pread part of the file and verify the data
    int blockSize = (int) (fileStatus.getBlockSize());
    data = new byte[blockSize];
    in = dfs.open(file);
    long offset = corruptedBlockIdx * fileStatus.getBlockSize();
    int readLen = in.read(offset, data, 0, data.length);
    Assert.assertEquals(data.length, readLen);
    Assert.assertArrayEquals(
      Arrays.copyOfRange(fileContent, corruptedBlockIdx * blockSize, (corruptedBlockIdx + 1)
          * blockSize), data);
    in.close();

    // Readfully and verify
    Assert.assertTrue(corruptedBlockIdx > 0);
    long pos = corruptedBlockIdx * blockSize - 100;
    int len = blockSize + 200;
    byte[] buffer = new byte[len];
    in = dfs.open(file);
    in.readFully(pos, buffer, 0, buffer.length);
    Assert.assertArrayEquals(Arrays.copyOfRange(fileContent, (int) pos, (int) (pos + len)), buffer);
    in.close();
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
    } while (dirIdx < 10);
    Assert.assertNotNull(blockFile);

    fs.delete(new Path(blockFile), false);
  }

  private void corruptChecksumFile(LocatedBlock block) throws Exception {
    long blockId = block.getBlock().getBlockId();
    String bpId = block.getBlock().getBlockPoolId();
    String dataDir = dfsCluster.getDataDirectory();
    int dirIdx = 1;

    String metaFile = null;
    FileSystem fs = FileSystem.getLocal(new Configuration());
    do {
      metaFile = dataDir + "/data" + (dirIdx++) + "/current/" + bpId + "/current/finalized/blk_"
          + blockId + "_" + block.getBlock().getGenerationStamp() + ".meta";
      if (fs.exists(new Path(metaFile))) {
        break;
      }
    } while (dirIdx < 10);
    Assert.assertNotNull(metaFile);

    FileStatus status = fs.getFileStatus(new Path(metaFile));
    int fileLen = (int) (status.getLen());

    byte[] fileContent = DFSTestUtil.readFileBuffer(fs, new Path(metaFile));

    // Corrupt the meta file
    FSDataOutputStream out = fs.create(new Path(metaFile), true);
    out.write(fileContent, 0, 128);
    for (int i = 0; i < fileLen - 128; ++i) {
      out.write(0x10);
    }
    out.close();
  }
}
