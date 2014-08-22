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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockOutputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import com.xiaomi.infra.ec.ErasureCodec;
import com.xiaomi.infra.ec.ErasureCodec.Algorithm;

/**
 * BlockCodec implements the main functionality of encoding and decoding blocks.
 */
public class BlockCodec {

  private static final Log LOG = LogFactory.getLog(BlockCodec.class);

  private final Configuration conf;
  private final FileSystem fs;
  private final long raidTimeWindowMs;
  private final int dataBlocksNum;
  private final int codingBlocksNum;
  private final long blockSize;
  private final ErasureCodec codec;
  private final DFSClient dfsClient;

  private static final Path RAID_ROOT = new Path("/raid");
  private static final String CODINF_FILE_SUFFIX = ".ec";
  private static final int ENCODE_DATA_SIZE = 4096;
  private static final int WORD_SIZE = 8;
  private static final OutputStream DUMMY_STREAM = new ByteArrayOutputStream(1);

  public BlockCodec(Configuration conf) throws IOException {
    this.raidTimeWindowMs = conf.getLong(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT);
    this.dataBlocksNum = conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
    this.codingBlocksNum = conf.getInt(
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
    this.blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

    this.conf = conf;
    this.fs = FileSystem.get(conf);
    checkRaidRoot();

    this.codec = new ErasureCodec.Builder(Algorithm.Reed_Solomon)
        .dataBlockNum(dataBlocksNum)
        .codingBlockNum(codingBlocksNum)
        .wordSize(WORD_SIZE)
        .build();
    if (fs instanceof DistributedFileSystem) {
      this.dfsClient = ((DistributedFileSystem) fs).getClient();
    } else {
      throw new IOException("Non-distributed filesystem is not supported");
    }
  }

  /**
   * Encodes specified file.
   */
  public void encode(Path file) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    // Check if the file is encodable
    if (!isFileEncodable(fileStatus)) {
      throw new IOException("File " + file + " is not encodable, try later");
    }

    // Get the block locations of the file
    long fileLen = fileStatus.getLen();
    BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileLen);
    Preconditions.checkState(fileStatus.getBlockSize() % ENCODE_DATA_SIZE == 0,
        "Block size must be multiple of " + ENCODE_DATA_SIZE);

    // Create tmp coding file output stream
    Path codingFile = getCodingFile(file);
    Path tmpCodingFile = new Path(codingFile.toString() + ".tmp");
    FSDataOutputStream codingOut = fs.create(tmpCodingFile, true,
        conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
        fs.getDefaultReplication(tmpCodingFile), blockSize);

    // Encode the blocks
    int roundNum = locations.length / dataBlocksNum;
    for (int r = 0; r < roundNum; ++r) {
      encodeBlocks(file, fileStatus, codingOut,  r * dataBlocksNum,
          dataBlocksNum);
    }
    int remainingBlocks = locations.length % dataBlocksNum;
    if (remainingBlocks != 0) {
      encodeBlocks(file, fileStatus, codingOut,
          locations.length - remainingBlocks, remainingBlocks);
    }

    // Rename the tmp coding file to formal coding file
    try {
      codingOut.close();
      if (!fs.rename(tmpCodingFile, codingFile)) {
        LOG.error("Rename " + tmpCodingFile + " to " + codingFile + " failed");
        throw new IOException("Rename " + tmpCodingFile + " to " + codingFile
            + " failed");
      }
    } finally {
      // Remove the tmp coding file if it still exists
      fs.delete(tmpCodingFile, true);
    }

    // Change the data and coding file's replica number
    fs.setReplication(file, (short)1);
    fs.setReplication(codingFile, (short)1);
  }

  /**
   * Decodes corrupted blocks of specified file.
   */
  public void decode(Path file, int[] corruptedBlocks) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    long fileLen = fileStatus.getLen();
    long blockSize = fileStatus.getBlockSize();
    BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileLen);
    Path codingFile = getCodingFile(file);

    // Partition the corrupted blocks into groups
    int groupNum = locations.length / dataBlocksNum;
    if (locations.length % dataBlocksNum != 0) {
      groupNum++;
    }
    Map<Integer, OutputStream>[] erasuredDataInfos = new Map[groupNum];
    Map<Integer, OutputStream>[] erasuredCodingInfos = new Map[groupNum];
    partitionErasureBlocks(corruptedBlocks, locations.length,
        erasuredDataInfos, erasuredCodingInfos);
    // TODO: Before decoding, we should ensure that the corrupted blocks have
    // already be deleted.
    constructBlockOutputStreams(file, erasuredDataInfos);
    constructBlockOutputStreams(codingFile, erasuredCodingInfos);

    // Decode blocks within each group
    try {
      for (int i = 0; i < groupNum; ++i) {
        decodeBlocks(file, i, blockSize, erasuredDataInfos[i],
            erasuredCodingInfos[i]);
      }
    } finally {
      for (Map<Integer, OutputStream> map : erasuredDataInfos) {
        closeOutputStreams(map.values().toArray());
      }

      for (Map<Integer, OutputStream> map : erasuredCodingInfos) {
        closeOutputStreams(map.values().toArray());
      }
    }
  }

  /**
   * Encodes a specified group of blocks.
   */
  void encodeBlocks(Path file, FileStatus fileStatus,
      FSDataOutputStream codingOut, int start, int len) throws IOException {
    FSDataInputStream[] dataIns = new FSDataInputStream[len];
    try {
      for (int i = 0; i < dataIns.length; ++i) {
        dataIns[i] = fs.open(file);
      }

      byte[][] result = new byte[codingBlocksNum][(int)fileStatus.getBlockSize()];
      byte[][] data = new byte[dataBlocksNum][ENCODE_DATA_SIZE];
      long blockSize = fileStatus.getBlockSize();
      for (int r = 0; r < blockSize / ENCODE_DATA_SIZE; ++r) {
        // Read next piece of data
        for (int i = 0; i < dataIns.length; ++i) {
          long startPos = (start + i) * blockSize;
          dataIns[i].read(startPos + r * ENCODE_DATA_SIZE, data[i], 0,
              data[i].length);
        }

        // If not enough data blocks, fill 0s
        if (dataIns.length < dataBlocksNum) {
          for (int i = dataIns.length - 1; i < dataBlocksNum; ++i) {
            Arrays.fill(data[i], 0, data[i].length, (byte) 0);
          }
        }

        // Copy the coding piece to the result buffer
        byte[][] coding = codec.encode(data);
        for (int i = 0; i < coding.length; ++i) {
          System.arraycopy(coding[i], 0, result[i], r * ENCODE_DATA_SIZE,
              coding[i].length);
        }
      }

      writeEncodedData(codingOut, result);
    } finally {
      closeInputStreams(dataIns);
    }
  }

  /**
   * Decodes corrupted blocks within a specified group.
   */
  void decodeBlocks(Path file, int groupNo, long blockSize,
      Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures) throws IOException {
    FSDataInputStream[] dataIns = new FSDataInputStream[dataBlocksNum];
    FSDataInputStream[] codingIns = new FSDataInputStream[codingBlocksNum];
    try {
      constructBlockInputStreams(file, groupNo, dataErasures, codingErasures,
          dataIns, codingIns);

      byte[][] data = new byte[dataBlocksNum][ENCODE_DATA_SIZE];
      byte[][] coding = new byte[codingBlocksNum][ENCODE_DATA_SIZE];
      int[] erasures = getErasures(dataErasures, codingErasures);
      // Construct the corrupted blocks
      for (int r = 0; r < blockSize / ENCODE_DATA_SIZE; ++r) {
        int count = 0;
        // Read next piece of data
        for (int i = 0; i < dataBlocksNum; ++i) {
          int blockIdx = groupNo * dataBlocksNum + i;
          if (dataErasures.get(blockIdx) == null) {
            dataIns[i].read(blockIdx * blockSize + r * ENCODE_DATA_SIZE,
                data[i], 0, data[i].length);
            ++count;
          } else {
            // Here can ensure that the last remaining blocks supplement with 0s
            Arrays.fill(data[i], 0, data[i].length, (byte) 0);
          }
        }

        // Read next piece of coding
        for (int i = 0; i < codingBlocksNum; ++i) {
          int blockIdx = groupNo * codingBlocksNum + i;
          if (count < dataBlocksNum && codingErasures.get(blockIdx) == null) {
            codingIns[i].read(blockIdx * blockSize + r * ENCODE_DATA_SIZE,
                coding[i], 0, coding[i].length);
            ++count;
          } else {
            Arrays.fill(coding[i], 0, coding[i].length, (byte) 0);
          }
        }

        // Decode the corrupted data/coding and write back to the file
        codec.decode(erasures, data, coding);
        writeDecodedData(groupNo, dataErasures, codingErasures, data, coding);
      }
    } finally {
      closeInputStreams(dataIns);
      closeInputStreams(codingIns);
    }
  }

  boolean isFileEncodable(FileStatus fileStatus) throws IOException {
    if (!fileStatus.isFile()) {
      return false;
    }

    long currentTimeMs = System.currentTimeMillis();
    long lastModTimeMs = fileStatus.getModificationTime();
    long timeDiffMs = (currentTimeMs - lastModTimeMs);

    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      // For distributed filesystem, only closed and time threshold expired
      // files are allowed to be encoded
      return (timeDiffMs > raidTimeWindowMs &&
          dfs.isFileClosed(fileStatus.getPath()));
    } else {
      // For non-distributed filesystem, we only check modification time
      return timeDiffMs > raidTimeWindowMs;
    }
  }

  void checkRaidRoot() throws IOException {
    if (!fs.exists(RAID_ROOT)) {
      fs.mkdirs(RAID_ROOT);
    }
  }

  void writeEncodedData(OutputStream out, byte[][] data)
      throws IOException {
    for (int i = 0; i < data.length; ++i) {
      out.write(data[i]);
    }
  }

  void constructBlockInputStreams(Path file, int groupNo,
      Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures,
      FSDataInputStream[] dataIns, FSDataInputStream[] codingIns)
      throws IOException {
    int dataInsNum = 0;
    // Open data blocks for reading
    for (int i = 0; i < dataIns.length; ++i) {
      int blockIdx = groupNo * dataBlocksNum + i;
      if (dataErasures.get(blockIdx) == null) {
        FSDataInputStream in = fs.open(file);
        dataIns[i] = in;
        ++dataInsNum;
      }
    }

    Path codingFile = getCodingFile(file);
    int codingInsNum = 0;
    // Open coding blocks for reading
    for (int i = 0; i< codingIns.length; ++i) {
      if (dataInsNum + codingInsNum >= dataBlocksNum) {
        break;
      }

      int blockIdx = groupNo * codingBlocksNum + i;
      if (codingErasures.get(blockIdx) == null) {
        FSDataInputStream in = fs.open(codingFile);
        codingIns[i] = in;
        ++codingInsNum;
      }
    }
  }

  void writeDecodedData(int groupNo, Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures, byte[][] data,
      byte[][] coding) throws IOException {
    for (int i = 0; i < data.length; ++i) {
      int blockIdx = groupNo * dataBlocksNum + i;
      OutputStream out = dataErasures.get(blockIdx);
      if (out != null) {
        out.write(data[i]);
      }
    }

    for (int i = 0; i < coding.length; ++i) {
      int blockIdx = groupNo * codingBlocksNum + i;
      OutputStream out = codingErasures.get(blockIdx);
      if (out != null) {
        out.write(coding[i]);
      }
    }
  }

  void partitionErasureBlocks(int[] corruptedBlocks, int totalDataBlocksNum,
      Map<Integer, OutputStream>[] dataBlocksGroup,
      Map<Integer, OutputStream>[] codingBlocksGroup) {
    Preconditions.checkArgument(dataBlocksGroup.length ==
        codingBlocksGroup.length);
    for (int i = 0; i < dataBlocksGroup.length; ++i) {
      dataBlocksGroup[i] = new HashMap<Integer, OutputStream>();
      codingBlocksGroup[i] = new HashMap<Integer, OutputStream>();
    }

    for (int i = 0; i < corruptedBlocks.length; ++i) {
      if (corruptedBlocks[i] < totalDataBlocksNum) {
        // data blocks
        int index = corruptedBlocks[i] / dataBlocksNum;
        dataBlocksGroup[index] .put(corruptedBlocks[i], DUMMY_STREAM);
      } else {
        // coding blocks
        int index = (corruptedBlocks[i] - totalDataBlocksNum) / codingBlocksNum;
        codingBlocksGroup[index].put(corruptedBlocks[i] - totalDataBlocksNum,
            DUMMY_STREAM);
      }
    }
  }

  void constructBlockOutputStreams(Path file,
      Map<Integer, OutputStream>[] blocksGroups) throws IOException {
    for (Map<Integer, OutputStream> group : blocksGroups) {
      for (Map.Entry<Integer, OutputStream> entry : group.entrySet()) {
        int blockIdx = entry.getKey();
        LocatedBlock block = getBlock(file, blockIdx);
        BlockOutputStream stream = BlockOutputStream.createStream(
            dfsClient, block, block.getLocations());
        entry.setValue(stream);
      }
    }
  }

  LocatedBlock getBlock(Path file, int blockIdx) throws IOException {
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(),
        blockSize * blockIdx, blockSize);
    Preconditions.checkState(blocks != null);
    Preconditions.checkState(blocks.getLocatedBlocks().size() == 1);
    return blocks.getLocatedBlocks().get(0);
  }

  int[] getErasures(Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures) {
    int[] erasures = new int[dataErasures.size() + codingErasures.size()];
    int idx = 0;

    for (Map.Entry<Integer, OutputStream> entry : dataErasures.entrySet()) {
      erasures[idx++] = entry.getKey() % dataBlocksNum;
    }
    for (Map.Entry<Integer, OutputStream> entry : codingErasures.entrySet()) {
      erasures[idx++] = entry.getKey() % codingBlocksNum + dataBlocksNum;
    }
    return erasures;
  }

  <T> void closeOutputStreams(T[] streams)
      throws IOException {
    for (int i = 0; i < streams.length; ++i) {
      if (streams[i] != null) {
        if (streams[i] instanceof OutputStream) {
          ((OutputStream)streams[i]).close();
        }
      }
    }
  }

  void closeInputStreams(InputStream[] streams) throws IOException {
    for (int i = 0; i < streams.length; ++i) {
      if (streams[i] != null) {
        streams[i].close();
      }
    }
  }

  static Path getCodingFile(Path file) {
    return new Path(RAID_ROOT.toString() + file + CODINF_FILE_SUFFIX);
  }

  static Path getRaidRoot() {
    return RAID_ROOT;
  }
}
