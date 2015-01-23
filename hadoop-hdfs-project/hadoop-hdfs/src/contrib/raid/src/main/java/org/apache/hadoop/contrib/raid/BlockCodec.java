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
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
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
  private final ErasureCodec codec;
  private final DFSClient dfsClient;

  private static final Path RAID_ROOT = new Path("/raid");
  private static final String CODING_FILE_SUFFIX = ".ec";
  private static final String TEMP_CODINF_FILE_SUFFIX = ".tmp";
  private static final String DECODE_LOCK_FILE_SUFFIX = ".lock";
  private static final int STRIPE_SIZE = 4096;
  private static final int WORD_SIZE = 8;
  private static final byte COMPLEMENT_BYTE = (byte) 1;
  private static final OutputStream DUMMY_STREAM = new ByteArrayOutputStream(1);

  public BlockCodec(Configuration conf) throws IOException {
    this.raidTimeWindowMs = conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT);
    this.dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
    this.codingBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    checkRaidRoot();

    this.codec = new ErasureCodec.Builder(Algorithm.Reed_Solomon).dataBlockNum(dataBlocksNum)
        .codingBlockNum(codingBlocksNum).wordSize(WORD_SIZE).build();
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
    Preconditions.checkState(fileStatus.getBlockSize() % STRIPE_SIZE == 0,
      "Block size must be multiple of " + STRIPE_SIZE);

    // Create temporary coding file output stream
    Path codingFile = getCodingFile(file);
    Path tmpCodingFile = new Path(codingFile.toString() + TEMP_CODINF_FILE_SUFFIX);
    FSDataOutputStream codingOut = fs.create(tmpCodingFile, true, conf.getInt(
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT), fs
        .getDefaultReplication(tmpCodingFile), fileStatus.getBlockSize());

    // Encode the blocks
    int roundNum = (locations.length - 1) / dataBlocksNum + 1;
    for (int r = 0; r < roundNum; ++r) {
      encodeBlocks(file, fileStatus, locations.length, codingOut, r * dataBlocksNum,
        Math.min(dataBlocksNum, locations.length - r * dataBlocksNum));
    }

    // Rename the tmp coding file to formal coding file
    try {
      codingOut.close();
      if (!fs.rename(tmpCodingFile, codingFile)) {
        LOG.error("Rename " + tmpCodingFile + " to " + codingFile + " failed");
        throw new IOException("Rename " + tmpCodingFile + " to " + codingFile + " failed");
      }
    } finally {
      // Remove the tmp coding file if it still exists
      fs.delete(tmpCodingFile, true);
    }

    // Change the data and coding file's replica number
    fs.setReplication(file, (short) 1);
    fs.setReplication(codingFile, (short) 1);
  }

  /**
   * Decodes corrupted blocks of specified file.
   */
  public void decode(Path file, int[] corruptedBlocks) throws IOException {
    FSDataOutputStream lockOut = beginDecoding(fs, file);
    Map<Integer, OutputStream>[] erasuredDataInfos = null;
    Map<Integer, OutputStream>[] erasuredCodingInfos = null;
    try {
      FileStatus fileStatus = fs.getFileStatus(file);
      long fileLen = fileStatus.getLen();
      long blockSize = fileStatus.getBlockSize();
      BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileLen);
      Preconditions.checkState(locations.length > 0);
      Path codingFile = getCodingFile(file);

      // Partition the corrupted blocks into groups
      int groupNum = (locations.length - 1) / dataBlocksNum + 1;
      erasuredDataInfos = new Map[groupNum];
      erasuredCodingInfos = new Map[groupNum];
      partitionErasureBlocks(corruptedBlocks, locations.length, erasuredDataInfos,
        erasuredCodingInfos);
      constructBlockOutputStreams(file, erasuredDataInfos);
      constructBlockOutputStreams(codingFile, erasuredCodingInfos);

      // Decode blocks within each group
      for (int i = 0; i < groupNum; ++i) {
        decodeBlocks(file, fileStatus, locations.length, i, blockSize, erasuredDataInfos[i],
          erasuredCodingInfos[i]);
      }
    } finally {
      if (erasuredDataInfos != null) {
        for (Map<Integer, OutputStream> map : erasuredDataInfos) {
          closeStreams(map.values().toArray());
        }
      }

      if (erasuredCodingInfos != null) {
        for (Map<Integer, OutputStream> map : erasuredCodingInfos) {
          closeStreams(map.values().toArray());
        }
      }

      if (lockOut != null) {
        endDecoding(fs, file, lockOut);
      }
    }
  }

  /**
   * Decodes the specified range of the content of a file.
   */
  public byte[] decode(Path file, long offset, int length) throws IOException {
    Map<Integer, OutputStream> erasuredDataInfo = null;
    Map<Integer, OutputStream> erasuredCodingInfo = null;
    try {
      FileStatus fileStatus = fs.getFileStatus(file);
      BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      long blockSize = fileStatus.getBlockSize();
      long adjustedOffset = adjustOffset(offset % blockSize, STRIPE_SIZE);
      long adjustedLength = adjustLength(length, STRIPE_SIZE);
      int blockIdx = (int) (offset / blockSize);
      int groupNo = blockIdx / dataBlocksNum;

      erasuredDataInfo = new HashMap<Integer, OutputStream>();
      erasuredCodingInfo = new HashMap<Integer, OutputStream>();
      erasuredDataInfo.put(blockIdx, new ByteArrayOutputStream((int) adjustedLength));

      decodeData(file, fileStatus, locations.length, groupNo, blockSize, adjustedOffset,
        adjustedLength, erasuredDataInfo, erasuredCodingInfo);

      byte[] data = ((ByteArrayOutputStream) (erasuredDataInfo.get(blockIdx))).toByteArray();
      // For user aligned offset and length, we just return the decoded data,
      // otherwise, we will align the offset and length, and copy result from
      // the decoded data. So users who care performance should align the offset
      // and length by themselves.
      if (adjustedOffset == offset && adjustedLength == length) {
        return data;
      } else {
        byte[] result = new byte[length];
        System.arraycopy(data, (int) (offset % blockSize - adjustedOffset), result, 0, length);
        return result;
      }
    } finally {
      if (erasuredDataInfo != null) {
        closeStreams(erasuredDataInfo.values().toArray());
      }

      if (erasuredCodingInfo != null) {
        closeStreams(erasuredCodingInfo.values().toArray());
      }
    }
  }

  /**
   * Encodes a specified group of blocks.
   */
  void encodeBlocks(Path file, FileStatus fileStatus, int totalBlockNum,
      FSDataOutputStream codingOut, int start, int len) throws IOException {
    FSDataInputStream[] dataIns = new FSDataInputStream[len];
    try {
      for (int i = 0; i < dataIns.length; ++i) {
        dataIns[i] = fs.open(file);
      }

      byte[][] result = new byte[codingBlocksNum][(int) fileStatus.getBlockSize()];
      byte[][] data = new byte[dataBlocksNum][STRIPE_SIZE];
      long blockSize = fileStatus.getBlockSize();
      boolean isLastGroup = (totalBlockNum == (start + len));
      long lastBlockLen = (fileStatus.getLen() - 1) % blockSize + 1;

      for (int r = 0; r < blockSize / STRIPE_SIZE; ++r) {
        // Read next stripe of data
        for (int i = 0; i < dataIns.length; ++i) {
          long startPos = (start + i) * blockSize;
          long pos = startPos + r * STRIPE_SIZE;
          if (isLastGroup && (i == dataIns.length - 1) && lastBlockLen != blockSize) {
            // Process the last block whose length is smaller than 'blockSize'
            if (pos + data[i].length < fileStatus.getLen()) {
              dataIns[i].read(pos, data[i], 0, data[i].length);
            } else {
              if (pos < fileStatus.getLen()) {
                int readLen = (int) (fileStatus.getLen() - pos);
                dataIns[i].read(pos, data[i], 0, readLen);
                Arrays.fill(data[i], readLen, data[i].length, COMPLEMENT_BYTE);
              } else {
                Arrays.fill(data[i], 0, data[i].length, COMPLEMENT_BYTE);
              }
            }
          } else {
            dataIns[i].read(pos, data[i], 0, data[i].length);
          }
        }

        // If not enough data blocks, fill 0s
        if (dataIns.length < dataBlocksNum) {
          for (int i = dataIns.length; i < dataBlocksNum; ++i) {
            Arrays.fill(data[i], 0, data[i].length, COMPLEMENT_BYTE);
          }
        }

        // Copy the coding stripe to the result buffer
        byte[][] coding = codec.encode(data);
        for (int i = 0; i < coding.length; ++i) {
          System.arraycopy(coding[i], 0, result[i], r * STRIPE_SIZE, coding[i].length);
        }
      }

      writeEncodedData(codingOut, result);
    } finally {
      closeStreams(dataIns);
    }
  }

  /**
   * Decodes corrupted blocks within a specified group.
   */
  void decodeBlocks(Path file, FileStatus fileStatus, int totalBlockNum, int groupNo,
      long blockSize, Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures) throws IOException {
    decodeData(file, fileStatus, totalBlockNum, groupNo, blockSize, 0, blockSize, dataErasures,
      codingErasures);
  }

  void decodeData(Path file, FileStatus fileStatus, int totalBlockNum, int groupNo, long blockSize,
      long offset, long length, Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures) throws IOException {
    Preconditions.checkArgument(offset % STRIPE_SIZE == 0);
    Preconditions.checkArgument(length > 0 && length % STRIPE_SIZE == 0);
    FSDataInputStream[] dataIns = new FSDataInputStream[dataBlocksNum];
    FSDataInputStream[] codingIns = new FSDataInputStream[codingBlocksNum];
    try {
      constructBlockInputStreams(file, groupNo, dataErasures, codingErasures, dataIns, codingIns);

      byte[][] data = new byte[dataBlocksNum][STRIPE_SIZE];
      byte[][] coding = new byte[codingBlocksNum][STRIPE_SIZE];
      int[] erasures = getErasures(dataErasures, codingErasures);
      // Construct the corrupted blocks
      for (int r = 0; r < length / STRIPE_SIZE; ++r) {
        // Read next stripe of data
        int count = readDataStripe(file, fileStatus, totalBlockNum, groupNo, offset, r,
          dataErasures, dataIns, data);

        // Read next stripe of coding
        readCodingStripe(file, groupNo, blockSize, offset, r, codingErasures, codingIns, coding,
          count);

        // Decode the corrupted data/coding and write back to the file
        codec.decode(erasures, data, coding);
        writeDecodedData(fileStatus, totalBlockNum, groupNo, offset, r, dataErasures,
          codingErasures, data, coding);
      }
    } finally {
      closeStreams(dataIns);
      closeStreams(codingIns);
    }
  }

  int readDataStripe(Path file, FileStatus fileStatus, int totalBlockNum, int groupNo, long offset,
      int roundNo, Map<Integer, OutputStream> dataErasures, FSDataInputStream[] dataIns,
      byte[][] data) throws IOException {
    int readStripeCount = 0;
    long blockSize = fileStatus.getBlockSize();
    long lastBlockLen = (fileStatus.getLen() - 1) % blockSize + 1;
    for (int i = 0; i < dataBlocksNum; ++i) {
      int blockIdx = groupNo * dataBlocksNum + i;
      if (blockIdx < totalBlockNum && dataErasures.get(blockIdx) == null) {
        try {
          int pos = (int) (blockIdx * blockSize + offset + roundNo * STRIPE_SIZE);
          if (blockIdx == totalBlockNum - 1 && lastBlockLen != blockSize) {
            // Process the last block whose length is smaller than block size
            if (pos + data[i].length < fileStatus.getLen()) {
              dataIns[i].read(pos, data[i], 0, data[i].length);
            } else {
              if (pos < fileStatus.getLen()) {
                int readLen = (int) (fileStatus.getLen() - pos);
                dataIns[i].read(pos, data[i], 0, readLen);
                Arrays.fill(data[i], readLen, data[i].length, COMPLEMENT_BYTE);
              } else {
                Arrays.fill(data[i], 0, data[i].length, COMPLEMENT_BYTE);
              }
            }
          } else {
            dataIns[i].read(pos, data[i], 0, data[i].length);
          }
        } catch (IOException e) {
          if (isBlockCorrupted(e)) {
            dfsClient.reportBadBlocks(new LocatedBlock[] { getBlock(file, blockSize, blockIdx) });
          }
          throw e;
        }
        ++readStripeCount;
      } else {
        if (blockIdx >= totalBlockNum) {
          ++readStripeCount;
          Arrays.fill(data[i], 0, data[i].length, COMPLEMENT_BYTE);
        } else {
          Arrays.fill(data[i], 0, data[i].length, (byte) 0);
        }
      }
    }
    return readStripeCount;
  }

  int readCodingStripe(Path file, int groupNo, long blockSize, long offset, int roundNo,
      Map<Integer, OutputStream> codingErasures, FSDataInputStream[] codingIns, byte[][] coding,
      int dataStripeCount) throws IOException {
    int readStripeCount = 0;
    for (int i = 0; i < codingBlocksNum; ++i) {
      int blockIdx = groupNo * codingBlocksNum + i;
      if (readStripeCount < dataBlocksNum - dataStripeCount && codingErasures.get(blockIdx) == null) {
        try {
          long pos = blockIdx * blockSize + offset + roundNo * STRIPE_SIZE;
          codingIns[i].read(pos, coding[i], 0, coding[i].length);
        } catch (IOException e) {
          if (isBlockCorrupted(e)) {
            dfsClient.reportBadBlocks(new LocatedBlock[] { getBlock(getCodingFile(file), blockSize,
              blockIdx) });
          }
          throw e;
        }
        ++readStripeCount;
      } else {
        Arrays.fill(coding[i], 0, coding[i].length, (byte) 0);
      }
    }
    return readStripeCount;
  }

  boolean isFileEncodable(FileStatus fileStatus) throws IOException {
    if (!fileStatus.isFile()) {
      return false;
    }

    long currentTimeMs = System.currentTimeMillis();
    long lastModTimeMs = fileStatus.getModificationTime();
    long timeDiffMs = (currentTimeMs - lastModTimeMs);

    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      // For distributed filesystem, only closed and time threshold expired
      // files are allowed to be encoded
      return (timeDiffMs > raidTimeWindowMs && dfs.isFileClosed(fileStatus.getPath()));
    } else {
      // For non-distributed filesystem, we only check modification time
      return timeDiffMs > raidTimeWindowMs;
    }
  }

  public boolean isFileEncodable(Path file) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    return isFileEncodable(fileStatus);
  }

  public long estimateSaving(Path file) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    if (isFileEncodable(fileStatus) == true) {
      long origConsumption = fileStatus.getLen() * fileStatus.getReplication();
      long blockNum = (fileStatus.getLen() + fileStatus.getBlockSize() - 1)
          / fileStatus.getBlockSize();
      long groupNum = (blockNum + dataBlocksNum - 1) / dataBlocksNum;
      long expectedConsumption = groupNum * (dataBlocksNum + codingBlocksNum)
          * fileStatus.getBlockSize();
      return (origConsumption - expectedConsumption);
    }
    throw new IOException("Input file is not a valid candidate for encoding.");
  }

  void checkRaidRoot() throws IOException {
    if (!fs.exists(RAID_ROOT)) {
      fs.mkdirs(RAID_ROOT);
    }
  }

  void writeEncodedData(OutputStream out, byte[][] data) throws IOException {
    for (int i = 0; i < data.length; ++i) {
      out.write(data[i]);
    }
  }

  void constructBlockInputStreams(Path file, int groupNo, Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures, FSDataInputStream[] dataIns,
      FSDataInputStream[] codingIns) throws IOException {
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
    for (int i = 0; i < codingIns.length; ++i) {
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

  void writeDecodedData(FileStatus fileStatus, int totalBlockNum, int groupNo, long offset,
      int roundNo, Map<Integer, OutputStream> dataErasures,
      Map<Integer, OutputStream> codingErasures, byte[][] data, byte[][] coding) throws IOException {
    long blockSize = fileStatus.getBlockSize();
    for (int i = 0; i < data.length; ++i) {
      int blockIdx = groupNo * dataBlocksNum + i;
      long pos = blockIdx * blockSize + offset + roundNo * STRIPE_SIZE;
      OutputStream out = dataErasures.get(blockIdx);
      if (out != null) {
        if (blockIdx + 1 == totalBlockNum) {
          if (pos + data[i].length < fileStatus.getLen()) {
            out.write(data[i]);
          } else {
            if (pos < fileStatus.getLen()) {
              int writeLen = (int) (fileStatus.getLen() - pos);
              out.write(data[i], 0, writeLen);
            }
          }
        } else {
          out.write(data[i]);
        }
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
      Map<Integer, OutputStream>[] dataBlocksGroup, Map<Integer, OutputStream>[] codingBlocksGroup) {
    Preconditions.checkArgument(dataBlocksGroup.length == codingBlocksGroup.length);
    for (int i = 0; i < dataBlocksGroup.length; ++i) {
      dataBlocksGroup[i] = new HashMap<Integer, OutputStream>();
      codingBlocksGroup[i] = new HashMap<Integer, OutputStream>();
    }

    for (int i = 0; i < corruptedBlocks.length; ++i) {
      if (corruptedBlocks[i] < totalDataBlocksNum) {
        // data blocks
        int index = corruptedBlocks[i] / dataBlocksNum;
        dataBlocksGroup[index].put(corruptedBlocks[i], DUMMY_STREAM);
      } else {
        // coding blocks
        int index = (corruptedBlocks[i] - totalDataBlocksNum) / codingBlocksNum;
        codingBlocksGroup[index].put(corruptedBlocks[i] - totalDataBlocksNum, DUMMY_STREAM);
      }
    }
  }

  void constructBlockOutputStreams(Path file, Map<Integer, OutputStream>[] blocksGroups)
      throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    long blockSize = fileStatus.getBlockSize();
    for (Map<Integer, OutputStream> group : blocksGroups) {
      for (Map.Entry<Integer, OutputStream> entry : group.entrySet()) {
        int blockIdx = entry.getKey();
        LocatedBlock block = getBlock(file, blockSize, blockIdx);
        BlockOutputStream stream = BlockOutputStream.createStream(dfsClient, block,
          block.getLocations());
        entry.setValue(stream);
      }
    }
  }

  LocatedBlock getBlock(Path file, long blockSize, int blockIdx) throws IOException {
    LocatedBlocks blocks = dfsClient.getLocatedBlocks(file.toString(), blockSize * blockIdx,
      blockSize);
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

  <T> void closeStreams(T[] streams) throws IOException {
    for (int i = 0; i < streams.length; ++i) {
      if (streams[i] != null) {
        if (streams[i] instanceof Closeable) {
          ((Closeable) streams[i]).close();
        }
      }
    }
  }

  public static long adjustOffset(long offset, int eps) {
    Preconditions.checkArgument(eps > 0);
    if (offset % eps != 0) {
      offset = (offset / eps) * eps;
    }
    return offset;
  }

  public static long adjustLength(long length, int eps) {
    Preconditions.checkArgument(eps > 0);
    if (length % eps != 0) {
      length = (length / eps + 1) * eps;
    }
    return length;
  }

  public static Path getCodingFile(Path file) {
    return new Path(RAID_ROOT.toString() + file + CODING_FILE_SUFFIX);
  }

  public static Path getRaidRoot() {
    return RAID_ROOT;
  }

  private static Path getDecodeLockFile(Path file) {
    return new Path(RAID_ROOT.toString() + file + DECODE_LOCK_FILE_SUFFIX);
  }

  public static boolean isFileDecoding(FileSystem fs, Path file) throws IOException {
    checkDistributedFileSystem(fs);
    Path lockFile = getDecodeLockFile(file);
    if (!fs.exists(lockFile) || ((DistributedFileSystem) fs).isFileClosed(file)) {
      return false;
    }
    return true;
  }

  public static boolean isFileEncoded(FileSystem fs, Path file) throws IOException {
    return fs.exists(getCodingFile(file));
  }

  public static FSDataOutputStream beginDecoding(FileSystem fs, Path file) throws IOException {
    checkDistributedFileSystem(fs);
    Path lockFile = getDecodeLockFile(file);
    try {
      if (((DistributedFileSystem) fs).isFileClosed(lockFile)) {
        fs.delete(lockFile, false);
      }
    } catch (FileNotFoundException e) {
      // Ignored
    }
    FSDataOutputStream out = fs.create(lockFile, false);
    return out;
  }

  public static void endDecoding(FileSystem fs, Path file, FSDataOutputStream out)
      throws IOException {
    checkDistributedFileSystem(fs);
    Path lockFile = getDecodeLockFile(file);
    out.close();
    fs.delete(lockFile, false);
  }

  private static void checkDistributedFileSystem(FileSystem fs) throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("Non-distributed filesystem not supported");
    }
  }

  public static boolean isBlockCorrupted(IOException e) {
    if (e instanceof BlockMissingException || e instanceof ChecksumException) {
      return true;
    }
    return false;
  }

  public static int getStripeSize() {
    return STRIPE_SIZE;
  }
}
