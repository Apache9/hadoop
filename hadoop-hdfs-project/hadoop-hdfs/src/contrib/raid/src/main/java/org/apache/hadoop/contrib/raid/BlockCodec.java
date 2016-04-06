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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.NullOutputStream;
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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
  private final int stripSize;
  private final int wordSize;
  private final int codecBufSize;
  private final short replicaAfterEncode;

  private static final Path RAID_ROOT = new Path("/raid");
  private static final Path RAID_LIB = new Path("/raid/lib");
  private static final String CODING_FILE_SUFFIX = ".ec";
  private static final String TEMP_CODINF_FILE_SUFFIX = ".tmp";
  private static final String DECODE_LOCK_FILE_SUFFIX = ".lock";
  private static final byte COMPLEMENT_BYTE = (byte) 1;
  private static final OutputStream DUMMY_STREAM = new ByteArrayOutputStream(1);

  public BlockCodec(Configuration conf, FileSystem fs) throws IOException {
    this.raidTimeWindowMs = conf.getLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT);
    this.dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
    this.codingBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
    this.stripSize = conf.getInt(HdfsRaidConfigKeys.HDFS_RAID_CODEC_STRIP_SIZE,
      HdfsRaidConfigKeys.HDFS_RAID_CODEC_STRIP_SIZE_DEFAULT);
    this.wordSize = conf.getInt(HdfsRaidConfigKeys.HDFS_RAID_CODEC_WORD_SIZE,
      HdfsRaidConfigKeys.HDFS_RAID_CODEC_WORD_SIZE_DEFAULT);
    this.codecBufSize = conf.getInt(HdfsRaidConfigKeys.HDFS_RAID_CODEC_CODE_BUF_SIZE,
      HdfsRaidConfigKeys.HDFS_RAID_CODEC_CODE_BUF_SIZE_DEFAULT);
    this.replicaAfterEncode = (short) conf.getInt(
      HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_FILE_REPLICA,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_CODER_FILE_REPLICA_DEFAULT);
    Preconditions.checkState(((codecBufSize % stripSize) == 0) && (codecBufSize > stripSize));
    this.conf = conf;
    this.fs = fs;
    checkRaidRoot();

    this.codec = new ErasureCodec.Builder(Algorithm.Reed_Solomon).dataBlockNum(dataBlocksNum)
        .codingBlockNum(codingBlocksNum).wordSize(wordSize).build();
    if (fs instanceof DistributedFileSystem) {
      this.dfsClient = ((DistributedFileSystem) fs).getClient();
    } else {
      throw new IOException("Non-distributed filesystem is not supported");
    }
  }

  public BlockCodec(Configuration conf) throws IOException {
    this(conf, FileSystem.get(conf));
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
    Preconditions.checkState(fileStatus.getBlockSize() % stripSize == 0,
      "Block size must be multiple of " + stripSize);

    // Create temporary coding file output stream
    Path codingFile = getCodingFile(file);
    if (fs.exists(codingFile)) {
      if (fileStatus.getReplication() == replicaAfterEncode) {
        // The file has already been encoded by earlier incarnation
        fs.setReplication(codingFile, (short) 1);
        return;
      } else {
        // The earlier incarnation is in undefined state. Start coding from
        // scratch.
        fs.delete(codingFile, false);
      }
    }

    FSDataOutputStream[] codingOuts = new FSDataOutputStream[codingBlocksNum];
    Path[] tmpCodingFiles = new Path[codingBlocksNum];
    // Encode the blocks
    int roundNum = (locations.length - 1) / dataBlocksNum + 1;
    for (int r = 0; r < roundNum; ++r) {
      try {
        for (int i = 0; i < codingBlocksNum; i++) {
          tmpCodingFiles[i] = new Path(codingFile.toString() + TEMP_CODINF_FILE_SUFFIX + r + "_"
              + i);
          if (fs.exists(tmpCodingFiles[i])) {
            LOG.warn("Remove already existing temporary coding file "
                + tmpCodingFiles[i].toString() + ", which should have been delted. r is " + r
                + " i is " + i);
            fs.delete(tmpCodingFiles[i], false);
          }
          codingOuts[i] = fs.create(tmpCodingFiles[i], true, conf.getInt(
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT), fs
              .getDefaultReplication(tmpCodingFiles[i]), fileStatus.getBlockSize());
        }
        LOG.info("Created " + codingBlocksNum + " temporary coding files");
        encodeBlocks(file, fileStatus, locations.length, codingOuts, r * dataBlocksNum,
          Math.min(dataBlocksNum, locations.length - r * dataBlocksNum));

        for (int i = 0; i < codingBlocksNum; i++) {
          codingOuts[i].close();
        }

        // Concat tmp coding files to formal coding file
        if (r == 0) {
          Path[] concatSrcs = new Path[codingBlocksNum - 1];
          for (int i = 0; i < codingBlocksNum - 1; i++) {
            concatSrcs[i] = tmpCodingFiles[i + 1];
          }
          if (codingBlocksNum > 1) {
            fs.concat(tmpCodingFiles[0], concatSrcs);
          }
          if (fs.rename(tmpCodingFiles[0], codingFile) == false) {
            LOG.error("Rename " + tmpCodingFiles[0] + " to " + codingFile + " failed");
            throw new IOException("Rename " + tmpCodingFiles[0] + " to " + codingFile + " failed");
          }
        } else {
          fs.concat(codingFile, tmpCodingFiles);
        }

      } catch (IOException ioe) {
        // Fail to encoding. Delete the coding file.
        if (fs.exists(codingFile)) {
          LOG.info("Remove coding file " + codingFile.toString());
          fs.delete(codingFile, false);
        }
        LOG.warn("Failed to encode file " + file.toString(), ioe);
        throw ioe;
      } finally {
        for (int i = 0; i < codingBlocksNum; i++) {
          if (fs.exists(tmpCodingFiles[i])) {
            LOG.info("Remove temporary coding file " + tmpCodingFiles[i].toString());
            fs.delete(tmpCodingFiles[i], false);
          }
        }
      }
    }
    fileStatus = fs.getFileStatus(file);
    long fileModTime = fileStatus.getModificationTime();
    long currentTimeMs = System.currentTimeMillis();
    if ((fileModTime + raidTimeWindowMs > currentTimeMs)
        || !((DistributedFileSystem) fs).isFileClosed(file)) {
      // The file might have been re-opened for write, do not change replica and clean
      // the ec file.
      try {
        fs.delete(codingFile, false);
      } catch (Exception e) {
        // Log the message and let sweeper to handle it later.
        LOG.warn("Failed to delete ec file " + codingFile.toString()
            + ", whose source file is re-opened for write.");
      }
      return;
    }
    // Change the data and coding file's replica number
    fs.setReplication(file, replicaAfterEncode);
    fs.setReplication(codingFile, (short) 1);
  }

  /**
   * Decodes corrupted blocks of specified file.
   */
  public void decode(Path file, int[] corruptedBlocks, BlockTokenSecretManager tokenManager)
      throws IOException {
    FSDataOutputStream lockOut = beginDecoding(fs, file);
    if (lockOut == null) {
      // Two conditions:
      // (1) Somebody else is decoding the file. We should do nothing then.
      // (2) An earlier Fixer create this file and then exit abnormally without
      // deleting it in finally. (i.e. killed by yarn or os etc.) In this case,
      // the file would be closed in an hour. The later incarnation of fixer
      // after an hour would then be able to move on with this file.
      return;
    }
    Map<Integer, OutputStream>[] erasuredDataInfos = null;
    Map<Integer, OutputStream>[] erasuredCodingInfos = null;
    Map<Integer, List<LocatedBlock>> dataGrpLocs = null;
    Map<Integer, List<LocatedBlock>> codingGrpLocs = null;
    try {
      boolean needFix = false;
      FileStatus fileStatus = fs.getFileStatus(file);
      long fileLen = fileStatus.getLen();
      long blockSize = fileStatus.getBlockSize();
      BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileLen);
      Preconditions.checkState(locations.length > 0);
      Path codingFile = getCodingFile(file);
      FileStatus codingFileStatus = fs.getFileStatus(codingFile);
      long codingFileLen = codingFileStatus.getLen();
      long codingBlockSize = codingFileStatus.getBlockSize();
      BlockLocation[] codingFileLocations = fs.getFileBlockLocations(codingFileStatus, 0,
        codingFileLen);

      for (BlockLocation blk : locations) {
        if (blk.isCorrupt()) {
          needFix = true;
          break;
        }
      }
      if (needFix == false) {
        for (BlockLocation blk : codingFileLocations) {
          if (blk.isCorrupt()) {
            needFix = true;
            break;
          }
        }
      }
      if (needFix == false) {
        LOG.info("File " + file.toString() + " is not corrupted any more, skip to fix it.");
        return;
      }
      StringBuilder sb = new StringBuilder();
      for (int cblk : corruptedBlocks) {
        sb.append(" ").append(cblk);
      }
      LOG.debug("File " + file.toString() + " corrupted blks " + sb.toString());
      // Partition the corrupted blocks into groups
      int groupNum = (locations.length - 1) / dataBlocksNum + 1;
      erasuredDataInfos = new Map[groupNum];
      erasuredCodingInfos = new Map[groupNum];
      dataGrpLocs = new HashMap<Integer, List<LocatedBlock>>();
      codingGrpLocs = new HashMap<Integer, List<LocatedBlock>>();
      partitionErasureBlocks(corruptedBlocks, locations.length, erasuredDataInfos,
        erasuredCodingInfos);


      // Decode blocks within each group
      for (int i = 0; i < groupNum; ++i) {
        int corruptDataBlks = erasuredDataInfos[i].size();
        int corruptEcBlks = erasuredCodingInfos[i].size();
        if (corruptDataBlks + corruptEcBlks > codingBlocksNum) {
          StringBuilder msg = new StringBuilder();
          msg.append("Too many blocks corrupted in group ").append(i).append(" in file ")
              .append(file.toString()).append(". Just skip it.");
          LOG.warn(msg.toString());
        } else if (corruptDataBlks + corruptEcBlks != 0) {
          try {
            constructGroupLocation(file, locations.length, i, corruptedBlocks, dataGrpLocs);
            constructGroupLocation(codingFile, locations.length, i, corruptedBlocks, codingGrpLocs);
            constructBlockOutputStreams(file, i, erasuredDataInfos, dataGrpLocs, codingGrpLocs,
              tokenManager);
            constructBlockOutputStreams(codingFile, i, erasuredCodingInfos, dataGrpLocs,
              codingGrpLocs, tokenManager);
            decodeBlocks(file, fileStatus, locations.length, i, blockSize, erasuredDataInfos[i],
              erasuredCodingInfos[i]);
          } catch (IOException ioe) {
            LOG.error("Fail to decode group " + groupNum + " of " + file.toString(), ioe);
            throw ioe;
          }
          LOG.info("Decoded group " + groupNum + " of " + file.toString());
        } else {
          // No corrupted blocks in this group. Nothing to do.
          LOG.info("Skipped group " + groupNum + " since no blocks are corrupted in this group.");
        }
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
      if ((offset + length) / blockSize != offset / blockSize) {
        length = length - (int) ((offset + length) % blockSize);
      }
      long adjustedOffset = adjustOffset(offset % blockSize, stripSize);
      long adjustedLength = adjustLength(length, stripSize);
      int blockIdx = (int) (offset / blockSize);
      int groupNo = blockIdx / dataBlocksNum;
      Path sourceFile;
      Path codingFile;
      if (BlockCodec.isCodingFile(file)) {
        sourceFile = BlockCodec.getCodingFileSource(file);
        codingFile = file;
      } else {
        sourceFile = file;
        codingFile = BlockCodec.getCodingFile(file);
      }
      if (!fs.exists(sourceFile) || !fs.exists(codingFile)) {
        // It is not a coded file. Or it is a zombie file. Or it is a real corrupted file.
       LOG.debug("Not need to decode the file.");
        return null;
      }
      FileStatus sourceFileStatus = fs.getFileStatus(sourceFile);
      FileStatus codingFileStatus = fs.getFileStatus(codingFile);
      long srcOff = groupNo * dataBlocksNum * blockSize;
      long srcLen = dataBlocksNum * blockSize;
      if (srcOff + srcLen > sourceFileStatus.getLen()) {
        srcLen = sourceFileStatus.getLen() - srcOff;
      }
      BlockLocation[] srcLocs = fs.getFileBlockLocations(sourceFile, srcOff, srcLen);
      long codeOff = groupNo * codingBlocksNum * blockSize;
      long codeLen = codingBlocksNum * blockSize;
      if (codeOff + codeLen > codingFileStatus.getLen()) {
        codeLen = codingFileStatus.getLen() - codeOff;
      }
      BlockLocation[] codeLocs = fs.getFileBlockLocations(codingFile, codeOff, codeLen);
      int corruptedBlks = 0;
      for (BlockLocation blk : srcLocs) {
        if (blk.isCorrupt()) {
          corruptedBlks++;
        }
      }
      for (BlockLocation blk : codeLocs) {
        if (blk.isCorrupt()) {
          corruptedBlks++;
        }
      }
      if (corruptedBlks == 0) {
        LOG.info("No corrupted blocks found in file " + file.toString());
        // We continue the decoding since it might be a checksum error.
      } else {
        LOG.info("Found " + corruptedBlks + " blocks corrupted in " + file.toString()
            + ", trying to decode it ...");
      }
 
      erasuredDataInfo = new HashMap<Integer, OutputStream>();
      erasuredCodingInfo = new HashMap<Integer, OutputStream>();
      for (BlockLocation blk : srcLocs) {
        if (blk.isCorrupt()) {
         int blkIndex = (int) (blk.getOffset() / sourceFileStatus.getBlockSize());
         erasuredDataInfo.put(blkIndex, NullOutputStream.NULL_OUTPUT_STREAM);
        }
      }
      for (BlockLocation blk : codeLocs) {
        if (blk.isCorrupt()) {
          int blkIndex = (int) (blk.getOffset() / codingFileStatus.getBlockSize());
          erasuredCodingInfo.put(blkIndex, NullOutputStream.NULL_OUTPUT_STREAM);
        }
      }
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
  private void encodeBlocks(Path file, FileStatus fileStatus, int totalBlockNum,
      FSDataOutputStream[] codingOuts, int start, int len) throws IOException {
    Preconditions.checkState(codingOuts.length == codingBlocksNum);
    FSDataInputStream[] dataIns = new FSDataInputStream[len];
    try {
      for (int i = 0; i < dataIns.length; ++i) {
        dataIns[i] = fs.open(file);
      }
      byte[][] result;
      byte[][] data;
      // OutOfMemoryError has been thrown when doing UT.
      // Try to catch it rather than silent failure.
      try {
        result = new byte[codingBlocksNum][codecBufSize];
        data = new byte[dataBlocksNum][stripSize];
      } catch (Throwable t) {
        throw new IOException("Unable to get memory for encoding");
      }
      long blockSize = fileStatus.getBlockSize();
      boolean isLastGroup = (totalBlockNum == (start + len));
      long lastBlockLen = (fileStatus.getLen() - 1) % blockSize + 1;
      int codecBufIndex = 0;
      int codecBufWriteSentry = codecBufSize / stripSize;
      for (int r = 0; r < blockSize / stripSize; ++r) {
        // Read next stripe of data
        for (int i = 0; i < dataIns.length; ++i) {
          long startPos = (start + i) * blockSize;
          long pos = startPos + r * stripSize;
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
          System.arraycopy(coding[i], 0, result[i], codecBufIndex * stripSize, coding[i].length);
        }
        codecBufIndex += 1;
        if (codecBufIndex == codecBufWriteSentry) {
          writeEncodedData(codingOuts, result, codecBufIndex);
          codecBufIndex = 0;
        }
      }

      writeEncodedData(codingOuts, result, codecBufIndex);
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
    Preconditions.checkArgument(offset % stripSize == 0);
    Preconditions.checkArgument(length > 0 && length % stripSize == 0);
    FSDataInputStream[] dataIns = new FSDataInputStream[dataBlocksNum];
    FSDataInputStream[] codingIns = new FSDataInputStream[codingBlocksNum];
    try {
      constructBlockInputStreams(file, groupNo, dataErasures, codingErasures, dataIns, codingIns);

      byte[][] data = new byte[dataBlocksNum][stripSize];
      byte[][] coding = new byte[codingBlocksNum][stripSize];
      int[] erasures = getErasures(dataErasures, codingErasures);
      // Construct the corrupted blocks
      for (int r = 0; r < length / stripSize; ++r) {
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
          long pos = blockIdx * blockSize + offset + roundNo * stripSize;
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
          long pos = blockIdx * blockSize + offset + roundNo * stripSize;
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
      fs.mkdirs(RAID_LIB);
    }
  }

  void writeEncodedData(OutputStream[] outs, byte[][] data, int strips) throws IOException {
    Preconditions.checkState(outs.length == data.length);
    for (int i = 0; i < data.length; ++i) {
      outs[i].write(data[i], 0, stripSize * strips);
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
      long pos = blockIdx * blockSize + offset + roundNo * stripSize;
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
        LOG.info("corruptedBlocks " + corruptedBlocks[i]
            + "  totalDataBlocksNum " + totalDataBlocksNum + " dataBlocksNum "
            + dataBlocksNum + " grpSize " + dataBlocksGroup.length);
        dataBlocksGroup[index].put(corruptedBlocks[i], DUMMY_STREAM);
      } else {
        // coding blocks
        int index = (corruptedBlocks[i] - totalDataBlocksNum) / codingBlocksNum;
        LOG.info("corruptedBlocks " + corruptedBlocks[i]
            + "  totalDataBlocksNum " + totalDataBlocksNum
            + " codingBlocksNum " + codingBlocksNum + " grpSize "
            + codingBlocksGroup.length);
        codingBlocksGroup[index].put(corruptedBlocks[i] - totalDataBlocksNum, DUMMY_STREAM);
      }
    }
    for (int grp = 0; grp < dataBlocksGroup.length; grp++) {
      if (dataBlocksGroup[grp].keySet().size() == 0) {
        LOG.debug("grp " + grp + " is empty.");
      }
      for (Integer idx : dataBlocksGroup[grp].keySet()) {
        LOG.debug("grp " + grp + " blk " + idx + " stream "
            + ((dataBlocksGroup[grp].get(idx) == DUMMY_STREAM) ? "dummy stream" : "not dummy"));
      }
    }
    for (int grp = 0; grp < codingBlocksGroup.length; grp++) {
      if (codingBlocksGroup[grp].keySet().size() == 0) {
        LOG.debug("grp " + grp + " is empty.");
      }
      for (Integer idx : codingBlocksGroup[grp].keySet()) {
        LOG.debug("grp " + grp + " blk " + idx + " stream "
            + ((codingBlocksGroup[grp].get(idx) == DUMMY_STREAM) ? "dummy stream" : "not dummy"));
      }
    }
  }

  void constructGroupLocation(Path file, int totalBlks, int groupNo, int[] corruptedBlocks,
      Map<Integer, List<LocatedBlock>> blkLocs) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    boolean isCodingFile = isCodingFile(file);
    int blksPerGrp = isCodingFile ? codingBlocksNum : dataBlocksNum;
    long bytesPerGrp = blksPerGrp * fileStatus.getBlockSize();
    for (int i = 0; i < corruptedBlocks.length; i++) {
      if (isCodingFile && corruptedBlocks[i] < totalBlks) {
        continue;
      }
      if (!isCodingFile && corruptedBlocks[i] >= totalBlks) {
        continue;
      }
      int index = isCodingFile ? (corruptedBlocks[i] - totalBlks) : corruptedBlocks[i];
      int grpIdx = index / blksPerGrp;
      if (grpIdx != groupNo) {
        continue;
      }
      if (!blkLocs.containsKey(grpIdx)) {
        LOG.debug("Getting locations for " + grpIdx +" file is " + file.toString());
        LocatedBlocks blks = dfsClient.getLocatedBlocks(file.toUri().getPath(), grpIdx
            * bytesPerGrp,
          bytesPerGrp);
        LOG.debug("Put " + blks.toString() + " into group " + grpIdx + " of " + file.toString());
        blkLocs.put(grpIdx, blks.getLocatedBlocks());
      }
    }
  }

  void constructBlockOutputStreams(Path file, int groupNo,
      Map<Integer, OutputStream>[] blocksGroups,
      Map<Integer, List<LocatedBlock>> dataLocs, Map<Integer, List<LocatedBlock>> codingLocs,
      BlockTokenSecretManager tokenManager)
      throws IOException {
    boolean codingFile = isCodingFile(file);
    int blksPerGrp = codingFile ? codingBlocksNum : dataBlocksNum;
    for (Map<Integer, OutputStream> group : blocksGroups) {
      for (Map.Entry<Integer, OutputStream> entry : group.entrySet()) {
        int blockIdx = entry.getKey();
        int grpIdx = blockIdx / blksPerGrp;
        if (grpIdx != groupNo) {
          continue;
        }
        int offInGrp = blockIdx % blksPerGrp;
        LocatedBlock block = null;
        if (codingFile) {
          block = codingLocs.get(grpIdx).get(offInGrp);
        } else {
          block = dataLocs.get(grpIdx).get(offInGrp);
        }
        ArrayList<DatanodeInfo> grpDis = new ArrayList<DatanodeInfo>();
        if (dataLocs.containsKey(grpIdx)) {
          for (LocatedBlock dloc : dataLocs.get(grpIdx)) {
            for (DatanodeInfo di : dloc.getLocations()) {
              // Raid file's replication should be 1, which implies following check would always pass
              if (!grpDis.contains(di)) {
                grpDis.add(di);
              }
            }
          }
        }
        if (codingLocs.containsKey(grpIdx)) {
          for (LocatedBlock cloc : codingLocs.get(grpIdx)) {
            for (DatanodeInfo di : cloc.getLocations()) {
              // Raid file's replication should be 1, which implies following check would always pass
              if (!grpDis.contains(di)) {
                grpDis.add(di);
              }
            }
          }
        }
        DatanodeInfo[] dis = new DatanodeInfo[0];
        BlockOutputStream stream = null;
        block.setBlockToken(MRUtils.getAccessToken(block.getBlock(),
          EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE), tokenManager));
        try {
          stream = BlockOutputStream.createStream(dfsClient, block, grpDis.toArray(dis));
          LOG.info("Created stream for block " + block.toString() + " index " + blockIdx + " of "
              + file.toString());
        } catch (IOException ioe1) {
          // Try again with smaller excludedNodes
          LOG.warn(
            "Fail to create BlockOutputStream, will try again with smaller set of excluded nodes",
            ioe1);
          try {
            stream = BlockOutputStream.createStream(dfsClient, block, block.getLocations());
          } catch (IOException ioe2) {
            LOG.warn(
              "Fail to create BlockOutputStream, will try again with empty set of excluded nodes",
              ioe2);
            try {
              stream = BlockOutputStream.createStream(dfsClient, block, new DatanodeInfo[0]);
            } catch (IOException ioe3) {
              LOG.warn(
                "Still fail to create BlockOutputStream even with empty set of excluded nodes",
                ioe3);
              throw ioe3;
            }
          }
        }
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
    String s = file.toUri().getPath();
    return new Path(RAID_ROOT.toString() + s + CODING_FILE_SUFFIX);
  }

  public static boolean isCodingFile(String file) {
    return file.startsWith(RAID_ROOT.toString()) && file.endsWith(CODING_FILE_SUFFIX);
  }

  public static boolean isCodingFile(Path file) {
    String src = file.toUri().getPath();
    return isCodingFile(src);
  }

  public static Path getCodingFileSource(Path file) {
    Preconditions.checkArgument(isCodingFile(file));
    String s = file.toUri().getPath();
    return new Path(s.substring(RAID_ROOT.toString().length(), s.lastIndexOf(CODING_FILE_SUFFIX)));
  }

  public static Path getRaidRoot() {
    return RAID_ROOT;
  }

  public static Path getRaidLib() {
    return RAID_LIB;
  }

  private static Path getDecodeLockFile(Path file) {
    String s = file.toUri().getPath();
    return new Path(RAID_ROOT.toString() + s + DECODE_LOCK_FILE_SUFFIX);
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
      } else {
        if (((DistributedFileSystem) fs).exists(lockFile)) {
          // Do not rely on FileNotFoundException
          return null;
        }
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

  public int getStripeSize() {
    return stripSize;
  }

  public int getDataBlocksNum() {
    return dataBlocksNum;
  }

  public int getCodingBlocksNum() {
    return codingBlocksNum;
  }

  @VisibleForTesting
  public static String getCodingFIlePrefix() {
    return RAID_ROOT.toString();
  }

  @VisibleForTesting
  public static String getCodingFileSuffix() {
    return CODING_FILE_SUFFIX;
  }

}
