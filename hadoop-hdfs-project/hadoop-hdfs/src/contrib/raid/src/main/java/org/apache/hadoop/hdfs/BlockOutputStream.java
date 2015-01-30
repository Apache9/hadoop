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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.raid.HdfsRaidConfigKeys;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.base.Preconditions;

/**
 * Stream which is used to write a specified block to the HDFS.
 */
public class BlockOutputStream extends FSOutputSummer {

  private final LocatedBlock block;
  private final DFSClient dfsClient;
  private final DataChecksum checksum;
  private final Token<BlockTokenIdentifier> accessToken;
  private final DatanodeInfo[] datanodes;
  private final CachingStrategy cachingStrategy;

  private Socket socket;
  private DataOutputStream blockStream;
  private DataInputStream blockReplyStream;

  private int blockOffset;
  private int currentNodeIdx;
  private int currentSeqno;
  private Packet currentPacket;
  private boolean finished;

  private static final Log LOG = LogFactory.getLog(BlockOutputStream.class);

  /**
   * Create a new block output stream instance.
   * @param dfsClient The dfs client
   * @param block The block to write
   * @param excludedNodes The excluded datanodes
   * @return The created block output stream
   * @throws IOException
   */
  public static BlockOutputStream createStream(DFSClient dfsClient, LocatedBlock block,
      DatanodeInfo[] excludedNodes) throws IOException {
    ChecksumOpt checksumOpt = dfsClient.getConf().defaultChecksumOpt;
    DataChecksum checksum = DataChecksum.newDataChecksum(checksumOpt.getChecksumType(),
      checksumOpt.getBytesPerChecksum());

    Token<BlockTokenIdentifier> accessToken = block.getBlockToken();
    DatanodeInfo[] datanodes = getAvailableNodes(dfsClient, excludedNodes);
    CachingStrategy cachingStrategy = CachingStrategy.newDefaultStrategy();

    return new BlockOutputStream(dfsClient, block, checksum, accessToken, datanodes,
        cachingStrategy);
  }

  private BlockOutputStream(DFSClient dfsClient, LocatedBlock block, DataChecksum checksum,
      Token<BlockTokenIdentifier> accessToken, DatanodeInfo[] datanodes,
      CachingStrategy cachingStrategy) throws IOException {
    super(checksum, checksum.getBytesPerChecksum(), checksum.getChecksumSize());
    this.dfsClient = dfsClient;
    this.block = block;
    this.checksum = checksum;
    this.accessToken = accessToken;
    this.datanodes = datanodes;
    this.cachingStrategy = cachingStrategy;

    this.currentNodeIdx = 0;
    this.blockOffset = 0;
    this.currentSeqno = 0;
    this.finished = false;

    Preconditions.checkArgument(datanodes.length > 0);
    int retryTimes = this.dfsClient.getConfiguration().getInt(
      HdfsRaidConfigKeys.HDFS_RAIDNODE_DECODE_BLOCK_RETRY_TIMES_KEY,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_DECODE_BLOCK_RETRY_TIMES_DEFAULT);
    int retryCount = retryTimes;
    while (retryCount > 0) {
      try {
        long oldGS = block.getBlock().getGenerationStamp();
        createBlockOutputStream(this.datanodes[currentNodeIdx], ++oldGS);
        break;
      } catch (IOException e) {
        LOG.warn("Create block output stream failed", e);
        // TBD: Choose DN with max free space and also satisfy the block place policy
        currentNodeIdx = (currentNodeIdx + 1) % this.datanodes.length;
        --retryCount;
      }
    }
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!finished, "This stream cannot be reused!");
    // Create a new packet
    int chunkNum = len / checksum.getBytesPerChecksum();
    int chunkSize = checksum.getBytesPerChecksum() + checksum.getChecksumSize();
    int packetSize = chunkSize * chunkNum;
    if (len % checksum.getBytesPerChecksum() != 0) {
      chunkNum++;
      packetSize += len % checksum.getBytesPerChecksum() + checksum.getChecksumSize();
    }
    currentPacket = new Packet(packetSize, chunkNum, blockOffset);

    // Write data and checksum to the packet
    super.write(b, off, len);
    if (len % checksum.getBytesPerChecksum() != 0) {
      flushBuffer();
    }

    // Send the packet out and receive the response
    sendCurrentPacket();
    receiveResponse();
    blockOffset += len;

    // If encountering a block boundary, send an empty packet to indicate the
    // end of the block.
    if (blockOffset == block.getBlockSize()) {
      currentPacket = new Packet(0, 0, blockOffset);
      currentPacket.lastPacketInBlock = true;
      currentPacket.syncBlock = true;
      sendCurrentPacket();
      receiveResponse();
      finished = true;
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (!finished) {
      checkClosed();
      currentPacket = new Packet(0, 0, blockOffset);
      currentPacket.syncBlock = true;
      sendCurrentPacket();
      receiveResponse();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    flush();
    super.close();
    IOUtils.closeSocket(socket);
    IOUtils.closeStream(blockStream);
    IOUtils.closeStream(blockReplyStream);
    this.blockStream = null;
    this.blockReplyStream = null;
  }

  private void sendCurrentPacket() throws IOException {
    currentPacket.writeTo(blockStream);
    blockStream.flush();
  }

  private void receiveResponse() throws IOException {
    PipelineAck ack = new PipelineAck();
    ack.readFields(blockReplyStream);

    long seqNo = ack.getSeqno();
    Preconditions.checkState(seqNo + 1 == currentSeqno);

    Status status = ack.getReply(0);
    if (status != Status.SUCCESS) {
      throw new IOException("Bad response " + status + " for block " + block + " from datanode "
          + datanodes[currentNodeIdx]);
    }
  }

  @Override
  protected void writeChunk(byte[] data, int offset, int len, byte[] checksum) throws IOException {
    dfsClient.checkOpen();
    checkClosed();

    if (len > this.checksum.getBytesPerChecksum()) {
      throw new IOException("writeChunk() buffer size is " + len
          + " is larger than supported  bytesPerChecksum " + this.checksum.getBytesPerChecksum());
    }

    if (checksum.length != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be "
          + this.checksum.getChecksumSize() + " but found to be " + checksum.length);
    }

    Preconditions.checkNotNull(currentPacket);
    currentPacket.writeChecksum(checksum, 0, checksum.length);
    currentPacket.writeData(data, offset, len);
    currentPacket.numChunks++;
  }

  @Override
  protected void checkClosed() throws IOException {
    if (blockStream == null || blockReplyStream == null) {
      throw new IOException("Underlying stream is closed");
    }
  }

  /**
   * Get all datanodes that can be used to store the current block.
   * @param dfsClient The hdfs client
   * @param excludedNodes The excluded datanodes
   * @return The available datanodes
   * @throws IOException
   */
  static DatanodeInfo[] getAvailableNodes(DFSClient dfsClient, DatanodeInfo[] excludedNodes)
      throws IOException {
    DatanodeInfo[] liveNodes = dfsClient.datanodeReport(DatanodeReportType.LIVE);

    if (excludedNodes == null) {
      return liveNodes;
    }

    Set<DatanodeInfo> excludedNodesSet = new HashSet<DatanodeInfo>();
    excludedNodesSet.addAll(Arrays.asList(excludedNodes));
    List<DatanodeInfo> nodes = new LinkedList<DatanodeInfo>();
    for (DatanodeInfo info : liveNodes) {
      if (!excludedNodesSet.contains(info)) {
        nodes.add(info);
      }
    }
    // TODO: Should check rack info in the future
    return nodes.toArray(new DatanodeInfo[nodes.size()]);
  }

  /**
   * Creates the output stream that will be used to send the block data to datanode.
   * @param node The datanode to which the data will send to
   * @param newGS The new generate timestamp for the block
   */
  void createBlockOutputStream(DatanodeInfo node, long newGS) throws IOException {
    Status status = null;
    int refetchEncryptionKey = 1;
    while (true) {
      boolean result = false;
      try {
        socket = createSocket(node);
        long writeTimeout = dfsClient.getDatanodeWriteTimeout(1);
        OutputStream unbufOut = NetUtils.getOutputStream(socket, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(socket);

        if (dfsClient.shouldEncryptData()
            && !dfsClient.trustedChannelResolver.isTrusted(socket.getInetAddress())) {
          IOStreamPair encryptedStreams = DataTransferEncryptor.getEncryptedStreams(unbufOut,
            unbufIn, dfsClient.getDataEncryptionKey());
          unbufOut = encryptedStreams.out;
          unbufIn = encryptedStreams.in;
        }

        blockStream = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(unbufIn);

        // Send the request
        new Sender(blockStream).writeBlock(block.getBlock(), accessToken,
          dfsClient.getClientName(), new DatanodeInfo[] {}, null,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, 1, block.getBlockSize(), blockOffset,
          newGS, checksum, cachingStrategy);

        // Receive ack for connect
        BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(PBHelper
            .vintPrefixed(blockReplyStream));
        status = resp.getStatus();

        if (status == Status.ERROR_ACCESS_TOKEN) {
          throw new InvalidBlockTokenException(
              "Got access token error for connect ack form datanode: " + node);
        } else if (status != Status.SUCCESS) {
          throw new IOException("Bad connect ack from datanode: " + node);
        } else {
          result = true;
          break;
        }
      } catch (IOException e) {
        if (e instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
          continue;
        }
        result = false;
        throw e;
      } finally {
        if (!result) {
          IOUtils.closeSocket(socket);
          IOUtils.closeStream(blockStream);
          IOUtils.closeStream(blockReplyStream);
        }
      }
    }
  }

  /**
   * Creates a socket and connects to specified datanode.
   * @param node The datanode to connect
   * @return The created socket
   * @throws IOException
   */
  Socket createSocket(DatanodeInfo node) throws IOException {
    String dnAddr = node.getXferAddr(dfsClient.getConf().connectToDnViaHostname);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr);
    }

    InetSocketAddress sockAddr = NetUtils.createSocketAddr(dnAddr);
    Socket sock = dfsClient.socketFactory.createSocket();
    NetUtils.connect(sock, sockAddr, dfsClient.getRandomLocalInterfaceAddr(),
      dfsClient.getConf().socketTimeout);

    int timeout = dfsClient.getDatanodeReadTimeout(1);
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    return sock;
  }

  private class Packet {

    private long seqno; // Sequence number of buffer in block
    private long offsetInBlock; // Offset in block
    private boolean lastPacketInBlock; // Is this the last packet in block
    private boolean syncBlock; // Forces the current block to disk
    private int numChunks; // Number of chunks currently in packet
    private final int maxChunks; // Max chunks in packet
    private final byte[] buf; // The packet data buffer

    private int checksumStart;
    private int checksumPos;
    private int dataStart;
    private int dataPos;

    private static final long HEART_BEAT_SEQNO = -1L;

    Packet() {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = 0;
      this.seqno = HEART_BEAT_SEQNO;

      buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];
      checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
      checksumPos = PacketHeader.PKT_MAX_HEADER_LEN;
      dataPos = PacketHeader.PKT_MAX_HEADER_LEN;
      dataStart = PacketHeader.PKT_MAX_HEADER_LEN;
      maxChunks = 0;
    }

    /**
     * Create a new packet.
     * @param pktSize maximum size of the packet, including checksum data and actual data.
     * @param chunksPerPkt maximum number of chunks per packet.
     * @param offsetInBlock offset in bytes into the HDFS block.
     */
    Packet(int pktSize, int chunksPerPkt, long offsetInBlock) {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = offsetInBlock;
      this.seqno = currentSeqno;
      currentSeqno++;

      buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN + pktSize];

      checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
      checksumPos = checksumStart;
      dataStart = checksumStart + (chunksPerPkt * checksum.getChecksumSize());
      dataPos = dataStart;
      maxChunks = chunksPerPkt;
    }

    void writeData(byte[] inarray, int off, int len) {
      if (dataPos + len > buf.length) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, dataPos, len);
      dataPos += len;
    }

    void writeChecksum(byte[] inarray, int off, int len) {
      if (checksumPos + len > dataStart) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, checksumPos, len);
      checksumPos += len;
    }

    /**
     * Write the full packet, including the header, to the given output stream.
     */
    void writeTo(DataOutputStream stm) throws IOException {
      final int dataLen = dataPos - dataStart;
      final int checksumLen = checksumPos - checksumStart;
      final int pktLen = HdfsConstants.BYTES_IN_INTEGER + dataLen + checksumLen;

      PacketHeader header = new PacketHeader(pktLen, offsetInBlock, seqno, lastPacketInBlock,
          dataLen, syncBlock);

      if (checksumPos != dataStart) {
        // Move the checksum to cover the gap. This can happen for the last
        // packet or during an hflush/hsync call.
        System.arraycopy(buf, checksumStart, buf, dataStart - checksumLen, checksumLen);
        checksumPos = dataStart;
        checksumStart = checksumPos - checksumLen;
      }

      final int headerStart = checksumStart - header.getSerializedSize();
      assert checksumStart + 1 >= header.getSerializedSize();
      assert checksumPos == dataStart;
      assert headerStart >= 0;
      assert headerStart + header.getSerializedSize() == checksumStart;

      // Copy the header data into the buffer immediately preceding the
      // checksum data.
      System.arraycopy(header.getBytes(), 0, buf, headerStart, header.getSerializedSize());

      // Write the now contiguous full packet to the output stream.
      stm.write(buf, headerStart, header.getSerializedSize() + checksumLen + dataLen);
    }

    // get the packet's last byte's offset in the block
    long getLastByteOffsetBlock() {
      return offsetInBlock + dataPos - dataStart;
    }

    /**
     * Check if this packet is a heart beat packet
     * @return true if the sequence number is HEART_BEAT_SEQNO
     */
    private boolean isHeartbeatPacket() {
      return seqno == HEART_BEAT_SEQNO;
    }

    @Override
    public String toString() {
      return "packet seqno:" + this.seqno + " offsetInBlock:" + this.offsetInBlock
          + " lastPacketInBlock:" + this.lastPacketInBlock + " lastByteOffsetInBlock: "
          + this.getLastByteOffsetBlock();
    }
  }
}
