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
package org.apache.hadoop.hdfs.protocol.datatransfer.http2;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.util.DataChecksum;

public class ChunkInputStream extends FilterInputStream {

  private DataChecksum dataChecksum;

  private String fileName;

  private ByteBuffer buffer = ByteBuffer.wrap(new byte[0]);

  private long dataPos;

  private int skipBytes;

  public ChunkInputStream(InputStream inputStream) {
    super(inputStream);
  }

  public void setDataChecksum(DataChecksum dataChecksum) {
    this.dataChecksum = dataChecksum;
  }

  public void setChunkOffset(long chunkOffset) {
    this.dataPos = chunkOffset;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void setSkipBytes(long skipBytes) {
    this.skipBytes = (int) skipBytes;
  }

  @Override
  public int read() throws IOException {
    while (skipBytes > 0) {
      getByte();
      skipBytes--;
    }
    return getByte();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    while (this.skipBytes > 0) {
      if (buffer.remaining() > 0) {
        int skip = Math.min(buffer.remaining(), this.skipBytes);
        buffer.position(buffer.position() + skip);
        this.skipBytes -= skip;
      } else {
        byte[] data = new byte[this.skipBytes];
        int ret = in.read(data, 0, skipBytes);
        if (ret == -1) {
          return -1;
        } else {
          this.skipBytes -= ret;
        }
      }
    }
    if (buffer.remaining() > 0) {
      int nRead = Math.min(buffer.remaining(), len);
      this.buffer.get(b, off, nRead);
      return nRead;
    } else {
      return in.read(b, off, len);
    }
  }

  // MAX_SKIP_BUFFER_SIZE is used to determine the maximum buffer size to
  // use when skipping.
  private static final int MAX_SKIP_BUFFER_SIZE = 2048;

  @Override
  public long skip(long n) throws IOException {
    long remaining = n;
    int nr;

    if (n <= 0) {
      return 0;
    }

    int size = (int) Math.min(MAX_SKIP_BUFFER_SIZE, remaining);
    byte[] skipBuffer = new byte[size];
    while (remaining > 0) {
      nr = read(skipBuffer, 0, (int) Math.min(size, remaining));
      if (nr < 0) {
        break;
      }
      remaining -= nr;
    }

    return n - remaining;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  private int getByte() throws IOException {
    if (buffer.remaining() > 0) {
      return buffer.get() & 0xff;
    } else {
      OpReadBlockFrameHeaderProto frameHeaderProto =
          OpReadBlockFrameHeaderProto.parseDelimitedFrom(in);
      int numChunks = frameHeaderProto.getNumChunks();
      byte[] checksum = frameHeaderProto.getChecksums().toByteArray();
      if (checksum.length != dataChecksum.getChecksumSize() * numChunks) {
        throw new IOException(
            "checksum size not matched,expected size in header is "
                + dataChecksum.getChecksumSize()
                + ", but actual size in header frame is " + checksum.length);
      }
      int dataLength = frameHeaderProto.getDataLength();
      byte[] data = new byte[dataLength];
      IOUtils.readFully(in, data);
      this.dataChecksum.reset();
      this.verifyChecksum(checksum, data, dataPos);
      this.dataPos += dataLength;
      this.buffer = ByteBuffer.wrap(data);
      return buffer.get() & 0xff;
    }
  }

  private void verifyChecksum(byte[] checksum, byte[] data, long basePos)
      throws ChecksumException {
    ByteBuffer checksumBuffer = ByteBuffer.wrap(checksum);
    ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    this.dataChecksum.verifyChunkedSums(dataBuffer, checksumBuffer, fileName,
      basePos);
  }
}
