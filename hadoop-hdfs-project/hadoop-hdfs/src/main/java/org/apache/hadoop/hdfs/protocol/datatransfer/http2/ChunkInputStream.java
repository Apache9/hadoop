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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferV2Protos.OpReadBlockFrameHeaderProto;
import org.apache.hadoop.hdfs.web.http2.StreamListener;
import org.apache.hadoop.util.DataChecksum;
import org.mortbay.log.Log;

public class ChunkInputStream extends InputStream {

  private DataChecksum dataChecksum;

  private String fileName;

  private FrameInputStream frameInputStream;

  private ByteBuffer buffer = ByteBuffer.wrap(new byte[0]);

  private long dataPos = 0;

  public ChunkInputStream(StreamListener listener) {
    this.frameInputStream = new FrameInputStream(listener);
  }

  public void setDataChecksum(DataChecksum dataChecksum) {
    this.dataChecksum = dataChecksum;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public FrameInputStream getFrameInputStream() {
    return this.frameInputStream;
  }

  @Override
  public int read() throws IOException {
    if (buffer.remaining() > 0) {
      return buffer.get() & 0xff;
    } else {
      OpReadBlockFrameHeaderProto frameHeaderProto =
          OpReadBlockFrameHeaderProto.parseDelimitedFrom(frameInputStream);
      int numChunks = frameHeaderProto.getNumChunks();
      byte[] checksum = frameHeaderProto.getChecksums().toByteArray();
      if (checksum.length != dataChecksum.getChecksumSize() * numChunks) {
        throw new IOException("checksum size not matched,expected size in header is "
            + dataChecksum.getChecksumSize() + ", but actual size in header frame is "
            + checksum.length);
      }
      int dataLength = frameHeaderProto.getDataLength();
      Log.info("=== " + dataLength);
      byte[] data = new byte[dataLength];
      IOUtils.readFully(this.frameInputStream, data);
      Log.info("***");
      this.verifyChecksum(checksum, data, dataPos);
      this.dataChecksum.reset();
      this.dataPos += dataLength;
      this.buffer = ByteBuffer.wrap(data);
      return buffer.get() & 0xff;
    }
  }

  private void verifyChecksum(byte[] checksum, byte[] data, long basePos) throws ChecksumException {
    ByteBuffer checksumBuffer = ByteBuffer.wrap(checksum);
    ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    this.dataChecksum.verifyChunkedSums(dataBuffer, checksumBuffer, fileName, basePos);
  }

  @Override
  public long skip(long n) throws IOException {

    if (n <= 0) {
      return 0;
    }
    byte[] b = new byte[(int) n];
    IOUtils.readFully(this, b);
    return n;
  }
}
