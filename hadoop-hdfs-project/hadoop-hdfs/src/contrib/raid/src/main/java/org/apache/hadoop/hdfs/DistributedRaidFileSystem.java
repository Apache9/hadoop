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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.BlockCodec;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class DistributedRaidFileSystem extends FilterFileSystem {

  private BlockCodec blockCodec;

  public DistributedRaidFileSystem() {
    super(new DistributedFileSystem());
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    blockCodec = new BlockCodec(conf);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    RaidFsInputStream in = new RaidFsInputStream(this,
        fs.getConf(), f, bufferSize);
    return new FSDataInputStream(in);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("append() is not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean result = true;
    result = result && fs.rename(src, dst);
    result = result && fs.rename(BlockCodec.getCodingFile(src),
        BlockCodec.getCodingFile(dst));
    return result;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    boolean result = true;
    result = result && fs.delete(f, recursive);
    result = result && fs.delete(BlockCodec.getCodingFile(f), recursive);
    return result;
  }

  @Override
  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch (IOException e) {
        // Ignored
      }
    }
  }

  private static class RaidFsInputStream extends FSInputStream {

    private final DistributedRaidFileSystem fs;
    private final FileSystem rawFs;
    private final Configuration conf;
    private final FSDataInputStream underlyingStream;
    private final Path file;
    private final FileStatus fileStatus;
    private boolean fileEncoded = false;
    private long currentPos = 0;

    public RaidFsInputStream(DistributedRaidFileSystem fs, Configuration conf,
        Path file, int bufferSize) throws IOException {
      this.fs = fs;
      this.rawFs = this.fs.getRawFileSystem();
      this.conf = conf;
      this.underlyingStream = this.rawFs.open(file, bufferSize);
      this.file = file;
      this.fileStatus = this.rawFs.getFileStatus(this.file);
    }

    @Override
    public synchronized long skip(long bytes) throws IOException {
      currentPos += bytes;
      return underlyingStream.skip(bytes);
    }

    @Override
    public synchronized int available() throws IOException {
      return underlyingStream.available();
    }

    @Override
    public synchronized void mark(int i) {
      currentPos = i;
      underlyingStream.mark(i);
    }

    @Override
    public synchronized void reset() throws IOException {
      underlyingStream.reset();
      currentPos = underlyingStream.getPos();
    }

    @Override
    public synchronized boolean markSupported() {
      return underlyingStream.markSupported();
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
      currentPos = pos;
      underlyingStream.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return currentPos;
    }


    @Override
    public synchronized boolean seekToNewSource(long targetPos)
        throws IOException {
      return underlyingStream.seekToNewSource(targetPos);
    }

    @Override
    public synchronized int read() throws IOException {
      IOException ioe = null;
      try {
        int readLen = underlyingStream.read();
        currentPos += readLen;
        return readLen;
      } catch (IOException e) {
        ioe = e;
      }

      if (isBlockCorrupted(ioe) && isFileEncoded()) {
        int readLen = downgradeRead(getPos(), 1)[0];
        currentPos += readLen;
        underlyingStream.skip(readLen);
        return readLen;
      } else {
        throw ioe;
      }
    }

    @Override
    public int read(byte[] bytes) throws IOException {
      return read(bytes, 0, bytes.length);
    }

    @Override
    public synchronized int read(byte[] bytes, int offset, int length)
        throws IOException {
      IOException ioe = null;
      try {
        int readLen = underlyingStream.read(bytes, offset, length);
        currentPos += readLen;
        return readLen;
      } catch (IOException e) {
        ioe = e;
      }

      if (isBlockCorrupted(ioe) && isFileEncoded()) {
        byte[] result = downgradeRead(getPos(), length);
        System.arraycopy(result, 0, bytes, offset, result.length);
        currentPos += result.length;
        underlyingStream.skip(result.length);
        return result.length;
      } else {
        throw ioe;
      }
    }

    @Override
    public synchronized int read(long position, byte[] buffer, int offset,
        int length) throws IOException {
      IOException ioe = null;
      try {
        int readLen = underlyingStream.read(position, buffer, offset, length);
        return readLen;
      } catch (IOException e) {
        ioe = e;
      }

      if (isBlockCorrupted(ioe) && isFileEncoded()) {
        byte[] result = downgradeRead(position, length);
        System.arraycopy(result, 0, buffer, offset, result.length);
        underlyingStream.skip(result.length);
        return result.length;
      } else {
        throw ioe;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public synchronized void readFully(long position, byte[] buffer, int offset,
        int length) throws IOException {
      IOException ioe = null;
      try {
        underlyingStream.readFully(position, buffer, offset, length);
        currentPos += length;
      } catch (IOException e) {
        ioe = e;
      }

      if (isBlockCorrupted(ioe) && isFileEncoded()) {
        int totalReadLen = 0;
        while (totalReadLen < length) {
          byte[] result = downgradeRead(position + totalReadLen,
              length - totalReadLen);
          System.arraycopy(result, 0, buffer, offset + totalReadLen,
              result.length);
          totalReadLen += result.length;
        }
        currentPos += length;
        underlyingStream.skip(length);
      } else {
        throw ioe;
      }
    }

    @Override
    public synchronized void close() throws IOException {
      underlyingStream.close();
    }

    boolean isBlockCorrupted(IOException ioe) {
      return (ioe instanceof BlockMissingException) ||
          (ioe instanceof ChecksumException);
    }

    boolean isFileEncoded() throws IOException {
      if (!fileEncoded) {
        fileEncoded = BlockCodec.isFileEncoded(rawFs, file);
      }
      return fileEncoded;
    }

    byte[] downgradeRead(long offset, int length) throws IOException {
      // read at most untill the end of the current block
      long blockSize = fileStatus.getBlockSize();
      length = Math.min(length, (int) (blockSize - (offset % blockSize)));
      return fs.blockCodec.decode(file, offset, length);
    }
  }
}
