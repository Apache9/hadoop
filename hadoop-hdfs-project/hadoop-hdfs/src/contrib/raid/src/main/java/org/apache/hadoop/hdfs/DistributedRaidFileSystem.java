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
    // TBD: Should we allow appending to normal files which are not encoded?
    throw new UnsupportedOperationException("append() is not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean result = true;
    result =  fs.rename(src, dst);
    // Rename coding file only when source file is renamed successfully.
    if(result) {
      result =  fs.rename(BlockCodec.getCodingFile(src),
          BlockCodec.getCodingFile(dst));
      // TBD: if we fail to rename coding file, should we set back the file's 
      // replication so that the file's availability is not impacted
    }
    return result;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    boolean result = true;
    result =  fs.delete(f, recursive);
    // Delete coding file only when source file is deleted successfully.
    if(result) {
      // If fail to delete the coding file, let the zombie cleaner to remove it later.
      result = fs.delete(BlockCodec.getCodingFile(f), recursive);
    }
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
    private final FSDataInputStream underlyingStream;
    private final Path file;
    private final FileStatus fileStatus;
    private boolean fileEncoded = false;
    private long currentPos = 0;

    public RaidFsInputStream(DistributedRaidFileSystem fs, Configuration conf,
        Path file, int bufferSize) throws IOException {
      this.fs = fs;
      this.rawFs = this.fs.getRawFileSystem();
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
    public synchronized long getPos() throws IOException {
      return currentPos;
    }


    @Override
    public synchronized boolean seekToNewSource(long targetPos)
        throws IOException {
      return underlyingStream.seekToNewSource(targetPos);
    }

    @Override
    public synchronized int read() throws IOException {
      checkPos();
      IOException ioe = null;
      try {
        int readLen = underlyingStream.read();
        currentPos += readLen;
        return readLen;
      } catch (IOException e) {
        ioe = e;
      }

      if (isFileEncoded()) {
        int readLen = downgradeRead(getPos(), 1)[0];
        skipInternal(readLen);
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
      checkPos();
      IOException ioe = null;
      try {
        int readLen = underlyingStream.read(bytes, offset, length);
        currentPos += readLen;
        return readLen;
      } catch (IOException e) {
        ioe = e;
      }

      if (isFileEncoded()) {
        byte[] result = downgradeRead(getPos(), length);
        System.arraycopy(result, 0, bytes, offset, result.length);
        skipInternal(result.length);
        return result.length;
      } else {
        throw ioe;
      }
    }

    @Override
    public int read(long position, byte[] buffer, int offset,
        int length) throws IOException {
      checkPos();
      IOException ioe = null;
      try {
        int readLen = underlyingStream.read(position, buffer, offset, length);
        return readLen;
      } catch (IOException e) {
        ioe = e;
      }

      if (isFileEncoded()) {
        byte[] result = downgradeRead(position, length);
        System.arraycopy(result, 0, buffer, offset, result.length);
        return result.length;
      } else {
        throw ioe;
      }
    }

    @Override
    public synchronized void readFully(long position, byte[] buffer)
        throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public synchronized void readFully(long position, byte[] buffer, int offset,
        int length) throws IOException {
      checkPos();
      long blockSize = fileStatus.getBlockSize();
      int startBlockIdx = (int)((position - 1) / blockSize);
      int endBlockIdx = (int)((position + length - 1) / blockSize);
      int totalReadLen = 0;

      for (int i = startBlockIdx; i <= endBlockIdx; ++i) {
        long pos = position;
        if (i > startBlockIdx) {
          pos = i * blockSize;
        }

        int len = (int)((i + 1) * blockSize - pos);
        if (len + pos > position + length) {
          len = (int)((position + length - 1) % blockSize + 1);
        }

        IOException ioe = null;
        try {
          underlyingStream.read(pos, buffer, offset + totalReadLen, len);
          totalReadLen += len;
          continue;
        } catch (IOException e) {
          ioe = e;
        }

        if (isFileEncoded()) {
          byte[] result = downgradeRead(pos, len);
          System.arraycopy(result, 0, buffer, offset + totalReadLen, len);
          totalReadLen += result.length;
        } else {
          throw ioe;
        }
      }
    }

    @Override
    public synchronized void close() throws IOException {
      underlyingStream.close();
    }

    private void skipInternal(long bytes) {
      currentPos += bytes;
      try {
        underlyingStream.skip(bytes);
      } catch (IOException e) {
        // Ignored
      }
    }

    private void checkPos() throws IOException {
      if (underlyingStream.getPos() != currentPos) {
        throw new IOException("Read position of underlying stream is not " +
            "equal to current read position, underlyingPos=" +
            underlyingStream.getPos() + ", currentPos=" + currentPos);
      }
    }

    boolean isFileEncoded() throws IOException {
      if (!fileEncoded) {
        fileEncoded = BlockCodec.isFileEncoded(rawFs, file);
      }
      return fileEncoded;
    }

    byte[] downgradeRead(long offset, int length) throws IOException {
      // read at most until the end of the current block
      long blockSize = fileStatus.getBlockSize();
      length = Math.min(length, (int) (blockSize - (offset % blockSize)));
      return fs.blockCodec.decode(file, offset, length);
    }
  }
}
