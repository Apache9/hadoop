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

public class FrameInputStream extends InputStream {

  private ContinuousStreamListener listener;

  private ByteBuffer buffer = ByteBuffer.wrap(new byte[0]);

  public FrameInputStream(ContinuousStreamListener listener) {
    this.listener = listener;
  }

  @Override
  public int read() throws IOException {
    if (buffer.remaining() <= 0) {
      try {
        byte[] b = listener.getData();
        buffer = ByteBuffer.wrap(b);
        if (buffer.remaining() == 0) {
          return -1;
        } else {
          return buffer.get() & 0xff;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    } else {
      return buffer.get() & 0xff;
    }
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
    if (buffer.remaining() > 0) {
      int nRead = Math.min(buffer.remaining(), len);
      this.buffer.get(b, off, nRead);
      return nRead;
    } else {
      byte[] data;
      try {
        data = listener.getData();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (data.length == 0) {
        return -1;
      } else {
        this.buffer = ByteBuffer.wrap(data);
        int nRead = Math.min(this.buffer.remaining(), len);
        this.buffer.get(b, off, nRead);
        return nRead;
      }
    }

  }
}
