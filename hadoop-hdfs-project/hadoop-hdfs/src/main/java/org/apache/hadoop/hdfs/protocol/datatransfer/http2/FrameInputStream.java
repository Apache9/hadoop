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

import org.apache.hadoop.hdfs.web.http2.StreamListener;
import org.mortbay.log.Log;

public class FrameInputStream extends InputStream {

  private StreamListener listener;

  private ByteBuffer buffer = ByteBuffer.wrap(new byte[0]);

  public FrameInputStream(StreamListener listener) {
    this.listener = listener;
  }

  public static int count = 0;
  @Override
  public int read() throws IOException {
    if (buffer.remaining() <= 0) {
      try {
        Log.info("before get data, count=" + count);
        byte[] b = listener.getData();
        count+= b.length;
        Log.info("after get data " + b.length);
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
}
