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
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;

/**
 * A block reader that reads data over HTTP/2.
 */
@InterfaceAudience.Private
public class Http2BlockReader implements BlockReader {

  @Override
  public int read(ByteBuffer buf) throws IOException {
    // TODO Implement Http2BlockReader.read
    return 0;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    // TODO Implement Http2BlockReader.read
    return 0;
  }

  @Override
  public long skip(long n) throws IOException {
    // TODO Implement Http2BlockReader.skip
    return 0;
  }

  @Override
  public int available() throws IOException {
    // TODO Implement Http2BlockReader.available
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Implement Http2BlockReader.close

  }

  @Override
  public void readFully(byte[] buf, int readOffset, int amtToRead)
      throws IOException {
    // TODO Implement Http2BlockReader.readFully

  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    // TODO Implement Http2BlockReader.readAll
    return 0;
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public boolean isShortCircuit() {
    return false;
  }

  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    // TODO Implement Http2BlockReader.getClientMmap
    return null;
  }

}
