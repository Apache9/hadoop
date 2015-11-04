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

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before; 
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestXmReadWhileWriting {
  private static final Log LOG = LogFactory
      .getLog(TestXmReadWhileWriting.class);
  private static final int blockSize = 512;
  private static final int numBlksToWrite = 100;
  private static final int sizePerWrite = 32;
  private static final int sizePerRead = 32;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  
  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  private boolean validateSequentialBytes(byte[] buf, int startPos, int len) {
    for (int i = 0; i < len; i++) {
      int expected = (i + startPos) % 127;

      if (buf[i] % 127 != expected) {
        LOG.error(String.format("at position [%d], got [%d] and expected [%d]",
            startPos + i, buf[i], expected));

        return false;
      }
    }
    return true;
  }

  interface ReadWrapper {
    public void read(FSDataInputStream in, int seq) throws IOException;
  }

  class XmTestReader extends Thread {
    private ReadWrapper rw;
    private String src;

    XmTestReader(String file, ReadWrapper rw) {
      this.rw = rw;
      this.src = file;
    }
    
    @Override
    public void run() {
      long fileSize = blockSize * numBlksToWrite;
      int numToRead = (int) (fileSize / sizePerRead);
      try {
        FSDataInputStream in = fs.openEx(new Path(src));
        for (int i = 0; i < numToRead; i++) {
          rw.read(in, i);
        }
        in.close();
      } catch (IOException ioe) {
        LOG.warn("Fail to read file ", ioe);
        Assert.assertTrue(false);
      }
    }
  }
  
  class XmTestWriter extends Thread {
    String src;

    XmTestWriter(String file) {
      src = file;
    }

    @Override
    public void run() {
      long sizeToWrite = blockSize * numBlksToWrite;
      int numToWrite = (int) (sizeToWrite / sizePerWrite);
      try {
        FSDataOutputStream out =
            TestFileCreation.createFile(fs, new Path(src), 1);
        for (int i = 0; i < numToWrite; i++) {
          final byte[] writeBuf =
              DFSTestUtil.generateSequentialBytes(i * sizePerWrite,
                  sizePerWrite);
          out.write(writeBuf);
          try {
            Thread.sleep(1);
          } catch (InterruptedException ie) {
            // Ignore
          }
        }
        out.close();
      } catch (IOException ioe) {
        LOG.warn("Fail to write file ", ioe);
        Assert.assertTrue(false);
      }
    }

  }

  // Test interface XmDFSInputStream::read(final byte buf[], int off, int len)
  @Test
  public void testRead1() throws IOException{
    String file = "/read1";
    XmTestWriter writer = new XmTestWriter(file); 
    writer.start();

    try {
      Thread.sleep(5);
    } catch (InterruptedException ie) {
      // Ignore
    }

    final byte[] readBuf = new byte[blockSize * numBlksToWrite];
    XmTestReader reader = new XmTestReader(file, new ReadWrapper() {
      @Override
      public void read(FSDataInputStream in, int seq) throws IOException {
        in.read(readBuf, seq * sizePerRead, sizePerRead);
      }
    });
    reader.start();

    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or reader to complete", e);
      Thread.currentThread().interrupt();
    }
    Assert.assertTrue(validateSequentialBytes(readBuf, 0, readBuf.length));
  }

  // Test interface XmDFSInputStream::read(final ByteBuffer buf)
  @Test
  public void testRead2() {
    String file = "/read2";
    XmTestWriter writer = new XmTestWriter(file);
    writer.start();

    try {
      Thread.sleep(5);
    } catch (InterruptedException ie) {
      // Ignore
    }

    final byte[] readBuf = new byte[blockSize * numBlksToWrite];
    XmTestReader reader = new XmTestReader(file, new ReadWrapper() {
      @Override
      public void read(FSDataInputStream in, int seq) throws IOException {
        byte[] tmpReadBuf = new byte[sizePerRead];
        ByteBuffer bb = ByteBuffer.wrap(tmpReadBuf);
        in.read(bb);
        bb.rewind();
        bb.get(readBuf, sizePerRead * seq, sizePerRead);
      }
    });
    reader.start();

    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or reader to complete", e);
      Thread.currentThread().interrupt();
    }
    Assert.assertTrue(validateSequentialBytes(readBuf, 0, readBuf.length));
  }

  // Test interface XmDFSInputStream::read(long position, byte[] buffer, int
  // offset, int length)
  @Test
  public void testRead3() {
    String file = "/read3";
    XmTestWriter writer = new XmTestWriter(file);
    writer.start();

    try {
      Thread.sleep(5);
    } catch (InterruptedException ie) {
      // Ignore
    }

    final byte[] readBuf = new byte[blockSize * numBlksToWrite];
    XmTestReader reader = new XmTestReader(file, new ReadWrapper() {
      @Override
      public void read(FSDataInputStream in, int seq) throws IOException {
        in.read(sizePerRead * seq, readBuf, sizePerRead * seq, sizePerRead);
      }
    });
    reader.start();

    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or reader to complete", e);
      Thread.currentThread().interrupt();
    }
    Assert.assertTrue(validateSequentialBytes(readBuf, 0, readBuf.length));
  }
  
  // Test interface XmDFSInputStream::read()
  @Test
  public void testRead4() {
    String file = "/read4";
    XmTestWriter writer = new XmTestWriter(file);
    writer.start();

    try {
      Thread.sleep(5);
    } catch (InterruptedException ie) {
      // Ignore
    }

    final byte[] readBuf = new byte[blockSize * numBlksToWrite];
    XmTestReader reader = new XmTestReader(file, new ReadWrapper() {
      @Override
      public void read(FSDataInputStream in, int seq) throws IOException {
        for (int i = 0; i < sizePerRead; i++) {
        	readBuf[sizePerRead * seq + i] = (byte) in.read();
        }
      }
    });
    reader.start();

    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or reader to complete", e);
      Thread.currentThread().interrupt();
    }
    Assert.assertTrue(validateSequentialBytes(readBuf, 0, readBuf.length));
  }

  @Test
  public void testSeekRead() {
    String file = "/read5";
    XmTestWriter writer = new XmTestWriter(file);
    writer.start();

    try {
      Thread.sleep(5);
    } catch (InterruptedException ie) {
      // Ignore
    }
    final byte[] readBuf = new byte[blockSize * numBlksToWrite];
    XmTestReader reader = new XmTestReader(file, new ReadWrapper() {
      @Override
      public void read(FSDataInputStream in, int seq) throws IOException {
        long fileSize = blockSize * numBlksToWrite;
        int numToRead = (int) (fileSize / sizePerRead);
        int reverseSeq = numToRead - 1 - seq;
        in.seek((long) sizePerRead * reverseSeq);
        for (int i = 0; i < sizePerRead; i++) {
          readBuf[sizePerRead * reverseSeq + i] = (byte) in.read();
        }
      }
    });
    reader.start();

    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or reader to complete", e);
      Thread.currentThread().interrupt();
    }
    Assert.assertTrue(validateSequentialBytes(readBuf, 0, readBuf.length));
  }
}
