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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestDFSInputStream {
  static MiniDFSCluster cluster;
  static private FileSystem fs;
  final private static int sizeToReadWrite = 512;
  private static final Log LOG = LogFactory.getLog(TestDFSInputStream.class);
  private static final Path testFile = new Path("/test");

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
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

  @Test
  public void testExceptionInReadLength() throws Exception {
    FSDataOutputStream out = TestFileCreation.createFile(fs, testFile, 1);
    final byte[] writeBuf =
        DFSTestUtil.generateSequentialBytes(0, sizeToReadWrite);
    out.write(writeBuf);
    out.hflush();

    // Step 1: do not close file and verify that we can read out what we have
    // write
    final byte[] readBuf1 = new byte[sizeToReadWrite];
    FSDataInputStream in1 = fs.open(testFile);
    in1.read(readBuf1, 0, sizeToReadWrite);
    in1.close();
    Assert.assertTrue(validateSequentialBytes(readBuf1, 0, sizeToReadWrite));
    // The file should not have been closed
    Assert
        .assertTrue(((DistributedFileSystem) fs).isFileClosed(testFile) == false);

    // Step 2: emulate IOException during get last block length
    // Emulate exception during getting last block length
    boolean failedToReadLength = false;
    fs.getConf().setBoolean(DFSConfigKeys.DFS_CLIENT_READBLOCKLENGTH_EXCEPTION,
        true);
    FSDataInputStream in2 = null;
    try {
      in2 = fs.open(testFile);
    } catch (IOException ioe) {
      failedToReadLength = true;
    }
    Assert.assertTrue(failedToReadLength);
    fs.getConf().setBoolean(DFSConfigKeys.DFS_CLIENT_READBLOCKLENGTH_EXCEPTION,
        false);
    in2 = fs.open(testFile);
    final byte[] readBuf2 = new byte[sizeToReadWrite];
    in2.read(readBuf2, 0, sizeToReadWrite);
    Assert.assertTrue(validateSequentialBytes(readBuf2, 0, sizeToReadWrite));
    // The file should have been closed
    Assert
        .assertTrue(((DistributedFileSystem) fs).isFileClosed(testFile) == true);
  }

  @Test
  public void testSequenceFileCloseException() throws IOException {
    fs.getConf()
        .setBoolean("hadoop.sequencefile.close.emulate.exception", true);

    // create a sequence file 1
    Path path1 =
        new Path(System.getProperty("test.build.data", ".") + "/test1.seq");
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, fs.getConf(), path1, Text.class,
            NullWritable.class, CompressionType.RECORD);
    writer.append(new Text("file1-1"), NullWritable.get());
    writer.append(new Text("file1-2"), NullWritable.get());
    boolean exceptionInClose = false;
    try {
      writer.close();
    } catch (IOException ioe) {
      exceptionInClose = true;
    }
    Assert.assertTrue(exceptionInClose);

    Path path2 =
        new Path(System.getProperty("test.build.data", ".") + "/test2.seq");
    writer =
        SequenceFile.createWriter(fs, fs.getConf(), path2, Text.class,
            NullWritable.class, CompressionType.BLOCK);
    writer.append(new Text("file2-1"), NullWritable.get());
    writer.append(new Text("file2-2"), NullWritable.get());
    exceptionInClose = false;
    try {
      writer.close();
    } catch (IOException ioe) {
      exceptionInClose = true;
    }
    Assert.assertTrue(exceptionInClose);

    // The first reader gets 4 BuiltInZLibInflater instances from the CodecPool
    SequenceFile.Reader reader1 =
        new SequenceFile.Reader(fs, path1, fs.getConf());
    // read first value from reader1
    Text text = new Text();
    reader1.next(text);
    Assert.assertEquals("file1-1", text.toString());
    // The second reader _could_ get the same 4 BuiltInZLibInflater
    // instances from the CodePool as reader1
    SequenceFile.Reader reader2 =
        new SequenceFile.Reader(fs, path2, fs.getConf());
    // read first value from reader2
    reader2.next(text);
    Assert.assertEquals("file2-1", text.toString());
    // read second value from reader1
    reader1.next(text);
    Assert.assertEquals("file1-2", text.toString());
    // read second value from reader2 (this throws an exception)
    reader2.next(text);
    Assert.assertEquals("file2-2", text.toString());
    Assert.assertFalse(reader1.next(text));
    Assert.assertFalse(reader2.next(text));
    Assert.assertTrue(((DistributedFileSystem) fs).isFileClosed(new Path(System
        .getProperty("test.build.data", ".") + "/test1.seq")));
    Assert.assertTrue(((DistributedFileSystem) fs).isFileClosed(new Path(System
        .getProperty("test.build.data", ".") + "/test2.seq")));
    reader1.close();
    reader2.close();
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }
}
