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

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestXmReadCache {
  private static final Log LOG = LogFactory
    .getLog(TestXmReadCache.class);
  private static final int blockSize = 4*1024*1024;
  private static final int numBlksToWrite = 1;
  private static final int sizePerWrite = 4;
  private static final int sizePerRead = 4;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;

  private int totalReadSize = 0;
  private int totalWriteSize = 0;
  private String cacheStatus;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  class XmWriterThread extends Thread {
    String src;
    XmWriterThread(String file) {
      src = file;
    }

    // creates a file but does not close it
    public FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
      System.out.println("createFile: Created " + name + " with " + repl + " replica.");
      FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
          .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
      return stm;
    }

    @Override
    public void run() {
      long sizeToWrite = blockSize * numBlksToWrite;
      int numToWrite = (int) (sizeToWrite / sizePerWrite);
      totalWriteSize = 0;
      try {
        FSDataOutputStream out = createFile(fs, new Path(src), 1);

        // flush the first data to make sure the first block is generated
        int step = 100;
        int numStop = numToWrite/step;
        int curNum = 0;
        boolean firstHit = true;

        for (int i = 0; i < numToWrite; i++) {
          curNum ++;
          if (curNum== numStop) {
            if (firstHit) {
              out.hflush();
              firstHit = false;
            }
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // Ignore
            }
            curNum = 0;
          }
          out.writeInt(i);
          totalWriteSize += sizePerWrite;
        }

      } catch (IOException ioe) {
        LOG.warn("Fail to write file ", ioe);
        Assert.assertTrue(false);
      }
    }
  }

  class XmReaderThread extends Thread {
    XmReaderThread(String file) {
      src = file;
    }
    private String src;
    @Override
    public void run() {
      int value;
      long fileSize = blockSize * numBlksToWrite;
      int numToRead = (int) (fileSize / sizePerRead);
      totalReadSize = 0;

      try {
        FSDataInputStream in = fs.openEx(new Path(src));
        for (int i = 0; i < numToRead; i++) {
          value = in.readInt();
          totalReadSize += sizePerRead;
          LOG.warn("Read Time =" + i + " value=" + value);
        }
        XmDFSInputStream xmin =(XmDFSInputStream)in.getWrappedStream();
        cacheStatus = xmin.getCacheStatus();
        in.close();
      } catch (IOException ioe) {
        LOG.warn("Fail to read file ", ioe);
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void testRefreshLocatedBlocks() {

    String file = "/readRefreshLocatedBlocks.test2";
    XmWriterThread writer = new XmWriterThread(file);
    writer.start();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      // Ignore
    }

    totalReadSize = 0;
    final byte[] readBuf = new byte[blockSize * numBlksToWrite];
    XmReaderThread reader = new XmReaderThread(file);
    reader.start();

    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or reader to complete", e);
      Thread.currentThread().interrupt();
    }

    Assert.assertEquals(totalWriteSize, totalReadSize);

    String [] result = cacheStatus.split(";");
    String [] refreshLocatedBlocks = result[0].split(",");
    String str = refreshLocatedBlocks[1].substring(0,refreshLocatedBlocks[1].length()-1);
    long doRefreshLocatedBlocks = Integer.parseInt(str);
    Assert.assertTrue(doRefreshLocatedBlocks <= numBlksToWrite+1);

    String [] newCDP = result[1].split(",");
    str = newCDP[1].substring(0,newCDP[1].length()-1);
    long donewCDP = Integer.parseInt(str);
    Assert.assertTrue(donewCDP <= numBlksToWrite+1);
  }

}
