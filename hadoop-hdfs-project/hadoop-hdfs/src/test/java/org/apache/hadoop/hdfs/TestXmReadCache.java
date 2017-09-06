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
import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestXmReadCache {
  private static final int blockSize = 10*1024*1024;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private long tokenLifeTime = 1; //minute

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, tokenLifeTime);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, tokenLifeTime);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  public FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl) {
    try {
      FSDataOutputStream stm = fileSys.create(name, true,
        fileSys.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
      return stm;
    } catch (IOException e) {
      return null;
    }
  }

  private static String getKeyValue(String str, String key) {
    String a[] = str.split(" ");
    for (String s : a) {
      if (s.length() >= key.length() &&
        s.substring(0,key.length()).equals(key)) {
        String b[] = s.split("=");
        if (b.length == 2) {
          return b[1].trim();
        }
      }
    }
    return null;
  }

  interface IMessageOps {
    int read(FSDataInputStream in) throws IOException;
    void write(FSDataOutputStream out) throws IOException;
    boolean success();
  }

  class ThreadMessage extends Thread {
    protected String src;
    protected ArrayList<IMessageOps> listReadOps;
    protected long opsTime = 0;
    protected long opsCount = 0;
    ThreadMessage(String file, ArrayList<IMessageOps> listReadOps,
                  long opsTime, long opsCount) {
      this.src = file;
      this.listReadOps = listReadOps;
      this.opsTime = opsTime;
      this.opsCount = opsCount;
    }
  }

  class ThreadWritingMessage extends ThreadMessage {
    ThreadWritingMessage(String file, ArrayList<IMessageOps> listReadOps,
                         long opsTime, long opsCount) {
      super(file, listReadOps, opsTime, opsCount);
    }

    @Override
    public void run() {
      FSDataOutputStream out = createFile(fs, new Path(src), 1);
      for (IMessageOps ops : listReadOps) {
        long count = opsCount;
        long sleepTime = opsTime/opsCount;
        while(count-- > 0) {
          try {
            ops.write(out);
            Thread.sleep(sleepTime);
          } catch (Exception ie) {
            return;
          }
        }
      }
    }
  }

  class ThreadReadingMessage extends ThreadMessage {
    private String cacheStatus;
    ThreadReadingMessage(String file, ArrayList<IMessageOps> listReadOps,
                         long opsTime, long opsCount) {
      super(file, listReadOps, opsTime, opsCount);
    }

    public String getCacheStatus() { return cacheStatus; }

    @Override
    public void run() {
      try {
        FSDataInputStream in = fs.openEx(new Path(src));
        int ret = 0;
        long sleepTime = opsTime/opsCount/10;
        for (IMessageOps ops : listReadOps) {
          long count = opsCount;
          while(count-- > 0) {
            ret = ops.read(in);
            if (ret < 0) {
              return;
            }
            Thread.sleep(sleepTime);
          }
        }

        XmDFSInputStream xmin = (XmDFSInputStream) in.getWrappedStream();
        cacheStatus =  xmin.getCacheStatus();
        in.close();
      } catch (Exception ie) {
        return;
      }
    }
  }

  class MessageInt implements IMessageOps {
    int writeValue = 0;
    int ReadValue = 0;
    @Override
    public int read(FSDataInputStream in) throws IOException{
      ReadValue = in.readInt();
      return 4;
    }
    public void write(FSDataOutputStream out) throws IOException {
      out.writeInt(++writeValue);
      out.hflush();
    }
    public boolean success() {
      return (writeValue == ReadValue);
    }
  }

  class MessageSeek implements IMessageOps {
    int writeValue = 0;
    int ReadValue = 0;
    @Override
    public int read(FSDataInputStream in) throws IOException{
      in.seek(in.getPos()+4);
      ReadValue = in.readInt();
      return 8;
    }
    public void write(FSDataOutputStream out) throws IOException {
      out.writeInt(++writeValue);
      out.writeInt(++writeValue);
      out.hflush();
    }
    public boolean success() {
      return (writeValue == ReadValue);
    }
  }

  class MessageLong implements IMessageOps {
    long writeValue = 0;
    long ReadValue = 0;
    @Override
    public int read(FSDataInputStream in) throws IOException{
      ReadValue = in.readLong();
      return 8;
    }
    public void write(FSDataOutputStream out) throws IOException {
      out.writeLong(++writeValue);
      out.hflush();
    }
    public boolean success() {
      return (writeValue == ReadValue);
    }
  }

  class MessageFullyBuffer implements IMessageOps {
    long writeValue = 0;
    long ReadValue = 0;
    byte[] readBuf = new byte[8];
    byte[] writeBuf = new byte[8];
    @Override
    public int read(FSDataInputStream in) throws IOException{
      in.readFully(readBuf);
      ReadValue++;
      return 8;
    }
    public void write(FSDataOutputStream out) throws IOException {
      out.write(writeBuf);
      out.hflush();
      writeValue++;
    }
    public boolean success() {
      return (writeValue == ReadValue);
    }
  }

  @Test
  public void testReadCrossTokenExpired() {
    // This test will take about 6 minutes !!!
    ArrayList<IMessageOps> listReadOps = new ArrayList<IMessageOps>();
    listReadOps.add(new MessageInt());
    listReadOps.add(new MessageSeek());
    listReadOps.add(new MessageLong());
    listReadOps.add(new MessageFullyBuffer());

    //for quickly running
    //opsTime = 3000;
    //opsCount = opsTime/50;
    String file = "/testReadCrossTokenExpired.dat";
    long opsTime = tokenLifeTime*60*1000*3/2;
    long opsCount = opsTime/50;
    ThreadReadingMessage reader =
      new ThreadReadingMessage(file, listReadOps, opsTime, opsCount);
    ThreadWritingMessage writer =
      new ThreadWritingMessage(file, listReadOps, opsTime, opsCount);
    writer.start();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    reader.start();
    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      Assert.assertTrue(false);
    }

    for (IMessageOps op : listReadOps) {
      Assert.assertTrue(op.success());
    }
  }

  @Test
  public void testReadCacheStatus() {
    ArrayList<IMessageOps> listReadOps = new ArrayList<IMessageOps>();
    listReadOps.add(new MessageInt());

    String file = "/testReadCacheStatus.dat";
    long opsTime = 2000;
    long opsCount = opsTime/10;
    ThreadReadingMessage reader =
      new ThreadReadingMessage(file, listReadOps, opsTime, opsCount);
    ThreadWritingMessage writer =
      new ThreadWritingMessage(file, listReadOps, opsTime, opsCount);
    writer.start();
    try {
      Thread.sleep(200);
    } catch (InterruptedException ie) {
      // Ignore
    }
    reader.start();
    try {
      writer.join();
      reader.join();
    } catch (InterruptedException e) {
      Assert.assertTrue(false);
    }

    String cacheStatus = reader.getCacheStatus();
    int requestedGetLocatedBlocks = Integer.parseInt(getKeyValue(cacheStatus,"RequestedGetLocatedBlocks"));
    int doneGetLocatedBlocks = Integer.parseInt(getKeyValue(cacheStatus,"DoneGetLocatedBlocks"));
    int requestedNewCDP = Integer.parseInt(getKeyValue(cacheStatus,"RequestedNewCDP"));
    int doneNewCDP = Integer.parseInt(getKeyValue(cacheStatus,"DoneNewCDP"));
    Assert.assertTrue(requestedGetLocatedBlocks > doneGetLocatedBlocks);
    Assert.assertTrue(requestedNewCDP > doneNewCDP);
    Assert.assertTrue(doneGetLocatedBlocks < 2);
    Assert.assertTrue(doneNewCDP < 2);
  }

  @Test
  public void testWriteDataBetweenOpenAndRead() throws IOException{
    String file = "/testWriteDataBetweenOpenAndRead.dat";
    FSDataOutputStream out = createFile(fs, new Path(file), 1);
    FSDataInputStream in = fs.openEx(new Path(file));
    int i = 0;
    out.writeInt(i++);
    out.writeInt(i);
    out.hflush();
    out.close();

    i = in.readInt();
    Assert.assertEquals(0, i);
    i = in.readInt();
    Assert.assertEquals(1, i);
    in.close();
  }
}
