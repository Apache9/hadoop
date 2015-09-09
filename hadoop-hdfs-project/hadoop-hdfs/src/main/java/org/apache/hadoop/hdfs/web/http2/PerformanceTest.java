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
package org.apache.hadoop.hdfs.web.http2;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class PerformanceTest {

  private int bufferSize = 4096;

  public void prepare(String[] args) throws IOException {
    Path file = new Path(args[1]);
    long length = Long.parseLong(args[2]);
    long blockSize = Long.parseLong(args[3]);
    byte[] b = new byte[bufferSize];
    try (FileSystem fs = FileSystem.get(new Configuration());
        FSDataOutputStream out =
            fs.create(file, true, bufferSize, fs.getDefaultReplication(file),
              blockSize)) {
      for (long remaining = length; remaining > 0;) {
        ThreadLocalRandom.current().nextBytes(b);
        int toWrite = (int) Math.min(remaining, bufferSize);
        out.write(b, 0, toWrite);
        remaining -= toWrite;
      }
    }
  }

  private void consume(FSDataInputStream in, int len, byte[] buf)
      throws IOException {
    for (int remaining = len; remaining > 0;) {
      int read = in.read(buf, 0, Math.min(remaining, buf.length));
      if (read < 0) {
        throw new EOFException("Unexpected EOF got, should still have "
            + remaining + " bytes remaining");
      }
      remaining -= read;
    }
  }

  private void pread(FSDataInputStream in, int position, int len, byte[] buf)
      throws IOException {
    for (int remaining = len; remaining > 0;) {
      int read = in.read(position + (len - remaining), buf, 0, remaining);
      if (read < 0) {
        throw new EOFException();
      }
      remaining -= read;
    }
  }

  private int randPos(int seekBound) {
    return seekBound > 0 ? ThreadLocalRandom.current().nextInt(seekBound) : 0;

  }

  private void doTest(FileSystem fs, Path file, int concurrency,
      final int readCountPerThread, final int readLength, final boolean pread,
      final AtomicLong cost) throws IOException, InterruptedException {
    // warm up
    try (FSDataInputStream input = fs.open(file)) {
      input.read();
    }
    long fileLength = fs.getFileStatus(file).getLen();
    final int seekBound =
        (int) Math.min(fileLength, Integer.MAX_VALUE) - readLength;
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency, new ThreadFactoryBuilder()
            .setNameFormat("DFSClient-%d").setDaemon(true).build());
    List<FSDataInputStream> inputs = new ArrayList<FSDataInputStream>();
    long start;
    try {
      for (int i = 0; i < concurrency; i++) {
        final FSDataInputStream input = fs.open(file);
        inputs.add(input);
      }
      start = System.nanoTime();
      for (int i = 0; i < concurrency; i++) {
        final FSDataInputStream input = inputs.get(i);
        final Random rand = new Random(i);
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              if (pread) {
                byte[] buf = new byte[readLength];
                for (int j = 0; j < readCountPerThread; ++j) {
                  pread(input, randPos(seekBound), readLength, buf);
                }
              } else {
                byte[] buf = new byte[bufferSize];
                for (int j = 0; j < readCountPerThread; ++j) {
                  input.seek(seekBound > 0 ? rand.nextInt(seekBound) : 0);
                  consume(input, readLength, buf);
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
              System.exit(1);
            }
          }
        });
      }
      executor.shutdown();
      if (!executor.awaitTermination(15, TimeUnit.MINUTES)) {
        throw new IOException("wait timeout");
      }
      cost.set((System.nanoTime() - start) / 1000000);
    } finally {
      for (FSDataInputStream input : inputs) {
        input.close();
      }
    }
  }

  public void testReadPerformance(String[] args) throws IOException,
      InterruptedException {
    boolean useHttp2 = args[0].equals("http2");
    Path file = new Path(args[1]);
    int concurrency = Integer.parseInt(args[2]);
    int readCountPerThread = Integer.parseInt(args[3]);
    int readLength = Integer.parseInt(args[4]);
    boolean pread = args.length > 5 && args[5].equals("pread");
    Configuration conf = new Configuration();
    if (useHttp2) {
      conf.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    }
    AtomicLong cost = new AtomicLong(0);
    try (FileSystem fs = FileSystem.get(conf)) {
      doTest(fs, file, concurrency, readCountPerThread, readLength, pread, cost);
    }
    if (useHttp2) {
      System.err.println("******* time based on http2 " + cost.get());
    } else {
      System.err.println("******* time based on tcp " + cost.get());
    }
  }

  public void testHttp2SmallReadInsideEventLoop(String[] args)
      throws IOException, InterruptedException {
    String file = args[1];
    int readCountPerThread = Integer.parseInt(args[2]);
    int readLength = Integer.parseInt(args[3]);
    NioEventLoopGroup workerGroup = new NioEventLoopGroup();
    Channel channel = null;
    final Configuration conf = new Configuration();
    try (FileSystem fs = FileSystem.get(conf)) {
      LocatedBlock block =
          ((DistributedFileSystem) fs).getClient().getLocatedBlocks(file, 0)
              .get(0);
      final Http2SmallReadTestingHandler handler =
          new Http2SmallReadTestingHandler();
      channel =
          new Bootstrap()
              .group(workerGroup)
              .channel(NioSocketChannel.class)
              .handler(new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(Channel ch) throws Exception {
                  ch.pipeline().addLast(
                    ClientHttp2ConnectionHandler.create(ch, conf), handler);
                }

              })
              .connect(
                NetUtils.createSocketAddr(block.getLocations()[0].getInfoAddr()))
              .sync().channel();
      channel.writeAndFlush(new ReadBlockTestContext(readLength,
          readCountPerThread, block));
      long cost = handler.getCost();
      System.err.println("******* time based on http2 " + cost);
    } finally {
      if (channel != null) {
        channel.close();
      }
      workerGroup.shutdownGracefully();
    }
  }

  private void readAsync(final CountDownLatch latch,
      final AtomicLong unscheduled, final AtomicLong remaining,
      final DFSInputStream in, final int seekBound, final long position,
      final byte[] b, final int off, final int len) throws IOException {
    in.asyncRead(position, b, off, len).addListener(
      new FutureListener<Integer>() {

        @Override
        public void operationComplete(Future<Integer> future) throws Exception {
          if (future.isSuccess()) {
            if (len == future.get().intValue()) {
              if (remaining.decrementAndGet() == 0) {
                latch.countDown();
                return;
              }
              if (unscheduled.decrementAndGet() >= 0) {
                readAsync(latch, unscheduled, remaining, in, seekBound,
                  randPos(seekBound), b, 0, b.length);
              }
            } else {
              readAsync(latch, unscheduled, remaining, in, seekBound, position
                  + future.get().intValue(), b, off + future.get().intValue(),
                len - future.get().intValue());
            }
          } else {
            future.cause().printStackTrace();
            System.exit(1);
          }
        }
      });
  }

  public void testHttp2AsyncRead(String[] args) throws IOException,
      InterruptedException {
    String file = args[1];
    int concurrency = Integer.parseInt(args[2]);
    int readCountPerThread = Integer.parseInt(args[3]);
    int readLength = Integer.parseInt(args[4]);
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicLong unscheduled =
        new AtomicLong(concurrency * readCountPerThread - concurrency);
    AtomicLong remaining = new AtomicLong(concurrency * readCountPerThread);
    long cost;
    try (FileSystem fs = FileSystem.get(conf)) {
      long fileLength = fs.getFileStatus(new Path(file)).getLen();
      int seekBound =
          (int) Math.min(fileLength, Integer.MAX_VALUE) - readLength;
      try (DFSInputStream in =
          ((DistributedFileSystem) fs).getClient().open(file)) {
        long start = System.nanoTime();
        for (int i = 0; i < concurrency; i++) {
          readAsync(latch, unscheduled, remaining, in, seekBound,
            randPos(seekBound), new byte[readLength], 0, readLength);
        }
        latch.await(15, TimeUnit.MINUTES);
        cost = (System.nanoTime() - start) / 1000000;
      }
    }
    System.err.println("******* time based on http2 " + cost);
  }

  public void doWork(String[] args) throws IOException, InterruptedException {
    if (args[0].equals("prepare")) {
      prepare(args);
    } else if (args[0].equals("noswitch")) {
      testHttp2SmallReadInsideEventLoop(args);
    } else if (args[0].equals("async")) {
      testHttp2AsyncRead(args);
    } else {
      testReadPerformance(args);
    }
  }

  public static void main(String[] args) throws IllegalArgumentException,
      InterruptedException, IOException {
    new PerformanceTest().doWork(args);
  }

}
