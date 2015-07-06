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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

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

  private void
      doTest(FileSystem fs, Path file, int concurrency,
          final int readCountPerThread, final int readLength,
          final AtomicLong cost) throws IOException, InterruptedException {
    long fileLength = fs.getFileStatus(file).getLen();
    final int seekBound =
        (int) Math.min(fileLength, Integer.MAX_VALUE) - readLength;
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency, new ThreadFactoryBuilder()
            .setNameFormat("DFSClient-%d").setDaemon(true).build());
    List<FSDataInputStream> inputs = new ArrayList<FSDataInputStream>();
    try {
      for (int i = 0; i < concurrency; i++) {
        final FSDataInputStream input = fs.open(file);
        inputs.add(input);
        final Random rand = new Random(i);
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              byte[] buf = new byte[bufferSize];
              long start = System.nanoTime();
              for (int j = 0; j < readCountPerThread; ++j) {
                input.seek(rand.nextInt(seekBound));
                consume(input, readLength, buf);
              }
              cost.addAndGet((System.nanoTime() - start) / 1000);
            } catch (Exception e) {
              e.printStackTrace();
              System.exit(1);
            }
          }
        });
      }
    } finally {
      for (FSDataInputStream input : inputs) {
        input.close();
      }
    }
    executor.shutdown();
    if (!executor.awaitTermination(15, TimeUnit.MINUTES)) {
      throw new IOException("wait timeout");
    }
  }

  public void testReadPerformance(String[] args) throws IOException,
      InterruptedException {
    boolean useHttp2 = args[0].equals("http2");
    Path file = new Path(args[1]);
    int concurrency = Integer.parseInt(args[2]);
    int readCountPerThread = Integer.parseInt(args[3]);
    int readLength = Integer.parseInt(args[4]);
    Configuration conf = new Configuration();
    if (useHttp2) {
      conf.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    }
    AtomicLong cost = new AtomicLong(0);
    try (FileSystem fs = FileSystem.get(conf)) {
      // warm up
      try (FSDataInputStream in = fs.open(file)) {
        in.read();
      }
      doTest(fs, file, concurrency, readCountPerThread, readLength, cost);
    }
    if (useHttp2) {
      System.err.println("******* time based on http2 " + cost.get());
    } else {
      System.err.println("******* time based on tcp " + cost.get());
    }
  }

  public void doWork(String[] args) throws IOException, InterruptedException {
    if (args[0].equals("prepare")) {
      prepare(args);
    } else {
      testReadPerformance(args);
    }
  }

  public static void main(String[] args) throws IllegalArgumentException,
      InterruptedException, IOException {
    new PerformanceTest().doWork(args);
  }

}
