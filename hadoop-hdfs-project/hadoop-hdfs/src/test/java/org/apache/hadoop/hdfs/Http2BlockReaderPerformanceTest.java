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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class Http2BlockReaderPerformanceTest {

  private static Random random = new Random();

  private final boolean pread;

  private final boolean noChecksum;

  private int bufferSize = 4096;

  public Http2BlockReaderPerformanceTest(boolean pread, boolean noChecksum) {
    this.pread = pread;
    this.noChecksum = noChecksum;
  }

  public void prepare(String[] args) throws IOException {
    Path file = new Path(args[1]);
    long length = Long.parseLong(args[2]);
    long blockSize = Long.parseLong(args[3]);
    byte[] b = new byte[bufferSize];
    FileSystem fs = null;
    FSDataOutputStream out = null;
    try {
      fs = FileSystem.get(new Configuration());
      out = fs.create(file, true, bufferSize,
            fs.getDefaultReplication(file), blockSize);
      for (long remaining = length; remaining > 0;) {
        random.nextBytes(b);
        int toWrite = (int) Math.min(remaining, bufferSize);
        out.write(b, 0, toWrite);
        remaining -= toWrite;
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (out != null) {
        out.close();
      }
    }
  }

  private void consume(FSDataInputStream in, int len, byte[] buf)
      throws IOException {
    for (int remaining = len; remaining > 0;) {
      int read = in.read(buf, 0, Math.min(remaining, buf.length));
      if (read < 0) {
        throw new EOFException("Unexpected EOF got, should still have " +
            remaining + " bytes remaining");
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

  private void doTest(FileSystem fs, Path file, int concurrency,
      final int readCountPerThread, final int readLength, final boolean pread,
      final AtomicLongArray cost) throws IOException, InterruptedException {
    // warm up
    FSDataInputStream fsinput = null;
    try {
      fsinput = fs.open(file);
      fsinput.read(0, new byte[1], 0, 1);
    } finally {
      if (fsinput != null) {
        fsinput.close();
      }
    }
    long fileLength = fs.getFileStatus(file).getLen();
    final long seekBound = Math.min(fileLength, Integer.MAX_VALUE) - readLength;
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency, new ThreadFactoryBuilder()
            .setNameFormat("DFSClient-%d").setDaemon(true).build());
    List<FSDataInputStream> inputs = new ArrayList<FSDataInputStream>();
    try {
      for (int i = 0; i < concurrency; i++) {
        final FSDataInputStream input = fs.open(file);
        inputs.add(input);
      }
      for (int i = 0; i < concurrency; i++) {
        final int index = i;
        final FSDataInputStream input = inputs.get(i);
        final int seekPos =
            (int) Math.min(fileLength / concurrency * i, seekBound);
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              long start = System.nanoTime();
              if (pread) {
                byte[] buf = new byte[readLength];
                for (int j = 0; j < readCountPerThread; ++j) {
                  pread(input, seekPos, readLength, buf);
                }
              } else {
                byte[] buf = new byte[bufferSize];
                for (int j = 0; j < readCountPerThread; ++j) {
                  input.seek(seekPos);
                  consume(input, readLength, buf);
                }
              }
              cost.set(index,
                  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
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
    } finally {
      for (FSDataInputStream input : inputs) {
        input.close();
      }
    }
  }

  public void testReadPerformance(String[] args)
      throws IOException, InterruptedException {
    boolean useHttp2 = args[0].equals("http2");
    Path file = new Path(args[1]);
    int concurrency = Integer.parseInt(args[2]);
    int readCountPerThread = Integer.parseInt(args[3]);
    int readLength = Integer.parseInt(args[4]);
    Configuration conf = new Configuration();
    if (useHttp2) {
      conf.setLong(DFSConfigKeys.DFS_CLIENT_HTTP2_MAX_READ_LENGTH_KEY,
          Long.MAX_VALUE);
    }
    AtomicLongArray cost = new AtomicLongArray(concurrency);
    FileSystem fs = null;
    try {
      fs = FileSystem.get(conf);
      if (noChecksum) {
        fs.setVerifyChecksum(false);
      }
      doTest(fs, file, concurrency, readCountPerThread, readLength, pread,
          cost);
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
    long max = 0, min = Long.MAX_VALUE, sum = 0;
    for (int i = 0; i < concurrency; i++) {
      long latency = cost.get(i);
      if (max < latency) {
        max = latency;
      }
      if (min > latency) {
        min = latency;
      }
      sum += latency;
    }
    System.err.println(String.format(
        "******* time based on %s, min: %d ms, max: %d ms, avg: %.2f ms",
        useHttp2 ? "HTTP/2" : "TCP", min, max, (double) sum / concurrency));
  }

  public void doWork(String[] args) throws IOException, InterruptedException {
    if (args[0].equals("prepare")) {
      prepare(args);
    } else {
      testReadPerformance(args);
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException {
    int times = Integer.parseInt(args[0]);
    List<String> argList = new ArrayList<String>();
    boolean pread = false;
    boolean noChecksum = false;
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--pread")) {
        pread = true;
      } else if (arg.equals("--nochecksum")) {
        noChecksum = true;
      } else {
        argList.add(arg);
      }
    }
    for (int i = 0; i < times; i++) {
      new Http2BlockReaderPerformanceTest(pread, noChecksum)
          .doWork(argList.toArray(new String[0]));
    }
  }
}
