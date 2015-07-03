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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class PerformanceTest {

  private Configuration conf;

  private boolean useHttp2 = false;

  private int concurrency = 10;

  private int bufferSize = 4096;

  private int len = 100;

  private int readCount = 1000;

  private FileSystem fs;

  public PerformanceTest(String useHttp2, int concurrency, int len,
      int readCount) throws IOException {
    conf = new Configuration();
    this.useHttp2 = useHttp2.equals("tcp") ? false : true;
    this.len = len;
    this.concurrency = concurrency;
    this.readCount = readCount;
    if (this.useHttp2) {
      conf.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    }
    this.fs = FileSystem.get(conf);
  }

  private void consume(FSDataInputStream in, byte[] buf) throws IOException {
    for (int remaining = len; remaining > 0;) {
      int read = in.read(buf);
      if (read < 0) {
        throw new EOFException("Unexpected EOF got, should still have "
            + remaining + " bytes remaining");
      }
      remaining -= read;
    }
  }

  private void prepare(Path file) throws IllegalArgumentException, IOException {
    byte[] b = new byte[bufferSize];
    try (FSDataOutputStream out = fs.create(file)) {
      for (int remaining = len; remaining > 0;) {
        ThreadLocalRandom.current().nextBytes(b);
        int toWrite = Math.min(remaining, bufferSize);
        out.write(b, 0, toWrite);
        remaining -= toWrite;
      }
    }
    // warm up
    try (FSDataInputStream in = fs.open(file)) {
      consume(in, b);
    }
  }

  public void test() throws InterruptedException, IllegalArgumentException,
      IOException {
    final Path file = new Path("/test");
    prepare(file);
    long start = System.currentTimeMillis();
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency, new ThreadFactoryBuilder()
            .setNameFormat("DFSClient-%d").setDaemon(true).build());
    for (int i = 0; i < concurrency; ++i) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            byte[] buf = new byte[bufferSize];
            for (int j = 0; j < readCount; ++j) {
              try (FSDataInputStream in = fs.open(file)) {
                consume(in, buf);
              }
            }
          } catch (Exception e) {
            System.err.println("failed");
            System.exit(1);
          }
        }
      });
    }
    executor.shutdown();
    executor.awaitTermination(15, TimeUnit.MINUTES);
    if (this.useHttp2) {
      System.err.println("******* time based on http2 "
          + (System.currentTimeMillis() - start));
    } else {
      System.err.println("******* time based on tcp "
          + (System.currentTimeMillis() - start));
    }

  }

  public static void main(String[] args) throws IllegalArgumentException,
      InterruptedException, IOException {
    PerformanceTest performance =
        new PerformanceTest(args[0], Integer.parseInt(args[1]),
            Integer.parseInt(args[2]), Integer.parseInt(args[3]));
    performance.test();
  }

}
