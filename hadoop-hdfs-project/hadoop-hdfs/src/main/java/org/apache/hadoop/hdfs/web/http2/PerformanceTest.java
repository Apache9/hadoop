package org.apache.hadoop.hdfs.web.http2;

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

  private int len = 100;

  private int readCount = 1000;

  public PerformanceTest(String useHttp2, int concurrency, int len, int readCount) {
    conf = new Configuration();
    this.useHttp2 = useHttp2.equals("tcp") ? false : true;
    this.len = len;
    this.concurrency = concurrency;
    this.readCount = readCount;
    if (this.useHttp2) {
      conf.setBoolean(HdfsClientConfigKeys.Read.Http2.KEY, true);
    }
  }

  private void prepare(String fileName) throws IllegalArgumentException, IOException {
    FSDataOutputStream out = FileSystem.get(conf).create(new Path(fileName));
    byte[] b = new byte[len];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.close();
  }

  public void test() throws InterruptedException, IllegalArgumentException, IOException {

    final String fileName = "/test";
    prepare(fileName);
    long start = System.currentTimeMillis();
    ExecutorService executor =
        Executors.newFixedThreadPool(concurrency,
          new ThreadFactoryBuilder().setNameFormat("DFSClient-%d").setDaemon(true).build());

    for (int i = 0; i < concurrency; ++i) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            for (int j = 0; j < readCount; ++j) {
              FSDataInputStream inputStream = FileSystem.get(conf).open(new Path(fileName));
              if (len != ByteStreams.toByteArray(inputStream).length) {

              }
              inputStream.close();
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
      System.err.println("******* time based on http2 " + (System.currentTimeMillis() - start));
    } else {
      System.err.println("******* time based on tcp " + (System.currentTimeMillis() - start));
    }

  }

  public static void main(String[] args) throws IllegalArgumentException, InterruptedException,
      IOException {
    PerformanceTest performance =
        new PerformanceTest(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]),
            Integer.parseInt(args[3]));
    performance.test();
  }

}
