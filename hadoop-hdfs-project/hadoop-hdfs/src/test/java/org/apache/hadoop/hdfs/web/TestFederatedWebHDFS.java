package org.apache.hadoop.hdfs.web;

import java.net.URI;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.web.TestWebHDFS.Ticker;
import org.junit.Assert;
import org.junit.Test;

public class TestFederatedWebHDFS {

  // case 1 : verify the configuration "fs.webhdfs.impl" and
  // "fs.webhdfs.disable.cache" would not impact normal webhdfs
  // case 2 : normal create/write/open/read
  // case 3 : test case cover other interface

  static final Log LOG = LogFactory.getLog(TestWebHDFS.class);

  static final Random RANDOM = new Random();

  static final long systemStartTime = System.nanoTime();

  /** A timer for measuring performance. */
  static class Ticker {
    final String name;
    final long startTime = System.nanoTime();
    private long previousTick = startTime;

    Ticker(final String name, String format, Object... args) {
      this.name = name;
      LOG.info(String.format("\n\n%s START: %s\n", name,
          String.format(format, args)));
    }

    void tick(final long nBytes, String format, Object... args) {
      final long now = System.nanoTime();
      if (now - previousTick > 10000000000L) {
        previousTick = now;
        final double mintues = (now - systemStartTime) / 60000000000.0;
        LOG.info(String.format("\n\n%s %.2f min) %s %s\n", name, mintues,
            String.format(format, args), toMpsString(nBytes, now)));
      }
    }

    void end(final long nBytes) {
      final long now = System.nanoTime();
      final double seconds = (now - startTime) / 1000000000.0;
      LOG.info(String.format("\n\n%s END: duration=%.2fs %s\n", name, seconds,
          toMpsString(nBytes, now)));
    }

    String toMpsString(final long nBytes, final long now) {
      final double mb = nBytes / (double) (1 << 20);
      final double mps = mb * 1000000000.0 / (now - startTime);
      return String.format("[nBytes=%.2fMB, speed=%.2fMB/s]", mb, mps);
    }
  }

  // case 1
  @Test(timeout = 300000)
  public void testNormalWebHdfs() throws Exception {
    normalFileTest(10L << 20); // 10MB file length
  }

  /** Test read and write large files. */
  static void normalFileTest(final long fileLength) throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    conf.set("fs.webhdfs.impl", FederatedWebHdfsFileSystem.class.getName());
    conf.setBoolean("fs.webhdfs.disable.cache", true);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();

      final FileSystem fs =
          WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsFileSystem.SCHEME);
      final Path dir = new Path("/test/largeFile");
      Assert.assertTrue(fs.mkdirs(dir));

      final byte[] data = new byte[1 << 20];
      RANDOM.nextBytes(data);

      final byte[] expected = new byte[2 * data.length];
      System.arraycopy(data, 0, expected, 0, data.length);
      System.arraycopy(data, 0, expected, data.length, data.length);

      final Path p = new Path(dir, "file");
      final Ticker t = new Ticker("WRITE", "fileLength=" + fileLength);
      final FSDataOutputStream out = fs.create(p);
      try {
        long remaining = fileLength;
        for (; remaining > 0;) {
          t.tick(fileLength - remaining, "remaining=%d", remaining);

          final int n = (int) Math.min(remaining, data.length);
          out.write(data, 0, n);
          remaining -= n;
        }
      } finally {
        out.close();
      }
      t.end(fileLength);

      Assert.assertEquals(fileLength, fs.getFileStatus(p).getLen());

      final long smallOffset = RANDOM.nextInt(1 << 20) + (1 << 20);
      final long largeOffset = fileLength - smallOffset;
      final byte[] buf = new byte[data.length];

      verifySeek(fs, p, largeOffset, fileLength, buf, expected);
      verifySeek(fs, p, smallOffset, fileLength, buf, expected);

      verifyPread(fs, p, largeOffset, fileLength, buf, expected);
    } finally {
      cluster.shutdown();
    }
  }

  static void checkData(long offset, long remaining, int n, byte[] actual,
      byte[] expected) {
    if (RANDOM.nextInt(100) == 0) {
      int j = (int) (offset % actual.length);
      for (int i = 0; i < n; i++) {
        if (expected[j] != actual[i]) {
          Assert.fail("expected[" + j + "]=" + expected[j] + " != actual[" + i
              + "]=" + actual[i] + ", offset=" + offset + ", remaining="
              + remaining + ", n=" + n);
        }
        j++;
      }
    }
  }

  /** test seek */
  static void verifySeek(FileSystem fs, Path p, long offset, long length,
      byte[] buf, byte[] expected) throws IOException {
    long remaining = length - offset;
    long checked = 0;
    LOG.info("XXX SEEK: offset=" + offset + ", remaining=" + remaining);

    final Ticker t =
        new Ticker("SEEK", "offset=%d, remaining=%d", offset, remaining);
    final FSDataInputStream in = fs.open(p, 64 << 10);
    in.seek(offset);
    for (; remaining > 0;) {
      t.tick(checked, "offset=%d, remaining=%d", offset, remaining);
      final int n = (int) Math.min(remaining, buf.length);
      in.readFully(buf, 0, n);
      checkData(offset, remaining, n, buf, expected);

      offset += n;
      remaining -= n;
      checked += n;
    }
    in.close();
    t.end(checked);
  }

  static void verifyPread(FileSystem fs, Path p, long offset, long length,
      byte[] buf, byte[] expected) throws IOException {
    long remaining = length - offset;
    long checked = 0;
    LOG.info("XXX PREAD: offset=" + offset + ", remaining=" + remaining);

    final Ticker t =
        new Ticker("PREAD", "offset=%d, remaining=%d", offset, remaining);
    final FSDataInputStream in = fs.open(p, 64 << 10);
    for (; remaining > 0;) {
      t.tick(checked, "offset=%d, remaining=%d", offset, remaining);
      final int n = (int) Math.min(remaining, buf.length);
      in.readFully(offset, buf, 0, n);
      checkData(offset, remaining, n, buf, expected);

      offset += n;
      remaining -= n;
      checked += n;
    }
    in.close();
    t.end(checked);
  }

  // case 2
  @Test(timeout = 300000)
  public void testFedFile() throws Exception {
    fedFileTest(10L << 20, "/user/test", "/home/test"); // 10MB file length
  }

  /** Test read and write large files. */
  static void fedFileTest(final long fileLength, String... dirs)
      throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.webhdfs.impl", FederatedWebHdfsFileSystem.class.getName());
    conf.setBoolean("fs.webhdfs.disable.cache", true);
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    FileSystem fHdfs1 = cluster.getFileSystem(0);
    FileSystem fHdfs2 = cluster.getFileSystem(1);
    ConfigUtil.addLink(conf, "/home", fHdfs1.getUri());
    ConfigUtil.addLink(conf, "/user", fHdfs2.getUri());
    conf.set("fs.defaultFS", "hdfs://default");
    try {
      cluster.waitActive();

      for (String dir : dirs) {
        final FileSystem fs =
            FileSystem.get(new URI("webhdfs://default"), conf);
        Assert.assertTrue(fs.mkdirs(new Path(dir)));

        final byte[] data = new byte[1 << 20];
        RANDOM.nextBytes(data);

        final byte[] expected = new byte[2 * data.length];
        System.arraycopy(data, 0, expected, 0, data.length);
        System.arraycopy(data, 0, expected, data.length, data.length);

        final Path p = new Path(dir, "file");
        final Ticker t = new Ticker("WRITE", "fileLength=" + fileLength);
        final FSDataOutputStream out = fs.create(p);
        try {
          long remaining = fileLength;
          for (; remaining > 0;) {
            t.tick(fileLength - remaining, "remaining=%d", remaining);

            final int n = (int) Math.min(remaining, data.length);
            out.write(data, 0, n);
            remaining -= n;
          }
        } finally {
          out.close();
        }
        t.end(fileLength);

        Assert.assertEquals(fileLength, fs.getFileStatus(p).getLen());

        final long smallOffset = RANDOM.nextInt(1 << 20) + (1 << 20);
        final long largeOffset = fileLength - smallOffset;
        final byte[] buf = new byte[data.length];

        verifySeek(fs, p, largeOffset, fileLength, buf, expected);
        verifySeek(fs, p, smallOffset, fileLength, buf, expected);

        verifyPread(fs, p, largeOffset, fileLength, buf, expected);
      }
    } finally {
      cluster.shutdown();
    }
  }

  // case 3: tbd
}
