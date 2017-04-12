package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Pattern;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.FederatedDFSFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.junit.Test;

public class TestFederatedFsck {
  static final String auditLogFile = System.getProperty("test.build.dir",
      "build/test") + "/TestFsck-audit.log";

  // Pattern for:
  // allowed=true ugi=name ip=/address cmd=FSCK src=/ dst=null perm=null
  static final Pattern fsckPattern = Pattern.compile("allowed=.*?\\s"
      + "ugi=.*?\\s" + "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s"
      + "cmd=fsck\\ssrc=\\/\\sdst=null\\s" + "perm=null\\s" + "proto=.*");
  static final Pattern getfileinfoPattern = Pattern
      .compile("allowed=.*?\\s" + "ugi=.*?\\s"
          + "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s"
          + "cmd=getfileinfo\\ssrc=\\/\\sdst=null\\s" + "perm=null\\s"
          + "proto=.*");

  static final Pattern numCorruptBlocksPattern = Pattern
      .compile(".*Corrupt blocks:\t\t([0123456789]*).*");

  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  static String runFsck(Configuration conf, int expectedErrCode,
      boolean checkErrorCode, String... path) throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    ((Log4JLogger) FSPermissionChecker.LOG).getLogger().setLevel(Level.ALL);
    int errCode = ToolRunner.run(new DFSck(conf, out), path);
    if (checkErrorCode) {
      assertEquals(expectedErrCode, errCode);
    }
    ((Log4JLogger) FSPermissionChecker.LOG).getLogger().setLevel(Level.INFO);
    FSImage.LOG.error("OUTPUT = " + bStream.toString());
    return bStream.toString();
  }

  /** do fsck */
  @Test
  public void testFsck() throws Exception {
    DFSTestUtil util =
        new DFSTestUtil.Builder().setName("TestFsck").setNumFiles(20).build();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final long precision = 1L;
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
          precision);
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);

      cluster =
          new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
              .numDataNodes(4).build();
      cluster.waitClusterUp();

      FileSystem fHdfs1 = cluster.getFileSystem(0);
      FileSystem fHdfs2 = cluster.getFileSystem(1);
      ConfigUtil.addLink(conf, "/fs1", fHdfs1.getUri());
      ConfigUtil.addLink(conf, "/fs2", fHdfs2.getUri());
      conf.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
      conf.setBoolean("fs.hdfs.impl.disable.cache", true);
      conf.set("fs.defaultFS", "hdfs://default");
      fs = FileSystem.get(conf);
      final String fileName = "/fs1/srcdat";
      util.createFiles(fs, fileName);
      util.waitReplication(fs, fileName, (short) 3);
      final Path file = new Path(fileName);
      long aTime = fs.getFileStatus(file).getAccessTime();
      Thread.sleep(precision);
      String outStr = runFsck(conf, 0, true, "/fs1");
      assertEquals(aTime, fs.getFileStatus(file).getAccessTime());
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      if (fs != null) {
        try {
          fs.close();
        } catch (Exception e) {
        }
      }
      cluster.shutdown();
    } finally {
    }
  }

}
