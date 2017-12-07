package com.xiaomi.infra.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xiaomi.infra.hadoop.io.SequenceFileWriter;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSequenceFileWriter {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private Random rand;
  static private final int BLOCK_SIZE = 512;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RECOVER_ON_CLOSE_EXCEPTION, true);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
    rand = new Random();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  private void runCommand(DFSAdmin admin, boolean expectError, String... args)
      throws Exception {
    runCommand(admin, args, expectError);
  }

  private void runCommand(DFSAdmin admin, String args[], boolean expectEror)
      throws Exception {
    int val = admin.run(args);
    if (expectEror) {
      assertEquals(val, -1);
    } else {
      assertTrue(val >= 0);
    }
  }

  private String getRandomString() {
    Integer a = rand.nextInt(Integer.MAX_VALUE);
    return a.toString();
  }

  @Test
  public void testQuotaExceeded() throws Exception {
    DFSAdmin admin = new DFSAdmin(conf);
    int quotaSize = 3 * BLOCK_SIZE / 2;
    fs.mkdirs(new Path("/test"));
    runCommand(admin, false, "-setSpaceQuota", Integer.toString(quotaSize),
        "/test");
    SequenceFileWriter writer = new SequenceFileWriter("/test/file1", "BLOCK", "default", conf);
    // The data is compressed, so we do not know exactly how many writes would make the quota exceeded. Just set a big up-limit.
    int writeNum = 0;
    int lastWriteNum = 0;

    try {
      do {
        String key = getRandomString();
        String val = getRandomString();
        writer.append(key.getBytes(), val.getBytes());

        if (writeNum - lastWriteNum > 100) {
          lastWriteNum = writeNum;
          writer.hflush(true);
        }
        writeNum++;
      } while (writeNum < 80 * BLOCK_SIZE);
      writer.close();
    } catch (Exception exp) {
      assertTrue(writer.isQuotaExceeded());
    }

    ContentSummary c;
    c = ((DistributedFileSystem) fs).getContentSummary(new Path("/test"));
    FileStatus st = fs.getFileStatus(new Path("/test/file1"));

    assertTrue(c.getSpaceConsumed() < quotaSize + st.getBlockSize()
        * st.getReplication());
  }

  @Test
  public void testOverWriteConfig() throws Exception {
    conf.setBoolean("hadoop.sequencefile.create.overwrite", true);
    fs.mkdirs(new Path("/test"));
    SequenceFileWriter writer = new SequenceFileWriter("/test/file1", "BLOCK", "default", conf);
    // The data is compressed, so we do not know exactly how many writes would make the quota exceeded. Just set a big up-limit.
    int writeNum = 0;
    int lastWriteNum = 0;

    try {
      do {
        String key = getRandomString();
        String val = getRandomString();
        writer.append(key.getBytes(), val.getBytes());

        if (writeNum - lastWriteNum > 100) {
          lastWriteNum = writeNum;
          writer.hflush(true);
        }
        writeNum++;
      } while (writeNum < 80 * BLOCK_SIZE);
      writer.close();
    } catch (Exception exp) {
      assertTrue(false);
    }

    writer = new SequenceFileWriter("/test/file1", "BLOCK", "default", conf);

    try {
      do {
        String key = getRandomString();
        String val = getRandomString();
        writer.append(key.getBytes(), val.getBytes());

        if (writeNum - lastWriteNum > 100) {
          lastWriteNum = writeNum;
          writer.hflush(true);
        }
        writeNum++;
      } while (writeNum < 80 * BLOCK_SIZE);
      writer.close();
    } catch (Exception exp) {
      assertTrue(false);
    }

    conf.setBoolean("hadoop.sequencefile.create.overwrite", false);

    try {
      writer = new SequenceFileWriter("/test/file1", "BLOCK", "default", conf);
    } catch (Exception e) {
      assertTrue(e instanceof FileAlreadyExistsException);
    }


  }
}
