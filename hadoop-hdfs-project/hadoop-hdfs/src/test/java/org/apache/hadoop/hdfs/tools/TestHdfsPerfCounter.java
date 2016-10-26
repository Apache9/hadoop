package org.apache.hadoop.hdfs.tools;

import com.xiaomi.common.perfcounter.PerfCounter;
import com.xiaomi.infra.hadoop.HdfsPerfCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHdfsPerfCounter {
  private Configuration conf;
  private MiniDFSCluster cluster = null;
  private DistributedFileSystem fs = null;

  @Before public void setup() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_PERFCOUNTER_ENABLED_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024*1024); // set blocksize to 1M
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RPC_PERFCOUNTER, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_PERFCOUNTER_INTERVAL_MS, 1000);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    fs = (DistributedFileSystem)cluster.getFileSystem();
    HdfsPerfCounter.setConf(conf);
  }

  private String CounterName(String method) {
    return HdfsPerfCounter.HDFS_PERFCOUNTER_PREFIX + method;
  }

  @Test
  public void testPerfCounter() throws IOException {
    DFSClient client = fs.getClient();
    client.mkdirs("/tmp/foo", null, true);
    client.mkdirs("/tmp/bar", null, true);
    client.delete("/tmp", true);
    assertEquals(PerfCounter.getCounterValue(HdfsPerfCounter.HDFS), 3);
    assertTrue(PerfCounter.getTimerMeanValue(HdfsPerfCounter.HDFS) > 0);
    assertEquals(PerfCounter.getCounterValue(CounterName("mkdirs")), 2);
    assertEquals(PerfCounter.getCounterValue(CounterName("delete")), 1);

    OutputStream out = client.create("/tmp/file1", true);
    int data_len = 1024*480;
    out.write(new byte[data_len], 0, data_len);
    out.close();
    assertEquals(PerfCounter.getCounterValue(CounterName("create")), 1);
    assertEquals(PerfCounter.getCounterValue(CounterName("sendPacket")), 9);
    assertEquals(PerfCounter.getCounterValue(CounterName("complete")), 1);

    InputStream in = client.open("/tmp/file1");
    in.read(new byte[data_len], 0, data_len);
    in.close();
    assertEquals(PerfCounter.getCounterValue(CounterName("open")), 1);
    assertEquals(PerfCounter.getCounterValue(CounterName("readBuffer")), 1);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      throw new IOException("Test interrupted");
    }

    assertEquals(PerfCounter.getLongGaugeValue("HDFS-NameNode-mkdirs-Qps"), 2);
    assertEquals(PerfCounter.getLongGaugeValue("HDFS-NameNode-delete-Qps"), 1);
    assertEquals(PerfCounter.getLongGaugeValue("HDFS-NameNode-complete-Qps"), 1);
  }
}
