package org.apache.hadoop.hdfs.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xiaomi.infra.hadoop.FalconSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.Canary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

//@RunWith(MockitoJUnitRunner.class)
public class TestCanary {

   public class DfsController implements Runnable {
    private String operation;
    private String node_type;
    private int node_index;
    private long sleepTime;

    DfsController(String op, String node, int node_idx, long waitTime) {
      operation = op;
      node_type = node;
      sleepTime = waitTime;
      node_index = node_idx;
    }

    public void run() {
      try {
        Thread.sleep(sleepTime);

        MiniDFSCluster dfs = TestCanary.this.dfsCluster;
        if (node_type.equals("DN")) {
          if (operation.equals("start")) {
            TestCanary.this.startDataNodes(node_index);
          } else if (operation.equals("stop")) {
            dfs.stopDataNode(node_index);
          } else {
            System.out.println("invalid operation " + operation + "for DN");
          }
        } else if (node_type.equals("NN")) {
          if (operation.equals("stop")) {
            dfs.shutdownNameNode(node_index);
          } else if (operation.equals("restart")) {
            dfs.restartNameNode();
          } else if (operation.equals("to_standby")) {
            dfs.transitionToStandby(node_index);
          } else if (operation.equals("to_active")) {
            dfs.transitionToActive(node_index);
          } else {
            System.out.println("invalid operation " + operation + "for NN");
          }
        }
      } catch (Exception e) {
        System.out.println("operation failed due to exception" + e.getMessage());
      }
    }
   }

  private static final String NSID = "ns1";
  private Configuration conf;
  private MiniQJMHACluster cluster;
  private MiniDFSCluster dfsCluster;
  private int fileLen = 10*1024;

  @Before
  public void setup() throws InterruptedException, IOException {
    //Logger.getRootLogger().setLevel(Level.DEBUG);
    conf = new Configuration();

    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);

    //minimize test delay
    conf.setInt("hdfs.canary.rpc.timeout", 200);
    conf.setInt("hdfs.canary.retry.interval", 100);
    conf.setInt("hdfs.canary.rpc.max.retries", 2);
    conf.setInt("hdfs.canary.read.timeout", 500);
    conf.setInt("hdfs.canary.failover.max.retries", 2);

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 0);
    //conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, fileLen/2);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 2*1024*1024);

    cluster = new MiniQJMHACluster.Builder(conf).build();
    dfsCluster = cluster.getDfsCluster();
    dfsCluster.transitionToActive(0);
    startDataNodes(3);

    setHAConf(conf, dfsCluster.getNameNode(0).getHostAndPort(),
            dfsCluster.getNameNode(1).getHostAndPort());
    setJNConf();
    assertTrue(HAUtil.isHAEnabled(conf, NSID));
  }

  private void startDataNodes(int numDataNodes) throws IOException{
    dfsCluster.startDataNodes(conf, numDataNodes, StorageType.DEFAULT, true,
            null, null, null, null, false, false, false, null);
    dfsCluster.waitClusterUp();
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  private void setHAConf(Configuration conf, String nn1Addr, String nn2Addr) {
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://" + NSID);
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, NSID);
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, NSID);
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, NSID), "nn1,nn2");
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, NSID, "nn1"), nn1Addr);
    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, NSID, "nn2"), nn2Addr);
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "127.0.0.1:34567");
  }

  // Canary get jmx of JN from JN's http port, which is parsed from config
  // MiniJournalCluster will overwrite the http port config item when initialize, and assign
  // random port for it's JournalNodes
  // This function could update the JN's real http port to conf, in a hack way
  private void setJNConf() throws InterruptedException, IOException{
    MiniJournalCluster jCluster = cluster.getJournalCluster();
    // Restart operation will dump the http-port to conf
    jCluster.restartJournalNode(0);
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, jCluster.getJournalNode(0).getConf()
        .get(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY));
  }

  @Test
  public void testBasic() throws Exception {
    //dfsCluster.shutdownNameNode(1);
    //MockSink sink = new MockSink();
    ToolRunner.run(conf, new Canary(), new String[]{"-interval", "0"});
  }

  @Test
  public void testAvailability() throws Exception {
    dfsCluster.transitionToStandby(0);
    Thread t1 = new Thread(new DfsController("to_active", "NN", 0, 5200));
    Thread t2 = new Thread(new DfsController("to_standby", "NN", 0, 8000));
    Thread t3 = new Thread(new DfsController("to_active", "NN", 0, 11200));
    t1.start();
    t2.start();
    t3.start();

    /*
    Thread t1 = new Thread(new DfsController("stop", "NN", 0, 5200));
    Thread t2 = new Thread(new DfsController("stop", "NN", 1, 7200));
    t1.start();
    t2.start();
    */

    FalconSink sink = new FalconSink();
    sink.setConf(conf);
    ToolRunner.run(conf, new Canary(sink), new String[]{"-interval", "5"});
  }

  @Test
  public void testFalconSink() throws Exception {
    dfsCluster.stopDataNode(0);
    dfsCluster.stopDataNode(7);
    FalconSink sink = new FalconSink();
    sink.setConf(conf);
    ToolRunner.run(conf, new Canary(sink), new String[]{"-interval", "0"});
  }

  @Test
  public void testCorruptBlocks() throws Exception {
    DistributedFileSystem dfs = dfsCluster.getFileSystem(0);
    Path corruptFile = new Path("/testMissingBlocks/corruptFile");
    DFSTestUtil.createFile(dfs, corruptFile, fileLen, (short)1, 0);

    // Corrupt the block
    ExtendedBlock block = DFSTestUtil.getFirstBlock(dfs, corruptFile);
    MiniDFSCluster.corruptReplica(0, block);
    MiniDFSCluster.corruptReplica(1, block);
    MiniDFSCluster.corruptReplica(2, block);

    // read the file so that the corrupt block is reported to NN
    FSDataInputStream in = dfs.open(corruptFile);
    try {
      in.readFully(new byte[fileLen]);
    } catch (ChecksumException ignored) { // checksum error is expected.
    }
    in.close();

    System.out.println("Waiting for missing blocks count to increase...");

    while (dfs.getMissingBlocksCount() <= 0) {
      Thread.sleep(100);
    }
    assertTrue(dfs.getMissingBlocksCount() == 1);

    //MockSink sink = new MockSink();
    //ToolRunner.run(conf, new Canary(sink), new String[]{"-interval", "0"});
  }

  @Test
  public void testGetCapacityRemaining() throws IOException {
    Canary canary = new Canary(new Canary.StdOutSink());
    canary.setConf(conf);
    canary.checkClusterCapacityRemaining();
  }

  @Test
  public void testOnError() throws Exception {
    dfsCluster.shutdown();
    dfsCluster.restartNameNodes();
    dfsCluster.transitionToActive(0);

    dfsCluster.stopDataNode(0);
    dfsCluster.stopDataNode(0);

    //MockSink sink = new MockSink();
    //ToolRunner.run(conf, new Canary(sink), new String[]{"-interval", "0"});
  }
}
