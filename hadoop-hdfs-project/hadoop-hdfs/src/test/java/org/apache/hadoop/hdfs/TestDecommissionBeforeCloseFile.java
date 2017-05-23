package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDecommissionBeforeCloseFile {
  public static final Log LOG = LogFactory.getLog(TestDecommission.class);
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds
  static final int BLOCKREPORT_INTERVAL_MSEC = 1000; // block report in msec
  static final int NAMENODE_REPLICATION_INTERVAL = 1; // replication interval

  final Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;
  FileSystem localFileSys;
  Configuration conf;
  MiniDFSCluster cluster = null;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir =
        new Path(workingDir, PathUtils.getTestDirName(getClass())
            + "/work-dir/decommission");
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");

    // Setup conf
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
        NAMENODE_REPLICATION_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 1);
    conf.setInt(
        DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        1);

    writeConfigFile(hostsFile, null);
    writeConfigFile(excludeFile, null);
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void writeConfigFile(Path name, ArrayList<String> nodes)
      throws IOException {
    // delete if it already exists
    if (localFileSys.exists(name)) {
      localFileSys.delete(name, true);
    }

    FSDataOutputStream stm = localFileSys.create(name);

    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(NameNode nn, Configuration conf)
      throws IOException {
    return new DFSClient(nn.getNameNodeAddress(), conf);
  }

  static void refreshNodes(final FSNamesystem ns, final Configuration conf)
      throws IOException {
    ns.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }

  /* Validate cluster has expected number of datanodes */
  private static void validateCluster(DFSClient client, int numDNs)
      throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDNs, info.length);
  }

  /**
   * Start a MiniDFSCluster
   * 
   * @throws IOException
   */
  private void startCluster(int numNameNodes, int numDatanodes,
      Configuration conf) throws IOException {
    cluster =
        new MiniDFSCluster.Builder(conf)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(numNameNodes))
            .numDataNodes(numDatanodes).build();
    cluster.waitActive();
    for (int i = 0; i < numNameNodes; i++) {
      DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
      validateCluster(client, numDatanodes);
    }
  }

  private DatanodeInfo decommissionNode(int nnIndex,
      ArrayList<DatanodeInfo> decommissionedNodes) throws IOException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    int index = 0;
    boolean found = false;
    while (!found) {
      index = myrand.nextInt(info.length);
      if (!info[index].isDecommissioned()) {
        found = true;
      }
    }
    String nodename = info[index].getXferAddr();
    LOG.info("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>();
    if (decommissionedNodes != null) {
      for (DatanodeInfo dn : decommissionedNodes) {
        nodes.add(dn.getName());
      }
    }
    nodes.add(nodename);
    writeConfigFile(excludeFile, nodes);
    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    DatanodeInfo ret =
        NameNodeAdapter
            .getDatanode(cluster.getNamesystem(nnIndex), info[index]);
    return ret;
  }

  @Test(timeout = 360000)
  public void testDecommissionBeforeCloseFile() throws IOException {
    startCluster(1, 5, conf);
    // create and write a file that contains one blocks of data
    DistributedFileSystem fileSys = cluster.getFileSystem();
    FSDataOutputStream stm =
        fileSys.create(
            new Path("/test"),
            true,
            fileSys.getConf().getInt(
                CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
            (short) 2, blockSize);
    byte[] buffer = new byte[blockSize / 3];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    LocatedBlocks locs = fileSys.getClient().getLocatedBlocks("/test", 0);
    Assert.assertEquals(1, locs.getLocatedBlocks().size());
    ArrayList<DatanodeInfo> ad = new ArrayList<DatanodeInfo>();
    for (LocatedBlock lb : locs.getLocatedBlocks()) {
      for (DatanodeInfo di : lb.getLocations()) {
        ad.add(di);
      }
    }
    stm =
        fileSys.append(
            new Path("/test"),
            fileSys.getConf().getInt(
                CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096));
    stm.write(buffer);
    // decomm
    decommissionNode(0, ad);
    // Make sure the decomm cannot be done in quite a long duration
    try {
      Thread.sleep(15000); // Wait 15 sec
    } catch (InterruptedException ie) {
      // Ignore
    }
    for (DatanodeInfo di : ad) {
      DatanodeInfo ndi =
          NameNodeAdapter.getDatanode(cluster.getNamesystem(0), di);
      Assert.assertTrue(ndi.isDecommissionInProgress());
    }
    locs = fileSys.getClient().getLocatedBlocks("/test", 0);
    Assert.assertEquals(1, locs.getLocatedBlocks().size());
    for (LocatedBlock lb : locs.getLocatedBlocks()) {
      for (DatanodeInfo di : lb.getLocations()) {
        DatanodeInfo ndi =
            NameNodeAdapter.getDatanode(cluster.getNamesystem(0), di);
        Assert.assertTrue(ndi.isDecommissionInProgress());
      }
    }
    try {
      stm.close();
      Assert.assertTrue(false);
    } catch (IOException ioe) {
      // Threee replicas are in decommissioning, the file cannot be closed.
      Assert.assertTrue(true);
    }
    try {
      Thread.sleep(15000); // Wait 15 sec
    } catch (InterruptedException ie) {
      // Ignore
    }
    System.out.println("block is "
        + locs.getLocatedBlocks().get(0).getBlock().getBlockId());
    for (DatanodeInfo di : ad) {
      DatanodeInfo ndi =
          NameNodeAdapter.getDatanode(cluster.getNamesystem(0), di);
      System.out.println("ndi state is " + ndi.getAdminState());
      Assert.assertTrue(ndi.isDecommissioned());
    }
    locs = fileSys.getClient().getLocatedBlocks("/test", 0);
    Assert.assertEquals(1, locs.getLocatedBlocks().size());
    boolean normalDn = false;
    for (LocatedBlock lb : locs.getLocatedBlocks()) {
      for (DatanodeInfo di : lb.getLocations()) {
        DatanodeInfo ndi =
            NameNodeAdapter.getDatanode(cluster.getNamesystem(0), di);
        if (!ndi.isDecommissionInProgress() && !ndi.isDecommissioned()) {
          normalDn = true;
          break;
        }
      }
      if (normalDn) {
        break;
      }
    }
    Assert.assertTrue(normalDn);
    stm.close();
  }
}
