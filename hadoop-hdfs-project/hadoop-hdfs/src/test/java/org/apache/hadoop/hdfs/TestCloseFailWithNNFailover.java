package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NSConf;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloseFailWithNNFailover {
  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  static final int blockSize = 8192;
  static final String testDir = "/test";

  @BeforeClass
  public static void setup() throws IOException {
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setBoolean("dfs.namenode.acls.enabled", true);
    CONF.setLong("dfs.ha.tail-edits.period", 1);
    CONF.setLong("dfs.ha.log-roll.period", 1);
    MiniDFSNNTopology top = MiniDFSNNTopology.simpleHATopology();
    cluster =
        new MiniDFSCluster.Builder(CONF).nnTopology(top).numDataNodes(2)
            .build();
    cluster.waitClusterUp();
    cluster.transitionToActive(0);
    cluster.transitionToStandby(1);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    cluster.shutdown();
  }

  private void switchActiveNN() throws IOException {
    // wait three edit log tailer period so that all remaining edit logs
    // have been rolled
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    if (cluster.getNameNode(0).isStandbyState()) {
      Assert.assertFalse(cluster.getNameNode(1).isStandbyState());
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
    } else {
      Assert.assertTrue(cluster.getNameNode(1).isStandbyState());
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
    }
  }

  private DistributedFileSystem getFileSystem() throws IOException {
    if (cluster.getNameNode(0).isStandbyState()) {
      Assert.assertFalse(cluster.getNameNode(1).isStandbyState());
      return cluster.getFileSystem(1);
    } else {
      Assert.assertTrue(cluster.getNameNode(1).isStandbyState());
      return cluster.getFileSystem(0);
    }
  }

  @Test
  public void testCloseFailWithNNFailover() throws IOException {
    String str = "testCloseFailWithNNFailover";
    final Path writeFile = new Path(testDir + "/1");
    final Path unCloseFile = new Path(testDir + "/2");

    DistributedFileSystem dfs = getFileSystem();
    dfs.mkdirs(new Path(testDir));
    OutputStream out = dfs.create(writeFile);
    out.write(str.getBytes());
    out.close();

    OutputStream ucout = dfs.create(unCloseFile);
    ucout.write(str.getBytes());

    out = dfs.append(writeFile);
    dfs.setQuota(new Path(testDir), 1, Long.MAX_VALUE);
    // set quota so that later commitBlockSynchronization would fail
    LocatedBlocks locs =
        dfs.getClient().getLocatedBlocks(writeFile.toString(), 0);
    Assert.assertTrue(locs.getLocatedBlocks().size() == 1);
    long gen1 = locs.getLocatedBlocks().get(0).getBlock().getGenerationStamp();
    dfs.recoverLease(writeFile);
    // wait for commitBlockSynchronization
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    locs = dfs.getClient().getLocatedBlocks(writeFile.toString(), 0);
    Assert.assertTrue(locs.getLocatedBlocks().size() == 1);
    long gen2 = locs.getLocatedBlocks().get(0).getBlock().getGenerationStamp();
    Assert.assertFalse(dfs.isFileClosed(writeFile));
    Assert.assertTrue(gen2 > gen1);

    // failover
    switchActiveNN();
    // wait for log replay
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    dfs = getFileSystem();
    Assert.assertFalse(dfs.isFileClosed(writeFile));
    dfs.recoverLease(writeFile);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    locs = dfs.getClient().getLocatedBlocks(writeFile.toString(), 0);
    Assert.assertTrue(locs.getLocatedBlocks().size() == 1);
    long gen3 = locs.getLocatedBlocks().get(0).getBlock().getGenerationStamp();
    Assert.assertFalse(dfs.isFileClosed(writeFile));
    System.out.println("gen3 is " + gen3 + " gen2 is " + gen2);
    Assert.assertTrue(gen3 > gen2);

    // fail over again
    switchActiveNN();
    // wait for log replay
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    dfs = getFileSystem();
    Assert.assertFalse(dfs.isFileClosed(writeFile));
    dfs.recoverLease(writeFile);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    locs = dfs.getClient().getLocatedBlocks(writeFile.toString(), 0);
    Assert.assertTrue(locs.getLocatedBlocks().size() == 1);
    long gen4 = locs.getLocatedBlocks().get(0).getBlock().getGenerationStamp();
    Assert.assertTrue(dfs.isFileClosed(writeFile));
    System.out.println("gen4 is " + gen4 + " gen3 is " + gen3);
    Assert.assertTrue(gen4 == gen3);

    // fail over again
    switchActiveNN();
    // wait for log replay
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    dfs = getFileSystem();
    Assert.assertTrue(dfs.isFileClosed(writeFile));
    locs = dfs.getClient().getLocatedBlocks(writeFile.toString(), 0);
    Assert.assertTrue(locs.getLocatedBlocks().size() == 1);
    long gen5 = locs.getLocatedBlocks().get(0).getBlock().getGenerationStamp();
    System.out.println("gen5 is " + gen5 + " gen4 is " + gen4);
    Assert.assertTrue(gen5 == gen4);
  }
}
