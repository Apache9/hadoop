package org.apache.hadoop.hdfs;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NSConf;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.ipc.StandbyException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;

public class TestFederationRenameWithFailover {
  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs1;
  private static FileSystem fHdfs2;
  private static final int BASE_PORT = 12345;

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setBoolean("dfs.namenode.acls.enabled", true);
    CONF.setLong("dfs.ha.tail-edits.period", 1);
    CONF.setLong("dfs.ha.log-roll.period", 1);
    MiniDFSNNTopology top = MiniDFSNNTopology.simpleHAFederatedTopology(2);
    int idx = 0;
    for (NSConf nc : top.getNameservices()) {
      for (NNConf nn : nc.getNNs()) {
        nn.setIpcPort(BASE_PORT + idx);
        idx++;
      }
    }
    cluster =
        new MiniDFSCluster.Builder(CONF).nnTopology(top).numDataNodes(2)
            .build();
    cluster.waitClusterUp();

    cluster.restartNameNodes();

    cluster.transitionToActive(0);
    cluster.transitionToStandby(1);
    cluster.transitionToActive(2);
    cluster.transitionToStandby(3);
  }

  @After
  public void tearDown() throws IOException {
    // cluster.shutdown();
  }

  interface ASSwitcher {
    public void switchActiveNN(int nsIdx) throws IOException;
  }

  private DistributedFileSystem getClientHdfs(int nsIdx) throws IOException {
    int first = 2 * nsIdx;
    int second = 2 * nsIdx + 1;
    if (cluster.getNameNode(first).isActiveState()) {
      Assert.assertFalse(cluster.getNameNode(second).isActiveState());
      return cluster.getFileSystem(first);
    } else {
      Assert.assertTrue(cluster.getNameNode(second).isActiveState());
      return cluster.getFileSystem(second);
    }
  }

  private void testNamenodeFailoverInternal(ASSwitcher switcher)
      throws IOException {
    if (!cluster.getNameNode(0).isActiveState()) {
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
    }
    if (!cluster.getNameNode(2).isActiveState()) {
      cluster.transitionToStandby(3);
      cluster.transitionToActive(2);
    }
    String str = "testNamenodeFailover";
    String pathPrefix = "/nnfailover" + switcher.getClass().getName();
    fHdfs1 = getClientHdfs(0);
    fHdfs2 = getClientHdfs(1);
    DistributedFileSystem dfs1 =
        (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    DistributedFileSystem dfs2 =
        (DistributedFileSystem) fHdfs2.getDistributedFileSystem();
    final int TEST_RENAME_COUNT = 100;

    // create source dirs
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      String name = pathPrefix + i;
      fHdfs1.mkdirs(new Path(name), null);
      String fname = name + "/afile";
      OutputStream out = fHdfs1.create(new Path(fname));
      out.close();
      fname = name + "/testfile";
      out = fHdfs1.create(new Path(fname));
      out.write(str.getBytes());
      out.close();
      fname = name + "/testfile1";
      out=fHdfs1.create(new Path(fname));
      out.close();
      
      out = fHdfs1.create(new Path("/spmodify/testfile"));
      out.write(str.getBytes());
      out.close();
    }

    // do rename src phase1
    System.out.println("Step 1 ...");
    DirectorySubTree[] ds = new DirectorySubTree[TEST_RENAME_COUNT];
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      String name = pathPrefix + i;
      ds[i] =
          dfs1.renameSrcPhase1(name, dfs1.getUri().toString(), name, dfs2
              .getUri().toString());
    }
    // Wait for log replay to finish
    try {
      Thread.sleep(4000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(cluster
          .getNameNode(0)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), true));
    }
    // verify rename id
    Assert.assertEquals(cluster.getNameNode(0).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);
    switcher.switchActiveNN(0);
    // verify rename record and renameid after failover
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(cluster
          .getNameNode(1)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), true));
    }
    // verify rename id
    Assert.assertEquals(cluster.getNameNode(0).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);

    // Active/Standby has been switched, get new hdfs client
    fHdfs1 = getClientHdfs(0);
    fHdfs2 = getClientHdfs(1);
    dfs1 = (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    dfs2 = (DistributedFileSystem) fHdfs2.getDistributedFileSystem();

    // do rename dest phase1
    BlocksToDup[] btd = new BlocksToDup[TEST_RENAME_COUNT];
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      String name = pathPrefix + i;
      btd[i] =
          dfs2.renameDestPhase1(name, dfs1.getUri().toString(), name, dfs2
              .getUri().toString(), ds[i]);
    }
    // Wait for log replay to finish
    try {
      Thread.sleep(4000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(cluster
          .getNameNode(2)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), false));
    }
    // verify blockid and genstamp
    boolean isBlkIdSeq = true;
    long lastBlkId = 0;
    boolean isGenSeq = true;
    long lastGen = 0;
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      for (int j = 0; j < btd[i].size(); j++) {
        if (btd[i].get(j).getDstBlockId() < lastBlkId) {
          isBlkIdSeq = false;
          break;
        } else {
          lastBlkId = btd[i].get(j).getDstBlockId();
        }
        if (btd[i].get(j).getDstBlockGenStamp() < lastGen) {
          isGenSeq = false;
          break;
        } else {
          lastGen = btd[i].get(j).getDstBlockGenStamp();
        }
      }
    }
    // verify blkid and gen stamp
    Assert.assertTrue(isBlkIdSeq);
    Assert.assertTrue(isGenSeq);
    int lastBtdsz = btd[TEST_RENAME_COUNT - 1].size();
    Assert.assertEquals(cluster.getNameNode(2).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Assert.assertEquals(cluster.getNameNode(3).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Block blk =
        new Block(btd[0].get(0).getDstBlockId(), 0, btd[0].get(0)
            .getDstBlockGenStamp());
    Assert.assertEquals(cluster.getNameNode(2).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());
    Assert.assertEquals(cluster.getNameNode(3).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());
    switcher.switchActiveNN(1);
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(cluster
          .getNameNode(3)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), false));
    }
    Assert.assertEquals(cluster.getNameNode(2).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Assert.assertEquals(cluster.getNameNode(3).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Assert.assertEquals(cluster.getNameNode(2).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());
    Assert.assertEquals(cluster.getNameNode(3).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());

    // do blks move
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      FederationRenameBlockCollector frbc =
          new FederationRenameBlockCollector(ds[i], btd[i], CONF);
      frbc.linkBlocksToNewPool();
    }

    // Active/Standby has been switched, get new hdfs client
    fHdfs1 = getClientHdfs(0);
    fHdfs2 = getClientHdfs(1);
    dfs1 = (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    dfs2 = (DistributedFileSystem) fHdfs2.getDistributedFileSystem();

    // do src phase2
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      boolean sp2 = dfs1.renameSrcPhase2(ds[i].getRenameId(), false);
      Assert.assertTrue(sp2);
    }
    // Wait for log replay to finish
    try {
      Thread.sleep(4000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(cluster
          .getNameNode(1)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), true));
    }
    switcher.switchActiveNN(0);
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(cluster
          .getNameNode(0)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), true));
    }
    // verify rename id
    Assert.assertEquals(cluster.getNameNode(0).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);

    // do dst phase2
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      boolean dp2 =
          dfs2.renameDestPhase2(ds[i].getRenameId(), dfs1.getUri().toString());
      Assert.assertTrue(dp2);
    }
    // Wait for log replay to finish
    try {
      Thread.sleep(4000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(cluster
          .getNameNode(3)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), false));
    }
    switcher.switchActiveNN(1);
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(cluster
          .getNameNode(2)
          .getRpcServer()
          .renameRecordExist(ds[i].getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), false));
    }
    // verify blockid and genstamp
    Assert.assertEquals(cluster.getNameNode(3).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Assert.assertEquals(cluster.getNameNode(3).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());
  }

  @Test
  public void testNormalNNFailover() throws IOException {
    testNamenodeFailoverInternal(new ASSwitcher() {
      public void switchActiveNN(int nsIdx) throws IOException {
        int first = 2 * nsIdx;
        int second = 2 * nsIdx + 1;
        // wait three edit log tailer period so that all remaining edit logs
        // have been rolled
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ie) {
          // Ignore
        }
        if (cluster.getNameNode(first).isActiveState()) {
          Assert.assertFalse(cluster.getNameNode(second).isActiveState());
          System.out.println("To make standby " + first);
          cluster.transitionToStandby(first);
          System.out.println("To make active " + second);
          cluster.transitionToActive(second);
        } else {
          Assert.assertTrue(cluster.getNameNode(second).isActiveState());
          System.out.println("To make standy " + second);
          cluster.transitionToStandby(second);
          System.out.println("To make active " + first);
          cluster.transitionToActive(first);
        }
      }
    });
  }

  @Test
  public void testNNFailoverWithReboot() throws IOException {
    testNamenodeFailoverInternal(new ASSwitcher() {
      public void switchActiveNN(int nsIdx) throws IOException {
        int first = 2 * nsIdx;
        int second = 2 * nsIdx + 1;
        // wait three edit log tailer period so that all remaining edit logs
        // have been rolled
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ie) {
          // Ignore
        }
        if (cluster.getNameNode(first).isActiveState()) {
          Assert.assertFalse(cluster.getNameNode(second).isActiveState());
          System.out.println("To restart " + first);
          cluster.restartNameNode(first);
          cluster.transitionToStandby(first);
          System.out.println("To make active " + second);
          cluster.transitionToActive(second);
        } else {
          Assert.assertTrue(cluster.getNameNode(second).isActiveState());
          System.out.println("To restart " + second);
          cluster.restartNameNode(second);
          cluster.transitionToStandby(second);
          System.out.println("To make active " + first);
          cluster.transitionToActive(first);
        }
      }
    });
  }

  @Test
  public void testRenameToDifferentDestPathWithFailover() throws Exception {
    String src = "/user/foo";
    String dst = "/user/bar";
    fHdfs1 = getClientHdfs(0);
    fHdfs2 = getClientHdfs(1);
    DistributedFileSystem srcFs =
        (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    DistributedFileSystem dstFs =
        (DistributedFileSystem) fHdfs2.getDistributedFileSystem();
    srcFs.mkdirs(new Path(src));
    dstFs.mkdirs(new Path(dst));
    DirectorySubTree subTree = srcFs.renameSrcPhase1(src,
        srcFs.getUri().toString(), dst, dstFs.getUri().toString());
    if (subTree != null && subTree.getSize() > 0) {
      BlocksToDup blksToDup = dstFs.renameDestPhase1(src,
          srcFs.getUri().toString(), dst, dstFs.getUri().toString(), subTree);

      cluster.transitionToStandby(2);
      cluster.transitionToActive(3);
      Thread.sleep(3000);
      fHdfs2 = getClientHdfs(1);
      dstFs = (DistributedFileSystem) fHdfs2.getDistributedFileSystem();

      FederationRenameBlockCollector frbc = null;
      if (blksToDup.size() != 0) {
        frbc = new FederationRenameBlockCollector(subTree, blksToDup, CONF);
      }
      // ask DN to add new link
      if (frbc != null) {
        frbc.linkBlocksToNewPool();
      }

      // skip renameSrcPhase2
      Assert.assertTrue(dstFs.renameDestPhase2(subTree.getRenameId(),
          srcFs.getUri().toString()));
    }
    Assert.assertTrue(dstFs.exists(new Path(dst + "/foo")));
  }

  @Test
  public void testRenameRecordExistWithFailover() throws IOException {
    if (!cluster.getNameNode(0).isActiveState()) {
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
    }
    if (!cluster.getNameNode(2).isActiveState()) {
      cluster.transitionToStandby(3);
      cluster.transitionToActive(2);
    }
    String str = "testRenameRecordExistWithFailover";
    String pathPrefix = "/testRenameRecordExistWithFailover";
    fHdfs1 = getClientHdfs(0);
    fHdfs2 = getClientHdfs(1);
    DistributedFileSystem dfs1 =
        (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    DistributedFileSystem dfs2 =
        (DistributedFileSystem) fHdfs2.getDistributedFileSystem();
    final int TEST_RENAME_COUNT = 100;

    String name = pathPrefix;
    fHdfs1.mkdirs(new Path(name), null);
    String fname = name + "/afile";
    OutputStream out = fHdfs1.create(new Path(fname));
    out.close();
    fname = name + "/testfile";
    out = fHdfs1.create(new Path(fname));
    out.write(str.getBytes());
    out.close();
    fname = name + "/testfile1";
    out = fHdfs1.create(new Path(fname));
    out.close();

    out = fHdfs1.create(new Path("/spmodify/testfile"));
    out.write(str.getBytes());
    out.close();

    // do rename src phase1
    DirectorySubTree ds;
    name = pathPrefix;
    ds = dfs1.renameSrcPhase1(name, dfs1.getUri().toString(), name,
        dfs2.getUri().toString());
    // Wait for log replay to finish
    try {
      Thread.sleep(4000);
    } catch (InterruptedException ie) {
      // Ignore
    }
    Assert.assertTrue(cluster.getNameNode(0).getRpcServer()
        .renameRecordExist(ds.getRenameId(), dfs1.getUri().toString(),
            dfs2.getUri().toString(), true));
    try {
      cluster.getNameNode(1).getRpcServer()
          .renameRecordExist(ds.getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), true);
      Assert.assertTrue("We should catch StandbyException here.", false);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof StandbyException);
    }
    // verify rename id
    Assert.assertEquals(
        cluster.getNameNode(0).getNamesystem().getCurrentRenameId(),
        ds.getRenameId() + 1);
    Assert.assertEquals(
        cluster.getNameNode(1).getNamesystem().getCurrentRenameId(),
        ds.getRenameId() + 1);
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);
    // verify rename record and renameid after failover
    Assert.assertTrue(cluster.getNameNode(1).getRpcServer()
        .renameRecordExist(ds.getRenameId(), dfs1.getUri().toString(),
            dfs2.getUri().toString(), true));
    try {
      cluster.getNameNode(0).getRpcServer()
          .renameRecordExist(ds.getRenameId(), dfs1.getUri().toString(),
              dfs2.getUri().toString(), true);
      Assert.assertTrue("We should catch StandbyException here.", false);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof StandbyException);
    }
    // verify rename id
    Assert.assertEquals(
        cluster.getNameNode(0).getNamesystem().getCurrentRenameId(),
        ds.getRenameId() + 1);
    Assert.assertEquals(
        cluster.getNameNode(1).getNamesystem().getCurrentRenameId(),
        ds.getRenameId() + 1);
  }

}
