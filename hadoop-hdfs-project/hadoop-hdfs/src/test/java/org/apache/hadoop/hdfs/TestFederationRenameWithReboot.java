package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.FederationRenameInvalidArgument;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFederationRenameWithReboot {
  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs1;
  private static FileSystem fHdfs2;

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setBoolean("dfs.namenode.acls.enabled", true);
    cluster =
        new MiniDFSCluster.Builder(CONF)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs1 = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
  }

  @After
  public void tearDown() throws IOException {
    // cluster.shutdown();
  }

  @Test
  public void testNamenodeRestart() throws IOException {
    String str = "testNamenodeRestart";
    DistributedFileSystem dfs1 =
        (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    DistributedFileSystem dfs2 =
        (DistributedFileSystem) fHdfs2.getDistributedFileSystem();
    final int TEST_RENAME_COUNT = 100;

    // create source dirs
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      String name = "/nnrestart" + i;
      String fname = name + "/testfile";
      fHdfs1.mkdirs(new Path(name), null);
      OutputStream out = fHdfs1.create(new Path(fname));
      out.write(str.getBytes());
      out.close();
      out = fHdfs1.create(new Path("/spmodify/testfile"));
      out.write(str.getBytes());
      out.close();
    }

    // do rename src phase1
    DirectorySubTree[] ds = new DirectorySubTree[TEST_RENAME_COUNT];
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      String name = "/nnrestart" + i;
      ds[i] =
          dfs1.renameSrcPhase1(name, dfs1.getUri().toString(), name, dfs2
              .getUri().toString());
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(dfs1.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), true));
    }
    // save name space to make sure it would test save/load image
    dfs1.dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    dfs1.saveNamespace();
    // restart src namenode and wait it boot up again
    cluster.restartNameNode(0);
    // The name node is just restarted, the first RPC will get EOFexcpeiton.
    // Simply ignore it.
    try {
      dfs1.renameRecordExist(ds[0].getRenameId(), dfs1.getUri().toString(),
          dfs2.getUri().toString(), true);
    } catch (EOFException eof) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(dfs1.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), true));
    }
    // verify rename id
    Assert.assertEquals(cluster.getNameNode(0).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);

    // do rename dest phase1
    BlocksToDup[] btd = new BlocksToDup[TEST_RENAME_COUNT];
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      String name = "/nnrestart" + i;
      btd[i] =
          dfs2.renameDestPhase1(name, dfs1.getUri().toString(), name, dfs2
              .getUri().toString(), ds[i]);
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(dfs2.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), false));
    }
    // save name space to make sure it would test save/load image
    dfs2.dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    dfs2.saveNamespace();
    // restart dst namenode and wait it boot up again
    cluster.restartNameNode(1);
    // The name node is just restarted, the first RPC will get EOFexcpeiton.
    // Simply ignore it.
    try {
      dfs2.renameRecordExist(ds[0].getRenameId(), dfs1.getUri().toString(),
          dfs2.getUri().toString(), false);
    } catch (EOFException eof) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertTrue(dfs2.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), false));
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
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Block blk =
        new Block(btd[0].get(0).getDstBlockId(), 0, btd[0].get(0)
            .getDstBlockGenStamp());
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());

    // do blks move
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      FederationRenameBlockCollector frbc =
          new FederationRenameBlockCollector(ds[i], btd[i], CONF);
      frbc.linkBlocksToNewPool();
    }

    // do src phase2
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      boolean sp2 = dfs1.renameSrcPhase2(ds[i].getRenameId(), false);
      Assert.assertTrue(sp2);
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(dfs1.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), true));
    }
    // save name space to make sure it would test save/load image
    dfs1.dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    dfs1.saveNamespace();
    // restart src namenode and wait it boot up again
    cluster.restartNameNode(0);
    // The name node is just restarted, the first RPC will get EOFexcpeiton.
    // Simply ignore it.
    try {
      dfs1.renameRecordExist(ds[0].getRenameId(), dfs1.getUri().toString(),
          dfs2.getUri().toString(), true);
    } catch (EOFException eof) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(dfs1.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), true));
    }
    // verify rename id
    Assert.assertEquals(cluster.getNameNode(0).getNamesystem()
        .getCurrentRenameId(), ds[TEST_RENAME_COUNT - 1].getRenameId() + 1);

    // do dst phase2
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      boolean dp2 =
          dfs2.renameDestPhase2(ds[i].getRenameId(), dfs1.getUri().toString());
      Assert.assertTrue(dp2);
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(dfs2.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), false));
    }
    // save name space to make sure it woule test save/load image
    dfs2.dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    dfs2.saveNamespace();
    // restart dst namenode and wiat it boot up again
    cluster.restartNameNode(1);
    // The name node is just restarted, the first RPC will get EOFexcpeiton.
    // Simply ignore it.
    try {
      dfs2.renameRecordExist(ds[0].getRenameId(), dfs1.getUri().toString(),
          dfs2.getUri().toString(), false);
    } catch (EOFException eof) {
      // Ignore
    }
    for (int i = 0; i < TEST_RENAME_COUNT; i++) {
      Assert.assertFalse(dfs2.renameRecordExist(ds[i].getRenameId(), dfs1
          .getUri().toString(), dfs2.getUri().toString(), false));
    }
    // verify blockid and genstamp
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getBlockIdGenerator().getCurrentValue(), btd[TEST_RENAME_COUNT - 1]
        .get(lastBtdsz - 1).getDstBlockId());
    Assert.assertEquals(cluster.getNameNode(1).getNamesystem()
        .getCurrentGenStamp(blk), btd[TEST_RENAME_COUNT - 1].get(lastBtdsz - 1)
        .getDstBlockGenStamp());
  }
}
