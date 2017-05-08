package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.server.namenode.FederationRenameException;
import org.apache.hadoop.ipc.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFederationRename {
  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs1;
  private static FileSystem fHdfs2;

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setLong(DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT, 10000);
    CONF.setLong(DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT, 10000);
    CONF.setBoolean("dfs.namenode.acls.enabled", true);
    cluster =
        new MiniDFSCluster.Builder(CONF)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs1 = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
    ConfigUtil.addLink(CONF, "/home", fHdfs1.getUri());
    ConfigUtil.addLink(CONF, "/user", fHdfs2.getUri());
    CONF.set("fs.defaultFS", "hdfs://default");
  }

  @After
  public void tearDown() throws IOException {
    // cluster.shutdown();
  }

  private void dumpDir(FileSystem fs, Path p) throws IOException {
    FileStatus[] sts = fs.listStatus(p);
    System.out.println("File " + p.toString() + " and its children are:");
    for (FileStatus st : sts) {
      System.out.println(st.getPath().toString());
      if (st.isDirectory()) {
        dumpDir(fs, st.getPath());
      }
    }
  }

  private void basicTestEnvSetup() throws IOException {
    fHdfs1.mkdirs(new Path("/a/b"), null);
    fHdfs1.mkdirs(new Path("/a/c"), null);
    fHdfs2.mkdirs(new Path("/c/d"), null);
  }

  @Test
  public void doBasicTest() throws IOException {
    String str = "Just a test";
    basicTestEnvSetup();
    Assert.assertTrue(fHdfs1 instanceof DistributedFileSystem);
    Assert.assertTrue(fHdfs2 instanceof DistributedFileSystem);
    Assert.assertFalse(fHdfs1 instanceof FederatedDFSFileSystem);
    Assert.assertFalse(fHdfs2 instanceof FederatedDFSFileSystem);
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
      Assert.assertTrue(dfs instanceof DistributedFileSystem);
      Assert.assertTrue(dfs instanceof FederatedDFSFileSystem);
      OutputStream out = dfs.create(new Path("/home/a/b/testfile"));
      out.write(str.getBytes());
      out.close();
      FileStatus sFstatus = dfs.getFileStatus(new Path("/home/a/b/testfile"));
      AclStatus sAclStatus = dfs.getAclStatus(new Path("/home/a/b/testfile"));
      boolean rename = dfs.rename(new Path("/home/a"), new Path("/user/c/a"));
      Assert.assertTrue(rename);
      dumpDir(fHdfs1, new Path("/"));
      dumpDir(fHdfs2, new Path("/"));
      Assert.assertTrue(dfs.exists(new Path("/user/c/a/b/testfile")));
      Assert.assertFalse(dfs.exists(new Path("/home/a")));

      InputStream in = dfs.open(new Path("/user/c/a/b/testfile"));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String res = reader.readLine();
      in.close();
      Assert.assertEquals(str, res);
      FileStatus dFstatus = dfs.getFileStatus(new Path("/user/c/a/b/testfile"));
      AclStatus dAclStatus = dfs.getAclStatus(new Path("/user/c/a/b/testfile"));
      Assert.assertEquals(sFstatus.getLen(), dFstatus.getLen());
      Assert.assertEquals(sFstatus.isDir(), dFstatus.isDir());
      Assert.assertEquals(sFstatus.getReplication(), dFstatus.getReplication());
      Assert.assertEquals(sFstatus.getBlockSize(), dFstatus.getBlockSize());
      Assert.assertTrue(sFstatus.getPermission().equals(
          dFstatus.getPermission()));
      Assert.assertTrue(sFstatus.getOwner().equals(dFstatus.getOwner()));
      Assert.assertTrue(sFstatus.getGroup().equals(dFstatus.getGroup()));
      Assert.assertTrue(sAclStatus.equals(dAclStatus));
    } finally {
      CONF.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    }
  }

  @Test
  public void testDestException() throws IOException {
    fHdfs1.mkdirs(new Path("/neg1/neg2"), null);
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
      Assert.assertTrue(dfs instanceof DistributedFileSystem);
      Assert.assertTrue(dfs instanceof FederatedDFSFileSystem);
      try {
        boolean rename =
            dfs.rename(new Path("/home/neg1"), new Path(
                "/user/notexist1/notexist2/notexist3"));
      } catch (IOException ioe) {
        Assert.assertTrue(dfs.delete(new Path("/home/neg1/neg2")));
        return;
      }
      Assert.assertTrue(false);
    } finally {
      CONF.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    }
  }

  private void aclTestEnvSetup() throws IOException {
    fHdfs1.mkdirs(new Path("/sacl1/sacl2/sacl3"), null);
    fHdfs1.create(new Path("/sacl1/sacl4")).close();
  }

  private String generateACL() {
    StringBuilder aclBuilder = new StringBuilder();
    int aclUsers = 5;
    int aclGrps = 5;
    String acls[] = { "r--", "rw-", "r-x", "rwx" };
    Random rd = new Random();
    for (int i = 0; i < aclUsers; i++) {
      if (i != 0) {
        aclBuilder.append(",");
      }
      String user = "user:user" + i;
      aclBuilder.append(user).append(":").append(acls[rd.nextInt(acls.length)]);
    }

    for (int i = 0; i < aclGrps; i++) {
      if (aclUsers > 0 || i != 0) {
        aclBuilder.append(",");
      }
      String grp = "group:grp" + i;
      aclBuilder.append(grp).append(":").append(acls[rd.nextInt(acls.length)]);
    }

    return aclBuilder.toString();
  }

  @Test
  public void testRenameWithAcl() throws IOException {
    aclTestEnvSetup();
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
      String aclstr = generateACL();
      System.out.println("aclstr1 : " + aclstr);
      List<AclEntry> aclEntries = AclEntry.parseAclSpec(aclstr, true);
      dfs.modifyAclEntries(new Path("/home/sacl1"), aclEntries);
      aclstr = generateACL();
      System.out.println("aclstr2 : " + aclstr);
      aclEntries = AclEntry.parseAclSpec(aclstr, true);
      dfs.modifyAclEntries(new Path("/home/sacl1/sacl2"), aclEntries);
      aclstr = generateACL();
      System.out.println("aclstr4 : " + aclstr);
      aclEntries = AclEntry.parseAclSpec(aclstr, true);
      dfs.modifyAclEntries(new Path("/home/sacl1/sacl4"), aclEntries);
      aclstr = generateACL();
      System.out.println("aclstr3 : " + aclstr);
      aclEntries = AclEntry.parseAclSpec(aclstr, true);
      dfs.modifyAclEntries(new Path("/home/sacl1/sacl2/sacl3"), aclEntries);

      AclStatus sacl1 = dfs.getAclStatus(new Path("/home/sacl1"));
      System.out.println("sacl1 : " + sacl1);
      AclStatus sacl2 = dfs.getAclStatus(new Path("/home/sacl1/sacl2"));
      System.out.println("sacl2 : " + sacl2);
      AclStatus sacl3 = dfs.getAclStatus(new Path("/home/sacl1/sacl2/sacl3"));
      System.out.println("sacl3 : " + sacl3);
      AclStatus sacl4 = dfs.getAclStatus(new Path("/home/sacl1/sacl4"));
      System.out.println("sacl4 : " + sacl4);

      boolean rename =
          dfs.rename(new Path("/home/sacl1"), new Path("/user/sacl1"));

      Assert.assertTrue(rename);
      AclStatus dacl1 = dfs.getAclStatus(new Path("/user/sacl1"));
      System.out.println("dacl1 : " + dacl1);
      AclStatus dacl2 = dfs.getAclStatus(new Path("/user/sacl1/sacl2"));
      System.out.println("dacl2 : " + dacl2);
      AclStatus dacl3 = dfs.getAclStatus(new Path("/user/sacl1/sacl2/sacl3"));
      System.out.println("dacl3 : " + dacl3);
      AclStatus dacl4 = dfs.getAclStatus(new Path("/user/sacl1/sacl4"));
      System.out.println("dacl4 : " + dacl4);

      Assert.assertTrue((dacl1.equals(sacl1)));
      Assert.assertTrue(dacl2.equals(sacl2));
      Assert.assertTrue(dacl3.equals(sacl3));
      Assert.assertTrue(dacl4.equals(sacl4));
    } finally {
      CONF.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    }
  }

  private void fixerTestEnvSetup(String str) throws IOException {
    fHdfs1.mkdirs(new Path("/sp1"), null);
    OutputStream out = fHdfs1.create(new Path("/sp1/testfile"));
    out.write(str.getBytes());
    out.close();

    fHdfs1.mkdirs(new Path("/dp1"), null);
    out = fHdfs1.create(new Path("/dp1/testfile"));
    out.write(str.getBytes());
    out.close();

    fHdfs1.mkdirs(new Path("/sp2"), null);
    out = fHdfs1.create(new Path("/sp2/testfile"));
    out.write(str.getBytes());
    out.close();

    fHdfs2.mkdirs(new Path("/dest"));
  }

  @Test
  public void testFixer() throws IOException {
    String str = "testSourceFixer";
    fixerTestEnvSetup(str);

    DistributedFileSystem dfs1 =
        (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    DistributedFileSystem dfs2 =
        (DistributedFileSystem) fHdfs2.getDistributedFileSystem();

    // Source fixer case 1: reanemSrcPhase1 is called then client terminate
    DirectorySubTree sp1 =
        dfs1.renameSrcPhase1("/sp1", dfs1.getUri().toString(), "/dest/sp1",
            dfs2.getUri().toString());
    Assert.assertTrue(sp1 != null);

    // Source fixer case 2: renameDstPhase1 is called then client terminate
    DirectorySubTree dp1 =
        dfs1.renameSrcPhase1("/dp1", dfs1.getUri().toString(), "/dest/dp1",
            dfs2.getUri().toString());
    Assert.assertTrue(dp1 != null);
    BlocksToDup dp1BlksToDup =
        dfs2.renameDestPhase1("/dp1", dfs1.getUri().toString(), "/dest/dp1",
            dfs2.getUri().toString(), dp1);
    Assert.assertTrue(dp1BlksToDup != null);

    // Source fixer case 3 : renameSrcPhase2 is called then client terminate
    DirectorySubTree sp2 =
        dfs1.renameSrcPhase1("/sp2", dfs1.getUri().toString(), "/dest/sp2",
            dfs2.getUri().toString());
    Assert.assertTrue(sp2 != null);
    BlocksToDup sp2BlksToDup =
        dfs2.renameDestPhase1("/sp2", dfs1.getUri().toString(), "/dest/sp2",
            dfs2.getUri().toString(), sp2);
    Assert.assertTrue(sp2BlksToDup != null);
    FederationRenameBlockCollector frbc =
        new FederationRenameBlockCollector(sp2, sp2BlksToDup, CONF);
    frbc.linkBlocksToNewPool();
    boolean sp2Sp2Res = dfs1.renameSrcPhase2(sp2.getRenameId(), false);

    try {
      Thread.sleep(15000);
    } catch (InterruptedException ie) {
    }

    // case1
    Assert.assertTrue(dfs1.exists(new Path("/sp1")));
    Assert.assertFalse(dfs2.exists(new Path("/dest/sp1")));
    Assert.assertFalse(dfs1.renameRecordExist(sp1.getRenameId(), dfs1.getUri()
        .toString(), dfs2.getUri().toString(), true));

    // case2
    Assert.assertFalse(dfs1.exists(new Path("/dp1")));
    Assert.assertTrue(dfs2.exists(new Path("/dest/dp1/testfile")));
    InputStream in = dfs2.open(new Path("/dest/dp1/testfile"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String res = reader.readLine();
    in.close();
    Assert.assertEquals(str, res);
    Assert.assertFalse(dfs1.renameRecordExist(dp1.getRenameId(), dfs1.getUri()
        .toString(), dfs2.getUri().toString(), true));
    Assert.assertFalse(dfs2.renameRecordExist(dp1.getRenameId(), dfs1.getUri()
        .toString(), dfs2.getUri().toString(), false));

    // case3
    Assert.assertFalse(dfs1.exists(new Path("/sp2")));
    Assert.assertTrue(dfs2.exists(new Path("/dest/sp2/testfile")));
    in = dfs2.open(new Path("/dest/sp2/testfile"));
    reader = new BufferedReader(new InputStreamReader(in));
    res = reader.readLine();
    in.close();
    Assert.assertEquals(str, res);
    Assert.assertFalse(dfs1.renameRecordExist(sp2.getRenameId(), dfs1.getUri()
        .toString(), dfs2.getUri().toString(), true));
    Assert.assertFalse(dfs2.renameRecordExist(sp2.getRenameId(), dfs1.getUri()
        .toString(), dfs2.getUri().toString(), false));
  }

  @Test
  public void testRenameUnclosedFile() throws IOException {
    String str = "Federation rename : testRenameUnclosedFile";
    OutputStream out = fHdfs1.create(new Path("/testRenameUnclosedFile"));
    out.write(str.getBytes());

    try {
      DistributedFileSystem dfs1 =
          (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
      DistributedFileSystem dfs2 =
          (DistributedFileSystem) fHdfs2.getDistributedFileSystem();
      dfs1.renameSrcPhase1("/testRenameUnclosedFile", dfs1.getUri().toString(),
          "/testRenameUnclosedFile", dfs2.getUri().toString());
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe instanceof FederationRenameException);
      return;
    }
    Assert.assertTrue(false);
  }

  private void modificationTestEnvSetup(String str) throws IOException {
    fHdfs1.mkdirs(new Path("/spmodify"), null);
    OutputStream out = fHdfs1.create(new Path("/tmpfile"));
    out.write(str.getBytes());
    out.close();
    out = fHdfs1.create(new Path("/spmodify/testfile"));
    out.write(str.getBytes());
    out.close();
  }

  @Test
  public void testModifyRenamedPath() throws IOException {
    String str = "testModifyRenamePath";
    modificationTestEnvSetup(str);
    DistributedFileSystem dfs1 =
        (DistributedFileSystem) fHdfs1.getDistributedFileSystem();
    DistributedFileSystem dfs2 =
        (DistributedFileSystem) fHdfs2.getDistributedFileSystem();

    // case1: make new directory in source of renamed path
    // case2: append in source of renamed path
    // case3: create new file in source of renamed path
    DirectorySubTree ds =
        dfs1.renameSrcPhase1("/spmodify", dfs1.getUri().toString(),
            "/spmodify", dfs2.getUri().toString());
    Assert.assertTrue(ds != null);
    boolean ioexcepted = false;
    try {
      dfs1.mkdirs(new Path("/spmodify/testdir"));
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe.getMessage()
          .contains("federation rename directory"));
      ioexcepted = true;
    }
    Assert.assertTrue(ioexcepted);
    ioexcepted = false;

    try {
      dfs1.append(new Path("/spmodify/testfile"));
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe.getMessage()
          .contains("federation rename directory"));
      ioexcepted = true;
    }
    Assert.assertTrue(ioexcepted);
    ioexcepted = false;

    try {
      dfs1.create(new Path("/spmodify/newtestfile"));
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe.getMessage()
          .contains("federation rename directory"));
      ioexcepted = true;
    }
    Assert.assertTrue(ioexcepted);
    ioexcepted = false;

    // case4: make new directory in destination of renamed path
    // case5: append in destination of renamed path
    // case6: create new file in source of renamed path
    BlocksToDup blksToDup =
        dfs2.renameDestPhase1("/spmodify", dfs1.getUri().toString(),
            "/spmodify", dfs2.getUri().toString(), ds);
    Assert.assertTrue(blksToDup != null);
    try {
      dfs2.mkdirs(new Path("/spmodify/testdir"));
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe.getMessage()
          .contains("federation rename directory"));
      ioexcepted = true;
    }
    Assert.assertTrue(ioexcepted);
    ioexcepted = false;

    try {
      dfs2.append(new Path("/spmodify/testfile"));
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe.getMessage()
          .contains("federation rename directory"));
      ioexcepted = true;
    }
    Assert.assertTrue(ioexcepted);
    ioexcepted = false;

    try {
      dfs2.create(new Path("/spmodify/newtestfile"));
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      Assert.assertTrue(ioe.getMessage()
          .contains("federation rename directory"));
      ioexcepted = true;
    }
    Assert.assertTrue(ioexcepted);
    ioexcepted = false;

    // case7: make new directory in successfully renamed path
    // case8: append file in successfully renamed path
    // case9: create new file in successfully renamed path
    FederationRenameBlockCollector frbc =
        new FederationRenameBlockCollector(ds, blksToDup, CONF);
    frbc.linkBlocksToNewPool();
    boolean sp2 = dfs1.renameSrcPhase2(ds.getRenameId(), false);
    Assert.assertTrue(sp2);
    boolean dp2 =
        dfs2.renameDestPhase2(ds.getRenameId(), dfs1.getUri().toString());
    Assert.assertTrue(dp2);
    try {
      dfs2.mkdirs(new Path("/spmodify/testdir"));
    } catch (Exception e) {
      Assert.assertTrue(false);
    }

    try {
      FSDataOutputStream out = dfs2.append(new Path("/spmodify/testfile"));
      out.close();
    } catch (Exception e) {
      Assert.assertTrue(false);
    }

    try {
      FSDataOutputStream out = dfs2.create(new Path("/spmodify/newtestfile"));
      out.close();
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testRenameToDifferentDestPath() throws Exception {
    basicTestEnvSetup();
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
      FileSystemTestHelper.createFile(dfs, new Path("/home/a/b/file"));
      dfs.mkdirs(new Path("/user/c/b"));
      dfs.rename(new Path("/home/a/b"), new Path("/user/c/b"));
      Assert.assertTrue(dfs.exists(new Path("/user/c/b/b")));
      Assert.assertTrue(dfs.exists(new Path("/user/c/b/b/file")));
      Assert.assertFalse(dfs.exists(new Path("/home/a/b")));

      FileSystemTestHelper.createFile(dfs, new Path("/home/a/c/file"));
      dfs.rename(new Path("/home/a/c"), new Path("/user/c/newDir"));
      Assert.assertTrue(dfs.exists(new Path("/user/c/newDir")));
      Assert.assertTrue(dfs.exists(new Path("/user/c/newDir/file")));
      Assert.assertFalse(dfs.exists(new Path("/home/a/c")));
    } finally {
      CONF.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    }
  }

  @Test
  public void testRenameOnSameNN() throws Exception {
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    CONF.setBoolean("fs.hdfs.impl.disable.cache", true);
    try {
      Configuration config = new Configuration(CONF);
      ConfigUtil.addLink(config, "/x/y/z",
          new URI(fHdfs2.getUri().toString() + "/x/y/z"));
      ConfigUtil.addLink(config, "/x/y/x",
          new URI(fHdfs2.getUri().toString() + "/x/y/x"));
      fHdfs2.mkdirs(new Path("/x/y/z"));
      fHdfs2.mkdirs(new Path("/x/y/x"));
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(config);
      dfs.mkdirs(new Path("/x/y/z/dir/"));
      FileSystemTestHelper.createFile(dfs, new Path("/x/y/z/dir/file"));
      dfs.rename(new Path("/x/y/z/dir"), new Path("/x/y/x/dir"));
    } finally {
      CONF.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    }
  }
}
