package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import junit.framework.Assert;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFederationTrashOp {
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
    CONF.setLong(FS_TRASH_INTERVAL_KEY, 10); // 10 mins
    cluster =
        new MiniDFSCluster.Builder(CONF)
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs1 = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
    ConfigUtil.addLink(CONF, "/fs1", fHdfs1.getUri());
    ConfigUtil.addLink(CONF, "/fs2", fHdfs2.getUri());
    CONF.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    CONF.set("fs.defaultFS", "hdfs://default");
  }

  @AfterClass
  public static void tearDown() throws IOException {
    cluster.shutdown();
  }

  private static String runLsTrash(final FsShell shell, String path)
      throws Exception {
    System.out.println("path=" + path);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      shell.run(new String[] { "-ls", path });
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("Ls results:\n" + "<results>\n " + results
        + "</results>");
    return results;
  }

  private static String runDu(final FsShell shell, String path)
      throws Exception {
    System.out.println("path=" + path);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      shell.run(new String[] { "-du", path });
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("Du results:\n" + "<results>\n " + results
        + "</results>");
    return results;
  }

  private static String runRestoreTrash(final FsShell shell, String path)
      throws Exception {
    System.out.println("Path = " + path);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      shell.run(new String[] { "-restoreTrash", path });
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("RestoreTrash results:\n" + "<results>\n " + results
        + "</results>");
    return results;
  }

  private static String runDeleteTrash(final FsShell shell, String path)
    throws Exception {
    System.out.println("Path = " + path);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      shell.run(new String[] { "-rmTrash", path });
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("DeleteTrash results:\n" + "<results>\n " + results
      + "</results>");
    return results;
  }

  @Test
  public void testBasicTrashOp() throws Exception {
    // Step 1: create dir and files in different fs via FederatedDFSFileSystem's
    // interfaces
    String str = "testbasictrashop";
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
    Assert.assertTrue(dfs instanceof DistributedFileSystem);
    Assert.assertTrue(dfs instanceof FederatedDFSFileSystem);
    dfs.mkdirs(new Path("/fs1/dir1"), null);
    OutputStream out = dfs.create(new Path("/fs1/dir1/testfile"));
    out.write(str.getBytes());
    out.close();
    dfs.mkdirs(new Path("/fs2/dir2"), null);
    out = dfs.create(new Path("/fs2/dir2/testfile"));
    out.write(str.getBytes());
    out.close();
    Assert.assertTrue(fHdfs1.exists(new Path("/dir1/testfile")));
    Assert.assertTrue(fHdfs2.exists(new Path("/dir2/testfile")));

    // Step 2: rm these dirs via FederatedDFSFileSystem's interfaces and verify
    // they are in their own trash
    String testFile1 = "/fs1/dir1";
    String testFile2 = "/fs2/dir2";
    FsShell shell = new FsShell(CONF);
    String[] argv = new String[] { "-rm", "-r", testFile1 };
    int res = ToolRunner.run(shell, argv);
    assertEquals("rm failed", 0, res);
    argv = new String[] { "-rm", "-r", testFile2 };
    res = ToolRunner.run(shell, argv);
    assertEquals("rm failed", 0, res);
    String trashFile1 =
        "/user/" + System.getProperty("user.name") + "/.Trash/Current/"
            + "dir1";
    String trashFile2 =
        "/user/" + System.getProperty("user.name") + "/.Trash/Current/"
            + "dir2";
    System.out
        .println("trashFile1 " + trashFile1 + " trashFile2 " + trashFile2);

    // Step 3: verify that ls and du works
    Assert.assertTrue(fHdfs1.exists(new Path(trashFile1)));
    Assert.assertTrue(fHdfs2.exists(new Path(trashFile2)));
    String trash1LsRes = runLsTrash(shell, trashFile1);
    Assert.assertTrue(trash1LsRes.contains(trashFile1));
    Assert.assertFalse(trash1LsRes.contains("No such file or directory"));
    String trash2LsRes = runLsTrash(shell, trashFile2);
    Assert.assertTrue(trash2LsRes.contains(trashFile2));
    Assert.assertFalse(trash2LsRes.contains("No such file or directory"));
    String trashPath = "/user/" + System.getProperty("user.name") + "/.Trash";
    String trashRes = trashRes = runLsTrash(shell, trashPath);
    Assert.assertTrue(trashRes.contains("Found 1 items"));
    Assert.assertFalse(trash2LsRes.contains("No such file or directory"));
    String trashCurPath =
        "/user/" + System.getProperty("user.name") + "/.Trash/Current";
    trashRes = trashRes = runLsTrash(shell, trashCurPath);
    Assert.assertTrue(trashRes.contains(trashFile1));
    Assert.assertTrue(trashRes.contains(trashFile2));
    // TBD: Verify du res
    String trash1Du = runDu(shell, trashFile1);
    String trash2Du = runDu(shell, trashFile2);
    String trashDu = runDu(shell, trashPath);
    String trashCurDu = runDu(shell, trashCurPath);
    Assert.assertTrue(trash1Du.contains(trashFile1));
    Assert.assertTrue(trash2Du.contains(trashFile2));

    // Step 4: restore those files and verify their sizes are correct
    runRestoreTrash(shell, trashFile1);
    runRestoreTrash(shell, trashFile2);
    Assert.assertTrue(fHdfs1.exists(new Path("/dir1")));
    Assert.assertTrue(fHdfs2.exists(new Path("/dir2")));
    Assert.assertTrue(fHdfs1.exists(new Path("/dir1/testfile")));
    Assert.assertTrue(fHdfs2.exists(new Path("/dir2/testfile")));
    runDu(shell, "/fs1/dir1");
    runDu(shell, "/fs2/dir2");

    // Step 5: move one file to trash and delete it from trash
    String oneFile1 = testFile1 + "/testfile";
    String oineTrashFile1 = trashFile1 + "/testfile";
    argv = new String[] { "-rm", "-r", oneFile1};
    res = ToolRunner.run(shell, argv);
    assertFalse(fHdfs1.exists(new Path(oneFile1)));
    assertTrue(fHdfs1.exists(new Path(oineTrashFile1)));

    runDeleteTrash(shell, oineTrashFile1);
    assertFalse(fHdfs1.exists(new Path(oineTrashFile1)));

    // Step 6: delete trasn with *
    argv = new String[] { "-rm", "-r", testFile1};
    res = ToolRunner.run(shell, argv);
    argv = new String[] { "-rm", "-r", testFile2};
    res = ToolRunner.run(shell, argv);

    assertTrue(fHdfs1.exists(new Path(trashFile1)));
    assertTrue(fHdfs2.exists(new Path(trashFile2)));
    String trashAllFiles = "/user/" + System.getProperty("user.name") + "/.Trash/Current/*";
    runDeleteTrash(shell, trashAllFiles);
    assertFalse(fHdfs1.exists(new Path(trashFile1)));
    assertFalse(fHdfs2.exists(new Path(trashFile2)));
  }

  @Test
  public void testDeleteTrashNegative () throws Exception {
    FsShell shell = new FsShell(CONF);
    Path fileNO = new Path("/fs1/nothisfile");
    String msg = runDeleteTrash(shell, fileNO.toString());
    assertTrue(msg.contains("No such file or directory"));

    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(CONF);
    Path file1 = new Path("/fs1/f1");
    DFSTestUtil.createFile(dfs, file1, 512, (short)1, 0);
    msg = runDeleteTrash(shell, file1.toString());
    assertTrue(msg.contains("Input path is not in any trash"));

    Path file2 = new Path("/fs1/.Trash/f2");
    DFSTestUtil.createFile(dfs, file2, 512, (short)1, 0);
    msg = runDeleteTrash(shell, file2.toString());
    assertTrue(msg.contains("Input path is not in any trash"));
  }
}