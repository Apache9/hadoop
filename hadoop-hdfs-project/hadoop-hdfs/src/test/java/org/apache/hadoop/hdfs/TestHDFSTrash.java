/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.getRemoteUser;
import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_TRASH_CHECKPOINT_INTERVAL_KEY;


/**
 * Test trash using HDFS
 */
public class TestHDFSTrash {
  private static MiniDFSCluster cluster = null;
  private static final int MSECS_PER_MINUTE = 60 * 1000;
  private static final long threadSleepTime = 3000;// sleep time 3seconds

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.shell.delete.checkpath", "/user/*/:/tmp");
    conf.set(FS_TRASH_INTERVAL_KEY, "2"); // default trashttl 120 seconds
    conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // emptier interval 6 seconds
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) { cluster.shutdown(); }
  }

  @Test
  public void testTrash() throws IOException {
    TestTrash.trashShell(cluster.getFileSystem(), new Path("/"));
  }

  @Test
  public void testNonDefaultFS() throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, fs.getUri().toString());
    TestTrash.trashNonDefaultFS(conf);
  }
  public String rmShell(String cmd, Path path, FileSystem fs, Configuration conf)
          throws IOException {
    FsShell shell = new FsShell();
    shell.setConf(conf);
    PrintStream stdout = System.out;
    PrintStream stderr = System.err;
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    PrintStream newOut = new PrintStream(byteStream);
    System.setOut(newOut);
    System.setErr(newOut);
    String[] args = new String[2];
    args[0] = cmd;
    args[1] = path.toString();
    try {
      shell.run(args);
    } catch (Exception e) {
      System.err.println(
              "Exception raised from Trash.run " + e.getLocalizedMessage());
    }
    String output = byteStream.toString();
    System.setOut(stdout);
    System.setErr(stderr);
    return output;
  }

  public Path mkFile(Path file, FileSystem fs) throws IOException {
    final long seed = 0xDEADBEEFL;
    FSDataOutputStream stm = fs.create(file);
    byte[] buffer = new byte[10];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    return file;
  }
  @Test
  public void testProhibitDeleteByShell() throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    Path base = new Path("/");

    // test rmr /user/test
    {
      String cmd = "-rmr";
      Path myPath = new Path(base, "user/test");
      assertTrue(fs.mkdirs(myPath));
      String output = rmShell(cmd, myPath, fs, conf);
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // test rm /user/myFile
    {
      String cmd = "-rm";
      Path myFile = new Path(base, "user/myFile");
      myFile = mkFile(myFile, fs);
      String output = rmShell(cmd, myFile, fs, conf);
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // test rmr /user
    {
      String cmd = "-rmr";
      Path myPath = new Path(base, "user");
      assertTrue(fs.exists(myPath));
      String output = rmShell(cmd, myPath, fs, conf);
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("as it contains the trash") != -1);

    }
    // test rmr /tmp/test
    {
      String cmd = "-rmr";
      Path myPath = new Path(base, "tmp/test");
      assertTrue(fs.mkdirs(myPath));
      String output = rmShell(cmd, myPath, fs, conf);
      assertTrue("Delete Successful", output.indexOf("to trash at") != -1);
    }
    // test rm /tmp/myFile
    {
      String cmd = "-rm";
      Path myFile = new Path(base, "tmp/myFile");
      myFile = mkFile(myFile, fs);
      String output = rmShell(cmd, myFile, fs, conf);
      assertTrue("Delete Successful", output.indexOf("to trash at") != -1);
    }
    // test rmr /tmp
    {
      String cmd = "-rmr";
      Path myPath = new Path(base, "tmp");
      assertTrue(fs.mkdirs(myPath));
      String output = rmShell(cmd, myPath, fs, conf);
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // test rmr /
    {
      String cmd = "-rmr";
      Path myPath = base;
      String output = rmShell(cmd, myPath, fs, conf);
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("as it contains the trash") != -1);
    }

  }

  @Test
  public void testProhibitDeleteByMethod() throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();

    // rename
    String trashPath = "/user/huanghaibin/.Trash/Current";
    // rename /user to trash
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user")));
        fs.rename(new Path("/user"), new Path(trashPath, "user"));

      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // rename /user/dir1 to trash
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user/dir1")));
        fs.rename(new Path("/user/dir1"), new Path(trashPath,"user/dir2"));
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // rename /tmp to trash
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/tmp")));
        fs.rename(new Path("/tmp"), new Path(trashPath,"tmp"));
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // rename /user/user1 to /user/user2
    {
      assertTrue(fs.mkdirs(new Path("/user/user1")));
      assertTrue(fs.rename(new Path("/user/user1"), new Path("/user/user2")));
    }

    // delete

    // delete /user
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user")));
        fs.delete(new Path("/user"), true);
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // delete /user -skipTrash
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user")));
        fs.delete(new Path("/user"), true, true);
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // delete /user/dir2
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user/dir2")));
        fs.delete(new Path("/user/dir2"), true);
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // delete /user/dir2 -skipTrash
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user/dir2")));
        fs.delete(new Path("/user/dir2"), true, true);
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // delete /tmp
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/tmp")));
        fs.delete(new Path("/tmp"), true);
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // delete /tmp -skipTrash
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/tmp")));
        fs.delete(new Path("/tmp"), true, true);
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // delete /tmp/dir2
    {
      assertTrue(fs.mkdirs(new Path("/tmp/dir2")));
      assertTrue(fs.delete(new Path("/tmp/dir2"), true));
    }
    // delete /tmp/dir3 -skipTrash
    {
      assertTrue(fs.mkdirs(new Path("/tmp/dir3")));
      assertTrue(fs.delete(new Path("/tmp/dir3"), true, true));
    }

    // moveToTrash

    // moveToTrash /user
    Trash trash = new Trash(fs, conf);
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user")));
        trash.moveToTrash(new Path("/user"));
      } catch (Exception e) {
        output = e.toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("as it contains the trash") != -1);
    }
    // moveToTrash /user/dir3
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/user/dir3")));
        trash.moveToTrash(new Path("/user/dir3"));
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // moveToTrash /tmp
    {
      String output = null;
      try {
        assertTrue(fs.mkdirs(new Path("/tmp")));
        trash.moveToTrash(new Path("/tmp"));
      } catch (Exception e) {
        output = e.getCause().toString();
      }
      assertTrue("Test Prohibit Delete Successful",
          output.indexOf("Permission denied") != -1);
    }
    // moveToTrash /tmp/dir4
    {
      assertTrue(fs.mkdirs(new Path("/tmp/dir4")));
      assertTrue(trash.moveToTrash(new Path("/tmp/dir4")));
    }
  }

  @Test
  public void testExistingFileTrash() throws IOException {
    /**
     *  case:
     *  1.create /tmp/a
     *  2.moveToTrash /tmp/a
     *  3.create /tmp/a/b
     *  4.moveToTrash /tmp/a/b
     */
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    Trash trash = new Trash(fs, conf);
    Path path1 = new Path("/tmp/a");
    fs.create(path1);
    assertTrue(trash.moveToTrash(path1));

    Path path2 = new Path("/tmp/a/b");
    fs.create(path2);
    assertTrue(trash.moveToTrash(path2));
  }

  public String runShell(String args[]) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    FsShell shell = new FsShell();
    shell.setConf(conf);
    PrintStream stdout = System.out;
    PrintStream stderr = System.err;
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    PrintStream newOut = new PrintStream(byteStream);
    System.setOut(newOut);
    System.setErr(newOut);
    try {
      shell.run(args);
    } catch (Exception e) {
      System.err.println(
          "Exception raised from Trash.run " + e.getLocalizedMessage());
    }
    String output = byteStream.toString();
    System.setOut(stdout);
    System.setErr(stderr);
    return output;
  }

  @Test
  public void testTrashPathAndTrashTTL() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    FsShell shell = new FsShell();
    shell.setConf(conf);

    shell.run(new String[] { "-mkdir", "-p", "/user/testUser" });
    shell.run(new String[] { "-chown", "testUser", "/user/testUser" });

    UserGroupInformation ugi = UserGroupInformation
        .createUserForTesting("testUser", new String[] { "testUser" });
    UserGroupInformation.setLoginUser(ugi);

    // ex1
    {
      shell.run(new String[] { "-mkdir", "/user/testUser/dir1" });
      shell.run(new String[] { "-rmr", "/user/testUser/dir1" });
      String trash = "/user/" + getRemoteUser().getShortUserName()
          + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
          + "/dir1";
      assertTrue(fs.exists(new Path(trash)));
      shell.run(new String[] { "-rmr", "/user/testUser/.Trash" });
      assertTrue(!fs.exists(new Path(trash)));
    }

    // ex2
    {
      shell.run(new String[] { "-mkdir", "/user/testUser/dir2" });
      shell.run(new String[] { "-rmr", "-skipTrash", "/user/testUser/dir2" });
      String trash = "/user/" + getRemoteUser().getShortUserName()
          + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
          + "/dir2";
      assertTrue(!fs.exists(new Path(trash)));
    }

    // ex3
    {
      shell.run(new String[] { "-mkdir", "-p", "/user/testUser/dir3" });
      shell.run(new String[] { "-setfattr", "-n", "user.istrashpath", "-v",
          "01", "/user/testUser/dir3" });
      shell.run(new String[] { "-rmr", "-skipTrash", "/user/testUser/dir3" });
      String trash = "/user/" + getRemoteUser().getShortUserName()
          + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
          + "/dir3";
      assertTrue(fs.exists(new Path(trash)));
      shell.run(new String[] { "-rmr", "/user/testUser/.Trash" });
      assertTrue(!fs.exists(new Path(trash)));
    }

    // ex4
    {
      shell.run(new String[] { "-mkdir", "-p", "/user/testUser/dir4/sub" });
      shell.run(new String[] { "-setfattr", "-n", "user.istrashpath", "-v",
          "01", "/user/testUser/dir4" });
      shell.run(
          new String[] { "-rmr", "-skipTrash", "/user/testUser/dir4/sub" });
      String trash = "/user/" + getRemoteUser().getShortUserName()
          + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
          + "/dir4/sub";
      assertTrue(fs.exists(new Path(trash)));
      shell.run(new String[] { "-rmr", "/user/testUser/.Trash" });
      assertTrue(!fs.exists(new Path(trash)));
    }

    // ex5
    {
      shell.run(new String[] { "-mkdir", "/user/testUser/dir5" });
      shell.run(new String[] { "-setfattr", "-n", "user.istrashpath", "-v",
          "01", "/user/testUser" });
      shell.run(new String[] { "-rmr", "-skipTrash", "/user/testUser/dir5" });
      String trash = "/user/" + getRemoteUser().getShortUserName()
          + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
          + "/dir5";
      assertTrue(fs.exists(new Path(trash)));
      String msg = runShell(new String[] { "-rmr", "/user/testUser/.Trash" });
      assertTrue(
          msg.indexOf("Only super user could delete the trash file") != -1);
    }

    // ex6
    {
      shell.run(new String[] { "-mkdir", "-p", "/user/testUser/dir6/sub" });
      shell.run(new String[] { "-setfattr", "-n", "user.istrashpath", "-v",
          "01", "/user/testUser" });
      shell.run(new String[] { "-setfattr", "-n", "user.istrashpath", "-v",
          "01", "/user/testUser/dir6" });
      shell.run(
          new String[] { "-rmr", "-skipTrash", "/user/testUser/dir6/sub" });
      String trash = "/user/" + getRemoteUser().getShortUserName()
          + "/.Trash/Current/user/" + getRemoteUser().getShortUserName()
          + "/dir6/sub";
      assertTrue(fs.exists(new Path(trash)));
      String msg = runShell(new String[] { "-rmr", "/user/testUser/.Trash" });
      assertTrue(
          msg.indexOf("Only super user could delete the trash file") != -1);
    }

    //trashTTL
    Trash trash = new Trash(conf);

    // Start Emptier in background
    Runnable emptier = trash.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();
    //ex1
    {
      shell.run(new String[] { "-mkdir", "/user/testUser/dir1" });
      shell.run(new String[] { "-setfattr", "-n", "user.trashttl", "-v", "1",
              "/user/testUser" }); //set trashttl 1min
      shell.run(new String[] { "-rmr", "/user/testUser/dir1" });
      String trashPath =
              "/user/" + getRemoteUser().getShortUserName() + "/.Trash";
      assertTrue(verifyTrashPath(fs,trashPath));
    }

    //ex2
    {
      shell.run(new String[] { "-mkdir", "/user/testUser/dir2" });
      shell.run(new String[] { "-setfattr", "-n", "user.trashttl", "-v", "1",
              "/user/testUser" }); //set trashttl 1min
      shell.run(new String[] { "-setfattr", "-n", "user.istrashpath", "-v",
              "01", "/user/testUser" }); //open soft delete
      shell.run(new String[] { "-rmr", "/user/testUser/dir2" });

      String trashPath =
              "/user/" + getRemoteUser().getShortUserName() + "/.Trash";
      assertTrue(verifyTrashPath(fs,trashPath));
    }

  }

  boolean verifyTrashPath(FileSystem fs, String trashPath)
      throws IOException, InterruptedException {
    boolean res = false;
    Configuration conf = fs.getConf();
    long emptierInterval =
        (long) (conf.getFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY,
            FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT) * MSECS_PER_MINUTE);
    assertTrue(emptierInterval != 0);
    assertTrue(fs.listStatus(new Path(trashPath)).length == 1);
    byte[] value =
        fs.getXAttr(new Path("/user/testUser"), new String("user.trashttl"));
    String s = new String(value);
    long trashttl = Long.parseLong(s) * MSECS_PER_MINUTE;
    assertTrue(trashttl == 60000);
    long now = Time.now();
    long span = 0;
    while (true) {
      if (fs.listStatus(new Path(trashPath)).length == 0) {
        span = Time.now() - now;
        break;
      }
      Thread.sleep(threadSleepTime);
    }
    if (span >= trashttl
        && span <= trashttl + emptierInterval + threadSleepTime) {
      res = true;
    }
    return res;
  }
}
