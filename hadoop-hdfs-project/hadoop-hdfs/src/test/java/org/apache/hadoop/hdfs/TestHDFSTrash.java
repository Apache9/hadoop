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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Test trash using HDFS
 */
public class TestHDFSTrash {
  private static MiniDFSCluster cluster = null;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.shell.delete.checkpath", "/user/*/:/tmp");
    conf.set("fs.trash.interval", "5");
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
}
