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

package org.apache.hadoop.tools;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;

/**
 * A JUnit test for copying files recursively.
 */

public class TestDistCpSystem extends TestCase {
  
  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";
  
  private class FileEntry {
    String path;
    boolean isDir;
    public FileEntry(String path, boolean isDir) {
      this.path = path;
      this.isDir = isDir;
    }
    String getPath() { return path; }
    boolean isDirectory() { return isDir; }
  }
  
  private void createFiles(FileSystem fs, String topdir,
      FileEntry[] entries) throws IOException {
    for (FileEntry entry : entries) {
      Path newpath = new Path(topdir + "/" + entry.getPath());
      if (entry.isDirectory()) {
        fs.mkdirs(newpath);
      } else {
        OutputStream out = fs.create(newpath);
        try {
          out.write((topdir + "/" + entry).getBytes());
          out.write("\n".getBytes());
        } finally {
          out.close();
        }
      }
    }
  }
   
  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, FileEntry[] files) throws IOException {
      Path root = new Path(topdir);
      List<FileStatus> statuses = new ArrayList<FileStatus>();
      
      for (int idx = 0; idx < files.length; ++idx) {
        Path newpath = new Path(root, files[idx].getPath());
        statuses.add(fs.getFileStatus(newpath));
      }
      return statuses.toArray(new FileStatus[statuses.size()]);
    }
  

  /** delete directory and everything underneath it.*/
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }
   
  private void testPreserveUserHelper(
      FileEntry[] srcEntries,
      FileEntry[] dstEntries,
      boolean createSrcDir,
      boolean createTgtDir,
      boolean update) throws Exception {
    Configuration conf = null;
    MiniDFSCluster cluster = null;
    try {
      final String testRoot = "/testdir";
      final String testSrcRel = SRCDAT;
      final String testSrc = testRoot + "/" + testSrcRel;
      final String testDstRel = DSTDAT;
      final String testDst = testRoot + "/" + testDstRel;

      conf = new Configuration(); 
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
      fs.mkdirs(new Path(testRoot));
      if (createSrcDir) {
        fs.mkdirs(new Path(testSrc));
      }
      if (createTgtDir) {
        fs.mkdirs(new Path(testDst));
      }
      
      createFiles(fs, testRoot, srcEntries);
      FileStatus[] srcstats = getFileStatus(fs, testRoot, srcEntries);
      for(int i = 0; i < srcEntries.length; i++) {
        fs.setOwner(srcstats[i].getPath(), "u" + i, null);
      }  
      String[] args = update? new String[]{"-pu", "-update", nnUri+testSrc,
          nnUri+testDst} : new String[]{"-pu", nnUri+testSrc, nnUri+testDst};
            
      ToolRunner.run(conf, new DistCp(), args);
      
      String realTgtPath = testDst;
      if (!createTgtDir) {
        realTgtPath = testRoot;
      }
      FileStatus[] dststat = getFileStatus(fs, realTgtPath, dstEntries);
      for(int i = 0; i < dststat.length; i++) {
        assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
      }
      deldir(fs, testRoot);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  private MiniDFSCluster setupNewDFSCluster(Configuration conf) throws IOException {
    Configuration tmpConf = new Configuration(conf);
    File baseDir = new File("./target/test-dir-"
            + UUID.randomUUID().toString().substring(0, 4) + "/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    tmpConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(tmpConf).numDataNodes(3)
            .format(true).build();
    cluster.waitClusterUp();
    return cluster;
  }

  public void testTargetParentMirror() throws Exception {
    FileEntry[] srcfiles = { new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/a", false), new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false) };
    FileEntry[] dstfiles = { new FileEntry(DSTDAT, true),
        new FileEntry(DSTDAT + "/a", false), new FileEntry(DSTDAT + "/b", true),
        new FileEntry(DSTDAT + "/b/c", false) };

    final String testSrc = "/testdir/";
    MiniDFSCluster cluster1 = null;
    MiniDFSCluster cluster2 = null;
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.namenode.acls.enabled", "true");
      cluster1 = setupNewDFSCluster(conf);
      cluster2 = setupNewDFSCluster(conf);

      FileSystem srcFs = cluster1.getFileSystem();
      FileSystem dstFs = cluster2.getFileSystem();
      srcFs.mkdirs(new Path(testSrc));
      createFiles(srcFs, testSrc, srcfiles);

      String owner = "u_li";
      String group = "g_li";
      FsPermission perm = new FsPermission((short) 00777);
      List<AclEntry> aclEntries = srcFs.getAclStatus(new Path(testSrc)).getEntries();
      aclEntries.add(AclEntry.parseAclEntry("user:u_wei:rwx", true));

      srcFs.mkdirs(new Path(testSrc));
      srcFs.setOwner(new Path(testSrc), "u_li", "g_li");
      srcFs.setPermission(new Path(testSrc), new FsPermission((short) 00777));
      srcFs.modifyAclEntries(new Path(testSrc), aclEntries);
      srcFs.setOwner(new Path(testSrc+SRCDAT), "u_li", "g_li");
      srcFs.setPermission(new Path(testSrc+SRCDAT), new FsPermission((short) 00777));
      srcFs.modifyAclEntries(new Path(testSrc+SRCDAT), aclEntries);

      // test copy with update, parent should be /testdir and /testdir/dstdat
      String[] args = new String[] { "-Parent", "mirror", "-update", srcFs.getUri()+""+testSrc + SRCDAT,
              dstFs.getUri()+testSrc + SRCDAT };
      ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(dstFs,testSrc,owner,group,perm,aclEntries,true);
      testDirPreserve(dstFs,testSrc+SRCDAT,owner,group,perm,aclEntries,true);
      isDistCpSuccess(dstFs,testSrc,srcfiles);

      // test copy to a non-exited target path, parent should be /testdir
      dstFs.delete(new Path(testSrc),true);
      args = new String[] { "-Parent", "mirror", srcFs.getUri()+""+testSrc + SRCDAT,
          dstFs.getUri()+testSrc + DSTDAT };
      ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(dstFs,testSrc,owner,group,perm,aclEntries,true);
      testDirPreserve(dstFs,testSrc+DSTDAT,owner,group,perm,aclEntries,false);
      isDistCpSuccess(dstFs,testSrc,dstfiles);

      // test copy to an existed target path, parent should be /testdir
      dstFs.delete(new Path(testSrc),true);
      dstFs.mkdirs(new Path(testSrc));
      args = new String[] { "-Parent", "mirror", srcFs.getUri()+""+testSrc + SRCDAT,
              dstFs.getUri()+testSrc };
      ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(dstFs,testSrc,owner,group,perm,aclEntries,false);
      testDirPreserve(dstFs,testSrc+SRCDAT,owner,group,perm,aclEntries,false);
      isDistCpSuccess(dstFs,testSrc,srcfiles);
    } finally {
      if (cluster1 != null) {
        cluster1.shutdown();
      }
      if (cluster2 != null) {
        cluster2.shutdown();
      }
    }
  }

  private void testDirPreserve(FileSystem dstFs, String parent, String owner,
      String group, FsPermission perm, List<AclEntry> entries, boolean hopeTrue) throws IOException {
    FileStatus status = dstFs.getFileStatus(new Path(parent));
    Set<AclEntry> set = new HashSet<AclEntry>(dstFs.getAclStatus(new Path(parent)).getEntries());
    if (hopeTrue) {
      assertTrue(status.getOwner().equals(owner));
      assertTrue(status.getGroup().equals(group));
      assertTrue(status.getPermission().equals(perm));
      assertTrue(set.containsAll(entries));
    } else {
      assertFalse(status.getPermission().equals(perm));
      assertFalse(set.containsAll(entries));
    }
  }

  private void isDistCpSuccess(FileSystem fs, String topdir,
      FileEntry[] entries) throws IOException {
    for (FileEntry entry : entries) {
      Path newpath = new Path(topdir + "/" + entry.getPath());
      if (entry.isDirectory()) {
        assertTrue(fs.getFileStatus(newpath).isDirectory());
      } else {
        assertTrue(fs.getFileStatus(newpath).isFile());
      }
    }
  }

  public void testTargetParentSpecify() throws Exception {
    FileEntry[] srcfiles = { new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/a", false), new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false) };

    final String testSrc = "/testdir/";
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.namenode.acls.enabled", "true");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
      fs.mkdirs(new Path(testSrc));
      createFiles(fs, testSrc, srcfiles);

      String owner = "u_li";
      String group = "g_li";
      FsPermission perm = new FsPermission((short) 511);//511==00777
      List<AclEntry> aclEntries = fs.getAclStatus(new Path(testSrc)).getEntries();
      aclEntries.add(AclEntry.parseAclEntry("user:u_wei:rwx", true));

      // test copy with update, parent should be /testdir，/testdir/aha，/testdir/aha/srcdat
      fs.delete(new Path(testSrc+"aha"),true);
      String[] args = new String[] { "-Parent",
          "owner=u_li,group=g_li,permission=511,acl=user:u_wei:rwx,acl=user:u_lun:rwx",
          "-update", "/testdir/" + SRCDAT, testSrc + "aha/" + SRCDAT };
      ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(fs,testSrc,owner,group,perm,aclEntries,false);// doesn't modify for existed parent
      testDirPreserve(fs,testSrc+"aha/",owner,group,perm,aclEntries,true);
      testDirPreserve(fs,testSrc+"aha/"+SRCDAT,owner,group,perm,aclEntries,true);
      isDistCpSuccess(fs,testSrc+"aha",srcfiles);

      // test copy to a non-exited target path, parent should be /testdir,/testdir/aha
      fs.delete(new Path(testSrc+"aha"),true);
      args = new String[] { "-Parent",
          "owner=u_li,group=g_li,permission=511,acl=user:u_wei:rwx,acl=user:u_lun:rwx",
          "/testdir/" + SRCDAT, testSrc + "aha/" + SRCDAT };
      ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(fs,testSrc,owner,group,perm,aclEntries,false);
      testDirPreserve(fs,testSrc+"aha/",owner,group,perm,aclEntries,true);
      testDirPreserve(fs,testSrc+"aha/"+SRCDAT,owner,group,perm,aclEntries,false);
      isDistCpSuccess(fs,testSrc+"aha/",srcfiles);

      // test copy to an existed target path, parent should be /testdir,/testdir/dstdat
      fs.delete(new Path(testSrc+"aha"),true);
      fs.mkdirs(new Path(testSrc+DSTDAT));
      args = new String[] { "-Parent",
          "owner=u_li,group=g_li,permission=511,acl=user:u_wei:rwx,acl=user:u_lun:rwx",
          "/testdir/" + SRCDAT, testSrc + DSTDAT };
      ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(fs,testSrc,owner,group,perm,aclEntries,false);
      testDirPreserve(fs,testSrc+DSTDAT,owner,group,perm,aclEntries,false);
      testDirPreserve(fs,testSrc+DSTDAT+"/"+SRCDAT,owner,group,perm,aclEntries,false);
      isDistCpSuccess(fs,testSrc+DSTDAT,srcfiles);

      // test multi src paths, parent should be /testdir,/testdir/dstdat
      fs.delete(new Path(testSrc+DSTDAT),true);
      fs.mkdirs(new Path(testSrc+"src2/aha"));
      args = new String[] { "-Parent",
              "owner=u_li,group=g_li,permission=511,acl=user:u_wei:rwx,acl=user:u_lun:rwx",
              "/testdir/" + SRCDAT, testSrc+"src2", testSrc + DSTDAT };
            ToolRunner.run(conf, new DistCp(), args);
      testDirPreserve(fs,testSrc,owner,group,perm,aclEntries,false);
      testDirPreserve(fs,testSrc+DSTDAT,owner,group,perm,aclEntries,true);
      testDirPreserve(fs,testSrc+DSTDAT+"/"+SRCDAT,owner,group,perm,aclEntries,false);
      testDirPreserve(fs,testSrc+DSTDAT+"/src2/aha",owner,group,perm,aclEntries,false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testTargetParentCreate() throws Exception {
    final String testSrc = "/testdir/";
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.namenode.acls.enabled", "true");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      DistCp distCp = new DistCp();
      distCp.setConf(conf);
      Field inputOptions = DistCp.class.getDeclaredField("inputOptions");
      Method createParentTargetPath = DistCp.class.getDeclaredMethod("createParentTargetPath");
      inputOptions.setAccessible(true);
      createParentTargetPath.setAccessible(true);
      // test copy with update, parent should be /testdir，/testdir/aha，/testdir/aha/srcdat
      fs.delete(new Path(testSrc + "aha"), true);
      String[] args = new String[] { "-Parent", "create", "-update",
          "/testdir/" + SRCDAT, testSrc + "aha/" + SRCDAT };
      inputOptions.set(distCp,(OptionsParser.parse(args)));
      createParentTargetPath.invoke(distCp);
      assertTrue(fs.exists(new Path(testSrc+"aha/"+SRCDAT)));

      // test copy to a non-exited target path, parent should be /testdir,/testdir/aha
      fs.delete(new Path(testSrc + "aha"), true);
      args = new String[] { "-Parent", "create", "/testdir/" + SRCDAT,
          testSrc + "aha/" + SRCDAT };
      inputOptions.set(distCp,(OptionsParser.parse(args)));
      createParentTargetPath.invoke(distCp);
      assertTrue(fs.exists(new Path(testSrc+"aha/")));
      assertFalse(fs.exists(new Path(testSrc+"aha/"+SRCDAT)));

      // test copy to an existed target path, parent should be /testdir,/testdir/dstdat
      // do nothing

      // test multi src paths target paths, parent should be /testdir,/testdir/dstdat
      fs.delete(new Path(testSrc + DSTDAT), true);
      args = new String[] { "-Parent", "create", "/testdir/" + SRCDAT,
          testSrc + "src2", testSrc + DSTDAT };
      inputOptions.set(distCp,(OptionsParser.parse(args)));
      createParentTargetPath.invoke(distCp);
      assertTrue(fs.exists(new Path(testSrc+DSTDAT)));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testPreserveUseNonEmptyDir() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };

    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true),
        new FileEntry(DSTDAT + "/a", false),
        new FileEntry(DSTDAT + "/b", true),
        new FileEntry(DSTDAT + "/b/c", false)
    };

    testPreserveUserHelper(srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(srcfiles, dstfiles, false, false, false);
  }
  
 
  public void testPreserveUserEmptyDir() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true)
    };
    
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true)
    };
    
    testPreserveUserHelper(srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(srcfiles, dstfiles, false, false, false);
  }

  public void testPreserveUserSingleFile() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, false)
    };
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, false)
    };
    testPreserveUserHelper(srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(srcfiles, dstfiles, false, false, false);
  }
  
  public void testPreserveUserNonEmptyDirWithUpdate() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };

    FileEntry[] dstfiles = {
        new FileEntry("a", false),
        new FileEntry("b", true),
        new FileEntry("b/c", false)
    };

    testPreserveUserHelper(srcfiles, dstfiles, true, true, true);
  }

}