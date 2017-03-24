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
package org.apache.hadoop.fs.viewfs;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestMergedInodeTreeResolve {
  private InodeTree<FileSystem> fsState;
  private URI myUri;
  private UserGroupInformation ugi;
  private String authority = "default";
  private FileSystem fsTarget;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fsTarget = FileSystem.getLocal(new Configuration());
    myUri = new URI(FsConstants.VIEWFS_SCHEME, authority, "/", null, null);
    ugi = UserGroupInformation.getCurrentUser();

    addNestedMountPoints(conf);
    fsState = getFsState(conf);
  }

  public InodeTree<FileSystem> getFsState(final Configuration config) throws
    URISyntaxException, IOException{
    return new MergedInodeTree<FileSystem>(config, authority) {

      @Override
      protected FileSystem getTargetFileSystem(final URI uri)
          throws URISyntaxException, IOException {
        return new ChRootedFileSystem(uri, config);
      }

      @Override
      protected FileSystem getTargetFileSystem(final AbstractINodeDir<FileSystem> dir) {
        return new ViewFileSystem.InternalDirOfViewFs(dir, Time.now(), ugi,
            myUri);
      }

      @Override
      protected FileSystem getTargetFileSystem(URI[] mergeFsURIList)
          throws URISyntaxException, UnsupportedFileSystemException {
        throw new UnsupportedFileSystemException("mergefs not implemented");
        // return MergeFs.createMergeFs(mergeFsURIList, config);
      }
    };
  }
  
  public void addNestedMountPoints(Configuration config) {
    ConfigUtil.addLink(config, "/mergedBase",
        new Path("mergedBase").toUri());
    ConfigUtil.addLink(config, "/mergedBase/subdir1",
        new Path("mergedBase/subdir1").toUri());
    ConfigUtil.addLink(config, "/mergedBase/subdir1/subdir2",
        new Path( "mergedBase/subdir1/subdir2").toUri());
    ConfigUtil.addLink(config, "/long/long/dir",
            new Path("/long/long/dir").toUri());
  }

  @Test
  public void testResolveWithRootInMountTable() throws Exception {
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, "/", new Path("/").toUri());
    fsState = getFsState(config);

    InodeTree.ResolveResult<FileSystem> res = fsState.resolve("/NotExistDir", true);
    Assert.assertEquals(res.resolvedPath, "/");
    res = fsState.resolve("/NotExistDir", false);
    Assert.assertEquals(res.resolvedPath, "/");
    res = fsState.resolve("/mergedBase", true);
    Assert.assertFalse(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/mergedBase");
    res = fsState.resolve("/mergedBase", false);
    Assert.assertTrue(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/");
    res = fsState.resolve("/mergedBase/subdir1/subdir2", true);
    Assert.assertFalse(res.isInternalDir());
    res = fsState.resolve("/mergedBase/subdir1/subdir2", false);
    Assert.assertTrue(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/mergedBase/subdir1");
    res = fsState.resolve("/mergedBase/subdir1/subdirNotExist", false);
    Assert.assertFalse(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/mergedBase/subdir1");
    res = fsState.resolve("/long/long", true);
    Assert.assertTrue(res.isInternalDir());
    res = fsState.resolve("/long/long", false);
    Assert.assertTrue(res.isInternalDir());
    res = fsState.resolve("/long/long/dirNotExist", true);
    Assert.assertFalse(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/");
    res = fsState.resolve("/long/long/dirNotExist", false);
    Assert.assertFalse(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/");
  }

  @Test
  public void testResolveWithoutRootInMountTable() throws Exception {
    try {
      fsState.resolve("/NotExistDir", true);
      Assert.fail("/NotExistDir should not resolve susscess");
    } catch (FileNotFoundException e) {}
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve("/NotExistDir", false);
    Assert.assertTrue(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/");

    res = fsState.resolve("/mergedBase", false);
    Assert.assertTrue(res.isInternalDir());
    Assert.assertTrue(res.targetFileSystem instanceof ViewFileSystem.InternalDirOfViewFs);
    FileStatus status[] = res.targetFileSystem.listStatus(new Path(res.resolvedPath));
    boolean foundTarget = false;
    for (FileStatus s : status) {
      if (s.getPath().toUri().getPath().equals("/mergedBase")) {
        foundTarget = true;
        break;
      }
    }
    Assert.assertTrue(foundTarget);

    res = fsState.resolve("/mergedBase", true);
    Assert.assertFalse(res.isInternalDir());
    res = fsState.resolve("/mergedBase/subdir1/subdir2", false);
    Assert.assertTrue(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/mergedBase/subdir1");
    res = fsState.resolve("/mergedBase/subdir1/subdirNotExist", false);
    Assert.assertFalse(res.isInternalDir());
    Assert.assertEquals(res.resolvedPath, "/mergedBase/subdir1");
  }
}
