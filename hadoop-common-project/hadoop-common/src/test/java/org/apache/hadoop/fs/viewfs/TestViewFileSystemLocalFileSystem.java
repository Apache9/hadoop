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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;


/**
 * 
 * Test the ViewFileSystemBaseTest using a viewfs with authority: 
 *    viewfs://mountTableName/
 *    ie the authority is used to load a mount table.
 *    The authority name used is "default"
 *
 */

public class TestViewFileSystemLocalFileSystem extends ViewFileSystemBaseTest {

  @Override
  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = FileSystem.getLocal(new Configuration());
    super.setUp();
    
  }

  @Override
  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
    super.tearDown();
  }

  // LocalFileSystem not support federation rename
  @Override
  @Test(expected=IOException.class)
  public void testRenameAcrossMounts1() throws IOException{
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    fsView.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
  }

}
