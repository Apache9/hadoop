/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataBlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FSCachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test to make sure df can run and work.
 */
public class TestReplicaCachingGetSpaceUsed {
  public static final File DF_DIR = GenericTestUtils.getTestDir("testdfsspace");
  public static final int FILE_SIZE = 1024;
  private static final String BASE_DIR =
      new FileSystemTestHelper().getTestRootDir();
  private static final int NUM_INIT_VOLUMES = 2;
  private static final String[] BLOCK_POOL_IDS = { "bpid-0", "bpid-1" };

  // Use to generate storageUuid
  private static final DataStorage dsForStorageUuid =
      new DataStorage(new StorageInfo(HdfsServerConstants.NodeType.DATA_NODE));

  private Configuration conf;
  private DataStorage storage;
  private DataBlockScanner scanner;
  private FsDatasetImpl dataset;

  private static Storage.StorageDirectory createStorageDirectory(File root) {
    Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
    dsForStorageUuid.createStorageID(sd);
    return sd;
  }

  private static void createStorageDirs(DataStorage storage, Configuration conf,
      int numDirs) throws IOException {
    List<Storage.StorageDirectory> dirs = new ArrayList<>();
    List<String> dirStrings = new ArrayList<>();
    for (int i = 0; i < numDirs; i++) {
      File loc = new File(BASE_DIR + "/data" + i);
      dirStrings.add(loc.toString());
      loc.mkdirs();
      dirs.add(createStorageDirectory(loc));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  @Before
  public void setUp() throws IOException {
    final DataNode datanode = Mockito.mock(DataNode.class);
    storage = Mockito.mock(DataStorage.class);
    scanner = Mockito.mock(DataBlockScanner.class);

    this.conf = new Configuration();
    final DNConf dnConf = new DNConf(conf);

    when(datanode.getConf()).thenReturn(conf);
    when(datanode.getDnConf()).thenReturn(dnConf);
    when(datanode.getBlockScanner()).thenReturn(scanner);

    createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
    dataset = new FsDatasetImpl(datanode, storage, conf);
    for (String bpid : BLOCK_POOL_IDS) {
      dataset.addBlockPool(bpid, conf);
    }
    dataset.volumeMap.add(BLOCK_POOL_IDS[0],
        new FinalizedReplica(1, 512, 11, null, null));
    dataset.volumeMap.add(BLOCK_POOL_IDS[0],
        new ReplicaBeingWritten(2, 256, 12, null, null, null, 0));

    FileUtil.fullyDelete(DF_DIR);
    assertTrue(DF_DIR.mkdirs());
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(DF_DIR);
  }

  @Test
  public void testCanBuildRun() throws Exception {
    File file = new File(DF_DIR, "testCanBuild");
    assertTrue(file.createNewFile());

    GetSpaceUsed instance = new FSCachingGetSpaceUsed.Builder()
        .setVolume(dataset.volumes.volumes.get(0)).setBpid(BLOCK_POOL_IDS[0])
        .setPath(file).setInterval(50060)
        .setKlass(ReplicaCachingGetSpaceUsed.class).build();

    assertTrue(instance instanceof ReplicaCachingGetSpaceUsed);
    assertEquals(384, instance.getUsed());
    ((ReplicaCachingGetSpaceUsed) instance).close();
  }
}