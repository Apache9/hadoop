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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFsVolumeSpi {
  private static FsDatasetImpl dataset(DataNode dn) {
    return (FsDatasetImpl)DataNodeTestUtils.getFSDataset(dn);
  }
  @Test
  public void testInitDuReserve() throws Exception {
    // bring up a cluster of 3
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, 1024 * 1024 * 1024);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    DataNode dn = cluster.getDataNodes().get(0);
    for (FsVolumeSpi v : dataset(dn).getVolumes()) {
      final FsVolumeImpl volume = (FsVolumeImpl)v;
      assertEquals(volume.getReserved(), 1024 * 1024 * 1024);
    }
    cluster.shutdown();

    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENT_KEY, 5);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dn = cluster.getDataNodes().get(0);
    for (FsVolumeSpi v : dataset(dn).getVolumes()) {
      final FsVolumeImpl volume = (FsVolumeImpl)v;
      assertEquals(volume.getReserved(), volume.getFsCapacity() * 5 / 100);
    }
    cluster.shutdown();

    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENT_KEY, 5);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, 1024 * 1024 * 1024);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dn = cluster.getDataNodes().get(0);
    for (FsVolumeSpi v : dataset(dn).getVolumes()) {
      final FsVolumeImpl volume = (FsVolumeImpl)v;
      assertEquals(volume.getReserved(), volume.getFsCapacity() * 5 / 100);
    }
    cluster.shutdown();

    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENT_KEY, 101);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, 1024 * 1024 * 1024);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dn = cluster.getDataNodes().get(0);
    for (FsVolumeSpi v : dataset(dn).getVolumes()) {
      final FsVolumeImpl volume = (FsVolumeImpl)v;
      assertEquals(volume.getReserved(), 1024 * 1024 * 1024);
    }
    cluster.shutdown();

    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENT_KEY, -1);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, 1024 * 1024 * 1024);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dn = cluster.getDataNodes().get(0);
    for (FsVolumeSpi v : dataset(dn).getVolumes()) {
      final FsVolumeImpl volume = (FsVolumeImpl)v;
      assertEquals(volume.getReserved(), 1024 * 1024 * 1024);
    }
    cluster.shutdown();

  }

}
