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
package org.apache.hadoop.hdfs.tools;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.FederatedDFSFileSystem;
import org.apache.hadoop.hdfs.HdfsMountpointRenewer;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDFSAdmin {
  private MiniDFSCluster cluster;
  private DFSAdmin admin;
  private DataNode datanode;
  private final String clusterName = "test-cluster";

  @Before
  public void setUp() throws Exception {
    MiniDFSNNTopology topology = new MiniDFSNNTopology().addNameservice(
        new MiniDFSNNTopology.NSConf(clusterName)
            .addNN(new MiniDFSNNTopology.NNConf("host0").setIpcPort(0))
            .addNN(new MiniDFSNNTopology.NNConf("host1").setIpcPort(0)));
    cluster =
        new MiniDFSCluster.Builder(new Configuration()).nnTopology(topology)
            .numDataNodes(1).build();
    cluster.transitionToActive(0);
    cluster.waitActive();

    admin = new DFSAdmin();
    datanode = cluster.getDataNodes().get(0);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private List<String> getReconfigureStatus(String nodeType, String address)
      throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bufOut);
    ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(bufErr);
    admin.getReconfigurationStatus(nodeType, address, out, err);
    Scanner scanner = new Scanner(bufOut.toString());
    List<String> outputs = Lists.newArrayList();
    while (scanner.hasNextLine()) {
      outputs.add(scanner.nextLine());
    }
    return outputs;
  }

  @Test(timeout = 30000)
  public void testGetReconfigureStatus()
      throws IOException, InterruptedException {
    ReconfigurationUtil ru = mock(ReconfigurationUtil.class);
    datanode.setReconfigurationUtil(ru);

    List<ReconfigurationUtil.PropertyChange> changes =
        new ArrayList<ReconfigurationUtil.PropertyChange>();
    File newDir = new File(cluster.getDataDirectory(), "data_new");
    newDir.mkdirs();
    changes.add(new ReconfigurationUtil.PropertyChange(
        DFS_DATANODE_DATA_DIR_KEY, newDir.toString(),
        datanode.getConf().get(DFS_DATANODE_DATA_DIR_KEY)));
    changes.add(new ReconfigurationUtil.PropertyChange(
        "randomKey", "new123", "old456"));
    when(ru.parseChangedProperties(any(Configuration.class),
        any(Configuration.class))).thenReturn(changes);

    final int port = datanode.getIpcPort();
    final String address = "localhost:" + port;

    assertThat(admin.startReconfiguration("datanode", address), is(0));

    List<String> outputs = null;
    int count = 100;
    while (count > 0) {
      outputs = getReconfigureStatus("datanode", address);
      if (!outputs.isEmpty() && outputs.get(0).contains("finished")) {
        break;
      }
      count--;
      Thread.sleep(100);
    }
    assertTrue(count > 0);
    assertThat(outputs.size(), is(8));  // 3 (SUCCESS) + 4 (FAILED)

    List<StorageLocation> locations = DataNode.getStorageLocations(
        datanode.getConf());
    assertThat(locations.size(), is(1));
    assertThat(locations.get(0).getFile(), is(newDir));
    // Verify the directory is appropriately formatted.
    assertTrue(new File(newDir, Storage.STORAGE_DIR_CURRENT).isDirectory());

    int successOffset = outputs.get(1).startsWith("SUCCESS:") ? 1 : 5;
    int failedOffset = outputs.get(1).startsWith("FAILED:") ? 1: 4;
    assertThat(outputs.get(successOffset),
        containsString("Change property " + DFS_DATANODE_DATA_DIR_KEY));
    assertThat(outputs.get(successOffset + 1),
        is(allOf(containsString("From:"), containsString("data1"),
            containsString("data2"))));
    assertThat(outputs.get(successOffset + 2),
        is(not(anyOf(containsString("data1"), containsString("data2")))));
    assertThat(outputs.get(successOffset + 2),
        is(allOf(containsString("To"), containsString("data_new"))));
    assertThat(outputs.get(failedOffset),
        containsString("Change property randomKey"));
    assertThat(outputs.get(failedOffset + 1),
        containsString("From: \"old456\""));
    assertThat(outputs.get(failedOffset + 2),
        containsString("To: \"new123\""));
  }

  @Test
  public void testGetDistributedFileSystemEx() throws Exception {
    URI uri1 = new URI("hdfs://c4tst-fed-1");
    URI uri2 = new URI("hdfs://c4tst-fed-2");
    URI uri3 = new URI("hdfs://c4tst-fed-3");
    Path path = Mockito.mock(Path.class);
    Mockito.when(path.toUri()).thenReturn(uri2);

    FederatedDFSFileSystem dfs1 = Mockito.mock(FederatedDFSFileSystem.class);
    FederatedDFSFileSystem dfs2 = Mockito.mock(FederatedDFSFileSystem.class);
    Mockito.when(dfs1.supportFederation()).thenReturn(true);
    Mockito.when(dfs1.getUri()).thenReturn(uri1);
    Mockito.when(dfs1.getDistributedFileSystem()).thenReturn(dfs1);

    Mockito.when(dfs2.supportFederation()).thenReturn(true);
    Mockito.when(dfs2.getUri()).thenReturn(uri2);
    Mockito.when(dfs2.getDistributedFileSystem()).thenReturn(dfs2);
    FileSystem[] fileSystems = new FileSystem[]{dfs1, dfs2};

    Mockito.when(dfs2.getChildFileSystems()).thenReturn(fileSystems);
    assertEquals(dfs2, admin.getDistributedFileSystemEx(dfs2, path));


    Mockito.when(path.toUri()).thenReturn(uri3);
    FederatedDFSFileSystem dfs3 = Mockito.mock(FederatedDFSFileSystem.class);
    FederatedDFSFileSystem dfs4 = Mockito.mock(FederatedDFSFileSystem.class);
    Mockito.when(dfs3.supportFederation()).thenReturn(true);
    Mockito.when(dfs3.getUri()).thenReturn(uri1);
    Mockito.when(dfs3.getDistributedFileSystem()).thenReturn(dfs3);

    Mockito.when(dfs4.supportFederation()).thenReturn(true);
    Mockito.when(dfs4.getUri()).thenReturn(uri2);
    Mockito.when(dfs4.getDistributedFileSystem()).thenReturn(dfs4);
    fileSystems = new FileSystem[]{dfs3, dfs4};

    Mockito.when(dfs4.getChildFileSystems()).thenReturn(fileSystems);
    assertEquals(dfs4, admin.getDistributedFileSystemEx(dfs4, path));

    Mockito.when(path.toUri()).thenReturn(uri1);
    FederatedDFSFileSystem dfs5 = Mockito.mock(FederatedDFSFileSystem.class);
    FederatedDFSFileSystem dfs6 = Mockito.mock(FederatedDFSFileSystem.class);
    Mockito.when(dfs5.supportFederation()).thenReturn(false);
    Mockito.when(dfs5.getUri()).thenReturn(uri1);
    Mockito.when(dfs5.getDistributedFileSystem()).thenReturn(dfs5);

    Mockito.when(dfs6.supportFederation()).thenReturn(false);
    Mockito.when(dfs6.getUri()).thenReturn(uri2);
    Mockito.when(dfs6.getDistributedFileSystem()).thenReturn(dfs6);
    fileSystems = new FileSystem[]{dfs5, dfs6};

    Mockito.when(dfs6.getChildFileSystems()).thenReturn(fileSystems);
    assertEquals(dfs6, admin.getDistributedFileSystemEx(dfs6, path));
  }

  @Test
  public void testUpdateMPT2Zk() throws Exception {
    Configuration config = new Configuration(false);
    config.set(DFSConfigKeys.DFS_CLIENT_ZOOKEEPER_OBSERVER,
        "tj-hadoop-staging-zk01.kscn:21000");
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://" + clusterName + "/");
    config.setClass("fs.hdfs.impl", FederatedDFSFileSystem.class,
        FileSystem.class);

    HdfsMountpointRenewer hmpr = new HdfsMountpointRenewer();
    hmpr.initialize(clusterName, config,
        new MountpointRenewer.RenewMountpoint() {
          @Override
          public void doUpdateMountpoint() {
          }
        });
    hmpr.deleteMptConfFromZookeeper(config);

    // test update mpt when znode doesn't exist
    addNs(clusterName + "-3", config, 0, 1, "/dir-in-3");
    testUpdateMpt2Zk(config, hmpr);
    // test incremental update mpt when znode exists
    addNs(clusterName + "-4", config, 0, 1, "/dir-in-4");
    testUpdateMpt2Zk(config, hmpr);
  }

  private void addNs(String ns, Configuration config, int host0, int host1,
      String link) throws URISyntaxException {
    String prens = config.get(DFSConfigKeys.DFS_NAMESERVICES);
    config.set(DFSConfigKeys.DFS_NAMESERVICES,
        prens == null ? ns : prens + ", " + ns);
    config.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX, ns),
        ConfiguredFailoverProxyProvider.class.getName());
    config.set(
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, ns),
        "host0,host1");
    config.set(DFSUtil
        .addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, ns,
            "host0"), cluster.getNameNode(host0).getHostAndPort());
    config.set(DFSUtil
        .addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, ns,
            "host1"), cluster.getNameNode(host1).getHostAndPort());
    ConfigUtil.addLink(config, clusterName, link,
        new URI("hdfs://" + ns + link));
  }

  private void testUpdateMpt2Zk(Configuration config,
      HdfsMountpointRenewer hmpr)
      throws IOException, KeeperException, InterruptedException {
    String newMptConfString =
        HdfsMountpointRenewer.serializeMountpoint2String(config, clusterName);
    final DFSAdmin dfsAdmin = new DFSAdmin(config);
    final String[] argv = new String[] { "-updateMptOnZk" };
    UserGroupInformation ugi = UserGroupInformation
        .createUserForTesting("hdfs", new String[] { "hdfs" });
    ugi.doAs(new PrivilegedAction<Object>() {
      @Override public Object run() {
        int res = 0;
        try {
          res = ToolRunner.run(dfsAdmin, argv);
        } catch (Exception e) {
          assert false;
        }
        assertEquals(0, res);
        return null;
      }
    });
    byte[] zkData = hmpr.getMptConfFromZookeeper(config);
    assertTrue(Arrays.equals(zkData, newMptConfString.getBytes()));
  }
}