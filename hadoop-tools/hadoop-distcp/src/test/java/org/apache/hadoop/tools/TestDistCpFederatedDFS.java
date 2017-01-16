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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FederatedHdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.FederatedDFSFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

public class TestDistCpFederatedDFS {
  private static final Log LOG =
      LogFactory.getLog(TestDistCpFederatedDFS.class);
  private Configuration conf;
  private Configuration mrConf;
  private MiniDFSCluster cluster1;
  private MiniDFSCluster cluster2;
  private MiniMRYarnCluster mrCluster;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    mrConf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    // Bump up replication interval so that we only run replication
    // checks explicitly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 600);
    // Increase max streams so that we re-replicate quickly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 1000);
    conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);
    // disable hdfs impl cache
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    try {
      cluster1 = setupNewDFSCluster();
      cluster2 = setupNewDFSCluster();
      setupFederationConfig(conf);
      setupFederationConfig(mrConf);
      setupMRCluster();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private MiniDFSCluster setupNewDFSCluster() throws IOException {
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

  private void setupFederationConfig(Configuration config) throws Exception {
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://test-cluster/");
    config.set("fs.hdfs.impl", FederatedDFSFileSystem.class.getName());
    config.set("fs.AbstractFileSystem.hdfs.impl", FederatedHdfs.class.getName());
    String cluster1NNAddress =
        "hdfs://" + cluster1.getNameNode().getHostAndPort();
    String cluster2NNAddress =
        "hdfs://" + cluster2.getNameNode().getHostAndPort();
    cluster1.getFileSystem().mkdir(new Path("/home"), null);
    cluster2.getFileSystem().mkdir(new Path("/user"), null);
    ConfigUtil.addLink(config, "test-cluster", "/home",
        new URI(cluster1NNAddress + "/home"));
    ConfigUtil.addLink(config, "test-cluster", "/user",
        new URI(cluster2NNAddress + "/user"));
  }


  private void setupMRCluster() throws IOException {
      mrCluster = new MiniMRYarnCluster(this.getClass().getName(), 3);
      mrConf.set(MRJobConfig.MR_AM_STAGING_DIR, "/user/yarn/apps_staging_dir");
      mrCluster.init(mrConf);
      mrCluster.start();
  }

  @Test
  public void testDistCpJob() throws Exception {
    // prepare data
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    dfs.mkdirs(new Path("/home/foo"));
    OutputStream out = dfs.create(new Path("/home/foo/test-file"));
    out.write("Hello, Federation, it's a test of distcp job".getBytes());
    out.close();

    // submit mapreduce job
    JobConf jobConf = new JobConf(mrCluster.getConfig());
    jobConf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 10000);
    DistCp distcp = new DistCp(jobConf, null);
    String[] arg = {"/home/foo", "/user/"};

    distcp.run(arg);

    Assert.assertTrue(dfs.exists(new Path("/user/foo/test-file")));
  }
}
