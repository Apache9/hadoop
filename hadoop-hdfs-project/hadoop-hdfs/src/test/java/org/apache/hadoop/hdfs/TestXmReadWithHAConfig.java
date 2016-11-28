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

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before; 
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;

public class TestXmReadWithHAConfig {
  private static final Log LOG = LogFactory
      .getLog(TestXmReadWhileWriting.class);
  private static final int blockSize = 512;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private static final String LOGICAL_NAME = "minidfs";
  private static final MiniDFSNNTopology topo = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf(LOGICAL_NAME).addNN(
          new MiniDFSNNTopology.NNConf("nn1")).addNN(
          new MiniDFSNNTopology.NNConf("nn2")));
  
  @Before
  public void setUp() throws IOException {
    conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    cluster = null;
    fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
          .numDataNodes(1).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);

      cluster.waitActive();

      fs = cluster.getFileSystem(0);

      cluster.transitionToActive(0);

    } catch (IOException ioe) {
      LOG.warn("Fail to init HA config ", ioe);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    LOG.info("shutdown the HA cluster");
    cluster.shutdown();
  }

  public void writeDataToFile (String src, int sizeToWrite) throws IOException {
    int numToWrite = (int) (sizeToWrite / blockSize);
    try {
      FSDataOutputStream out =
          TestFileCreation.createFile(fs, new Path(src), 1);
      for (int i = 0; i < numToWrite; i++) {
        final byte[] writeBuf =
            DFSTestUtil.generateSequentialBytes(i * blockSize,
                blockSize);
        out.write(writeBuf);
      }
      out.close();
    } catch (IOException ioe) {
      LOG.warn("Fail to write file ", ioe);
      Assert.assertTrue(false);
    }
  }

  public void readFromFile (String src, int sizeToRead, int toSeek) throws IOException {
    int numToRead = (int) (sizeToRead / blockSize);

    try {
      final byte[] readBuf = new byte[sizeToRead];
      FSDataInputStream in = fs.openEx(new Path(src));
      if (toSeek > 0) {
        in.seek(toSeek);
      }
      for (int i = 0; i < numToRead; i++) {
        in.read(readBuf, toSeek + i * blockSize, blockSize);
      }
      in.close();
    } catch (IOException ioe) {
      LOG.warn("Fail to read file ", ioe);
    }
  }

  // Test if the ZK runtime exception is translated to IOException properly
  @Test
  public void testHandlingZKException() throws IOException{
    String file = "/read1";
    try {
      writeDataToFile(file, blockSize * 1);
    } catch (IOException ioe) {
      LOG.error(ioe);
    }

    try {
      readFromFile(file, blockSize * 2, 0);
    } catch (IOException ioe) {
      LOG.error(ioe);
    }

    try {
      readFromFile(file, blockSize * 1, blockSize * 1);
    } catch (IOException ioe) {
      LOG.error(ioe);
    } finally {
      IOUtils.cleanup(null, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }

  }
}
