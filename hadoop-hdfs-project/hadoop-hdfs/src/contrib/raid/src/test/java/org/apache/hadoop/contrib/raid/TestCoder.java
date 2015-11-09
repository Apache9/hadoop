/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.contrib.raid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.Coder.CounterName;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestCoder {

  @Test
  public void testCoder() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();
    FileSystem dfs = FileSystem.get(conf);

    Path collectResultFile = new Path("/result.txt");
    FSDataOutputStream out = dfs.create(collectResultFile);
    Text text = new Text("/result.txt\tInvalid");
    out.write(text.getBytes());
    out.write("\n".getBytes());
    out.write(text.getBytes());
    out.close();

    // Decoding and Encoding function is tested in TestBlockCodec, so here only
    // tests the coder's MapReduce process.
    Path outputPath = new Path("/testCoder");
    Coder coder = new Coder(collectResultFile, 1, outputPath, conf);
    conf.set("mapreduce.framework.name", "local");
    conf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 0);
    coder.run();
    Assert.assertEquals(2l, coder.getCounter(CounterName.EncodeFiles).getValue()
        + coder.getCounter(CounterName.EncodeFail).getValue());

    dfsCluster.shutdown();
  }
}
