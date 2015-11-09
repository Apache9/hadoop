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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.raid.Collector.CollectorMapper;
import org.apache.hadoop.contrib.raid.Collector.CollectorReducer;
import org.apache.hadoop.contrib.raid.RaidTask.TaskPurpose;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCollector {

  private MapDriver<Object, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

  @Before
  public void setUp() throws Exception {
    CollectorMapper mapper = new CollectorMapper();
    CollectorReducer reducer = new CollectorReducer();

    mapDriver = MapDriver.newMapDriver(mapper);
    mapDriver.getContext().getConfiguration()
        .setEnum(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_TASK_TYPE, TaskPurpose.Encode);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapReduceDriver.getConfiguration().setEnum(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_TASK_TYPE,
      TaskPurpose.Encode);
  }

  @Test
  public void testMapper() throws Exception {
    mapDriver.withInput(new Text(), new Text("/test/abc"))
        .withOutput(new Text("/test/abc"), new Text("Encode")).runTest();
  }

  @Test
  public void testReducer() throws Exception {
    List<Text> values = new ArrayList<Text>(1);
    values.add(new Text("Encode"));
    reduceDriver.withInput(new Text("/test/abc"), values)
        .withOutput(new Text("/test/abc"), new Text("Encode")).runTest();
  }

  @Test
  public void testMapReduce() throws Exception {
    mapReduceDriver.withInput(new Text(), new Text("/test/b"))
        .withInput(new Text(), new Text("/test/a"))
        .withOutput(new Text("/test/a"), new Text("Encode"))
        .withOutput(new Text("/test/b"), new Text("Encode")).runTest();
  }

  @Test
  public void testCollector() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();

    FileSystem dfs = dfsCluster.getFileSystem();
    dfs.mkdirs(new Path("/user/test1"));
    dfs.mkdirs(new Path("/user/test2"));
    dfs.create(new Path("/user/test1/a.txt")).close();
    dfs.create(new Path("/user/test2/b.txt")).close();
    dfs.mkdirs(new Path("/user/test1/foo"));
    dfs.create(new Path("/user/test1/foo/c.txt")).close();
    dfs.mkdirs(new Path("/user/test3"));

    // This file is already encoded
    dfs.mkdirs(new Path("/raid/user/test1"));
    dfs.create(BlockCodec.getCodingFile(new Path("/user/test2/b.txt"))).close();

    List<Path> rootDirs = new ArrayList<Path>(2);
    rootDirs.add(new Path("/user/test1"));
    rootDirs.add(new Path("/user/test2"));
    Collector collector = new Collector(rootDirs, new Path("/user/test3/result"),
        TaskPurpose.Encode, conf);
    Thread.sleep(3000);

    conf.set("mapreduce.framework.name", "local");
    conf.setLong(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS, 3000);
    conf.setBoolean(HdfsRaidConfigKeys.HDFS_RAIDNODE_SKIP_ENCODE_SPACE_CHECK_KEY, true);
    collector.run();

    // Assert the job is successful
    Assert.assertTrue(dfs.exists(new Path("/user/test3/result/_SUCCESS")));

    // Verify the result
    FSDataInputStream in = dfs.open(new Path("/user/test3/result/part-r-00000"));
    Set<Path> raidFiles = new HashSet<Path>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = reader.readLine()) != null) {
      String path = line.split("\t")[0];
      raidFiles.add(new Path(path));
    }
    Assert.assertTrue(raidFiles.contains(new Path("/user/test1/a.txt")));
    Assert.assertFalse(raidFiles.contains(new Path("/user/test2/b.txt")));
    Assert.assertTrue(raidFiles.contains(new Path("/user/test1/foo/c.txt")));

    reader.close();
    dfsCluster.shutdown();
  }
}
