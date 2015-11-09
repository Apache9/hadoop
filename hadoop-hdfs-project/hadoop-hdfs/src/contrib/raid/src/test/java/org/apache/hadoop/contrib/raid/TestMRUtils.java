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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;

public class TestMRUtils {

  @Test
  public void testReadWriteJobId() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();

    String jobId = "job_1111111_1111";
    Path file = new Path("/test.jobid");
    MRUtils.writeJobId(conf, file, jobId);
    String id = MRUtils.readJobId(conf, file);

    Assert.assertEquals(jobId, id);

    dfsCluster.shutdown();
  }
}
