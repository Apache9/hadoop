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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;

public class MRUtils {

  public static void killJob(Configuration conf, String jobId) {
    JobID id = JobID.forName(jobId);
    try {
      JobClient jobClient = new JobClient(conf);
      RunningJob job = jobClient.getJob(id);
      if (job != null) {
        job.killJob();
      }
    } catch (IOException e) {
      // Ignored
    }
  }

  public static void writeJobId(Configuration conf, Path file, String jobId) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(file, true);
    out.write(jobId.getBytes());
    out.close();
  }

  public static String readJobId(Configuration conf, Path file) throws IOException,
      FileNotFoundException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(file)) {
      throw new FileNotFoundException();
    }
    FSDataInputStream in = fs.open(file);
    byte[] buffer = new byte[1024];
    int readLen = in.read(buffer);
    in.close();
    return new String(buffer, 0, readLen);
  }
  
  private static Path getCodecLibraryPath(Configuration conf) throws IOException {
    String libPath = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_CODEC_LIBRARY_PATH);
    if (libPath == null) {
      return null;
    }
    FileSystem localFs;
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException ioe) {
      throw new RuntimeException("problem getting local fs", ioe);
    }
    return new Path(libPath).makeQualified(localFs);
  }

  public static void cacheCodecLib(Configuration conf, Job job) throws IOException {
    Path libPath = getCodecLibraryPath(conf);
    if (libPath != null) {
      Path jerasurePath = new Path(libPath, BlockCodec.getJerasureLibName());
      Path gfCompletePath = new Path(libPath, BlockCodec.getGfLibName());
      job.addFileToClassPath(jerasurePath);
      job.addFileToClassPath(gfCompletePath);
    }
  }
}
