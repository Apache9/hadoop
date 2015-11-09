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
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;

public class MRUtils {

  private static final String CODEC_LIB_PATH = "lib/native/";
  private static final String JERASURE_LIBNAME = "libJerasure.so";
  private static final String GF_LIBNAME = "libgf_complete.so.1";
  private static final String JAR_LIB_PATH = "share/hadoop/hdfs/lib/";
  private static final Log LOG = LogFactory.getLog(Fixer.class);

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
  
  public static BlockTokenSecretManager getBlockTokenSecretManager(final Configuration conf)
      throws IOException {
    return SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<BlockTokenSecretManager>() {
      @Override
      public BlockTokenSecretManager run() throws IOException {
        URI nameNodeUri = (URI) DFSUtil.getNsServiceRpcUris(conf).toArray()[0];
        NamenodeProtocol namenode = NameNodeProxies.createProxy(conf, nameNodeUri,
          NamenodeProtocol.class).getProxy();
        ExportedBlockKeys keys = namenode.getBlockKeys();
        boolean isBlockTokenEnabled = keys.isBlockTokenEnabled();
        NamespaceInfo namespaceInfo = namenode.versionRequest();
        String blockPoolId = namespaceInfo.getBlockPoolID();
        BlockTokenSecretManager blockTokenSecretManager = null;
        if (isBlockTokenEnabled) {
          long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
          long blockTokenLifetime = keys.getTokenLifetime();
          LOG.info("Block token params received from NN: keyUpdateInterval="
              + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
              + blockTokenLifetime / (60 * 1000) + " min(s)");
          String encryptionAlgorithm = conf.get(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
          blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval,
              blockTokenLifetime, blockPoolId, encryptionAlgorithm);
          blockTokenSecretManager.addKeys(keys);
        }
        return blockTokenSecretManager;
      }
    });
  }

  public static Token<BlockTokenIdentifier> getAccessToken(ExtendedBlock eb,
      EnumSet<BlockTokenSecretManager.AccessMode> access, BlockTokenSecretManager tokenManager)
      throws IOException {
    if (tokenManager == null) {
      return BlockTokenSecretManager.DUMMY_TOKEN;
    } else {
      return tokenManager.generateToken(null, eb, access);
    }
  }

  private static Path getRaidLibraryPath(Configuration conf) throws IOException {
    String libPath = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_LIBRARY_PATH);
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

  public static void cacheCodecLib(Configuration conf) throws IOException {
    Path libPath = getRaidLibraryPath(conf);
    if (libPath != null) {
      Path jerasurePath = new Path(libPath, CODEC_LIB_PATH + JERASURE_LIBNAME);
      Path gfCompletePath = new Path(libPath, CODEC_LIB_PATH + GF_LIBNAME);
      // Copy to hdfs
      Path jerasureHdfsPath = new Path(BlockCodec.getRaidLib() + "/" + JERASURE_LIBNAME);
      Path gfCompleteHdfsPath = new Path(BlockCodec.getRaidLib() + "/" + GF_LIBNAME);
      FileSystem hdfs = FileSystem.get(conf);
      hdfs.copyFromLocalFile(false, true, jerasurePath, jerasureHdfsPath);
      hdfs.copyFromLocalFile(false, true, gfCompletePath, gfCompleteHdfsPath);
    
      DistributedCache.addFileToClassPath(jerasureHdfsPath, conf);
      DistributedCache.addFileToClassPath(gfCompleteHdfsPath, conf);
    }
  }

  public static void cacheJarLib(Configuration conf) throws IOException {
    Path libPath = getRaidLibraryPath(conf);
    if (libPath != null) {
      String rawJars = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_JARS_TO_CACHE);
      if (rawJars != null) {
        String[] jars = rawJars.trim().split("\\s+");
        FileSystem hdfs = FileSystem.get(conf);
        for (String jar: jars) {
          Path jarPath = new Path(libPath, JAR_LIB_PATH + jar);
          Path jarHdfsPath = new Path(BlockCodec.getRaidLib() + "/" + jar);
          hdfs.copyFromLocalFile(false, true, jarPath, jarHdfsPath);
          DistributedCache.addFileToClassPath(jarHdfsPath, conf); 
        }
      }
    }

  }
}
