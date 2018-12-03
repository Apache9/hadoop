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

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEEP_COPY_REPLICA_THRESHOLD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DEEP_COPY_REPLICA_THRESHOLD_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.REPLICA_CACHING_GET_SPACE_USED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_KEY;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.FSCachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.util.Time;

/**
 * Fast but inaccurate class to tell how much space HDFS is using. This class
 * makes the assumption that the entire mount is used for HDFS and that no two
 * hdfs data dirs are on the same disk.
 *
 * To use set fs.getspaceused.classname to
 * org.apache.hadoop.hdfs.server.datanode.fsdataset
 * .impl.ReplicaCachingGetSpaceUsed in your core-site.xml
 *
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
@InterfaceStability.Evolving
public class ReplicaCachingGetSpaceUsed extends FSCachingGetSpaceUsed {
  public static final Log LOG =
      LogFactory.getLog(ReplicaCachingGetSpaceUsed.class);
  private FsVolumeImpl volume;
  private String bpid;
  private Configuration conf;
  private long deepCopyReplicaThresholdMs;
  private long replicaCachingGetSpaceUsedThresholdMs;

  public ReplicaCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
    volume = builder.getVolume();
    bpid = builder.getBpid();
    conf = builder.getConf();
    deepCopyReplicaThresholdMs = conf.getLong(DEEP_COPY_REPLICA_THRESHOLD_KEY,
        DEEP_COPY_REPLICA_THRESHOLD_DEFAULT);
    replicaCachingGetSpaceUsedThresholdMs =
        conf.getLong(REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_KEY,
            REPLICA_CACHING_GET_SPACE_USED_DEFAULT);
  }

  public ReplicaCachingGetSpaceUsed(CachingGetSpaceUsed.Builder builder)
      throws IOException {
    super(builder);
  }

  @Override
  protected void refresh() {
    long start = Time.monotonicNow();
    long dfsUsed = 0;
    long count = 0;

    FsDatasetImpl fsDataset = volume.getDataset();
    Collection<ReplicaInfo> replicaInfos =
        (Collection<ReplicaInfo>) fsDataset.deepCopyReplica(bpid);
    long cost = Time.monotonicNow() - start;
    if (cost > deepCopyReplicaThresholdMs) {
      LOG.info("blockPoolSlice: " + bpid + " replicas size:"
          + replicaInfos.size() + " copy replicas duration: "
          + (Time.monotonicNow() - start) + "ms");
    }

    try {
      if (CollectionUtils.isNotEmpty(replicaInfos)) {
        for (ReplicaInfo replicaInfo : replicaInfos) {
          if (Objects.equals(replicaInfo.getVolume().getStorageID(),
              volume.getStorageID())) {
            dfsUsed += replicaInfo.getBytesOnDisk();
            dfsUsed += replicaInfo.getMetaFile().length();
            count++;
          }
        }
      }

      this.used.set(dfsUsed);
      cost = Time.monotonicNow() - start;
      if (cost > replicaCachingGetSpaceUsedThresholdMs) {
        LOG.info("refresh dfs used, bpid: " + bpid + " replicas size: " + count
            + " dfsUsed: " + this.used + " on volume: " + volume.getStorageID()
            + " duration: " + (Time.monotonicNow() - start) + "ms");
      }
    } catch (Exception e) {
      LOG.error("replicaCachingGetSpaceUsed refresh error", e);
    }
  }
}
