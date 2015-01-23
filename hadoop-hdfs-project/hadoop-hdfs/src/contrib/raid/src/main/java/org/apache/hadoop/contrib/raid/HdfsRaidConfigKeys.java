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

public class HdfsRaidConfigKeys {

  public static final String HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY = "hdfs.raidnode.raid.data.blocks.num";
  public static final int HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT = 6;

  public static final String HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY = "hdfs.raidnode.raid.coding.blocks.num";
  public static final int HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT = 3;

  public static final String HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY = "hdfs.raidnode.coder.map.task.num";
  public static final int HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT = 1;

  public static final String HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY = "hdfs.raidnode.collector.result.dir";
  public static final String HDFS_RAIDNODE_COLLECTOR_RESULT_FILE_KEY = "hdfs.raidnode.collector.result.file";
  public static final String HDFS_RAIDNODE_CODER_RESULT_DIR_KEY = "hdfs.raidnode.coder.result.dir";

  /** Comma separated directories that need to do raid */
  public static final String HDFS_RAIDNODE_RAIDABLE_ROOT_DIRS_KEY = "hdfs.raidnode.raidable.root.dirs";

  /** Time of how long a file can be encoded after it is closed. */
  public static final String HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS = "hdfs.raidnode.raid.file.time.window.ms";
  public static final long HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT = 24 * 3600 * 1000l;

  public static final String HDFS_RAIDNODE_DECODE_BLOCK_RETRY_TIMES_KEY = "hdfs.raidnode.decode.block.retry.times";
  public static final int HDFS_RAIDNODE_DECODE_BLOCK_RETRY_TIMES_DEFAULT = 3;

  public static final String HDFS_RAIDNODE_HANDLER_COUNT_KEY = "hdfs.raidnode.handler.count";
  public static final int HDFS_RAIDNODE_HANDLER_COUNT_DEFAULT = 4;

  public static final String HDFS_RAIDNODE_IPC_ADDRESS_KEY = "hdfs.raidnode.ipc.address";
  public static final int HDFS_RAIDNODE_IPC_DEFAULT_PORT = 60020;
  public static final String HDFS_RAIDNODE_IPC_ADRESS_DEFAULT = "0.0.0.0:"
      + HDFS_RAIDNODE_IPC_DEFAULT_PORT;
}
