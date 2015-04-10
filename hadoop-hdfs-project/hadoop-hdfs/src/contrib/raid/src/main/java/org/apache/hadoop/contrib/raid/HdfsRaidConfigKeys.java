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

  /**
   * The BLOCKS_NUM and CODING_BLOCKS_NUM should not be changed in one HDFS otherwise the decoding
   * would fail since the coding might use different K/C to encode a file. At this point, make it
   * configurable for debug/tuning purpose. TBD: Make BLOCKS_NUM and CODING_BLOCK_NUM immutable in
   * one HDFS.
   */
  public static final String HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY = "hdfs.raidnode.raid.data.blocks.num";
  public static final int HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT = 6;

  public static final String HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY = "hdfs.raidnode.raid.coding.blocks.num";
  public static final int HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT = 3;

  public static final String HDFS_RAIDNODE_CODER_MAP_TASK_NUM_KEY = "hdfs.raidnode.coder.map.task.num";
  public static final int HDFS_RAIDNODE_CODER_MAP_TASK_NUM_DEFAULT = 1;

  public static final String HDFS_RAIDNODE_FIXER_MAP_TASK_NUM_KEY = "hdfs.raidnode.fixer.map.task.num";
  public static final int HDFS_RAIDNODE_FIXER_MAP_TASK_NUM_DEFAULT = 1;

  public static final String HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_KEY = "hdfs.raidnode.mover.map.task.num";
  public static final int HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_DEFAULT = 1;

  public static final String HDFS_RAIDNODE_COLLECTOR_RESULT_DIR_KEY = "hdfs.raidnode.collector.result.dir";
  public static final String HDFS_RAIDNODE_COLLECTOR_RESULT_FILE_KEY = "hdfs.raidnode.collector.result.file";
  public static final String HDFS_RAIDNODE_CODER_RESULT_DIR_KEY = "hdfs.raidnode.coder.result.dir";
  public static final String HDFS_RAIDNODE_MOVER_RESULT_DIR_KEY = "hdfs.raidnode.mover.result.dir";
  public static final String HDFS_RAIDNODE_FIXER_RESULT_DIR_KEY = "hdfs.raidnode.fixer.result.dir";

  /** Comma separated directories that need to do raid or scan for block moving */
  public static final String HDFS_RAIDNODE_SCAN_ROOT_DIRS_KEY = "hdfs.raidnode.scan.root.dirs";

  // This is used to pass task type information to Map/Reduce.
  public static final String HDFS_RAIDNODE_RAID_TASK_TYPE = "hdfs.raidnode.raid.task.type";

  /**
   * The format of policy looks like: /p/a/t/h/1:3600 /p/a/t/h/2:7600
   */
  public static final String HDFS_RAIDNODE_RAID_POLICY_KEY = "hdfs.raidnode.policy.dirs";

  public static final String HDFS_RAIDNODE_RAID_POLICY_RELOAD_INTERVAL = "hdfs.raidnode.policy.reload.interval";

  public static final long HDFS_RAIDNODE_RAID_POLICY_RELOAD_INTERVAL_DEFAULT = 600 * 1000l; // default
                                                                                            // to 10
                                                                                            // mins

  /**
   * To define the frequency of zombie sweeper.
   */
  public static final String HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL = "hdfs.raidnode.zombie.sweeper.interval";

  public static final long HDFS_RAIDNODE_ZOMBIE_SWEEPER_INTERVAL_DEFAULT = 7 * 24 * 3600 * 1000l; // Once
                                                                                                  // a
                                                                                                  // week

  /**
   * To define the frequency of encoder.
   */
  public static final String HDFS_RAIDNODE_ENCODE_INTERVAL = "hdfs.raidnode.encode.interval";
  public static final long HDFS_RAIDNODE_ENCODE_INTERVAL_DEFAULT = 24 * 3600 * 1000l;

  /**
   * To define the frequency of fixer. The interval of fixer should be short since fixer is to fix
   * potential block lost issue. What's the best balance between perf and data availability?
   * (Ideally it should be the same as the duration of a corrupted block being fixed by NN.)
   */
  public static final String HDFS_RAIDNODE_FIXER_INTERVAL = "hdfs.raidnode.fixer.internal";
  public static final long HDFS_RAIDNODE_FIXER_INTERVAL_DEFAULT = 3600 * 1000l; // subject to change

  public static final String HDFS_RAIDNODE_MOVER_INTERVAL = "hdfs.raidnode.mover.interval";
  public static final long HDFS_RAIDNODE_MOVER_INTERVAL_DEFAULT = 2 * 3600 * 1000l; // Temporary -
                                                                                    // subject to
                                                                                    // change

  public static final String HDFS_RAIDNODE_MOVER_SHUFFLE_RACKS = "hdfs.raidnode.mover.shuffle.racks";
  public static final boolean HDFS_RAIDNODE_MOVER_SHUFFLE_RACKS_DEFAULT = false;

  public static final String HDFS_RAIDNODE_MOVER_CONNECT_TIMEOUT = "hdfs.raidnode.mover.connect.timeout";
  public static final int HDFS_RAIDNODE_MOVER_CONNECT_TIMEOUT_DEFAULT = 60 * 1000; // 60 secs

  public static final String HDFS_RAIDNODE_MOVER_MOVEONEBLOCK_TIMEOUT = "hdfs.raidnode.mover.moveoneblock.timeout";
  public static final int HDFS_RAIDNODE_MOVER_MOVEONEBLOCK_TIMEOUT_DEFAULT = 20 * 60 * 1000; // 20mins.
                                                                                             // The
                                                                                             // same
                                                                                             // as
                                                                                             // balancer.

  /** Time of how long a file can be encoded after it is closed. */
  public static final String HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS = "hdfs.raidnode.raid.file.time.window.ms";
  public static final long HDFS_RAIDNODE_RAID_FILE_TIME_WINDOW_MS_DEFAULT = 24 * 3600 * 1000l;

  public static final String HDFS_RAID_CODEC_STRIP_SIZE = "hdfs.raid.codec.strip.size";
  public static final int HDFS_RAID_CODEC_STRIP_SIZE_DEFAULT = 4096;

  public static final String HDFS_RAID_CODEC_WORD_SIZE = "hdfs.raid.codec.word.size";
  public static final int HDFS_RAID_CODEC_WORD_SIZE_DEFAULT = 8;

  /**
   * The buffer size used to hold coding result data before write out to HDFS. The value must be
   * dividable by STRIP_SIZE and can up to HDFS's block size.
   */
  public static final String HDFS_RAID_CODEC_CODE_BUF_SIZE = "hdfs.raid.codec.buf.size";
  public static final int HDFS_RAID_CODEC_CODE_BUF_SIZE_DEFAULT = 67108864; // 64M

  public static final String HDFS_RAIDNODE_DECODE_BLOCK_RETRY_TIMES_KEY = "hdfs.raidnode.decode.block.retry.times";
  public static final int HDFS_RAIDNODE_DECODE_BLOCK_RETRY_TIMES_DEFAULT = 3;

  public static final String HDFS_RAIDNODE_HANDLER_COUNT_KEY = "hdfs.raidnode.handler.count";
  public static final int HDFS_RAIDNODE_HANDLER_COUNT_DEFAULT = 4;

  public static final String HDFS_RAIDNODE_IPC_ADDRESS_KEY = "hdfs.raidnode.ipc.address";
  public static final int HDFS_RAIDNODE_IPC_DEFAULT_PORT = 60020;
  public static final String HDFS_RAIDNODE_IPC_ADRESS_DEFAULT = "0.0.0.0:"
      + HDFS_RAIDNODE_IPC_DEFAULT_PORT;

  public static final String HDFS_RAIDNODE_HTTP_ADDRESS_KEY = "hdfs.raidnode.http-address";
  public static final int HDFS_RAIDNODE_HTTP_PORT_DEFAULT = 8485;
  public static final String HDFS_RAIDNODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:"
      + HDFS_RAIDNODE_HTTP_PORT_DEFAULT;
  public static final String HDFS_RAIDNODE_HTTPS_ADDRESS_KEY = "hdfs.raidnode.https-address";
  public static final int HDFS_RAIDNODE_HTTPS_PORT_DEFAULT = 8486;
  public static final String HDFS_RAIDNODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:"
      + HDFS_RAIDNODE_HTTPS_PORT_DEFAULT;

  public static final String HDFS_RAIDNODE_KEYTAB_FILE_KEY = "hdfs.raidnode.keytab.file";
  public static final String HDFS_RAIDNODE_KERBEROS_PRINCIPAL_KEY = "hdfs.raidnode.kerberos.principal";
  public static final String HDFS_RAIDNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "hdfs.raidnode.kerberos.internal.spnego.principal";
}
