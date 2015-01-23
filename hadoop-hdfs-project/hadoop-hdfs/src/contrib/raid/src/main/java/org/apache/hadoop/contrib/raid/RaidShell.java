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

import java.io.IOException;
import java.util.List;
import java.net.InetSocketAddress;

import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import com.google.common.annotations.VisibleForTesting;

// This is the cmd line tool to get policy/metrics from
// RaidNode rpc server.
// It is also used as a testing and diagnosing tool.
// New command will be added during development, testing
// and deplopyment.
public class RaidShell extends Configured implements Tool {
  private Configuration conf;
  private BlockCodec codec;

  public RaidShell(Configuration conf) throws IOException {
    this.conf = conf;
    this.codec = new BlockCodec(this.conf);
  }

  @VisibleForTesting
  public Policy getPolicy() throws IOException {
    ClientRaidnodeProtocol crp = null;
    try {
      // Get policy information from raid node.
      InetSocketAddress rnAddr = NetUtils.createSocketAddr(conf
          .get(HdfsRaidConfigKeys.HDFS_RAIDNODE_IPC_ADDRESS_KEY));
      crp = ClientRaidnodeProtocolTranslatorPB.createClientRaidnodeProtocolProxy(rnAddr,
        UserGroupInformation.getCurrentUser(), conf);
      Policy policy = crp.getPolicyInfos(null);
      return policy;
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      if (crp != null) {
        RPC.stopProxy(crp);
      }
    }
  }

  // TBD: Add logic to handle transfer in multiple rounds.
  private void showPolicy() {
    try {
      Policy policy = getPolicy();
      policy.showPolicy();
    } catch (IOException ioe) {
      System.out.println("Failed to get policy information from raidnode.");
    }
  }

  private void encodeFile(String file) {
    try {
      codec.encode(new Path(file));
    } catch (IOException e) {
      System.out.println("Fail to encode " + file);
    }
    System.out.println("Done.");
  }

  // TBD: add the capability to support estimating files in a directory
  private void estimateSaving(String[] files) {
    for (String file : files) {
      try {
        long savedSpace = codec.estimateSaving(new Path(file));
        System.out.println("Encode " + file + " will save " + savedSpace + " bytes");
      } catch (IOException e) {
        // TBD: Add more specific exceptions for different errors.
        // Currently all errors will be taken as the "the file can
        // not be encoded".
        System.out.println("File " + file + " cannot be encoded.");
      }
    }
  }

  private static void printUsage() {
    System.out.println("Usage: java RaidShell");
    System.out.println("  [-showPolicy]");
    System.out.println("  [-encodeFile path]");
    System.out.println("  [-estimateSaving files]");
  }

  public int run(String[] args) {
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    if ("-showPolicy".equals(args[0])) {
      if (args.length > 1) {
        System.out.println("There should be no params after -showPolicy.");
        printUsage();
        return -1;
      }
      showPolicy();
    } else if ("-encodeFile".equals(args[0])) {
      if (args.length != 2) {
        System.out.println("There should be one and only one file following -encodeFile.");
        printUsage();
        return -1;
      }
      encodeFile(args[1]);
    } else if ("-estimateSaving".equals(args[0])) {
      if (args.length < 2) {
        System.out.println("There should be a list of files following -estimateSaving.");
        printUsage();
        return -1;
      }
      String[] files = new String[args.length - 1];
      for (int i = 0; i < files.length; i++)
        files[i] = args[i + 1];
      estimateSaving(files);
    } else {
      System.out.println("Unknow command.");
      printUsage();
      return -1;
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    RaidShell shell = new RaidShell(new Configuration());
    ToolRunner.run(shell, argv);
  }
}
