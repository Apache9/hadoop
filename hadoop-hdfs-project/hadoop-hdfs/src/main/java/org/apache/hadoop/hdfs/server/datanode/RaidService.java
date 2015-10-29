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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.RaidDatanodeProtocol;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.io.IOException;

@InterfaceAudience.Private
public class RaidService implements RaidDatanodeProtocol {

  public static final Log LOG = LogFactory.getLog(RaidService.class);  
  private DataNode dn;

  public RaidService(DataNode datanode) {
    dn = datanode;
  }

  public String getRaidReplicaState(ExtendedBlock b) throws IOException {
    Replica replica = null;
    LOG.info("To get replica state of block " + b.getLocalBlock());
    try {
      replica =
          dn.getFSDataset().getReplica(b.getBlockPoolId(), b.getBlockId());
    } catch (Throwable t) {
      if (t instanceof IOException) {
        throw (IOException) t;
      }
      throw new IllegalStateException(t);
    }
    if (replica == null) {
      return "undef";
    }

    switch (replica.getState()) {
    case FINALIZED:
      return "finalized";
    case RBW:
      return "rbw";
    case RWR:
      return "rwr";
    case TEMPORARY:
      return "temporary";
    }

    return "undef";
  }

  public void deleteReplica(ExtendedBlock b) throws IOException {
    try {
      LOG.info("To delete replica of block " + b.getLocalBlock());
      Block[] toDelete = new Block[] { b.getLocalBlock() };
      if (dn.blockScanner != null) {
        dn.blockScanner.deleteBlocks(b.getBlockPoolId(), toDelete);
      }
      // using global fsdataset
      dn.getFSDataset().invalidate(b.getBlockPoolId(), toDelete);
      LOG.info("Deleted replica of block " + b.getLocalBlock());
    } catch (Throwable t) {
      if (t instanceof IOException) {
        throw (IOException) t;
      }
      throw new IllegalStateException(t);
    }
  }
}
