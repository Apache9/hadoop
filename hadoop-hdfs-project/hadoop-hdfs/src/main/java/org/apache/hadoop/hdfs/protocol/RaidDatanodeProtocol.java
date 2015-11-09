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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSelector;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

/** An raidnode-datanode protocol to support raid funcationality 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY, clientPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
public interface RaidDatanodeProtocol {

  public static final long versionID = 1L;

  /** Return the state of a replica. */
  public String getRaidReplicaState(ExtendedBlock b) throws IOException;
  
  /**
   * Delete the replica. 
   * 
   * @param block the block to be deleted.

   * @throws IOException
   */
  public void deleteReplica(ExtendedBlock b) throws IOException;
  
}
