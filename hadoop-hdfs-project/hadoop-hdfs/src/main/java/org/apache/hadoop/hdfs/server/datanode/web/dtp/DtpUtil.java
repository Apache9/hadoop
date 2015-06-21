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
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import org.apache.hadoop.classification.InterfaceAudience;

import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

/**
 * 
 */
@InterfaceAudience.Private
class DtpUtil {

  private static final Log LOG = DtpUrlDispatcher.LOG;

  static void checkAccess(DataNode datanode, ExtendedBlock block,
      Token<BlockTokenIdentifier> token, Op op,
      BlockTokenIdentifier.AccessMode mode, SocketAddress remoteAddress)
      throws InvalidToken {
    if (datanode.isBlockTokenEnabled()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking block access token for block '"
            + block.getBlockId() + "' with mode '"
            + BlockTokenIdentifier.AccessMode.READ + "'");
      }
      try {
        datanode.getBlockPoolTokenSecretManager().checkAccess(token, null,
          block, BlockTokenIdentifier.AccessMode.READ);
      } catch (InvalidToken e) {
        LOG.warn("Block token verification failed: op=" + Op.READ_BLOCK
            + ", remoteAddress=" + remoteAddress + ", message="
            + e.getLocalizedMessage());
        throw e;
      }
    }
  }
}
