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

import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;

@InterfaceAudience.Private
public class DtpUtil {

  private static final Log LOG = LogFactory.getLog(DtpUtil.class);

  public static final int VERSION = 1;

  public static final String URL_PREFIX = "/dtp/v" + VERSION;

  public static final String OP_READ_BLOCK = "/read_block";

  static void checkAccess(DataNode datanode, ExtendedBlock block,
      Token<BlockTokenIdentifier> token, Op op,
      BlockTokenSecretManager.AccessMode mode, SocketAddress remoteAddress)
      throws InvalidToken {
    if (datanode.isBlockTokenEnabled()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking block access token for block '" +
            block.getBlockId() + "' with mode '" +
            BlockTokenSecretManager.AccessMode.READ + "'");
      }
      try {
        datanode.getBlockPoolTokenSecretManager().checkAccess(token, null,
            block, BlockTokenSecretManager.AccessMode.READ);
      } catch (InvalidToken e) {
        LOG.warn("Block token verification failed: op=" + Op.READ_BLOCK +
            ", remoteAddress=" + remoteAddress + ", message=" +
            e.getLocalizedMessage());
        throw e;
      }
    }
  }
}