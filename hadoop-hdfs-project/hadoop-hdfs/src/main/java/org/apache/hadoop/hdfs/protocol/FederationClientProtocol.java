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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;

import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;


/**********************************************************************
 * ClientProtocol is used by user code via 
 * {@link org.apache.hadoop.hdfs.DistributedFileSystem} class to communicate 
 * with the NameNode.  User code can manipulate the directory namespace, 
 * as well as open/close file streams, etc.
 *
 **********************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(DelegationTokenSelector.class)
public interface FederationClientProtocol {

  public static final long versionID = 69L;
  
  ///////////////////////////////////////
  // File contents
  ///////////////////////////////////////

  @AtMostOnce
  public DirectorySubTree renameSrcPhase1(String src, String srcId, String dst,
      String dstId) throws IOException;

  @AtMostOnce
  public boolean renameSrcPhase2(long renameId, boolean toCancel)
      throws IOException;

  @AtMostOnce
  public BlocksToDup renameDestPhase1(String src, String srcId, String dst,
      String dstId, DirectorySubTree subTree) throws IOException;

  @AtMostOnce
  public boolean renameDestPhase2(long renameId, String srcId)
      throws IOException;

  public boolean renameRecordExist(long renameId, String srcId, String dstId,
      boolean isSource) throws IOException;

  public String getPoolId() throws IOException;

  public DirectorySubTree getRenameDestSubTree(String dst) throws IOException;
}
