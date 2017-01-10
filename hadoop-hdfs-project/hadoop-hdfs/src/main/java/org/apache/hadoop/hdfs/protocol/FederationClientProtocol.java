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

  /**
   * Rename an item in the file system namespace.
   * @param src existing file or directory name.
   * @param dst new name.
   * @return true if successful, or false if the old name does not exist
   * or if the new name already belongs to the namespace.
   * 
   * @throws SnapshotAccessControlException if path is in RO snapshot
   * @throws IOException an I/O error occurred 
   */
  @AtMostOnce
  public boolean rename(String src, String dst, String dstId) 
      throws UnresolvedLinkException, SnapshotAccessControlException, IOException;


  /**
   * Rename src to dst.
   * <ul>
   * <li>Fails if src is a file and dst is a directory.
   * <li>Fails if src is a directory and dst is a file.
   * <li>Fails if the parent of dst does not exist or is a file.
   * </ul>
   * <p>
   * Without OVERWRITE option, rename fails if the dst already exists.
   * With OVERWRITE option, rename overwrites the dst, if it is a file 
   * or an empty directory. Rename fails if dst is a non-empty directory.
   * <p>
   * This implementation of rename is atomic.
   * <p>
   * @param src existing file or directory name.
   * @param dst new name.
   * @param options Rename options
   * 
   * @throws AccessControlException If access is denied
   * @throws DSQuotaExceededException If rename violates disk space 
   *           quota restriction
   * @throws FileAlreadyExistsException If <code>dst</code> already exists and
   *           <code>options</options> has {@link Rename#OVERWRITE} option
   *           false.
   * @throws FileNotFoundException If <code>src</code> does not exist
   * @throws NSQuotaExceededException If rename violates namespace 
   *           quota restriction
   * @throws ParentNotDirectoryException If parent of <code>dst</code> 
   *           is not a directory
   * @throws SafeModeException rename not allowed in safemode
   * @throws UnresolvedLinkException If <code>src</code> or
   *           <code>dst</code> contains a symlink
   * @throws SnapshotAccessControlException if path is in RO snapshot
   * @throws IOException If an I/O error occurred
   */
  @AtMostOnce
  public void rename2(String src, String dst, String dstId,
      Options.Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @AtMostOnce
  public DirectorySubTree renameSrcPhase1(String src, String srcId, String dst,
      String dstId) throws IOException;

  @AtMostOnce
  public boolean renameSrcPhase2(long renameId, boolean toCancel)
      throws IOException;

  @AtMostOnce
  public String renameDestPhase1(String src, String srcId, String dst,
      String dstId, DirectorySubTree subTree) throws IOException;

  @AtMostOnce
  public boolean renameDestPhase2(long renameId, String srcId)
      throws IOException;

  public boolean renameRecordExist(long renameId, String srcId, String dstId,
      boolean isSource) throws IOException;

  public String getPoolId() throws IOException;

}
