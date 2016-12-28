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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.QuotaSummary;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import javax.naming.OperationNotSupportedException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class FedeatedDFSFileSystem extends DistributedFileSystem {
  private ViewFileSystem viewFs = null;

  public FedeatedDFSFileSystem() {
  }

  @Override
  public String getScheme() {
    // will return hdfs
    return super.getScheme();
  }

  @Override
  public URI getUri() {
    return viewFs.getUri();
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    viewFs = new ViewFileSystem(conf);
    viewFs.initialize(uri, conf);
  }

  @Override
  public Path getWorkingDirectory() {
    return viewFs.getWorkingDirectory();
  }

  @Override
  public void setWorkingDirectory(final Path new_dir) {
    viewFs.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getHomeDirectory() {
    return viewFs.getHomeDirectory();
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    return viewFs.append(f, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f,
      FsPermission permission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress)
      throws IOException {
    return viewFs
        .createNonRecursive(f, permission, flag, bufferSize, replication,
            blockSize, progress);
  }

  @Override
  public boolean setReplication(Path src, short replication)
      throws IOException {
    return viewFs.setReplication(src, replication);
  }

  @Override public void setStoragePolicy(Path src, String policyName)
      throws IOException {
    throw new IOException("this operation is not supported on" +
    " FederatedDFSFileSystem");
  }

  @Override public BlockStoragePolicy[] getStoragePolicies()
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void concat(Path trg, Path[] psrcs) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return viewFs.rename(src, dst);
  }

  @Override public void rename(Path src, Path dst, Options.Rename... options)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    return viewFs.create(f, permission,
         overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override public HdfsDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress, InetSocketAddress[] favoredNodes)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public FSDataOutputStream create(Path f, FsPermission permission,
      EnumSet<CreateFlag> cflags, int bufferSize, short replication,
      long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override protected HdfsDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException,
      IOException {
    return viewFs.delete(f, recursive);
  }

  @Override public boolean delete(Path f, boolean recursive, boolean skipTrash)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return viewFs.getContentSummary(f);
  }

  @Override public QuotaSummary getQuotaSummary(Path f) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void setQuota(Path src, long namespaceQuota,
      long diskspaceQuota) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public FileStatus[] listStatus(Path p)
      throws FileNotFoundException, IOException {
    return viewFs.listStatus(p);
  }

  @Override protected RemoteIterator<LocatedFileStatus> listLocatedStatus(
      Path p, PathFilter filter) throws FileNotFoundException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean mkdir(Path f, FsPermission permission)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(final Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
      return delete(f, true);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus fs,
      long start, long len) throws IOException {
    return viewFs.getFileBlockLocations(fs, start, len);
  }

  @Override public BlockLocation[] getFileBlockLocations(Path p, long start,
      long len) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public BlockStorageLocation[] getFileBlockStorageLocations(
      List<BlockLocation> blocks)
      throws IOException, UnsupportedOperationException,
      InvalidBlockTokenException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    // this call will affect all namespace
    viewFs.setVerifyChecksum(verifyChecksum);
  }

  @Override public boolean recoverLease(Path f) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void setWriteChecksum(boolean writeChecksum) {
    // this call will affect all namespace
    viewFs.setWriteChecksum(writeChecksum);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    return viewFs.open(f, bufferSize);
  }

  @Override public FSDataInputStream openEx(Path f, int bufferSize)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public boolean mkdirs(final Path dir, final FsPermission permission)
      throws IOException {
   return  viewFs.mkdirs(dir, permission);
  }

  @Override protected boolean primitiveMkdir(Path f,
      FsPermission absolutePermission) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void close() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public DFSClient getClient() {
    return null;
  }

  @Override public FsStatus getStatus(Path p) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public DiskStatus getDiskStatus() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long getRawCapacity() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long getRawUsed() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long getMissingBlocksCount() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long getUnderReplicatedBlocksCount() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long getCorruptBlocksCount() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public RemoteIterator<Path> listCorruptFileBlocks(Path path)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public DatanodeInfo[] getDataNodeStats() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public DatanodeInfo[] getDataNodeStats(
      HdfsConstants.DatanodeReportType type) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void saveNamespace()
      throws AccessControlException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long rollEdits() throws AccessControlException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void refreshNodes() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void refreshTopology() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void finalizeUpgrade() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void metaSave(String pathname) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    // using the implementation of DistributedFileSystem
    // ViewFileSystem didn't implement this interface
    return super.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p)
      throws IOException {
    return viewFs.getServerDefaults(p);
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    return viewFs.resolvePath(p);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return viewFs.getFileStatus(f);
  }

  @Override public void createSymlink(Path target, Path link,
      boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, AccessControlException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean supportsSymlinks() {
    return false;
  }

  @Override public FileStatus getFileLinkStatus(Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, AccessControlException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public Path getLinkTarget(Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override protected Path resolveLink(Path f) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return viewFs.getFileChecksum(f);
  }

  @Override public FileChecksum getFileChecksum(Path f, long length)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public void setPermission(Path p, FsPermission permission)
      throws IOException {
    viewFs.setPermission(p, permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    viewFs.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime)
      throws IOException {
    viewFs.setTimes(p, mtime, atime);
  }

  @Override protected int getDefaultPort() {
    return -1;
  }

  @Override public Token<DelegationTokenIdentifier> getDelegationToken(
      String renewer) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void setBalancerBandwidth(long bandwidth)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public String getCanonicalServiceName() {
    return null;
  }

  @Override protected URI canonicalizeUri(URI uri) {
    return null;
  }

  @Override public boolean isInSafeMode() throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void allowSnapshot(Path path) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void disallowSnapshot(Path path) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void deleteSnapshot(Path snapshotDir, String snapshotName)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public SnapshotDiffReport getSnapshotDiffReport(Path snapshotDir,
      String fromSnapshot, String toSnapshot) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean isFileClosed(Path src) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long addCacheDirective(CacheDirectiveInfo info)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public long addCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void modifyCacheDirective(CacheDirectiveInfo info)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void modifyCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void removeCacheDirective(long id) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void addCachePool(CachePoolInfo info) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void modifyCachePool(CachePoolInfo info) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void removeCachePool(String poolName) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public RemoteIterator<CachePoolEntry> listCachePools()
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    viewFs.modifyAclEntries(path, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    viewFs.removeAclEntries(path, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    viewFs.removeDefaultAcl(path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    viewFs.removeAcl(path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec)
      throws IOException {
    viewFs.setAcl(path, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return viewFs.getAclStatus(path);
  }

  @Override public void createEncryptionZone(Path path, String keyName)
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public EncryptionZone getEZForPath(Path path) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    viewFs.setXAttr(path, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return viewFs.getXAttr(path, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return viewFs.getXAttrs(path);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    return viewFs.getXAttrs(path, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return viewFs.listXAttrs(path);
  }

  @Override public void removeXAttr(Path path, String name) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public void access(Path path, FsAction mode)
      throws AccessControlException, IOException {
    viewFs.access(path, mode);
  }

  @Override public Token<?>[] addDelegationTokens(String renewer,
      Credentials credentials) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public DFSInotifyEventInputStream getInotifyEventStream()
      throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public DFSInotifyEventInputStream getInotifyEventStream(
      long lastReadTxid) throws IOException {
    throw new IOException("this operation is not supported on" +
        " FederatedDFSFileSystem");
  }

  @Override public boolean supportRaid() {
    return false;
  }

  @Override public boolean isDistributedFileSystem() {
    return true;
  }

  @Override public FileSystem getDistributedFileSystem() {
    return null;
  }

  @Override
  public long getDefaultBlockSize() {
    // TODO:implement in the DFS logic
    return viewFs.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return viewFs.getDefaultBlockSize(f);
  }

  @Override
  public short getDefaultReplication() {
    // TODO:implement in the DFS logic
    return viewFs.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path f) {
    return viewFs.getDefaultReplication(f);
  }

  public FileSystem getTargetFileSystem(Path path) throws IOException {
      return viewFs.getTargetFileSystem(path);
  }
}
