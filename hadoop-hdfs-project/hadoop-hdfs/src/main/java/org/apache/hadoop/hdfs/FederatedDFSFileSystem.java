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
import org.apache.hadoop.fs.FilterFileSystem;
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
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFsFileStatus;
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
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class FederatedDFSFileSystem extends DistributedFileSystem {
  private ViewFileSystem viewFs = null;
  private URI uri;
  private static final String TRASH_STRING = ".Trash";
  private static final String TRASH_ROOT = "user";

  public static String getMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }

  public FederatedDFSFileSystem() {
  }

  @Override
  public String getScheme() {
    // will return hdfs
    return super.getScheme();
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    try {
      FederationUtil.confAllNamespace(uri.getAuthority(), conf);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    conf.setClass("fs.viewfs.mount.point.renewer.impl",
        HdfsMountpointRenewer.class, MountpointRenewer.class);
    viewFs = ReflectionUtils.newInstance(ViewFileSystem.class, conf);
    viewFs.initialize(uri, conf);
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path(this.uri.toString(), viewFs.getWorkingDirectory().toUri().getPath());
  }

  @Override
  public void setWorkingDirectory(final Path new_dir) {
    viewFs.setWorkingDirectory(convertToViewFsScheme(new_dir));
  }

  @Override
  public Path getHomeDirectory() {
    return new Path(this.uri.toString(), viewFs.getHomeDirectory().toUri().getPath());
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    return viewFs.append(convertToViewFsScheme(f), bufferSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flag, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    return viewFs.createNonRecursive(convertToViewFsScheme(f), permission, flag,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean setReplication(Path src, short replication)
      throws IOException {
    return viewFs.setReplication(convertToViewFsScheme(src), replication);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  // Not support concat operation across multiple namenodes
  // In that case will throw an WrongFs exception
  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    DistributedFileSystem dfs = getTargetDFS(trg);
    Path targetSrcPaths[] = new Path[psrcs.length];
    for (int i = 0; i < psrcs.length; i++) {
      targetSrcPaths[i] = getTargetPath(psrcs[i]);
    }
    dfs.concat(getTargetPath(trg), targetSrcPaths);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return viewFs.rename(convertToViewFsScheme(src),
        convertToViewFsScheme(dst));
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    return viewFs.create(convertToViewFsScheme(f), permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public HdfsDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress, InetSocketAddress[] favoredNodes)
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      EnumSet<CreateFlag> cflags, int bufferSize, short replication,
      long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
      throws IOException {
    return viewFs.create(convertToViewFsScheme(f), permission, cflags,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  protected HdfsDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException, IOException {
    return viewFs.delete(convertToViewFsScheme(f), recursive);
  }

  @Override
  public boolean delete(Path f, boolean recursive, boolean skipTrash)
      throws IOException {
    // no mater skipTrash true or not, delete directly
    return viewFs.delete(convertToViewFsScheme(f), recursive);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    if (!isTrashPath(f)) {
      return viewFs.getContentSummary(convertToViewFsScheme(f));
    } else {
      Path noSchemaPath = Path.getPathWithoutSchemeAndAuthority(f);
      FileSystem[] childrenFs = viewFs.getChildFileSystems();
      long length = 0;
      long fileCount = 0;
      long directoryCount = 0;
      long quota = 0;
      long spaceConsumed = 0;
      long spaceQuota = 0;
      int excepted = 0;
      IOException lastIoe = null;
      for (FileSystem childFs : childrenFs) {
        try {
          // We should not execute childFs.getContentSummary if the path doesn't exist in childFs.
          if (!childFs.exists(noSchemaPath)) {
            if (lastIoe == null) {
              lastIoe = new FileNotFoundException("File does not exist: " + noSchemaPath);
            }
            excepted++;
            continue;
          }
          ContentSummary oneCs = childFs.getContentSummary(noSchemaPath);
          length += oneCs.getLength();
          fileCount += oneCs.getFileCount();
          directoryCount += oneCs.getDirectoryCount();
          quota += oneCs.getQuota();
          spaceConsumed += oneCs.getSpaceConsumed();
          spaceQuota += oneCs.getSpaceQuota();
        } catch (IOException ioe) {
          // Ignore
          lastIoe = ioe;
          excepted++;
        }
      }
      if (excepted == childrenFs.length) {
        // all children fs excepted
        throw lastIoe;
      }
      return new ContentSummary(length, fileCount, directoryCount, quota,
          spaceConsumed, spaceQuota);
    }
  }

  @Override
  public QuotaSummary getQuotaSummary(Path f) throws IOException {
    boolean internalPath = false;
    DistributedFileSystem dfs = null;
    FileSystem fs = getTargetFileSystem(f);
    if (fs instanceof ViewFileSystem.InternalDirOfViewFs) {
      internalPath = true;
      // if it's internal node, try to resolve to default mountpoint
      fs = getAncestorMountPointFilesystem(f);
      if (fs == null) {
        throw new IOException("Can't get quota of internal path in mounttable");
      }
    }

    Path targetPath = getTargetPath(f);
    if (internalPath) {
      targetPath = new Path(fs.getUri().getPath(), f.toUri().getPath());
    }

    FileSystem rfs = ((FilterFileSystem) fs).getRawFileSystem();
    if (rfs instanceof DistributedFileSystem) {
      dfs = (DistributedFileSystem)rfs;
    } else {
      throw new IOException("Can't get DistributedFileSystem instance for path: " + f);
    }

    try {
      return dfs.getQuotaSummary(targetPath);
    } catch (FileNotFoundException e) {
      if (internalPath) {
        throw new IOException("Can't get quota of internal path in mounttable");
      }
      throw e;
    }
  }

  @Override
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota)
      throws IOException {
    boolean internalPath = false;
    DistributedFileSystem dfs = null;
    FileSystem fs = getTargetFileSystem(src);
    if (fs instanceof ViewFileSystem.InternalDirOfViewFs) {
      internalPath = true;
      // if it's internal node, try to resolve to default mountpoint
      fs = getAncestorMountPointFilesystem(src);
      if (fs == null) {
        throw new IOException("Can't get quota of internal path in mounttable");
      }
    }

    Path targetPath = getTargetPath(src);
    if (internalPath) {
      targetPath = new Path(fs.getUri().getPath(), src.toUri().getPath());
    }

    FileSystem rfs = ((FilterFileSystem) fs).getRawFileSystem();
    if (rfs instanceof DistributedFileSystem) {
      dfs = (DistributedFileSystem)rfs;
    } else {
      throw new IOException("Can't get DistributedFileSystem instance for path: " + src);
    }

    try {
      dfs.setQuota(targetPath, namespaceQuota, diskspaceQuota);
    } catch (FileNotFoundException e) {
      if (internalPath) {
        throw new IOException("Can't get quota of internal path in mounttable");
      }
      throw e;
    }
  }

  private boolean isTrashPath(Path p) {
    return p.toString().contains(TRASH_STRING)
        && p.toString().contains(TRASH_ROOT);
  }

  @Override
  public FileStatus[] listStatus(Path p)
      throws FileNotFoundException, IOException {
    if (!isTrashPath(p)) {
      FileStatus[] fsList = viewFs.listStatus(convertToViewFsScheme(p));
      FileStatus[] res = new FileStatus[fsList.length];
      for (int i = 0; i < fsList.length; i++) {
        if (fsList[i] instanceof ViewFsFileStatus) {
          ViewFsFileStatus vfs = (ViewFsFileStatus) fsList[i];
          Path realPath = new Path(vfs.getPath().toUri().getPath());
          res[i] = makeFileStatusQualified(vfs.getRawFileStatus(), realPath);
        } else {
          FileStatus status = fsList[i];
          URI fileUri = status.getPath().toUri();
          // InternalDirOfViewFs may return FileStatus with viewfs scheme
          if (!fileUri.getScheme().equals("hdfs")) {
            res[i] =
                makeFileStatusQualified(status, new Path(fileUri.getPath()));
          } else {
            res[i] = fsList[i];
          }
        }
      }
      return res;
    } else {
      Path noSchemaPath = Path.getPathWithoutSchemeAndAuthority(p);
      FileSystem[] childrenFs = viewFs.getChildFileSystems();
      List<FileStatus> fsList = new LinkedList<FileStatus>();
      for (FileSystem childFs : childrenFs) {
        FileStatus[] oneRes = null;
        try {
          oneRes = childFs.listStatus(noSchemaPath);
        } catch (IOException ioe) {
          // Ignore exception in one fs
        }
        if (oneRes != null && oneRes.length > 0) {
          for (FileStatus s : oneRes) {
            boolean exist = false;
            for (FileStatus existItem : fsList) {
              if (existItem.getPath().getName().equals(s.getPath().getName())) {
                exist = true;
                break;
              }
            }
            if (!exist) {
              // remove the namespace prefix
              s.setPath(new Path(s.getPath().toUri().getPath()));
              fsList.add(s);
            }
          }
        }
      }
      FileStatus[] res = fsList.toArray(new FileStatus[0]);
      Arrays.sort(res);
      return res;
    }
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter) throws FileNotFoundException, IOException {
    // copy the default implementation of FileSystem class
    return new RemoteIterator<LocatedFileStatus>() {
      private final FileStatus[] stats = listStatus(f, filter);
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i < stats.length;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException("No more entry in " + f);
        }
        FileStatus result = stats[i++];
        BlockLocation[] locs = result.isFile()
            ? getFileBlockLocations(result.getPath(), 0, result.getLen())
            : null;
        return new LocatedFileStatus(result, locs);
      }
    };
  }

  @Override
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(final Path f)
      throws AccessControlException, FileNotFoundException, IOException {
    return delete(f, true);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus fs, long start,
      long len) throws IOException {
    FileStatus vStatus = new FileStatus(fs);
    vStatus.setPath(convertToViewFsScheme(fs.getPath()));
    return viewFs.getFileBlockLocations(vStatus, start, len);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    return viewFs.getFileBlockLocations(convertToViewFsScheme(p), start, len);
  }

  @Override
  public BlockStorageLocation[] getFileBlockStorageLocations(
      List<BlockLocation> blocks) throws IOException,
      UnsupportedOperationException, InvalidBlockTokenException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    // this call will affect all namespace
    viewFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean recoverLease(Path f) throws IOException {
    DistributedFileSystem dfs = getTargetDFS(f);
    return dfs.recoverLease(getTargetPath(f));
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    // this call will affect all namespace
    viewFs.setWriteChecksum(writeChecksum);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return viewFs.open(convertToViewFsScheme(f), bufferSize);
  }

  @Override
  public FSDataInputStream openEx(Path f, int bufferSize) throws IOException {
    return viewFs.openEx(convertToViewFsScheme(f), bufferSize);
  }

  @Override
  public boolean mkdirs(final Path dir, final FsPermission permission)
      throws IOException {
    return viewFs.mkdirs(convertToViewFsScheme(dir), permission);
  }

  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void close() throws IOException {
    try {
      if (viewFs != null) {
        viewFs.close();
      }
    } finally {
      super.close();
    }
  }

  @Override
  public DFSClient getClient() {
    return null;
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    if (p != null) {
      return viewFs.getStatus(convertToViewFsScheme(p));
    }

    FileSystem[] childFs = viewFs.getChildFileSystems();
    if (childFs.length == 0) {
      throw new IOException("MountTable is empty!");
    }
    return childFs[0].getStatus(p);
  }

  @Override
  public DiskStatus getDiskStatus() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public long getRawCapacity() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public long getRawUsed() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public long getMissingBlocksCount() throws IOException {
    long res = 0;
    for (FileSystem fs : viewFs.getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        res += ((DistributedFileSystem)fs).getMissingBlocksCount();
      }
    }
    return res;
  }

  @Override
  public long getUnderReplicatedBlocksCount() throws IOException {
    long res = 0;
    for (FileSystem fs : viewFs.getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        res += ((DistributedFileSystem)fs).getUnderReplicatedBlocksCount();
      }
    }
    return res;
  }

  @Override
  public long getCorruptBlocksCount() throws IOException {
    long res = 0;
    for (FileSystem fs : viewFs.getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        res += ((DistributedFileSystem)fs).getCorruptBlocksCount();
      }
    }
    return res;
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
      throws IOException {
    return viewFs.listCorruptFileBlocks(convertToViewFsScheme(path));
  }

  @Override
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return this.getDataNodeStats(HdfsConstants.DatanodeReportType.ALL);
  }

  @Override
  public DatanodeInfo[] getDataNodeStats(HdfsConstants.DatanodeReportType type)
      throws IOException {
    for (FileSystem fs : viewFs.getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        return ((DistributedFileSystem) fs).getDataNodeStats(type);
      }
    }
    throw new IOException(
        "this operation is not supported on non DistributedFileSystem: "
            + getMethodName());
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    return false;
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public long rollEdits() throws AccessControlException, IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void refreshNodes() throws IOException {
    FileSystem[] childrenFs = viewFs.getChildFileSystems();
    for (FileSystem childFs : childrenFs) {
      if(childFs instanceof DistributedFileSystem){
        DistributedFileSystem childDFS = (DistributedFileSystem)childFs;
        childDFS.refreshNodes();
      }
    }
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void refreshTopology() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void metaSave(String pathname) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    FileSystem[] fsList = viewFs.getChildFileSystems();
    if (fsList.length > 0) {
      return fsList[0].getServerDefaults();
    } else {
      return viewFs.getServerDefaults();
    }
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return viewFs.getServerDefaults(convertToViewFsScheme(p));
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    return viewFs.resolvePath(convertToViewFsScheme(p));
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (!isTrashPath(f)) {
      ViewFsFileStatus status =
          (ViewFsFileStatus) viewFs.getFileStatus(convertToViewFsScheme(f));
      Path realPath = new Path(status.getPath().toUri().getPath());
      return makeFileStatusQualified(status.getRawFileStatus(), realPath);
    } else {
      FileSystem[] childrenFs = viewFs.getChildFileSystems();
      Path noSchemaPath = Path.getPathWithoutSchemeAndAuthority(f);
      IOException lastIoe = null;
      for (FileSystem childFs : childrenFs) {
        try {
          // If any childfs contains the item, return it
          FileStatus st = childFs.getFileStatus(noSchemaPath);
          st.setPath(new Path(st.getPath().toUri().getPath()));
          return st;
        } catch (IOException ioe) {
          // Ignore
          lastIoe = ioe;
        }
      }
      throw lastIoe;
    }
  }

  private FileStatus makeFileStatusQualified(FileStatus status) {
    return makeFileStatusQualified(status, status.getPath());
  }

  private FileStatus makeFileStatusQualified(FileStatus status, Path path) {
    Path p = path.makeQualified(this.getUri(), this.getWorkingDirectory());
    status.setPath(p);
    return status;
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, AccessControlException, IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public boolean supportsSymlinks() {
    return false;
  }

  @Override
  public FileStatus getFileLinkStatus(Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, AccessControlException, IOException {
    return getFileStatus(f);
  }

  @Override
  public Path getLinkTarget(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return viewFs.getLinkTarget(convertToViewFsScheme(f));
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return viewFs.getFileChecksum(convertToViewFsScheme(f));
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void setPermission(Path p, FsPermission permission)
      throws IOException {
    viewFs.setPermission(convertToViewFsScheme(p), permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    viewFs.setOwner(convertToViewFsScheme(p), username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    viewFs.setTimes(convertToViewFsScheme(p), mtime, atime);
  }

  @Override
  protected int getDefaultPort() {
    return -1;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    return null;
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    for (FileSystem fs : viewFs.getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        // the setBalancerBandwidth will made the namenode update bandwidth
        // on all datanodes by heartbeat command, so only need call once
        ((DistributedFileSystem) fs).setBalancerBandwidth(bandwidth);
        return;
      }
    }
    throw new IOException(
        "this operation is not supported on non DistributedFileSystem: "
            + getMethodName());
  }

  @Override
  public String getCanonicalServiceName() {
    return null;
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return uri;
  }

  @Override
  public boolean isInSafeMode() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void allowSnapshot(Path path) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void disallowSnapshot(Path path) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void deleteSnapshot(Path snapshotDir, String snapshotName)
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(Path snapshotDir,
      String fromSnapshot, String toSnapshot) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public boolean isFileClosed(Path src) throws IOException {
    DistributedFileSystem dfs = getTargetDFS(src);
    return dfs.isFileClosed(getTargetPath(src));
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo info) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo info) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void removeCachePool(String poolName) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    viewFs.modifyAclEntries(convertToViewFsScheme(path), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    viewFs.removeAclEntries(convertToViewFsScheme(path), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    viewFs.removeDefaultAcl(convertToViewFsScheme(path));
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    viewFs.removeAcl(convertToViewFsScheme(path));
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    viewFs.setAcl(convertToViewFsScheme(path), aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return viewFs.getAclStatus(convertToViewFsScheme(path));
  }

  @Override
  public void createEncryptionZone(Path path, String keyName)
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public EncryptionZone getEZForPath(Path path) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    viewFs.setXAttr(convertToViewFsScheme(path), name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return viewFs.getXAttr(convertToViewFsScheme(path), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return viewFs.getXAttrs(convertToViewFsScheme(path));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    return viewFs.getXAttrs(convertToViewFsScheme(path), names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return viewFs.listXAttrs(convertToViewFsScheme(path));
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public void access(Path path, FsAction mode)
      throws AccessControlException, IOException {
    viewFs.access(convertToViewFsScheme(path), mode);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
      throws IOException {
    return viewFs.addDelegationTokens(renewer, credentials);
  }

  @Override
  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    throw new IOException(
        "this operation is not supported on" + " FederatedDFSFileSystem: " + getMethodName());
  }

  @Override
  public boolean supportRaid() {
    return false;
  }

  @Override
  public boolean isDistributedFileSystem() {
    return true;
  }

  @Override
  public FileSystem getDistributedFileSystem() {
    return this;
  }

  @Override
  public long getDefaultBlockSize() {
    FileSystem[] fsList = viewFs.getChildFileSystems();
    if (fsList.length > 0) {
      return fsList[0].getDefaultBlockSize();
    } else {
      return viewFs.getDefaultBlockSize();
    }
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return viewFs.getDefaultBlockSize(convertToViewFsScheme(f));
  }

  @Override
  public short getDefaultReplication() {
    FileSystem[] fsList = viewFs.getChildFileSystems();
    if (fsList.length > 0) {
      return fsList[0].getDefaultReplication();
    } else {
      return viewFs.getDefaultReplication();
    }
  }

  @Override
  public short getDefaultReplication(Path f) {
    return viewFs.getDefaultReplication(convertToViewFsScheme(f));
  }

  public FileSystem getAncestorMountPointFilesystem(Path path) throws IOException {
    FileSystem fs = null;
    Path ancestorPath = path.getParent();
    while (ancestorPath != null) {
      FileSystem resFs = getTargetFileSystem(ancestorPath);
      if (resFs instanceof ViewFileSystem.InternalDirOfViewFs) {
        ancestorPath = ancestorPath.getParent();
        continue;
      }
      fs = resFs;
      break;
    }
    return fs;
  }

  public FileSystem getTargetFileSystem(Path path) throws IOException {
    return viewFs.getTargetFileSystem(convertToViewFsScheme(path));
  }

  private DistributedFileSystem getTargetDFS(Path path) throws IOException {
    FileSystem fs = getTargetFileSystem(path);
    fs = ((FilterFileSystem) fs).getRawFileSystem();
    if (fs instanceof DistributedFileSystem) {
      return (DistributedFileSystem)fs;
    }
    throw new IOException("Can't get DistributedFileSystem instance for path: " + path);
  }

  public Path getTargetPath(Path path) throws IOException {
    return viewFs.getTargetPath(convertToViewFsScheme(path));
  }

  @Override
  public boolean supportFederation() {
    return true;
  }

  @Override
  public boolean isUriCompatible(URI uri, Configuration conf) {
    return HAUtil.isFederationUri(conf, uri);
  }

  Path convertToViewFsScheme(Path p) {
    URI uri = p.toUri();
    if (uri.getScheme() != null && uri.getScheme().equals("hdfs")) {
      // for the path without authority, like hdfs:///foo/bar
      // should convert to path with default authority first
      uri = makeQualified(p).toUri();
      try {
        return new Path(new URI("viewfs", uri.getUserInfo(), uri.getHost(),
            uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()));
      } catch (URISyntaxException e) {
        // ignore
      }
    }
    return p;
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    return viewFs.getChildFileSystems();
  }
}
