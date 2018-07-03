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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.ViewFs;
import org.apache.hadoop.fs.viewfs.ViewFsFileStatus;
import org.apache.hadoop.hdfs.FederationConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MountPointRenewer;
import org.apache.hadoop.hdfs.MountPointRenewer.RenewMpt;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.zookeeper.KeeperException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Support federation directly on hdfs scheme It's an federation version of
 * class Hdfs, will replace class Hdfs on federation cluster. The corresponding
 * implementation of FileSystem subclass is FederatedDFSFileSystem
 *
 * Since the AbstractFileSystem is used in FileContext, we don't need to care
 * about the usage of instanceof, this class simply extends AbstractFileSystem
 * class (unlike the corresponding FederatedDFSFileSystem, which extends
 * DistributedFileSystem)
 */

public class FederatedHdfs extends AbstractFileSystem {
  private ViewFs viewFs;

  static {
    HdfsConfiguration.init();
  }

  public FederatedHdfs(final URI theUri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, HdfsConstants.HDFS_URI_SCHEME, false, -1);
    if (!isUriCompatible(theUri, conf)) {
      throw new URISyntaxException(theUri.toString(), "not an federation uri");
    }
    URI viewFsUri = convertToViewFsScheme(theUri);
    MountPointRenewer.updateMptFromZkOnce(theUri.getAuthority(), conf);
    viewFs = AbstractFileSystem.newInstance(ViewFs.class, viewFsUri, conf);
    viewFs.setRenewCheckerCb(new ViewFs.FsStateRenewChecker() {
      private String lastMountPointTable = null;
      private long lastCheckTime = Time.monotonicNow();
      @Override
      synchronized public boolean shouldRenew() {
        // Since the creation of this class is random, do not need another
        // random
        long checkInterval =
            conf.getLong(FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL,
                FederationConfigKeys.FEDFS_MOUNT_TABLE_RENEW_INTERVAL_DEFAULT);
        long now = Time.monotonicNow();
        if (now - lastCheckTime > checkInterval) {
          lastCheckTime = now;
          try {
            String mptFromZk = MountPointRenewer
                .getMptConfFromZookeeper(theUri.getAuthority(), conf);
            if (lastMountPointTable == null || !mptFromZk
                .equals(lastMountPointTable)) {
              lastMountPointTable = mptFromZk;
              return true;
            }
          } catch (Exception e) {
            LOG.warn("failed get mountpoint from zk", e);
          }
        }
        return false;
      }

      @Override
      public void updateMountponitConfig(Configuration conf) {
        try {
          MountPointRenewer.updateMountPointConfig(conf, lastMountPointTable,
              theUri.getAuthority());
        } catch (IOException e) {
          LOG.warn("update configuration failed.", e);
        }
      }
    });
  }

  @Override
  public int getUriDefaultPort() {
    return NameNode.DEFAULT_PORT;
  }
  
  @Override
  public FSDataOutputStream createInternal(Path f,
      EnumSet<CreateFlag> createFlag, FsPermission absolutePermission,
      int bufferSize, short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt, boolean createParent)
      throws IOException {
    return viewFs.createInternal(convertToViewFsScheme(f), createFlag,
        absolutePermission, bufferSize, replication, blockSize, progress,
        checksumOpt, createParent);
  }

  @Override
  public boolean delete(Path f, boolean recursive)
      throws IOException, UnresolvedLinkException {
    return viewFs.delete(convertToViewFsScheme(f), recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException, UnresolvedLinkException {
    return viewFs.getFileBlockLocations(convertToViewFsScheme(p), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f)
      throws IOException, UnresolvedLinkException {
    return viewFs.getFileChecksum(convertToViewFsScheme(f));
  }

  @Override
  public FileStatus getFileStatus(Path f)
      throws IOException, UnresolvedLinkException {
    ViewFsFileStatus vstatus =
        (ViewFsFileStatus) viewFs.getFileStatus(convertToViewFsScheme(f));
    Path realPath = new Path(vstatus.getPath().toUri().getPath());
    return makeFileStatusQualified(vstatus.getRawFileStatus(), realPath);
  }

  private FileStatus makeFileStatusQualified(FileStatus status) {
    return makeFileStatusQualified(status, status.getPath());
  }

  private FileStatus makeFileStatusQualified(FileStatus status, Path path) {
          Path p = path.makeQualified(this.getUri(), null);
    status.setPath(p);
    return status;
  }

  @Override
  public FileStatus getFileLinkStatus(Path f)
      throws IOException, UnresolvedLinkException {
    return viewFs.getFileLinkStatus(convertToViewFsScheme(f));
  }

  @Override
  public FsStatus getFsStatus() throws IOException {
    AbstractFileSystem[] fsList = viewFs.getChildFileSystems();
    if (fsList.length > 0) {
      return fsList[0].getFsStatus();
    } else {
      return viewFs.getFsStatus();
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    AbstractFileSystem[] fsList = viewFs.getChildFileSystems();
    if (fsList.length > 0) {
      return fsList[0].getServerDefaults();
    } else {
      return viewFs.getServerDefaults();
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p)
      throws FileNotFoundException, IOException {
    return viewFs.listLocatedStatus(convertToViewFsScheme(p));
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    final RemoteIterator<FileStatus> fsIter =
        viewFs.listStatusIterator(convertToViewFsScheme(f));
    return new RemoteIterator<FileStatus>() {
      final RemoteIterator<FileStatus> myIter;
      { // Init
        myIter = fsIter;
      }

      @Override
      public boolean hasNext() throws IOException {
        return myIter.hasNext();
      }

      @Override
      public FileStatus next() throws IOException {
        FileStatus status = myIter.next();
        if (status instanceof ViewFsFileStatus) {
          ViewFsFileStatus vstatus = (ViewFsFileStatus) status;
          Path realPath = new Path(vstatus.getPath().toUri().getPath());
          return makeFileStatusQualified(vstatus.getRawFileStatus(), realPath);
        }
        return status;
      }
    };
  }

  @Override
  public FileStatus[] listStatus(Path f)
      throws IOException, UnresolvedLinkException {
    FileStatus[] fsList = viewFs.listStatus(convertToViewFsScheme(f));
    FileStatus[] res = new FileStatus[fsList.length];
    for (int i = 0; i < fsList.length; i++) {
      if (fsList[i] instanceof ViewFsFileStatus) {
        ViewFsFileStatus vstatus = (ViewFsFileStatus) fsList[i];
        Path realPath = new Path(vstatus.getPath().toUri().getPath());
        res[i] = makeFileStatusQualified(vstatus.getRawFileStatus(), realPath);
      } else {
        res[i] = fsList[i];
      }
    }
    return res;
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
      throws IOException, UnresolvedLinkException {
    viewFs.mkdir(convertToViewFsScheme(dir), permission, createParent);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException, UnresolvedLinkException {
    return viewFs.open(convertToViewFsScheme(f), bufferSize);
  }

  @Override
  public void renameInternal(Path src, Path dst)
      throws IOException, UnresolvedLinkException {
    viewFs.renameInternal(convertToViewFsScheme(src),
        convertToViewFsScheme(dst));
  }

  @Override
  public void renameInternal(Path src, Path dst, boolean overwrite)
      throws IOException, UnresolvedLinkException {
    viewFs.renameInternal(convertToViewFsScheme(src),
        convertToViewFsScheme(dst), overwrite);
  }

  @Override
  public void setOwner(Path f, String username, String groupname)
      throws IOException, UnresolvedLinkException {
    viewFs.setOwner(convertToViewFsScheme(f), username, groupname);
  }

  @Override
  public void setPermission(Path f, FsPermission permission)
      throws IOException, UnresolvedLinkException {
    viewFs.setPermission(convertToViewFsScheme(f), permission);
  }

  @Override
  public boolean setReplication(Path f, short replication)
      throws IOException, UnresolvedLinkException {
    return viewFs.setReplication(convertToViewFsScheme(f), replication);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime)
      throws IOException, UnresolvedLinkException {
    viewFs.setTimes(convertToViewFsScheme(f), mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) throws IOException {
    viewFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws IOException, UnresolvedLinkException {
    viewFs.createSymlink(convertToViewFsScheme(target),
        convertToViewFsScheme(link), createParent);
  }

  @Override
  public Path getLinkTarget(Path p) throws IOException {
    return viewFs.getLinkTarget(convertToViewFsScheme(p));
  }

  @Override // AbstractFileSystem
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    return viewFs.getDelegationTokens(renewer);
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
    viewFs.removeXAttr(convertToViewFsScheme(path), name);
  }

  @Override
  public void access(Path path, final FsAction mode) throws IOException {
    viewFs.access(convertToViewFsScheme(path), mode);
  }

  @Override
  public Path resolvePath(final Path f) throws FileNotFoundException,
          AccessControlException, UnresolvedLinkException, IOException {
    return viewFs.resolvePath(convertToViewFsScheme(f));
  }

  URI convertToViewFsScheme(URI p) throws URISyntaxException {
    String hdfsScheme = "hdfs";
    if (p.getScheme() != null && p.getScheme().equals(hdfsScheme)) {
      return new URI("viewfs" + p.toString().substring(hdfsScheme.length()));
    }
    return p;
  }

  Path convertToViewFsScheme(Path p) {
    try {
      return new Path(convertToViewFsScheme(p.toUri()));
    } catch (URISyntaxException e) {
      return p;
    }
  }

  public boolean isUriCompatible(URI uri, Configuration config) {
    return HAUtil.isFederationUri(config, uri);
  }
}
