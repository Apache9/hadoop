package org.apache.hadoop.hdfs.web;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree;
import org.apache.hadoop.fs.viewfs.InodeTree.AbstractINodeDir;
import org.apache.hadoop.fs.viewfs.ChRootedFileSystem;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.fs.viewfs.MergedInodeTree;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystem.InternalDirOfViewFs;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.FsPathBooleanRunner;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.FsPathConnectionRunner;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.FsPathOutputStreamRunner;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.FsPathResponseRunner;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.FsPathRunner;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.OffsetUrlInputStream;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.OffsetUrlOpener;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.UnresolvedUrlOpener;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.ConcatSourcesParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DestinationParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.OldSnapshotNameParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.hdfs.web.resources.RenameOptionSetParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.SnapshotNameParam;
import org.apache.hadoop.hdfs.web.resources.XAttrEncodingParam;
import org.apache.hadoop.hdfs.web.resources.XAttrNameParam;
import org.apache.hadoop.hdfs.web.resources.XAttrSetFlagParam;
import org.apache.hadoop.hdfs.web.resources.XAttrValueParam;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

public class FederatedWebHdfsFileSystem extends WebHdfsFileSystem {
  private URI uri;
  InodeTree<FileSystem> fsState = null; // the fs state; ie the mount table
  final long creationTime;
  final UserGroupInformation ugi;

  public FederatedWebHdfsFileSystem() throws IOException {
    ugi = UserGroupInformation.getCurrentUser();
    creationTime = Time.now();
  }

  public synchronized void initialize(URI uri, Configuration conf)
      throws IOException {
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    renewFsState(conf, uri.getAuthority());
  }

  public void renewFsState(final Configuration conf, final String authority)
      throws IOException {
    try {
      // It's not convenient to create anonymous subclass instance using java
      // reflection, so here we don't use configuration to control the
      // implementation
      // class of InodeTree
      fsState = new MergedInodeTree<FileSystem>(conf, authority) {
        // fsState = new InodeTree<FileSystem>(conf, authority) {

        private URI getWebHDFSUri(URI hdfsUri) throws URISyntaxException {
          if (hdfsUri.getScheme().equals("hdfs")) {
            String uriPath = hdfsUri.getPath();
            if (HAUtil.isLogicalUri(conf, hdfsUri)) {
              URI myUri =
                  new URI(WebHdfsFileSystem.SCHEME + "://"
                      + hdfsUri.getAuthority()
                      + (uriPath == null ? "" : uriPath));
              return myUri;
            } else {
              String rpcAddrKey = null;
              for (Entry<String, String> si : conf) {
                if (si.getValue().equals(hdfsUri.getAuthority())) {
                  rpcAddrKey = si.getKey();
                  break;
                }
              }
              if (rpcAddrKey.contains("rpc-address")) {
                String httpKey =
                    rpcAddrKey.replace("rpc-address", "http-address");
                if (conf.get(httpKey) != null) {
                  URI myUri =
                      new URI(WebHdfsFileSystem.SCHEME + "://"
                          + conf.get(httpKey)
                          + (uriPath == null ? "" : uriPath));
                  return myUri;
                }
              }
            }
          }
          return null;
        }

        @Override
        protected FileSystem getTargetFileSystem(final URI uri)
            throws URISyntaxException, IOException {
          URI myUri = getWebHDFSUri(uri);
          if (myUri != null) {
            ChRootedFileSystem chrootedWebFs =
                new ChRootedFileSystem(myUri, conf);
            return chrootedWebFs;
          }
          return null;
        }

        @Override
        protected FileSystem getTargetFileSystem(
            final AbstractINodeDir<FileSystem> dir) throws URISyntaxException {
          return new InternalDirOfViewFs(dir, creationTime, ugi, uri);
        }

        @Override
        protected FileSystem getTargetFileSystem(URI[] mergeFsURIList)
            throws URISyntaxException, UnsupportedFileSystemException {
          throw new UnsupportedFileSystemException("mergefs not implemented");
          // return MergeFs.createMergeFs(mergeFsURIList, config);
        }
      };
    } catch (URISyntaxException e) {
      throw new IOException("URISyntax exception: " + authority);
    }
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  private String getUriPath(final Path p) {
    checkPath(p);
    String s = makeAbsolute(p).toUri().getPath();
    return s;
  }

  @Override
  public Path getWorkingDirectory() {
    return getHomeDirectory();
  }

  private Path makeAbsolute(final Path f) {
    return f.isAbsolute() ? f : new Path(getWorkingDirectory(), f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.getTargetFileSystem().getFileStatus(res.getRemainingPath());
  }

  @Override
  public AclStatus getAclStatus(Path f) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.getTargetFileSystem().getAclStatus(res.getRemainingPath());
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), false);
    return res.getTargetFileSystem().mkdirs(res.getRemainingPath(), permission);
  }

  public void createSymlink(Path destination, Path f, boolean createParent)
      throws IOException {
    throw new IOException(
        "createSymlink in FederatedWebHdfsFileSystem is not supported");
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    InodeTree.ResolveResult<FileSystem> resSrc =
        fsState.resolve(getUriPath(src), false);
	      
    if (resSrc.isInternalDir()) {
      throw new AccessControlException(
          "rename of internal dir of federated file system is forbidden");
    }
	         
	       InodeTree.ResolveResult<FileSystem> resDst =
        fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
      throw new AccessControlException(
          "rename of internal dir of federated file system is forbidden");
    }
	         
    // Alternate 1: renames within same file system - valid but we disallow
    // Alternate 2: (as described in next para - valid but we have disallowed it
    //
    // Note we compare the URIs. the URIs include the link targets.
    // hence we allow renames across mount links as long as the mount links
    // point to the same target.
    if (!resSrc.getTargetFileSystem().getUri()
        .equals(resDst.getTargetFileSystem().getUri())) {
      throw new IOException("Renames across Mount points not supported");
    }
    return resSrc.getTargetFileSystem().rename(resSrc.getRemainingPath(),
        resDst.getRemainingPath());
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(final Path src, final Path dst,
      final Options.Rename... options) throws IOException {
    InodeTree.ResolveResult<FileSystem> resSrc =
        fsState.resolve(getUriPath(src), false);

    if (resSrc.isInternalDir()) {
      throw new AccessControlException(
          "rename of internal dir of federated file system is forbidden");
    }

    InodeTree.ResolveResult<FileSystem> resDst =
        fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
      throw new AccessControlException(
          "rename of internal dir of federated file system is forbidden");
    }

    // Alternate 1: renames within same file system - valid but we disallow
    // Alternate 2: (as described in next para - valid but we have disallowed it
    //
    // Note we compare the URIs. the URIs include the link targets.
    // hence we allow renames across mount links as long as the mount links
    // point to the same target.
    if (!resSrc.getTargetFileSystem().getUri()
        .equals(resDst.getTargetFileSystem().getUri())) {
      throw new IOException("Renames across Mount points not supported");
    }
    ((WebHdfsFileSystem) resSrc.getTargetFileSystem()).rename(
        resSrc.getRemainingPath(), resDst.getRemainingPath(), options);
  }

  @Override
  public void setXAttr(Path p, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    res.getTargetFileSystem().setXAttr(res.getRemainingPath(), name, value,
        flag);
  }

  @Override
  public byte[] getXAttr(Path p, final String name) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    return res.getTargetFileSystem().getXAttr(res.getRemainingPath(), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path p) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    return res.getTargetFileSystem().getXAttrs(res.getRemainingPath());
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path p, final List<String> names)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    return res.getTargetFileSystem().getXAttrs(res.getRemainingPath(), names);
  }

  @Override
  public List<String> listXAttrs(Path p) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    return res.getTargetFileSystem().listXAttrs(res.getRemainingPath());
  }

  @Override
  public void removeXAttr(Path p, String name) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    res.getTargetFileSystem().removeXAttr(res.getRemainingPath(), name);
  }

  @Override
  public void setOwner(final Path p, final String owner, final String group)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    res.getTargetFileSystem().setOwner(res.getRemainingPath(), owner, group);
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    res.getTargetFileSystem().setPermission(res.getRemainingPath(), permission);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.getTargetFileSystem().modifyAclEntries(res.getRemainingPath(), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.getTargetFileSystem().removeAclEntries(res.getRemainingPath(), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.getTargetFileSystem().removeDefaultAcl(res.getRemainingPath());
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.getTargetFileSystem().removeAcl(res.getRemainingPath());
  }

  @Override
  public void setAcl(final Path p, final List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    res.getTargetFileSystem().setAcl(res.getRemainingPath(), aclSpec);
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    throw new IOException(
        "snapshot operation is not supported on federated file system");
  }

  @Override
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    throw new IOException(
        "snapshot operation is not supported on federated file system");
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    throw new IOException(
        "snapshot operation is not supported on federated file system");
  }

  @Override
  public boolean setReplication(final Path p, final short replication)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    return res.getTargetFileSystem().setReplication(res.getRemainingPath(),
        replication);
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(p), true);
    res.getTargetFileSystem().setTimes(res.getRemainingPath(), mtime, atime);
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public short getDefaultReplication() {
    return (short) getConf().getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  @Override
  public void concat(final Path trg, final Path[] srcs) throws IOException {
    throw new IOException("concat is not supported on federated file system");
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (FileNotFoundException e) {
      throw e;
    }
    assert (res.getRemainingPath() != null);
    return res.getTargetFileSystem().create(res.getRemainingPath(), permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.getTargetFileSystem().append(res.getRemainingPath(), bufferSize,
        progress);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.getRemainingPath().equals(new Path("/"))) {
      throw new AccessControlException(
          "cannot delete entries in mount point table");
    }
    return res.getTargetFileSystem().delete(res.getRemainingPath(), recursive);

  }

  @Override
  public FSDataInputStream open(final Path f, final int buffersize)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.getTargetFileSystem().open(res.getRemainingPath(), buffersize);
  }

  @Override
  public boolean supportFederation() {
    return true;
  }

  @Override
  public boolean isUriCompatible(URI uri, Configuration conf) {
    String authority = uri.getAuthority();
    if (authority == null) {
      // use authority in defaultFs
      authority = getDefaultUri(conf).getAuthority();
    }
    if (authority == null) {
      return false;
    }

    Set<String> mountTables = new HashSet<String>();
    for (Map.Entry<String, String> si : getConf()) {
      final String key = si.getKey();
      if (key.startsWith(Constants.CONFIG_VIEWFS_PREFIX)) {
        String substr =
            key.substring(Constants.CONFIG_VIEWFS_PREFIX.length() + 1);
        String tableName = substr.substring(0, substr.indexOf('.'));
        if (!mountTables.contains(tableName)) {
          mountTables.add(tableName);
        }
      }
    }
    if (mountTables.contains(authority)) {
      return true;
    }
    return false;
  }
}
