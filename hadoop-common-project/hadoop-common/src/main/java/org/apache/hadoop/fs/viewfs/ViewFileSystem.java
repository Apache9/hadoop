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
package org.apache.hadoop.fs.viewfs;

import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

/**
 * ViewFileSystem (extends the FileSystem interface) implements a client-side
 * mount table. Its spec and implementation is identical to {@link ViewFs}.
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFileSystem extends FileSystem {

  private static final Path ROOT_PATH = new Path(Path.SEPARATOR);

  static AccessControlException readOnlyMountTable(final String operation,
      final String p) {
    return new AccessControlException( 
        "InternalDir of ViewFileSystem is readonly; operation=" + operation + 
        "Path=" + p);
  }
  static AccessControlException readOnlyMountTable(final String operation,
      final Path p) {
    return readOnlyMountTable(operation, p.toString());
  }

  static public class Key {
    final String scheme;
    final String authority;

    public Key(URI uri) {
      scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
      authority =
          uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
    }

    @Override
    public int hashCode() {
      return (scheme + authority).hashCode();
    }

    static boolean isEqual(Object a, Object b) {
      return a == b || (a != null && a.equals(b));
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj != null && obj instanceof Key) {
        Key that = (Key) obj;
        return isEqual(this.scheme, that.scheme) && isEqual(this.authority,
            that.authority);
      }
      return false;
    }
  }

  static public class UnsafeCache {
    HashMap<Key, FileSystem> map = new HashMap<Key, FileSystem>();

    public FileSystem get(URI uri, Configuration config) throws IOException {
      Key key = new Key(uri);
      if (map.get(key) == null) {
        FileSystem fs =
            FileSystem.createFileSystemWithConfigurationService(uri, config);
        map.put(key, fs);
        return fs;
      } else {
        return map.get(key);
      }
    }

    public FileSystem[] getAll() {
      return map.values().toArray(new FileSystem[0]);
    }

    public FileSystem[] removeAll() {
      FileSystem[] res = map.values().toArray(new FileSystem[0]);
      map.clear();
      return res;
    }

    public void closeAll() {
      for (FileSystem fs : getAll()) {
        try {
          fs.close();
        } catch (IOException e) {
          LOG.debug("close fs failed.", e);
        }
      }
    }
  }

  static public class MountPoint {
    private Path src;       // the src of the mount
    private URI[] targets; //  target of the mount; Multiple targets imply mergeMount
    MountPoint(Path srcPath, URI[] targetURIs) {
      src = srcPath;
      targets = targetURIs;
    }
    Path getSrc() {
      return src;
    }
    URI[] getTargets() {
      return targets;
    }
  }

  final long creationTime; // of the the mount table
  final UserGroupInformation ugi; // the user/group of user who created mtable
  URI myUri;
  private Path workingDir;
  Configuration config;
  InodeTree<FileSystem> fsState;  // the fs state; ie the mount table
  Path homeDir = null;
  private ReentrantReadWriteLock fsStateLock = new ReentrantReadWriteLock();
  private String clusterName;
  private MountpointRenewer mountpointRenewer;
  private boolean useVSCache = true;
  private UnsafeCache cache = new UnsafeCache();

  /**
   * Make the path Absolute and get the path-part of a pathname.
   * Checks that URI matches this file system 
   * and that the path-part is a valid name.
   * 
   * @param p path
   * @return path-part of the Path p
   */
  private String getUriPath(final Path p) {
    checkPath(p);
    String s = makeAbsolute(p).toUri().getPath();
    return s;
  }
  
  private Path makeAbsolute(final Path f) {
    return f.isAbsolute() ? f : new Path(workingDir, f);
  }
  
  /**
   * This is the  constructor with the signature needed by
   * {@link FileSystem#createFileSystem(URI, Configuration)}
   * 
   * After this constructor is called initialize() is called.
   * @throws IOException 
   */
  public ViewFileSystem() throws IOException {
    ugi = UserGroupInformation.getCurrentUser();
    creationTime = Time.now();
  }

  @VisibleForTesting
  public FileSystem[] getCachedFileSystems() {
    try {
      fsStateLock.readLock().lock();
      if (cache == null) {
        return new FileSystem[0];
      }
      return cache.getAll();
    } finally {
      fsStateLock.readLock().unlock();
    }
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>viewfs</code>
   */
  @Override
  public String getScheme() {
    return "viewfs";
  }

  /**
   * Called after a new FileSystem instance is constructed.
   * @param theUri a uri whose authority section names the host, port, etc. for
   *          this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(final URI theUri, final Configuration conf)
      throws IOException {
    super.initialize(theUri, conf);
    setConf(conf);
    config = conf;
    useVSCache = config.getBoolean("fs.viewfs.use.vs.cache", true);
    // Now build  client side view (i.e. client side mount table) from config.
    final String authority = theUri.getAuthority();
    try {
      myUri = new URI(FsConstants.VIEWFS_SCHEME, authority, "/", null, null);
      this.clusterName = myUri.getAuthority();
      this.mountpointRenewer = MountpointRenewer
          .createMountpointRenewer(clusterName, conf,
              new MountpointRenewer.RenewMountpoint() {
                @Override public void doUpdateMountpoint() {
                  renewFsState();
                }
              });
      renewFsState();
      workingDir = this.getHomeDirectory();
    } catch (URISyntaxException e) {
      throw new IOException("URISyntax exception: " + theUri);
    }
  }

  public void renewFsState() {
    try {
      fsStateLock.writeLock().lock();
      if (cache == null) return;
      // It's not convenient to create anonymous subclass instance using java
      // reflection, so here we don't use configuration to control the implementation
      // class of InodeTree
      fsState = new MergedInodeTree<FileSystem>(config, clusterName) {

        @Override
        protected
        FileSystem getTargetFileSystem(final URI uri)
            throws URISyntaxException, IOException {
            FileSystem fs = null;
            if (useVSCache) {
              fs = cache.get(uri, config);
            } else {
              fs = FileSystem.get(uri, config);
            }
            return new ChRootedFileSystem(fs, uri);
        }

        @Override
        protected
        FileSystem getTargetFileSystem(final AbstractINodeDir<FileSystem> dir)
          throws URISyntaxException {
          return new InternalDirOfViewFs(dir, creationTime, ugi, myUri);
        }

        @Override
        protected
        FileSystem getTargetFileSystem(URI[] mergeFsURIList)
            throws URISyntaxException, UnsupportedFileSystemException {
          throw new UnsupportedFileSystemException("mergefs not implemented");
          // return MergeFs.createMergeFs(mergeFsURIList, config);
        }
      };
    } catch (IOException e) {
      LOG.warn("Failed renew fsStat" + clusterName, e);
    } catch (URISyntaxException e) {
      LOG.warn("Failed renew fsStat" + clusterName, e);
    } finally {
      fsStateLock.writeLock().unlock();
    }
  }
  
  /**
   * Convenience Constructor for apps to call directly
   * @param theUri which must be that of ViewFileSystem
   * @param conf
   * @throws IOException
   */
  ViewFileSystem(final URI theUri, final Configuration conf)
    throws IOException {
    this();
    initialize(theUri, conf);
  }
  
  /**
   * Convenience Constructor for apps to call directly
   * @param conf
   * @throws IOException
   */
  public ViewFileSystem(final Configuration conf) throws IOException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  public Path getTrashCanLocation(final Path f) throws FileNotFoundException {
    final InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.isInternalDir() ? null : res.targetFileSystem.getHomeDirectory();
  }
  
  @Override
  public URI getUri() {
    return myUri;
  }
  
  @Override
  public Path resolvePath(final Path f)
      throws IOException {
    final InodeTree.ResolveResult<FileSystem> res;
    res = fsStateResolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);
  }
  
  @Override
  public Path getHomeDirectory() {
    if (homeDir == null) {
      String base = fsStateGetHomeDirPrefixValue();
      if (base == null) {
        base = "/user";
      }
      homeDir = (base.equals("/") ? 
          this.makeQualified(new Path(base + ugi.getShortUserName())):
          this.makeQualified(new Path(base + "/" + ugi.getShortUserName())));
    }
    return homeDir;
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(final Path new_dir) {
    getUriPath(new_dir); // this validates the path
    workingDir = makeAbsolute(new_dir);
  }
  
  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.targetFileSystem.append(res.remainingPath, bufferSize, progress);
  }
  
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsStateResolve(getUriPath(f), false);
    } catch (FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createNonRecursive(res.remainingPath, permission,
         flags, bufferSize, replication, blockSize, progress);
  }
  
  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsStateResolve(getUriPath(f), false);
    } catch (FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.create(res.remainingPath, permission,
         overwrite, bufferSize, replication, blockSize, progress);
  }

  
  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath.equals(InodeTree.SlashPath)) {
      throw readOnlyMountTable("delete", f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
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
    final InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(fs.getPath()), true);
    return res.targetFileSystem.getFileBlockLocations(
          new ViewFsFileStatus(fs, res.remainingPath), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    
    // FileStatus#getPath is a fully qualified path relative to the root of 
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.
    
    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwener lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFileSystemFileStatus that 
    // works around.
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return new ViewFsFileStatus(status, this.makeQualified(f));
  }
  
  @Override
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.access(res.remainingPath, mode);
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);

    FileStatus[] statusList = new FileStatus[0];
    try {
       statusList = res.targetFileSystem.listStatus(res.remainingPath);
    } catch (FileNotFoundException e) {
      if (!(fsState instanceof MergedInodeTree))
          throw e;
    }

    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      ChRootedFileSystem targetFs;
      targetFs = (ChRootedFileSystem) res.targetFileSystem;
      int i = 0;
      for (FileStatus status : statusList) {
          String suffix = targetFs.stripOutRoot(status.getPath());
          statusList[i++] = new ViewFsFileStatus(status, this.makeQualified(
              suffix.length() == 0 ? f : new Path(res.resolvedPath, suffix)));
      }
    }
    if (!(fsState instanceof MergedInodeTree))
      return statusList;

    // if use mergedInodeTree, find more mountpoints to merge the listStatus result
    List<FileStatus> mergedList = new ArrayList<FileStatus>();
    InodeTree.MountPoint<FileSystem> currentNode = null;
    InodeTree.MountPoint<FileSystem> nearestAncestorNode = null;
    TreeMap<String, FileStatus> resMap = new TreeMap<String, FileStatus>() {};
    mountpointRenewer.updateMptFromZk(config);
    List<InodeTree.MountPoint<FileSystem>> pts = null;
    try {
      fsStateLock.readLock().lock();
      pts = fsState.getMountPoints();
    } finally {
      fsStateLock.readLock().unlock();
    }
    for (InodeTree.MountPoint<FileSystem> pt : pts) {
      if (!res.resolvedPath.startsWith(pt.src))
        continue;
      if (res.resolvedPath.equals(pt.src)) {
        currentNode = pt;
      } else {
        if (nearestAncestorNode == null || pt.src.startsWith(nearestAncestorNode.src))
          nearestAncestorNode = pt;
      }
    }

    try {
      if (currentNode != null) {
        // first check whether the current inode is a mountpoint
        // if it is, we don't need to check ancestor mountpoints
        if (currentNode.target instanceof InodeTree.AbstractINodeDir
                && res.remainingPath.equals(InodeTree.SlashPath)) {
          // only when current point is an internode mountpoint, we need to add the childrens
          // in mount table to list result
          InternalDirOfViewFs interFs = new InternalDirOfViewFs(
                  (InodeTree.AbstractINodeDir<FileSystem>) currentNode.target, creationTime,
                  ugi, myUri);
          mergedList.addAll(Arrays.asList(interFs.listStatus(res.remainingPath)));
        }
      } else if (nearestAncestorNode != null) {
        String pathDiff = res.resolvedPath.substring(nearestAncestorNode.src.length());
        if (!pathDiff.startsWith("/"))
          pathDiff = "/" + pathDiff;
        if (pathDiff.endsWith("/"))
          pathDiff = pathDiff.substring(0, pathDiff.length() - 1);

        mergedList.addAll(Arrays.asList(nearestAncestorNode.target.getFileSystem()
                .listStatus(new Path(pathDiff + res.remainingPath))));
      }
    } catch (FileNotFoundException e) {
      // ignore, leave mergedList empty
    }

    for (FileStatus status : mergedList) {
      String truePath = status.getPath().toUri().getPath();
      resMap.put(truePath, new ViewFsFileStatus(status,
              this.makeQualified(new Path(truePath))));
    }
    for (FileStatus status : statusList) {
      resMap.put(status.getPath().toUri().getPath(), status);
    }
    return resMap.values().toArray(new FileStatus[] {});
  }

  @Override
  public boolean mkdirs(final Path dir, final FsPermission permission)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(dir), false);
   return  res.targetFileSystem.mkdirs(res.remainingPath, permission);
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  
  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    // passing resolveLastComponet as false to catch renaming a mount point to 
    // itself. We need to catch this as an internal operation and fail.
    InodeTree.ResolveResult<FileSystem> resSrc =
        fsStateResolve(getUriPath(src), false);
  
    if (resSrc.isInternalDir()) {
      throw readOnlyMountTable("rename", src);
    }
      
    InodeTree.ResolveResult<FileSystem> resDst =
        fsStateResolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
          throw readOnlyMountTable("rename", dst);
    }
    /**
    // Alternate 1: renames within same file system - valid but we disallow
    // Alternate 2: (as described in next para - valid but we have disallowed it
    //
    // Note we compare the URIs. the URIs include the link targets. 
    // hence we allow renames across mount links as long as the mount links
    // point to the same target.
    if (!resSrc.targetFileSystem.getUri().equals(
              resDst.targetFileSystem.getUri())) {
      throw new IOException("Renames across Mount points not supported");
    }
    */
    
    //
    // Alternate 3 : renames ONLY within the the same mount links.
    //
    FileSystem srcFs = resSrc.targetFileSystem;
    FileSystem dstFs = resDst.targetFileSystem;
    Path srcFullPath = resSrc.remainingPath;
    Path dstFullPath = resDst.remainingPath;
    if (srcFs instanceof FilterFileSystem) {
      String srcRoot = srcFs.getUri().getPath();
      if (srcRoot == null || srcRoot.isEmpty()) {
        srcRoot = "";
      }
      if (srcRoot.endsWith("/")) {
        srcFullPath = new Path(srcRoot.substring(0, srcRoot.length()-1) + resSrc.remainingPath.toString());
      } else {
        srcFullPath = new Path(srcRoot + resSrc.remainingPath.toString());
      }
      srcFs = ((FilterFileSystem) srcFs).getRawFileSystem();
    }
    if (dstFs instanceof FilterFileSystem) {
        String dstRoot = dstFs.getUri().getPath();
        if (dstRoot == null || dstRoot.isEmpty()) {
          dstRoot = "";
        }
        if (dstRoot.endsWith("/")) {
          dstFullPath = new Path(dstRoot.substring(0, dstRoot.length()-1) + resDst.remainingPath.toString());
        } else {
          dstFullPath = new Path(dstRoot + resDst.remainingPath.toString());
        }
        dstFs = ((FilterFileSystem) dstFs).getRawFileSystem();
    }
    if (shouldDoFederateRename(resSrc, resDst)) {
      return srcFs.federationRename(srcFs, srcFullPath, dstFs, dstFullPath);
    }
    return srcFs.rename(srcFullPath, dstFullPath);
  }

  private boolean shouldDoFederateRename(
      InodeTree.ResolveResult<FileSystem> resSrc,
      InodeTree.ResolveResult<FileSystem> resDst) {
    if (resSrc.targetFileSystem == resDst.targetFileSystem)
      return false;

    URI srcFsUri = resSrc.targetFileSystem.getUri();
    URI dstFsUri = resDst.targetFileSystem.getUri();

    // judge if two different targetFileSystem instance actually
    // mount on same NN
    if (srcFsUri.getAuthority() == null || dstFsUri.getAuthority() == null
        || !srcFsUri.getAuthority().equals(dstFsUri.getAuthority()))
      return true;
    
    return false;
  }
  
  @Override
  public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.modifyAclEntries(res.remainingPath, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.removeAclEntries(res.remainingPath, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.removeDefaultAcl(res.remainingPath);
  }

  @Override
  public void removeAcl(Path path)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.removeAcl(res.remainingPath);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.setAcl(res.remainingPath, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    return res.targetFileSystem.getAclStatus(res.remainingPath);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    res.targetFileSystem.setXAttr(res.remainingPath, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttr(res.remainingPath, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttrs(res.remainingPath);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttrs(res.remainingPath, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    return res.targetFileSystem.listXAttrs(res.remainingPath);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path),
        true);
    res.targetFileSystem.removeXAttr(res.remainingPath, name);
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum) { 
    List<InodeTree.MountPoint<FileSystem>> mountPoints =
        fsStateGetMountPoints();
    for (InodeTree.MountPoint<FileSystem> mount : mountPoints) {
      mount.target.getFileSystem().setVerifyChecksum(verifyChecksum);
    }
  }
  
  @Override
  public long getDefaultBlockSize() {
    throw new NotInMountpointException("getDefaultBlockSize");
  }

  @Override
  public short getDefaultReplication() {
    throw new NotInMountpointException("getDefaultReplication");
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    throw new NotInMountpointException("getServerDefaults");
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    try {
      InodeTree.ResolveResult<FileSystem> res =
          fsStateResolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultBlockSize(res.remainingPath);
    } catch (FileNotFoundException e) {
      throw new NotInMountpointException(f, "getDefaultBlockSize");
    }
  }

  @Override
  public short getDefaultReplication(Path f) {
    try {
      InodeTree.ResolveResult<FileSystem> res =
          fsStateResolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultReplication(res.remainingPath);
    } catch (FileNotFoundException e) {
      throw new NotInMountpointException(f, "getDefaultReplication"); 
    }
  }

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.targetFileSystem.getServerDefaults(res.remainingPath);    
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(f), true);
    return res.targetFileSystem.getContentSummary(res.remainingPath);
  }

  @Override
  public void setWriteChecksum(final boolean writeChecksum) { 
    List<InodeTree.MountPoint<FileSystem>> mountPoints =
        fsStateGetMountPoints();
    for (InodeTree.MountPoint<FileSystem> mount : mountPoints) {
      mount.target.getFileSystem().setWriteChecksum(writeChecksum);
    }
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    List<InodeTree.MountPoint<FileSystem>> mountPoints =
        fsStateGetMountPoints();
    Set<FileSystem> children = new HashSet<FileSystem>();
    for (InodeTree.MountPoint<FileSystem> mountPoint : mountPoints) {
      FileSystem targetFs = mountPoint.target.getFileSystem();
      children.addAll(Arrays.asList(targetFs.getChildFileSystems()));
    }
    return children.toArray(new FileSystem[]{});
  }

  public FileSystem getTargetFileSystem(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    return res.targetFileSystem;
  }
  
  public Path getTargetPath(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsStateResolve(getUriPath(path), true);
    if (res.targetFileSystem instanceof FilterFileSystem) {
      String targetRoot = res.targetFileSystem.getUri().getPath();
      if (targetRoot == null || targetRoot.isEmpty()) {
        targetRoot = "";
      }
      if (targetRoot.endsWith("/")) {
        return new Path(targetRoot.substring(0, targetRoot.length()-1) + res.remainingPath.toString());
      } else {
        return new Path(targetRoot + res.remainingPath.toString());
      }
    } else {
      return path;
    }
  }

  /*
   * An instance of this class represents an internal dir of the viewFs 
   * that is internal dir of the mount table.
   * It is a read only mount tables and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  public static class InternalDirOfViewFs extends FileSystem {
    final InodeTree.AbstractINodeDir<FileSystem>  theInternalDir;
    final long creationTime; // of the the mount table
    final UserGroupInformation ugi; // the user/group of user who created mtable
    final URI myUri;
    
    public InternalDirOfViewFs(final InodeTree.AbstractINodeDir<FileSystem> dir,
        final long cTime, final UserGroupInformation ugi, URI uri) {
      myUri = uri;
      try {
        initialize(myUri, new Configuration());
      } catch (IOException e) {
        throw new RuntimeException("Cannot occur");
      }
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
    }

    static private void checkPathIsSlash(final Path f) throws IOException {
      if (!f.equals(InodeTree.SlashPath)) {
        throw new IOException (
        "Internal implementation error: expected file name to be /" );
      }
    }
    
    @Override
    public URI getUri() {
      return myUri;
    }

    @Override
    public Path getWorkingDirectory() {
      throw new RuntimeException (
      "Internal impl error: getWorkingDir should not have been called" );
    }

    @Override
    public void setWorkingDirectory(final Path new_dir) {
      throw new RuntimeException (
      "Internal impl error: getWorkingDir should not have been called" ); 
    }

    @Override
    public FSDataOutputStream append(final Path f, final int bufferSize,
        final Progressable progress) throws IOException {
      throw readOnlyMountTable("append", f);
    }

    @Override
    public FSDataOutputStream create(final Path f,
        final FsPermission permission, final boolean overwrite,
        final int bufferSize, final short replication, final long blockSize,
        final Progressable progress) throws AccessControlException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public boolean delete(final Path f, final boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public boolean delete(final Path f)
        throws AccessControlException, IOException {
      return delete(f, true);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(final FileStatus fs,
        final long start, final long len) throws 
        FileNotFoundException, IOException {
      checkPathIsSlash(fs.getPath());
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public FileChecksum getFileChecksum(final Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      checkPathIsSlash(f);
      // The mount point should not belong to any special user&group.
      // Usually the group is resolved by the namenode from the group
      // configuration files, we just use the user name as a workaround
      // since we do not get to any namenode for internal directories.
      String[] grps = ugi.getGroupNames();
      String user = ugi.getShortUserName();
      String grp;
      if (grps.length != 0) {
        grp = grps[0];
      } else {
        grp = user;
      }
      return new FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_555, user, grp,
          new Path(theInternalDir.fullPath).makeQualified(myUri, ROOT_PATH));
    }

    /**
     * Minor change of default implementation : Won't recursively call getContentSummary
     * if some child is a directory.
     */
    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
      FileStatus status = getFileStatus(f);
      if (status.isFile()) {
        // f is a file
        return new ContentSummary(status.getLen(), 1, 0);
      }
      // f is a directory
      long[] summary = { 0, 0, 1 };
      for (FileStatus s : listStatus(f)) {
        int fileCount = s.isDirectory() ? 0 : 1;
        ContentSummary c = new ContentSummary(s.getLen(), fileCount, 1 - fileCount);
        summary[0] += c.getLength();
        summary[1] += c.getFileCount();
        summary[2] += c.getDirectoryCount();
      }
      return new ContentSummary(summary[0], summary[1], summary[2]);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws AccessControlException,
        FileNotFoundException, IOException {
      checkPathIsSlash(f);
      FileStatus[] result = new FileStatus[theInternalDir.getChildren().size()];
      int i = 0;
      String user = ugi.getShortUserName();
      String grp = user;
      for (Entry<String, INode<FileSystem>> iEntry : theInternalDir
          .getChildren().entrySet()) {
        INode<FileSystem> inode = iEntry.getValue();
        Path quoalifiedFullPath =
            new Path(inode.fullPath).makeQualified(myUri, null);
        if (inode instanceof INodeLink) {
          INodeLink<FileSystem> link = (INodeLink<FileSystem>) inode;

          FileStatus status = link.getFileSystem().getFileStatus(new Path("/"));
          status.setPath(quoalifiedFullPath);
          status.setSymlink(link.getTargetLink());
          result[i++] = status;
        } else if (inode instanceof MergedInodeTree.INodeMerge
            && ((MergedInodeTree.INodeMerge) inode).getChildren().isEmpty()) {
          MergedInodeTree.INodeMerge<FileSystem> curNode =
              (MergedInodeTree.INodeMerge<FileSystem>) inode;
          Path remainingPath = new Path("/");
          Path symlink = null;
          if (curNode.getTargetUri() != null) {
            symlink = new Path(curNode.getTargetUri().getPath());
          }
          if (curNode.getTargetUri() != null
              && curNode.getFileSystem().exists(remainingPath)) {
            FileStatus status =
                curNode.getFileSystem().getFileStatus(remainingPath);
            status.setPath(quoalifiedFullPath);
            status.setSymlink(symlink);
            result[i++] = status;
          } else {
            result[i++] =
                new FileStatus(0, false, 0, 0, creationTime, creationTime,
                    PERMISSION_555, user, grp, symlink, quoalifiedFullPath);
          }
        } else {
          result[i++] = new FileStatus(0, true, 0, 0, creationTime,
              creationTime, PERMISSION_555, user, grp, quoalifiedFullPath);
        }
      }
      return result;
    }

    @Override
    public boolean mkdirs(Path dir, FsPermission permission)
        throws AccessControlException, FileAlreadyExistsException {
      if (theInternalDir.isRoot() && dir == null) {
        throw new FileAlreadyExistsException("/ already exits");
      }
      // Note dir starts with /
      if (theInternalDir.getChildren().containsKey(dir.toString().substring(1))) {
        return true; // this is the stupid semantics of FileSystem
      }
      throw readOnlyMountTable("mkdirs",  dir);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize)
        throws AccessControlException, FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public boolean rename(Path src, Path dst) throws AccessControlException,
        IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public void setOwner(Path f, String username, String groupname)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(Path f, FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public boolean setReplication(Path f, short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(Path f, long mtime, long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
      // Noop for viewfs
    }

    @Override
    public FsServerDefaults getServerDefaults(Path f) throws IOException {
      throw new NotInMountpointException(f, "getServerDefaults");
    }
    
    @Override
    public long getDefaultBlockSize(Path f) {
      throw new NotInMountpointException(f, "getDefaultBlockSize");
    }

    @Override
    public short getDefaultReplication(Path f) {
      throw new NotInMountpointException(f, "getDefaultReplication");
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("modifyAclEntries", path);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeAclEntries", path);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeDefaultAcl", path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeAcl", path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("setAcl", path);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
      checkPathIsSlash(path);
      String[] grps = ugi.getGroupNames();
      String user = ugi.getShortUserName();
      String grp;
      if (grps.length != 0) {
        grp = grps[0];
      } else {
        grp = user;
      }
      return new AclStatus.Builder().owner(user).group(grp)
          .addEntries(AclUtil.getMinimalAcl(PERMISSION_555)).stickyBit(false)
          .build();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
                         EnumSet<XAttrSetFlag> flag) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("setXAttr", path);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
      return new byte[0];
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
        throws IOException {
      return Collections.emptyMap();
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
      return Collections.emptyList();
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeXAttr", path);
    }
  }


  private InodeTree.ResolveResult<FileSystem> fsStateResolve(final String p,
      final boolean resolveLastComponent) throws FileNotFoundException {
    mountpointRenewer.updateMptFromZk(config);
    fsStateLock.readLock().lock();
    try {
      return fsState.resolve(p, resolveLastComponent);
    } finally {
      fsStateLock.readLock().unlock();
    }
  }

  private List<InodeTree.MountPoint<FileSystem>> fsStateGetMountPoints() {
    fsStateLock.readLock().lock();
    try {
      return fsState.getMountPoints();
    } finally {
      fsStateLock.readLock().unlock();
    }
  }

  private String fsStateGetHomeDirPrefixValue() {
    mountpointRenewer.updateMptFromZk(config);
    fsStateLock.readLock().lock();
    try {
      return fsState.getHomeDirPrefixValue();
    } finally {
      fsStateLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    UnsafeCache tmp = cache;
    fsStateLock.writeLock().lock();
    cache = null;
    fsStateLock.writeLock().unlock();
    // we shouldn't close all cached filesystems when holding fsStateLock.writeLock,
    // because this may cause deadlock.
    // e.g. If we call cache.closeAll() within writeLock：
    // Thread1:
    // JVM down -> FileSystem.ClientFinalizer.run() -> synchronized(FileSystem.CACHE)
    // -> ViewFileSystem.close() -> fsStateLock.writeLock()
    // Thread2:
    // ViewFileSystem.close() -> fsStateLock.writeLock() -> close child filesystem
    // -> FileSystem.close() -> synchronized (FileSystem.CACHE)
    if (tmp != null) {
      tmp.closeAll();
    }
    super.close();
  }
}
