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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * MergedInodeTree support nest mount table
 * e.g. We can add /user/ to nn1, and add /user/foo/ to another one, nn2
 *      with this configuration, all paths that under /user/foo/ are resolved
 *      to nn2, and other paths under /user/ are resolve to nn1
 */
public abstract class MergedInodeTree<T> extends InodeTree<T> {
  static class INodeMerge<T> extends AbstractINodeDir<T> {
    final Map<String, INode<T>> children = new HashMap<String, INode<T>>();
    URI targetUri = null;
    T fs = null;

    INodeMerge(final String pathToNode, final UserGroupInformation aUgi) {
      super(pathToNode, aUgi);
    }

    @Override
    public T getFileSystem() {
      return fs;
    }

    @Override
    Map<String, INode<T>> getChildren() {
      return children;
    }

    @Override boolean isRoot() {
      return fullPath.equals(SlashPath.toString());
    }

    @Override
    INodeMerge<T> resolve(final String pathComponent) {
      INode<T> n = children.get(pathComponent);
      if (n == null)
        return null;
      assert (n instanceof INodeMerge);
      return (INodeMerge<T>)n;
    }

    void add(final String pathComponent, final INodeMerge<T> node)
      throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new FileAlreadyExistsException();
      }
      children.put(pathComponent, node);
    }

    public void setFileSystem(T targetFs) {
      fs = targetFs;
    }

    void setTargetUri(URI uri) {
      targetUri = uri;
    }

    URI getTargetUri() {
      return targetUri;
    }
  }

  protected MergedInodeTree(final Configuration config, final String viewName)
      throws UnsupportedFileSystemException, URISyntaxException,
      FileAlreadyExistsException, IOException {
    super(config, viewName);
  }
  
  @Override
  protected INodeMerge<T> getInode(final String path,
      final UserGroupInformation aUgi, URI targetUri)
      throws URISyntaxException, IOException {
      INodeMerge<T> node = new INodeMerge<T>(path, aUgi);
      if (targetUri == null) {
        node.setFileSystem(getTargetFileSystem(node));
      } else {
        node.setTargetUri(targetUri);
        node.setFileSystem(getTargetFileSystem(targetUri));
      }
      return node;
  }

  private String concatPaths(String base, String component) {
    return new Path(base, component).toString();
  }
  
  @Override
  protected void createLink(final String src, final String target,
      final boolean isLinkMerge, final UserGroupInformation aUgi)
      throws URISyntaxException, IOException, FileAlreadyExistsException,
      UnsupportedFileSystemException {
    // Validate that src is valid absolute path
    final Path srcPath = new Path(src);
    if (!srcPath.isAbsoluteAndSchemeAuthorityNull()) {
      throw new IOException("ViewFs:Non absolute mount name in " +
              "config:" + src);
    }

    final String[] srcPaths = breakIntoPathComponents(src);
    assert (root instanceof INodeMerge);
    INodeMerge<T> curInode = (INodeMerge<T>)root;
    int i;
    for (i = 1; i < srcPaths.length-1; i++) {
      final String iPath = srcPaths[i];
      INodeMerge<T> nextInode = curInode.resolve(iPath);
      if (nextInode == null) {
        nextInode = getInode(concatPaths(curInode.fullPath, iPath), aUgi, null);
        curInode.add(iPath, nextInode);
      }
      curInode = nextInode;
    }

    URI targetUri = new URI(target);
    INodeMerge<T> lastInode = null;
    String iPath = null;
    if (src.equals("/")) {
      lastInode = curInode;
    } else {
      iPath = srcPaths[i];
      lastInode = curInode.resolve(iPath);
    }

    if (lastInode == null) {
      lastInode = getInode(concatPaths(curInode.fullPath, iPath), aUgi, targetUri);
      curInode.add(iPath, lastInode);
      mountPoints.add(new MountPoint<T>(src, lastInode));
    } else {
      if (lastInode.getTargetUri() == null) {
        lastInode.setFileSystem(getTargetFileSystem(targetUri));
        lastInode.setTargetUri(targetUri);
        mountPoints.add(new MountPoint<T>(lastInode.fullPath, lastInode));
      } else {
        throw new FileAlreadyExistsException("Path " + lastInode.fullPath + " already exists as link");
      }
    }
  }

  @Override
  ResolveResult<T> resolve(final String p, final boolean resolveLastComponent)
      throws FileNotFoundException {
    String[] path = breakIntoPathComponents(p);
    assert (root instanceof INodeMerge);
    INodeMerge<T> curInode = (INodeMerge<T>) root;

    int i;
    int end = path.length - (resolveLastComponent ? 0 : 1);
    INodeMerge<T> lastMountPointNode =
        curInode.getTargetUri() == null ? null : curInode;
    int tmpIndex = 1;

    for (i = 1; i < end; i++) {
      INodeMerge<T> nextInode = curInode.resolve(path[i]);
      if (nextInode == null)
        break;
      curInode = nextInode;
      if (curInode.getTargetUri() != null) {
        lastMountPointNode = curInode;
        tmpIndex = i + 1;
      }
    }

    int remainingPathFirstIndex = i;
    Path remainingPath;
    ResultKind resKind;

    if (path.length < 1) {
      // for root path
      if (resolveLastComponent && curInode.getTargetUri() != null) {
        resKind = ResultKind.isExternalDir;
      } else {
        resKind = ResultKind.isInternalDir;
      }
      remainingPath = SlashPath;
    } else {
      if (curInode.getChildren().isEmpty()) {
        // resolve to a leaf node
        resKind = ResultKind.isExternalDir;
      } else if (i < end || !resolveLastComponent) {
        // not resolved to last component
        if (!resolveLastComponent && curInode.resolve(path[i]) != null) {
          // given path is an internal path of mount table
          // PS: curInode.resolve(path[i]) != null means path[i] is the last component
          resKind = ResultKind.isInternalDir;
        } else if (lastMountPointNode != null) {
          // found a mountpoint in the ancestor dir of given path
          // or curInode is a mountpoint
          resKind = ResultKind.isExternalDir;
          curInode = lastMountPointNode;
          remainingPathFirstIndex = tmpIndex;
        } else if (!resolveLastComponent && i == end) {
          // last component is not a part of mount table
          resKind = ResultKind.isInternalDir;
        } else {
          throw new FileNotFoundException(
              StringUtils.join("/", Arrays.copyOfRange(path, 0, i)));
        }
      } else {
        // resolved last component, if corresponding inode is a mountpoint,
        // return as
        // an external inode, else return as an internal inode
        if (curInode.getTargetUri() != null) {
          resKind = ResultKind.isExternalDir;
        } else {
          resKind = ResultKind.isInternalDir;
        }
      }
      remainingPath = new Path("/" + StringUtils.join("/",
          Arrays.copyOfRange(path, remainingPathFirstIndex, path.length)));
    }
    T resultFs = curInode.getFileSystem();
    if (resKind == ResultKind.isInternalDir
        && curInode.getTargetUri() != null) {
      try {
        resultFs = getTargetFileSystem(curInode);
      } catch (URISyntaxException e) {
        throw new RuntimeException("got an unexpected URISyntaxException", e);
      }
    }
    return new ResolveResult<T>(resKind, resultFs, curInode.fullPath,
        remainingPath);
  }
}
