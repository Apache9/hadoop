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
package org.apache.hadoop.hdfs.server.ttlmanager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.ttlmanager.TtlPolicy.TtlTaskResult;

import com.google.common.annotations.VisibleForTesting;



/**
 * TtlPolicy defines how the TTL of each file and directory will be processed.
 */
public class TtlPolicy extends Policy<TtlTaskResult> {

  private static final Log LOG = LogFactory.getLog(TtlPolicy.class);
  private static final String NAME = "TTL";
  private static final String TTL_XATTR_NAME = "user.ttl";
  private static final String TTL_XATTR_PROPERTY = "user.ttlproperty";
  private static final Path TRASH = new Path(".Trash/");

  private long roundIntervalMs = 0;
  private boolean enableTrash;
  private TtlMetrics metrics;
  private Path  trash;

  public TtlPolicy(Configuration conf, TtlMetrics metrics) throws IOException {
    super(conf);
    roundIntervalMs = conf.getLong(
        DFSConfigKeys.HDFS_TTLMANAGER_TTL_ROUND_INTERVAL_MS,
        DFSConfigKeys.HDFS_TTLMANAGER_TTL_ROUND_INTERVAL_MS_DEFAULT);
    enableTrash = conf.getBoolean(
        DFSConfigKeys.HDFS_TTLMANAGER_ENABLE_TRASH_KEY,
        DFSConfigKeys.HDFS_TTLMANAGER_ENABLE_TRASH_DEFAULT);
    
    this.metrics = metrics;
    this.trash = new Path(fs.getHomeDirectory(), TRASH);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Type getType() {
    return Type.ONE_SHOT;
  }
  
  @VisibleForTesting
  public TtlMetrics getMetrics() {
    return metrics;
  }

  @Override
  public TtlTaskResult onStart() throws IOException {
    TtlTaskResult result = new TtlTaskResult();
    result.setStartTimeMs(System.currentTimeMillis());

    Path rootDir = new Path("/");
    traverseDirectoryTree(rootDir);

    result.setEndTimeMs(System.currentTimeMillis());
    return result;
  }

  @Override
  public void onSuccess(TtlTaskResult result) {
    long consumedTime = result.getConsumedTimeMs();
    
    if (consumedTime < roundIntervalMs) {
      delayMs = roundIntervalMs - consumedTime;
    } else {
      // Clear the last delay time
      delayMs = 0;
    }
    
    metrics.addTtlDurationInMs(consumedTime);
    // Enable the next round
    enable();
  }

  @Override
  public void onFailure(Throwable t) {
    LOG.error("Error occurred during task processing", t);
    metrics.incrTtlFailedTask();
    // Try again later. There might be transient errors.
    delayMs = roundIntervalMs;
    enable();
  }
  
  private boolean isInTrash(Path path) {
    return fs.makeQualified(path).toString().startsWith(trash.toString());
  }
  
  /**
   * Depth first traverse the specified directory tree, and process the ttl
   * for each of the files and directories. Note that here we can't guarantee
   * atomicity, so error must be handled properly, and the result is just a
   * best effort one.
   *
   * @param rootDir The directory tree root.
   */
  void traverseDirectoryTree(Path rootDir) {
    Stack<Path> stack = new Stack<Path>();
    List<TtlInfo> ttlInfos = new LinkedList<TtlInfo>();
    Map<Path, ChildrenInfo> childrenInfos = new HashMap<Path, ChildrenInfo>();
    Set<Path> visited = new HashSet<Path>();

    stack.add(rootDir);
    visited.add(rootDir);
    ttlInfos.add(getTtlInfo(rootDir));
    while (!stack.isEmpty()) {
      Path path = null;
      try {
        path = stack.peek();
        // Get the children info, either from the cache or from HDFS
        ChildrenInfo childrenInfo = childrenInfos.get(path);
        if (childrenInfo == null) {
          FileStatus[] children = fs.listStatus(path);
          childrenInfo = new ChildrenInfo(children);
          childrenInfos.put(path, childrenInfo);
        }

        if (fs.isDirectory(path) && childrenInfo.hasNextChild()) {
          // Process the next child
          FileStatus nextChild = childrenInfo.nextChild();
          if (!visited.contains(nextChild.getPath()) && !isInTrash(nextChild.getPath())) {
            ttlInfos.add(getTtlInfo(nextChild.getPath()));
            visited.add(nextChild.getPath());
            stack.push(nextChild.getPath());
          }
        } else {
          ProcessTtlInfos(path, ttlInfos);
          ttlInfos.remove(ttlInfos.size() - 1);
          stack.pop();
        }
      } catch (IOException e) {
        // The current path is error, just remove from the stack
        ttlInfos.remove(ttlInfos.size() - 1);
        stack.pop();
        LOG.warn("Error occurred during processing path " + path, e);
      }
    }
  }

  /**
   * Get the ttl information of specified path.
   *
   * @param path The path to get ttl
   * @return The ttl value if set, otherwise -1.
   */
  TtlInfo getTtlInfo(Path path) {
    try {
      Map<String, byte[]> ttlValue = fs.getXAttrs(path);
      if (ttlValue.get(TTL_XATTR_NAME) != null) {
        int ttl =
            ByteBuffer.wrap(ttlValue.get(TTL_XATTR_NAME)).asIntBuffer().get();
        if (ttl > 0) {
          int property = 0;
          if (ttlValue.get(TTL_XATTR_PROPERTY) != null) {
            property =
                ByteBuffer.wrap(ttlValue.get(TTL_XATTR_PROPERTY)).asIntBuffer()
                    .get();
          }
          return new TtlInfo(path, ttl, property);
        }
      }
    } catch (IOException e) {
      LOG.warn("Get ttl failed for path " + path);
    }
    return null;
  }

  private boolean isTtlExpire(FileStatus status, Path file, TtlInfo ti)
      throws IOException {
    int currentMin = (int) (System.currentTimeMillis() / 1000 / 60);
    if (!ti.sinceLastWrite()) {
      return (ti.getTtl() < currentMin);
    } else {
      int fileModTime = (int) (status.getModificationTime() / 1000 / 60);
      if (fileModTime + ti.getTtl() < currentMin) {
        if (!status.isDirectory() && (fs instanceof DistributedFileSystem)) {
          if (!((DistributedFileSystem) fs).isFileClosed(file)) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }

  private boolean shouldDeleteByTtl(Path file, TtlInfo ti) throws IOException {
    FileStatus status = fs.getFileStatus(file);
    if (!isTtlExpire(status, file, ti)) {
      return false;
    }
    if (status.isDirectory()) {
      // Root and non-empty directory cannot be deleted
      if (file.isRoot() || (fs.listStatus(file).length != 0)) {
        return false;
      }
      if (fs.makeQualified(ti.getPath()).toString()
          .equals(fs.makeQualified(file).toString())) {
        // The TTL is not inherited
        if (ti.keepEmptyDir()) {
          return false;
        }
      } else {
        // The TTL is inherited
        if (ti.keepEmptySubDir()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Process the ttl info for specified path.
   *
   * @param path     The path whose ttl will be processed
   * @param ttlInfos The TtlInfo list
   */
  void ProcessTtlInfos(Path path, List<TtlInfo> ttlInfos) {
    TtlInfo effectiveTtl = null;
    metrics.incrFilesScannedByTTL();
    // Get the effective ttl information
    for (TtlInfo info : ttlInfos) {
      if (info != null && info.getTtl() > 0) {
        effectiveTtl = info;
      }
    }

    if (effectiveTtl != null) {
      try {
        if (shouldDeleteByTtl(path, effectiveTtl)) {
          try {
            // Delete a non-directory file or a directory which is neither the
            // root nor a non-empty directory
            if (enableTrash) {
              Trash.moveToAppropriateTrash(fs, path, fs.getConf());
              metrics.incrFilesDeletedByTTL();
              LOG.info("Move ttl expired path " + path + " to trash successful, " +
                      "ttl comes from path " + effectiveTtl.getPath() + ", the ttl is " + effectiveTtl.getTtl()) ;
            } else {
              fs.delete(path, false);
              metrics.incrFilesDeletedByTTL();
              LOG.info("Delete ttl expired path " + path + " successful, " +
                      "ttl comes from path " + effectiveTtl.getPath() + ", the ttl is " + effectiveTtl.getTtl()) ;
            }
          } catch (IOException e) {
            LOG.warn("Delete ttl expired path " + path + " failed", e);
          }
        } else {
          LOG.debug("Path " + path
              + " should not be deleted by TTL according to configuration.");
        }
      } catch (IOException e) {
        LOG.warn("Fail to handle TTL for path" + path, e);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No ttl information for path " + path);
      }
    }
  }

  /**
   * TtlInfo class is used to record the ttl information of each path
   * during the traversing.
   */
  static class TtlInfo {
    private final Path path;
    private final int ttl;
    private final int property;
    private static final int SINCELASTWRITE = 0x1;
    private static final int KEEPEMPTYDIR = 0x2;
    private static final int KEEPEMPTYSUBDIR = 0x4;

    public TtlInfo(Path path, int ttl, int property) {
      this.path = path;
      this.ttl = ttl;
      this.property = property;
    }

    public Path getPath() {
      return path;
    }

    public int getTtl() {
      return ttl;
    }

    public boolean sinceLastWrite() {
      return ((property & SINCELASTWRITE) != 0);
    }

    public boolean keepEmptyDir() {
      return ((property & KEEPEMPTYDIR) != 0);
    }

    public boolean keepEmptySubDir() {
      return ((property & KEEPEMPTYSUBDIR) != 0);
    }

    @VisibleForTesting
    public int getProperty() {
      return property;
    }
  }

  /**
   * ChildrenInfo class is used to record the children's information for each
   * parent directory during the traversing.
   */
  private static class ChildrenInfo {
    private int nextChildId = 0;
    private final FileStatus[] children;

    public ChildrenInfo(FileStatus[] children) {
      this.children = children;
    }

    public boolean hasNextChild() {
      if (children == null || children.length == 0) {
        return  false;
      }
      return nextChildId < children.length;
    }

    FileStatus nextChild() {
      return children[nextChildId++];
    }
  }

  public static class TtlTaskResult {
    private long startTimeMs;
    private long endTimeMs;

    public long getStartTimeMs() {
      return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
    }

    public long getEndTimeMs() {
      return endTimeMs;
    }

    public void setEndTimeMs(long endTimeMs) {
      this.endTimeMs = endTimeMs;
    }

    public long getConsumedTimeMs() {
      return endTimeMs - startTimeMs;
    }
  }
}
