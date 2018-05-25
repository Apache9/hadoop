/* Licensed to the Apache Software Foundation (ASF) under one
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;

import org.apache.hadoop.fs.permission.AclStatus;

import java.util.ArrayList;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirectorySubTree {
  private int MAX_INODES;
  private int MAX_BLOCKS;
  private long renameId;
  private int consumedIdx = 0;
  private int blocksTotal = 0;
  private ArrayList<HdfsExtendedFileStatus> subTree;

  public static class HdfsExtendedFileStatus {
    private HdfsFileStatus fstatus;
    private AclStatus astatus;

    public HdfsFileStatus getFileStatus() {
      return fstatus;
    }

    public AclStatus getAclStatus() {
      return astatus;
    }

    public HdfsExtendedFileStatus(HdfsFileStatus inFstatus, AclStatus inAstatus) {
      fstatus = inFstatus;
      astatus = inAstatus;
    }

    @Override
    public String toString() {
      return "Status: " + fstatus.toString() + "  ACL : " + astatus.toString();
    }
  }

  public DirectorySubTree(int MAX_INODES, int MAX_BLOCKS) {
    this.subTree = new ArrayList<>();
    this.MAX_INODES = MAX_INODES;
    this.MAX_BLOCKS = MAX_BLOCKS;
  }

  public int remainingSize() {
    return MAX_INODES - subTree.size();
  }

  public int remainingBlocks() {
    return MAX_BLOCKS - blocksTotal;
  }

  public boolean addItem(HdfsExtendedFileStatus status) {
    if (subTree.size() < MAX_INODES) {
      subTree.add(status);
      return true;
    }
    return false;
  }

  public boolean incrBlocks(int value) {
    // avoid cross-border
    if ((long)blocksTotal + value <= MAX_BLOCKS) {
      blocksTotal += value;
      return true;
    }
    return false;
  }

  public int getBlocksTotal() {
    return this.blocksTotal;
  }

  public void setBlocksTotal(int blocksTotal) {
    this.blocksTotal = blocksTotal;
  }

  public HdfsExtendedFileStatus consumeItem() {
    if (consumedIdx < subTree.size()) {
      HdfsExtendedFileStatus res = subTree.get(consumedIdx);
      consumedIdx++;
      return res;
    }
    return null;
  }

  public HdfsExtendedFileStatus nextItemToConsume() {
    if (consumedIdx < subTree.size()) {
      HdfsExtendedFileStatus res = subTree.get(consumedIdx);
      return res;
    }
    return null;
  }

  public void setRenameId(long inId) {
    renameId = inId;
  }

  public int getSize() {
    return subTree.size();
  }

  public long getRenameId() {
    return renameId;
  }

  public HdfsExtendedFileStatus get(int idx) {
    return subTree.get(idx);
  }

  public void dumpSubTree(Log log) {
    log.info("subtree MAX_INODES " + subTree.size());
    for (int i = 0; i < subTree.size(); i++) {
      log.info(i + "th item is " + subTree.get(i));
    }
  }

  public long getLargestInodeId() {
    long res = 0;
    for (int i = 0; i < subTree.size(); i++) {
      if (subTree.get(i).fstatus.getFileId() > res) {
        res = subTree.get(i).fstatus.getFileId();
      }
    }
    return res;
  }

  public int getMAX_INODES() {
    return MAX_INODES;
  }

  public int getMAX_BLOCKS() {
    return MAX_BLOCKS;
  }
}
