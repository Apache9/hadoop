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

import org.apache.hadoop.hdfs.protocol.Block;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirectorySubTree {
  private int length;
  private long renameId;
  private int currentIdx = 0;
  private int consumedIdx = 0;
  private HdfsFileStatus[] subTree;

  public DirectorySubTree(int len) {
    subTree = new HdfsFileStatus[len];
    length = len;
  }

  public int remainingSize() {
    return length - currentIdx;
  }

  public boolean addItem(HdfsFileStatus status) {
    if (currentIdx < length) {
      subTree[currentIdx] = status;
      currentIdx++;
      return true;
    }
    return false;
  }

  public HdfsFileStatus consumeItem() {
    if (consumedIdx < currentIdx) {
      HdfsFileStatus res = subTree[consumedIdx];
      consumedIdx++;
      return res;
    }
    return null;
  }

  public HdfsFileStatus nextItemToConsume() {
    if (consumedIdx < currentIdx) {
      HdfsFileStatus res = subTree[consumedIdx];
      return res;
    }
    return null;
  }

  public void setRenameId(long inId) {
    renameId = inId;
  }

  public int getSize() {
    return currentIdx;
  }

  public long getRenameId() {
    return renameId;
  }

  public HdfsFileStatus get(int idx) {
    return subTree[idx];
  }

  public void dumpSubTree(Log log) {
    log.debug("subtree length " + subTree.length);
    for (int i = 0; i < subTree.length; i++) {
      log.debug(i + "th item is " + subTree[i]);
    }
  }
}
