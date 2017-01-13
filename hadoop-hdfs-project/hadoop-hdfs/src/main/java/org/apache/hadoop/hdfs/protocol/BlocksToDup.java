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

import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.hdfs.protocol.DirectorySubTree;

public class BlocksToDup {

  private String destPoolId;
  private List<DupBlockInfo> blks;

  public class DupBlockInfo {
    private long srcBlockId;
    private long dstBlockId;
    private long blkSize;
    private long genStamp;

    public long getSrcBlockId() {
      return srcBlockId;
    }

    public long getDstBlockId() {
      return dstBlockId;
    }

    public long getBlockSize() {
      return blkSize;
    }

    public long getBlockGenStamp() {
      return genStamp;
    }

    public DupBlockInfo(long srcId, long dstId, long sz, long genNum) {
      srcBlockId = srcId;
      dstBlockId = dstId;
      blkSize = sz;
      genStamp = genNum;
    }
  }

  public BlocksToDup(String poolId) {
    destPoolId = poolId;
    blks = new LinkedList<DupBlockInfo>();
  }

  public void addDupBlock(long srcId, long dstId, long sz, long genNum) {
    blks.add(new DupBlockInfo(srcId, dstId, sz, genNum));
  }

  public String getDstPoolId() {
    return destPoolId;
  }

  public int size() {
    return blks.size();
  }

  public DupBlockInfo get(int idx) {
    return blks.get(idx);
  }

  public static BlocksToDup buildFromSubTrees(DirectorySubTree srcTree,
      DirectorySubTree dstTree) {
    BlocksToDup res = null;
    if (srcTree == null || dstTree == null) {
      return res;
    }
    if (srcTree.getSize() != dstTree.getSize()) {
      return res;
    }
    for (int i = 0; i < srcTree.getSize(); i++) {
      HdfsFileStatus srcStatus = srcTree.get(i);
      HdfsFileStatus dstStatus = dstTree.get(i);
      if (srcStatus.isDir() != dstStatus.isDir()
          || srcStatus.isSymlink() != dstStatus.isSymlink()) {
        return res;
      }
      if (!srcStatus.isDir() && !srcStatus.isSymlink()) {
        LocatedBlocks sblks =
            ((HdfsLocatedFileStatus) srcStatus).getBlockLocations();
        LocatedBlocks dblks =
            ((HdfsLocatedFileStatus) dstStatus).getBlockLocations();
        List<LocatedBlock> slblks = sblks.getLocatedBlocks();
        List<LocatedBlock> dlblks = dblks.getLocatedBlocks();
        if (slblks.size() != dlblks.size()) {
          return res;
        }
        for (int j = 0; j < slblks.size(); j++) {
          long srcId = slblks.get(j).getBlock().getBlockId();
          long dstId = dlblks.get(j).getBlock().getBlockId();
          long srcSz = slblks.get(j).getBlockSize();
          long dstSz = dlblks.get(j).getBlockSize();
          long srcGen = slblks.get(j).getBlock().getGenerationStamp();
          long dstGen = dlblks.get(j).getBlock().getGenerationStamp();
          if (srcSz != dstSz || srcGen != dstGen) {
            return null;
          }
          if (res == null) {
            res = new BlocksToDup(dblks.get(j).getBlock().getBlockPoolId());
          }
          res.addDupBlock(srcId, dstId, srcSz, srcGen);
        }
      }
    }
    return res;
  }
}
