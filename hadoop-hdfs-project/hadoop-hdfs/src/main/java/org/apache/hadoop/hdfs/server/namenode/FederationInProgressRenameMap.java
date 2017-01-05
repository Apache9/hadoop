package org.apache.hadoop.hdfs.server.namenode;

import java.util.LinkedList;
import java.util.List;

public class FederationInProgressRenameMap {

  public class RenameRecord {
    String src;
    String dst;
    String srcId;
    String dstId;
    long renameId;
    long startTime;

    RenameRecord(String inSrc, String inSrcId, String inDst, String inDstId,
        long inRenameId,
        long inStart) {
      src = inSrc;
      dst = inDst;
      srcId = inSrcId;
      dstId = inDstId;
      renameId = inRenameId;
      startTime = inStart;
    }

    public String getSrc() {
      return src;
    }

    public String getDst() {
      return dst;
    }

    public String getSrcId() {
      return srcId;
    }

    public String getDstId() {
      return dstId;
    }

    public long getRenameId() {
      return renameId;
    }

    public long getStartTime() {
      return startTime;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("src = ").append(src).append(" dst = ").append(dst)
          .append(" srcId = ").append(srcId).append(" dstId = ").append(dstId)
          .append(" renameid = ").append(renameId).append(" start = ")
          .append(startTime);
      return builder.toString();
    }
  }

  private List<RenameRecord> sourceInProgress;
  private List<RenameRecord> destInProgress;

  FederationInProgressRenameMap() {
    sourceInProgress = new LinkedList<RenameRecord>();
    destInProgress = new LinkedList<RenameRecord>();
  }

  synchronized void addRenameRecord(long txid, String src, String srcId,
      String dst, String dstId, boolean isSource, long start) {
    if (isSource) {
      sourceInProgress
          .add(new RenameRecord(src, srcId, dst, dstId, txid, start));
    } else {
      destInProgress.add(new RenameRecord(src, srcId, dst, dstId, txid, start));
    }
  }

  synchronized void removeRenameRecord(long txid, String srcId, String dstId,
      boolean isSource) {
    RenameRecord rr = getRenameRecord(txid, srcId, dstId, isSource);
    if (rr == null) {
      return;
    }
    if (isSource) {
      sourceInProgress.remove(rr);
    } else {
      destInProgress.remove(rr);
    }
  }

  synchronized RenameRecord getRenameRecord(long txid, String srcId,
      String dstId, boolean isSource) {
    if (isSource) {
      for (RenameRecord rr : sourceInProgress) {
        // RenameId is unique in source list
        if (rr.getRenameId() == txid) {
          sourceInProgress.remove(rr);
          return rr;
        }
      }
    } else {
      for (RenameRecord rr : destInProgress) {
        if (rr.getRenameId() == txid && rr.getSrcId().equals(srcId)) {
          destInProgress.remove(rr);
          return rr;
        }
      }
    }
    return null;
  }
}
