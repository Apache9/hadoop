package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Time;

public class FederationInProgressRenameMap {
  public static final Log LOG = LogFactory
      .getLog(FederationInProgressRenameMap.class);
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
      NameNode.getNameNodeMetrics().incrInProgressFedRenameSrc();
      sourceInProgress
          .add(new RenameRecord(src, srcId, dst, dstId, txid, start));
    } else {
      NameNode.getNameNodeMetrics().incrInProgressFedRenameDest();
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
      NameNode.getNameNodeMetrics().decrInProgressFedRenameSrc();
      sourceInProgress.remove(rr);
    } else {
      NameNode.getNameNodeMetrics().decrInProgressFedRenameDest();
      destInProgress.remove(rr);
    }
  }

  synchronized RenameRecord getRenameRecord(long txid, String srcId,
      String dstId, boolean isSource) {
    if (isSource) {
      for (RenameRecord rr : sourceInProgress) {
        // RenameId is unique in source list
        if (rr.getRenameId() == txid) {
          return rr;
        }
      }
    } else {
      for (RenameRecord rr : destInProgress) {
        if (rr.getRenameId() == txid && rr.getSrcId().equals(srcId)) {
          return rr;
        }
      }
    }
    return null;
  }
  
  synchronized List<RenameRecord> getTimeoutItems(long timeout, boolean isSource) {
    long now = Time.now();
    List<RenameRecord> res = new LinkedList<RenameRecord>();
    List<RenameRecord> list = null;
    if (isSource) {
      list = sourceInProgress;
    } else {
      list = destInProgress;
    }
    long maxDuration = 0;
    for (int idx = 0; idx < list.size(); idx++) {
      RenameRecord rr = list.get(idx);
      long duration = now - rr.getStartTime();
      if (duration > maxDuration) {
        maxDuration = duration;
      }
      if (duration >= timeout) {
        res.add(rr);
      } else {
        break;
      }
    }
    if (isSource) {
      NameNode.getNameNodeMetrics().setOldestItemFedRenameSrc(maxDuration);
    } else {
      NameNode.getNameNodeMetrics().setOldestItemFedRenameDest(maxDuration);
    }
    return res;
  }

  synchronized List<String> getDestPathes() {
    List<String> res = new LinkedList<String>();
    for (int idx = 0; idx < destInProgress.size(); idx++) {
      RenameRecord rr = destInProgress.get(idx);
      res.add(rr.getDst());
    }
    return res;
  }

  synchronized List<String> getSrcPaths() {
    List<String> res = new LinkedList<String>();
    for (int idx = 0; idx < sourceInProgress.size(); idx++) {
      RenameRecord rr = sourceInProgress.get(idx);
      res.add(rr.getSrc());
    }
    return res;
  }

  synchronized List<String> getAllRenamePaths() {
    List<String> res = new LinkedList<String>();
    res.addAll(getSrcPaths());
    res.addAll(getDestPathes());
    return res;
  }
}
