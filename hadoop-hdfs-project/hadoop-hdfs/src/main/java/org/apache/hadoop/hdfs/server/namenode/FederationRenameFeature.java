package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.INode.Feature;

public class FederationRenameFeature implements Feature {
  private boolean isRenameSource;
  private long renameId;
  private String src;
  private String srcId;
  private String dst;
  private String dstId;
  private long start;

  FederationRenameFeature(boolean isSrc, long renameId, String src,
      String srcId, String dst, String dstId, long start) {
    this.isRenameSource = isSrc;
    this.renameId = renameId;
    this.src = src;
    this.srcId = srcId;
    this.dst = dst;
    this.dstId = dstId;
    this.start = start;
  }

  public boolean isSource() {
    return isRenameSource;
  }

  public long getRenameId() {
    return renameId;
  }

  public String getSrc() {
    return src;
  }

  public String getSrcId() {
    return srcId;
  }

  public String getDst() {
    return dst;
  }

  public String getDstId() {
    return dstId;
  }

  public long getStart() {
    return start;
  }
}
