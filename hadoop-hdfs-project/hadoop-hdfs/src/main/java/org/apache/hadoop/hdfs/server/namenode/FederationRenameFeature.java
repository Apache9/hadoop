package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.INode.Feature;

public class FederationRenameFeature implements Feature {
  private boolean isRenameSource;

  FederationRenameFeature(boolean isSrc) {
    this.isRenameSource = isSrc;
  }
}
