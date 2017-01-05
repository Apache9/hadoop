package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.FederationClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FederationInProgressRenameMap;
import org.apache.hadoop.hdfs.server.namenode.FederationInProgressRenameMap.RenameRecord;

public class FederationRenameFixer {

  Map<String, FederationClientProtocol> fedNNMap = null;

  public FederationRenameFixer() {
    fedNNMap = new HashMap<String, FederationClientProtocol>();
  }

  public void fixOneSourceItem(RenameRecord rr) throws IOException {
  }

  public void fixSource() throws IOException {
  }

}
