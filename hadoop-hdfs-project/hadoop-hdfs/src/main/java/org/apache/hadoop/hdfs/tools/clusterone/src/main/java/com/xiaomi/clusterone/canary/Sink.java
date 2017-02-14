package com.xiaomi.clusterone.canary;

import org.apache.hadoop.fs.Path;

/**
 * Created by xiegang1 on 17-1-17.
 */
public interface Sink {
  enum NodeType {
    NMAE_NODE, DATA_NODE, JOURNAL_NODE
  }

  enum NodeState {
    LIVE, FAILED, DIE
  }

  enum OpType {
    READ, WRITE
  }

  void init(Object o);
  void publishNodeHealth(Sink.NodeType type, String host, Sink.NodeState state);
  void publishTiming(Sink.OpType type, long msTime);
  void publishAvailableStatus(boolean isAvailable);
  void publishCorruptBlocks(Path corruptFilePath);
  void publishCapacityRemaining(double percent);
  void publishDataNodeLatency(String dataNode, Sink.OpType type, long msTime);
  void publishDNReadLatencyPercentitle();
  void publishMaxTxIdDelta(String ns, long maxTxDelta);
  void publishMaxJournalDelay(String ns, long maxJournalDelay);
  void publishMaxLiveNodesDiff(String ns, long maxLiveNodesDiff);
  void publishDatanodeAvailability();
  void publishDatanodeReadSLAAvailability();
  void publishDatanodeWriteSLAAvailability();
  void publishCorruptedFileNum(long num);

  void reportSummary();
}
