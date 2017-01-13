package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.protocol.FederationClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.FederationClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

public class FederationRenameBlockCollector {

  private Map<DatanodeInfo, BlocksToDup> dnBlkMap = null;
  private String srcPool = null;
  private Configuration conf = null;

  public FederationRenameBlockCollector(DirectorySubTree subTree,
      BlocksToDup blksToDup, Configuration inConf) {
    this.conf = inConf;
    this.dnBlkMap = new HashMap<DatanodeInfo, BlocksToDup>();
    int blksIdx = 0;
    for (int i = 0; i < subTree.getSize(); i++) {
      HdfsFileStatus st = subTree.get(i);
      if (!st.isDir() && !st.isSymlink()) {
        LocatedBlocks blks = ((HdfsLocatedFileStatus) st).getBlockLocations();
        List<LocatedBlock> lblks = blks.getLocatedBlocks();
        for (LocatedBlock lblk : lblks) {
          if (srcPool == null) {
            srcPool = lblk.getBlock().getBlockPoolId();
          }
          long srcId = blksToDup.get(blksIdx).getSrcBlockId();
          long dstId = blksToDup.get(blksIdx).getDstBlockId();
          blksIdx++;
          for (DatanodeInfo datanode : lblk.getLocations()) {
            if (dnBlkMap.containsKey(datanode)) {
              BlocksToDup dnBlkToDup = dnBlkMap.get(datanode);
              assert (dnBlkToDup != null);
              assert (srcId == lblk.getBlock().getBlockId());
              dnBlkToDup.addDupBlock(srcId, dstId, lblk.getBlockSize(), lblk
                  .getBlock().getGenerationStamp());
            } else {
              BlocksToDup dnBlkToDup =
                  new BlocksToDup(blksToDup.getDstPoolId());
              dnBlkToDup.addDupBlock(srcId, dstId, lblk.getBlockSize(), lblk
                  .getBlock().getGenerationStamp());
              dnBlkMap.put(datanode, dnBlkToDup);
            }
          }
        }
      }
    }
  }

  public void linkBlocksToNewPool() throws IOException {
    boolean connectViaHostName =
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,
            DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    for (Map.Entry<DatanodeInfo, BlocksToDup> item : dnBlkMap.entrySet()) {
      // TBD: Make this to be multiple threads
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      InetSocketAddress dnAddr =
          NetUtils.createSocketAddr(item.getKey()
              .getIpcAddr(connectViaHostName));
      FederationClientDatanodeProtocol fcdp =
          FederationClientDatanodeProtocolTranslatorPB
              .createFederationClientDatanodeProtocolProxy(dnAddr, ugi, conf);
      fcdp.addBlocksToNewPool(srcPool, item.getValue());
      // TBD: To add logic to verify that we can go-on to next step
    }
  }
}
