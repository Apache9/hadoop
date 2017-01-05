package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
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

  private Map<DatanodeInfo, List<Block>> dnBlkMap = null;
  private String srcPool = null;
  private String dstPool = null;
  private Configuration conf = null;

  public FederationRenameBlockCollector(String inSrcPool, String inDstPool,
      DirectorySubTree subTree, Configuration inConf) {
    this.srcPool = inSrcPool;
    this.dstPool = inDstPool;
    this.conf = inConf;
    this.dnBlkMap = new HashMap<DatanodeInfo, List<Block>>();
    for (int i = 0; i < subTree.getSize(); i++) {
      HdfsFileStatus st = subTree.get(i);
      if (!st.isDir() && !st.isSymlink()) {
        LocatedBlocks blks = ((HdfsLocatedFileStatus) st).getBlockLocations();
        List<LocatedBlock> lblks = blks.getLocatedBlocks();
        for (LocatedBlock lblk : lblks) {
          if (srcPool == null) {
            srcPool = lblk.getBlock().getBlockPoolId();
          }
          for (DatanodeInfo datanode : lblk.getLocations()) {
            if (dnBlkMap.containsKey(datanode)) {
              List<Block> blockList = dnBlkMap.get(datanode);
              assert (blockList != null);
              blockList.add(new Block(lblk.getBlock().getBlockId(), lblk
                  .getBlockSize(), lblk.getBlock().getGenerationStamp()));
            } else {
              List<Block> blockList = new LinkedList<Block>();
              blockList.add(new Block(lblk.getBlock().getBlockId(), lblk
                  .getBlockSize(), lblk.getBlock().getGenerationStamp()));
              dnBlkMap.put(datanode, blockList);
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
    for (Map.Entry<DatanodeInfo, List<Block>> item : dnBlkMap.entrySet()) {
      // TBD: Make this to be multiple threads
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      InetSocketAddress dnAddr =
          NetUtils.createSocketAddr(item.getKey()
              .getIpcAddr(connectViaHostName));
      FederationClientDatanodeProtocol fcdp =
          FederationClientDatanodeProtocolTranslatorPB
              .createFederationClientDatanodeProtocolProxy(dnAddr, ugi, conf);
      fcdp.addBlocksToNewPool(srcPool, dstPool,
          item.getValue().toArray(new Block[0]));
      // TBD: To add logic to verify that we can go-on to next step
    }
  }
}
