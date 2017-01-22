package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.hadoop.util.Daemon;

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
      HdfsFileStatus st = subTree.get(i).getFileStatus();
      if (!st.isDir() && !st.isSymlink()) {
        LocatedBlocks blks = ((HdfsLocatedFileStatus) st).getBlockLocations();
        List<LocatedBlock> lblks = blks.getLocatedBlocks();
        for (LocatedBlock lblk : lblks) {
          if (srcPool == null) {
            srcPool = lblk.getBlock().getBlockPoolId();
          }
          long srcId = blksToDup.get(blksIdx).getSrcBlockId();
          long dstId = blksToDup.get(blksIdx).getDstBlockId();
          long srcGs = blksToDup.get(blksIdx).getSrcBlockGenStamp();
          long dstGs = blksToDup.get(blksIdx).getDstBlockGenStamp();
          blksIdx++;
          for (DatanodeInfo datanode : lblk.getLocations()) {
            if (dnBlkMap.containsKey(datanode)) {
              BlocksToDup dnBlkToDup = dnBlkMap.get(datanode);
              assert (dnBlkToDup != null);
              assert (srcId == lblk.getBlock().getBlockId());
              dnBlkToDup.addDupBlock(srcId, dstId, lblk.getBlockSize(), srcGs,
                  dstGs);
            } else {
              BlocksToDup dnBlkToDup =
                  new BlocksToDup(blksToDup.getDstPoolId());
              dnBlkToDup.addDupBlock(srcId, dstId, lblk.getBlockSize(), srcGs,
                  dstGs);
              dnBlkMap.put(datanode, dnBlkToDup);
            }
          }
        }
      }
    }
  }

  private Callable<Block[]> getLinkBlocksTskForOneDataNode(
      final Map.Entry<DatanodeInfo, BlocksToDup> item,
      final boolean viaHostName, final UserGroupInformation ugi) {
    return new Callable<Block[]>() {
      @Override
      public Block[] call() throws Exception {
        InetSocketAddress dnAddr =
            NetUtils.createSocketAddr(item.getKey().getIpcAddr(viaHostName));
        FederationClientDatanodeProtocol fcdp =
            FederationClientDatanodeProtocolTranslatorPB
                .createFederationClientDatanodeProtocolProxy(dnAddr, ugi, conf);
        return fcdp.addBlocksToNewPool(srcPool, item.getValue());
      }
    };
  }

  private Executor initExecutor(int threads) {
    return
        new ThreadPoolExecutor(1, threads, 1, TimeUnit.MILLISECONDS,
            new SynchronousQueue<Runnable>(), new Daemon.DaemonFactory() {
              private final AtomicInteger threadIndex = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread t = super.newThread(r);
                t.setName("DfsClientFederation-"
                    + threadIndex.getAndIncrement());
                return t;
              }
            });
  }

  public void linkBlocksToNewPool() throws IOException {
    boolean connectViaHostName =
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,
            DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    int numDns = dnBlkMap.size();
    int maxThreads =
        conf.getInt(DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_MAX_THREAD,
            DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_MAX_THREAD_DEFAULT);
    int numThreads = (numDns > maxThreads) ? maxThreads : numDns;
    List<Long> notFinishedBlks = new LinkedList<Long>();
    for (Map.Entry<DatanodeInfo, BlocksToDup> item : dnBlkMap.entrySet()) {
      for (BlocksToDup.DupBlockInfo dbi : item.getValue().getDupBlocksInfo()) {
        if (!notFinishedBlks.contains(dbi.getSrcBlockId())) {
          notFinishedBlks.add(dbi.getSrcBlockId());
        }
      }
    }
    Executor linkExecutor = initExecutor(numThreads);
    CompletionService<Block[]> linkService =
        new ExecutorCompletionService<Block[]>(linkExecutor);
    List<Future<Block[]>> futures = new LinkedList<Future<Block[]>>();
    Exception lastExp = null;
    // TBD: Assert unlinkedBlks contain a src blk only once
    for (Map.Entry<DatanodeInfo, BlocksToDup> item : dnBlkMap.entrySet()) {
      Callable<Block[]> oneDnTsk =
          getLinkBlocksTskForOneDataNode(item, connectViaHostName, ugi);
      Future<Block[]> tskFuture = linkService.submit(oneDnTsk);
      futures.add(tskFuture);
    }
    while (!futures.isEmpty()) {
      Future<Block[]> finishedTsk = null;
      try {
        finishedTsk = linkService.take();
        Block[] blks = finishedTsk.get();
        futures.remove(finishedTsk);
        for (Block b : blks) {
          notFinishedBlks.remove(b.getBlockId());
        }
      } catch (Exception e) {
        if (finishedTsk != null) {
          futures.remove(finishedTsk);
        }
        lastExp = e;
      }
    }
    if (!notFinishedBlks.isEmpty()) {
      if (lastExp != null) {
        throw new IOException(lastExp);
      } else {
        throw new IOException(
            "Cannot move all blocks for the rename operation in different namenodes");
      }
    }
  }
}
