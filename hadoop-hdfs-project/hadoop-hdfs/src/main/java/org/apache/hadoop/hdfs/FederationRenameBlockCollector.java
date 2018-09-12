package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

public class FederationRenameBlockCollector {

  public static final Log LOG = LogFactory.getLog(FederationRenameBlockCollector.class);
  private Map<DatanodeInfo, BlocksToDup> dnBlkMap = null;
  private Map<DatanodeInfo, UserGroupInformation> dnTicketMap = null;
  private String srcPool = null;
  private Configuration conf = null;
  private TOKEN_FROM tokenFrom;
  static enum TOKEN_FROM {
    Subtree, Login
  }

  private void buildDNTicketMapFromSubtree(DirectorySubTree subTree) {
    for (int i = 0; i < subTree.getSize(); i++) {
      HdfsFileStatus st = subTree.get(i).getFileStatus();
      if (!st.isDir() && !st.isSymlink()) {
        LocatedBlocks blks = ((HdfsLocatedFileStatus) st).getBlockLocations();
        List<LocatedBlock> lblks = blks.getLocatedBlocks();
        for (LocatedBlock lblk : lblks) {
          for (DatanodeInfo datanode : lblk.getLocations()) {
            UserGroupInformation ticket = UserGroupInformation
                .createRemoteUser(lblk.getBlock().getLocalBlock().toString());
            ticket.addToken(lblk.getBlockToken());
            dnTicketMap.put(datanode, ticket);
          }
        }
      }
    }
  }

  public FederationRenameBlockCollector(DirectorySubTree subTree,
      BlocksToDup blksToDup, Configuration inConf) throws IOException {
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
          if (lblk.isCorrupt()) {
            throw new IOException(
                "There are corrupt blocks in source directory, cannot rename to another namespace.");
          }
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

    this.dnTicketMap = new HashMap<DatanodeInfo, UserGroupInformation>();
    String fedRenameTokenValue =
        conf.get(CommonConfigurationKeys.HADOOP_FED_RENAME_TOKEN,
            CommonConfigurationKeys.HADOOP_FED_RENAME_TOKEN_DEFAULT);
    tokenFrom = TOKEN_FROM.valueOf(fedRenameTokenValue);
    if (tokenFrom == TOKEN_FROM.Subtree) {
      buildDNTicketMapFromSubtree(subTree);
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
        Block[] res = fcdp.addBlocksToNewPool(srcPool, item.getValue());
        RPC.stopProxy(fcdp);
        return res;
      }
    };
  }

  private Executor initExecutor(int threads, int queueSize) {
    return new ThreadPoolExecutor(threads, threads, 1, TimeUnit.MILLISECONDS,
        new LinkedBlockingDeque<Runnable>(queueSize),
        new Daemon.DaemonFactory() {
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

  private void shutDownExecutor(final Executor exc) {
    Thread t = new Daemon(new Runnable() {
      @Override
      public void run() {
        ((ThreadPoolExecutor) exc).shutdownNow();
      }
    });
    t.start();
  }

  class BlkReplicaInfo {
    int total;
    int linked;

    BlkReplicaInfo(int inTotal, int inLinked) {
      total = inTotal;
      linked = inLinked;
    }
  }

  public void linkBlocksToNewPool() throws IOException {
    boolean connectViaHostName =
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,
            DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    int numDns = dnBlkMap.size();
    int maxThreads =
        conf.getInt(DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_MAX_THREAD,
            DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_MAX_THREAD_DEFAULT);
    int numThreads = (numDns > maxThreads) ? maxThreads : numDns;
    int executorQueueSize = conf.getInt(
        DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_EXECUTOR_QUEUE_SIZE,
        DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_EXECUTOR_QUEUE_SIZE_DEFAULT);
    executorQueueSize = numDns > executorQueueSize ? executorQueueSize : numDns;

    long linkTimeout =
        conf.getLong(
            DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_TIMEOUT_MS,
            DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_TIMEOUT_MS_DEFAULT);
    int minLinks =
        conf.getInt(DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOKCS_MINIMAL,
            DFSConfigKeys.DFS_FEDERATION_CLIENT_LINK_BLOCKS_MINIMAL_DEFAULT);

    Map<Long, BlkReplicaInfo> notFinishedBlks = new HashMap<Long, BlkReplicaInfo>();
    for (Map.Entry<DatanodeInfo, BlocksToDup> item : dnBlkMap.entrySet()) {
      for (BlocksToDup.DupBlockInfo dbi : item.getValue().getDupBlocksInfo()) {
        if (!notFinishedBlks.containsKey(dbi.getSrcBlockId())) {
          notFinishedBlks.put(dbi.getSrcBlockId(), new BlkReplicaInfo(1, 0));
        } else {
          notFinishedBlks.get(dbi.getSrcBlockId()).total += 1;
        }
      }
    }
    Executor linkExecutor = initExecutor(numThreads, executorQueueSize);
    try {
      CompletionService<Block[]> linkService =
          new ExecutorCompletionService<Block[]>(linkExecutor);
      List<Future<Block[]>> futures = new LinkedList<Future<Block[]>>();
      Exception lastExp = null;
      // TBD: Assert unlinkedBlks contain a src blk only once
      for (Map.Entry<DatanodeInfo, BlocksToDup> item : dnBlkMap.entrySet()) {
        UserGroupInformation ticket = tokenFrom == TOKEN_FROM.Login ?
            loginUser : dnTicketMap.get(item.getKey());
        LOG.debug(tokenFrom+" "+ticket);
        Callable<Block[]> oneDnTsk =
            getLinkBlocksTskForOneDataNode(item, connectViaHostName, ticket);
        Future<Block[]> tskFuture = linkService.submit(oneDnTsk);
        futures.add(tskFuture);
      }
      long leftTimeout = linkTimeout;
      boolean allDone = false;
      while (!futures.isEmpty() && (leftTimeout > 0)) {
        Future<Block[]> finishedTsk = null;
        try {
          long start = Time.monotonicNow();
          finishedTsk = linkService.poll(leftTimeout, TimeUnit.MILLISECONDS);
          if (finishedTsk == null) {
            // Timed out
            for (Future<Block[]> f : futures) {
              if (!f.isDone()) {
                f.cancel(true);
              }
            }
            break;
          }
          leftTimeout -= (Time.monotonicNow() - start);
          Block[] blks = finishedTsk.get();
          futures.remove(finishedTsk);
          for (Block b : blks) {
            if (notFinishedBlks.containsKey(b.getBlockId())) {
              int linked = notFinishedBlks.get(b.getBlockId()).linked + 1;
              int total = notFinishedBlks.get(b.getBlockId()).total;
              if (linked == total || linked >= minLinks) {
                notFinishedBlks.remove(b.getBlockId());
              } else {
                notFinishedBlks.get(b.getBlockId()).linked += 1;
              }
            }
            if (notFinishedBlks.isEmpty()) {
              allDone = true;
              break;
            }
          }
          if (allDone) {
            break;
          }
        } catch (Exception e) {
          if (finishedTsk != null) {
            futures.remove(finishedTsk);
          }
          lastExp = e;
        }
      }

      if (!allDone) {
        if (lastExp != null) {
          throw new IOException(lastExp);
        } else {
          throw new IOException(
              "Cannot move all blocks for the rename operation in different namenodes");
        }
      }
    } finally {
      shutDownExecutor(linkExecutor);
    }
  }
}
