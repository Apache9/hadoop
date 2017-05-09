package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.FederationRenameBlockCollector;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.BlocksToDup;
import org.apache.hadoop.hdfs.protocol.DirectorySubTree;
import org.apache.hadoop.hdfs.protocol.FederationClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FederationInProgressRenameMap;
import org.apache.hadoop.hdfs.server.namenode.FederationInProgressRenameMap.RenameRecord;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.ZkConfiguredFailoverProxyProvider;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Daemon;

public class FederationRenameFixer {

  private Map<String, FederationClientProtocol> fedNNMap = null;
  private Configuration conf;
  private FSNamesystem fsNameSys;
  private volatile boolean shouldRuning;
  static final Log LOG = LogFactory.getLog(FederationRenameFixer.class);
  Daemon sourceFixerThread = null;
  Daemon destFixerThread = null;

  public FederationRenameFixer(Configuration conf, FSNamesystem fsNameSys) {
    fedNNMap = new HashMap<String, FederationClientProtocol>();
    this.conf = conf;
    this.fsNameSys = fsNameSys;
  }

  public void activate() {
    long srcTimeout =
        conf.getLong(DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT,
            DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT_DEFAULT);
    long dstTimeout =
        conf.getLong(DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT,
            DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT_DEFAULT);
    shouldRuning = true;
    this.sourceFixerThread =
        new Daemon(new RenameFixerMonitor(true, srcTimeout / 3));
    this.destFixerThread =
        new Daemon(new RenameFixerMonitor(false, dstTimeout / 3));
    this.sourceFixerThread.start();
    this.destFixerThread.start();
    LOG.info("fixer is started");
  }

  public void deactivate() {
    shouldRuning = false;
    try {
      if (this.sourceFixerThread != null) {
        this.sourceFixerThread.interrupt();
      }
      if (this.destFixerThread != null) {
        this.destFixerThread.interrupt();
      }
      if (this.sourceFixerThread != null) {
        this.sourceFixerThread.join(2000);
      }
      if (this.destFixerThread != null) {
        this.destFixerThread.join(1000);
      }
    } catch (InterruptedException ie) {
    }
    this.sourceFixerThread = null;
    this.destFixerThread = null;
    LOG.info("fixer is stopped");
  }

  public boolean fixOneSourceItem(RenameRecord rr) throws IOException,
      URISyntaxException {
    // Step1 : Check if the dest record exist
    FederationClientProtocol fcp = fedNNMap.get(rr.getDstId());
    if (fcp == null) {
      conf.setBoolean(
          DFSConfigKeys.DFS_CLIENT_FAILOVER_PROVIDER_TOLERATE_EMPTY_NNADDR,
          true);
      URI dst = new URI(rr.getDstId());
      conf.set(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX
          + "." + dst.getAuthority(),
          ZkConfiguredFailoverProxyProvider.class.getName());
      AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);
      NameNodeProxies.ProxyAndInfo<FederationClientProtocol> fedProxyInfo =
          NameNodeProxies.createProxy(conf, new URI(rr.getDstId()),
              FederationClientProtocol.class, nnFallbackToSimpleAuth);
      fcp = fedProxyInfo.getProxy();
      fedNNMap.put(rr.getDstId(), fcp);
    }
    if (fcp.renameRecordExist(rr.getRenameId(), rr.getSrcId(), rr.getDstId(),
        false) == false) {
      // Case 1 : record does not exist on dest. Simply cancel the rename.
      return fsNameSys.federationRenameSrcPhase2(rr.getRenameId(), true);
    } else {
      // Case 2 : record does exist on dest. Redo the left steps of the two
      // phase commit.
      DirectorySubTree srcSubTree =
          fsNameSys.federationRenameBuildSubTree(rr.getSrc());
      DirectorySubTree dstSubTree = fcp.getRenameDestSubTree(rr.getDst());
      if (srcSubTree == null || dstSubTree == null) {
        return false;
      }
      BlocksToDup blksToDup =
          BlocksToDup.buildFromSubTrees(srcSubTree, dstSubTree);
      // Ask datanodes to add new links
      if (blksToDup.size() != 0) {
        FederationRenameBlockCollector frbc = null;
        frbc = new FederationRenameBlockCollector(srcSubTree, blksToDup, conf);
        frbc.linkBlocksToNewPool();
      }
      // Commit source
      fsNameSys.federationRenameSrcPhase2(rr.getRenameId(), false);
      // Commit dest
      return fcp.renameDestPhase2(rr.getRenameId(), rr.getSrcId());
    }
  }

  public void fixSource() throws IOException {
    long timeout =
        conf.getLong(DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT,
            DFSConfigKeys.DFS_FEDERATION_RENAME_SOURCE_TIMEOUT_DEFAULT);
    List<RenameRecord> items =
        fsNameSys.getFederationRenameMap().getTimeoutItems(timeout, true);
    for (int i = 0; i < items.size(); i++) {
      final RenameRecord rr = items.get(i);
      try {
        SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Boolean>() {
          @Override
          public Boolean run() throws Exception {
            boolean res = fixOneSourceItem(rr);
            LOG.info("Fixed source item " + rr);
            return res;
          }
        });
      } catch (Throwable t) {
        // Ignore
        LOG.warn("Fix rename item " + rr + " failed", t);
      }
    }
  }

  public boolean fixOneDestItem(RenameRecord rr) throws IOException,
      URISyntaxException {
    // Step1 : Check if the source record exist
    FederationClientProtocol fcp = fedNNMap.get(rr.getSrcId());
    if (fcp == null) {
      conf.setBoolean(
          DFSConfigKeys.DFS_CLIENT_FAILOVER_PROVIDER_TOLERATE_EMPTY_NNADDR,
          true);
      URI src = new URI(rr.getSrcId());
      conf.set(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX
          + "." + src.getAuthority(),
          ZkConfiguredFailoverProxyProvider.class.getName());
      AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);
      NameNodeProxies.ProxyAndInfo<FederationClientProtocol> fedProxyInfo =
          NameNodeProxies.createProxy(conf, new URI(rr.getSrcId()),
              FederationClientProtocol.class, nnFallbackToSimpleAuth);
      fcp = fedProxyInfo.getProxy();
      fedNNMap.put(rr.getSrcId(), fcp);
    }
    if (fcp.renameRecordExist(rr.getRenameId(), rr.getSrcId(), rr.getDstId(),
        true)) {
      // The rename record exists on source side, let source side handle it
      return false;
    } else {
      // Finish the left steps of the 2-pc
      return fsNameSys.federationRenameDestPhase2(rr.getRenameId(),
          rr.getSrcId());
    }
  }

  public void fixDest() throws IOException {
    long timeout =
        conf.getLong(DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT,
            DFSConfigKeys.DFS_FEDERATION_RENAME_DEST_TIMEOUT_DEFAULT);
    List<RenameRecord> items =
        fsNameSys.getFederationRenameMap().getTimeoutItems(timeout, false);
    for (int i = 0; i < items.size(); i++) {
      final RenameRecord rr = items.get(i);
      try {
        SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Boolean>() {
          @Override
          public Boolean run() throws Exception {
            boolean res = fixOneDestItem(rr);
            LOG.info("Fixed dest item " + rr);
            return res;
          }
        });
      } catch (Throwable t) {
        // Ignore
        LOG.warn("Fix rename item " + rr + " failed", t);
      }
    }
  }

  private class RenameFixerMonitor implements Runnable {
    private boolean isSource;
    private long recheckInterval;

    RenameFixerMonitor(boolean inIsSource, long inRecheckInterval) {
      isSource = inIsSource;
      recheckInterval = inRecheckInterval;
    }

    @Override
    public void run() {
      while (fsNameSys.isRunning() && shouldRuning) {
        try {
          Thread.sleep(recheckInterval);
          LOG.info("Federation rename fixer is scheduled");
          if (isSource) {
            fixSource();
          } else {
            fixDest();
          }
        } catch (Throwable t) {
          if (!fsNameSys.isRunning() || !shouldRuning) {
            if (!(t instanceof InterruptedException)) {
              LOG.info("Fixer received an exception, will exit ", t);
            }
            break;
          }
          LOG.error("RenameFixerMonitor thread received exception", t);
        }
      }
    }
  }
}
