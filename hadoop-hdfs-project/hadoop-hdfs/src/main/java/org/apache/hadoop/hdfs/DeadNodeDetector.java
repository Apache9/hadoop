package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by xiegang1 on 17-7-28.
 */
public class DeadNodeDetector implements Runnable {
  public static final Log LOG = LogFactory.getLog(DeadNodeDetector.class);

  private String name;
  private Configuration conf;
  //dead nodes shared by all the DFSInputStreams of the client
  private final ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes;
  private final ConcurrentHashMap<DFSInputStream, HashSet<DatanodeInfo>> DFSInputStreamNodesMap;

  private int deadNodeDetectTimeout = 0;
  private long deadNodeDetectInterval = 0;
  private long lastDetectLiveTS = 0;
  private long lastDetectDeadTS = 0;
  private long aliveNodeDetectInterval = 0;
  private int retries = 0;

  private ConcurrentHashMap<DatanodeInfo, DatanodeInfo> probeInProg = new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();

  private ArrayList<DatanodeInfo> liveNodesProbeQueue = new ArrayList<DatanodeInfo>();
  private ArrayList<DatanodeInfo> deadNodesProbeQueue = new ArrayList<DatanodeInfo>();
  private int maxLiveNodesProbeQueueLen = 0;
  private int maxDeadNodesProbeQueueLen = 0;

  private Thread probeLiveNodesSchedulerThr;
  private Thread probeDeadNodesSchedulerThr;

  private ExecutorService probeLiveNodesThreadPool;
  private ExecutorService probeDeadNodesThreadPool;

  private boolean enableProbeLiveNodes = false;

  private enum State {
    INIT,
    CHECK_ALIVE,
    CHECK_DEAD,
    IDLE,
    ERROR
  }

  private enum ProbeType {
    CHECK_ALIVE,
    CHECK_DEAD
  }

  private State state;

  public DeadNodeDetector(Configuration conf, String name) {
    this.deadNodes = new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
    this.DFSInputStreamNodesMap = new ConcurrentHashMap<DFSInputStream, HashSet<DatanodeInfo>>();

    this.name = name;
    this.conf = conf;

    deadNodeDetectTimeout = conf.getInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_TIMEOUT_DEFALT);
    deadNodeDetectInterval = conf.getLong(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_KEY,
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_INTERVAL_DEFALT);
    aliveNodeDetectInterval = conf.getLong(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_KEY,
        DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_INTERVAL_DEFALT);
    retries = conf.getInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_KEY,
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_RETRIES_DEFALT);

    maxLiveNodesProbeQueueLen = conf.getInt(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_QUEUE_MAX_KEY,
        DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_QUEUE_MAX_DEFALT);
    maxDeadNodesProbeQueueLen = conf.getInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_QUEUE_MAX_KEY,
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_QUEUE_MAX_DEFALT);

    int deadNodeDetectLiveThreads = conf.getInt(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_THREADS_KEY,
        DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_THREADS_DEFALT);
    int deadNodeDetectDeadThreads = conf.getInt(DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_THREADS_KEY,
        DFSConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_THREADS_DEFALT);

    enableProbeLiveNodes = conf.getBoolean(DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_KEY,
        DFSConfigKeys.DFS_CLIENT_LIVE_NODE_DETECT_ENABLE_DEFALT);

    lastDetectLiveTS = Time.monotonicNow();
    lastDetectDeadTS = Time.monotonicNow();

    probeLiveNodesThreadPool = Executors.newFixedThreadPool(deadNodeDetectLiveThreads);
    probeDeadNodesThreadPool = Executors.newFixedThreadPool(deadNodeDetectDeadThreads);

    probeDeadNodesSchedulerThr = new Thread(new ProbeScheduler(this, ProbeType.CHECK_DEAD));
    probeDeadNodesSchedulerThr.setDaemon(true);
    probeDeadNodesSchedulerThr.start();

    probeLiveNodesSchedulerThr = new Thread(new ProbeScheduler(this, ProbeType.CHECK_ALIVE));
    probeLiveNodesSchedulerThr.setDaemon(true);
    probeLiveNodesSchedulerThr.start();

    LOG.info("start dead node detector for DFSClient " + this.name);
    state = State.INIT;
  }

  private synchronized void addToLiveNodesProbeQueue(DatanodeInfo datanodeInfo) {
    if (liveNodesProbeQueue.size() >= maxLiveNodesProbeQueueLen) {
      return;
    }
    liveNodesProbeQueue.add(datanodeInfo);
  }


  private synchronized void addToDeadNodesProbeQueue(DatanodeInfo datanodeInfo) {
    if (deadNodesProbeQueue.size() > maxDeadNodesProbeQueueLen) {
      return;
    }
    deadNodesProbeQueue.add(datanodeInfo);
  }

  private synchronized DatanodeInfo pollFromLiveNodesProbeQueue() {
    if (liveNodesProbeQueue.isEmpty()) {
      return null;
    }
    return liveNodesProbeQueue.remove(0);
  }


  private synchronized DatanodeInfo pollFromDeadNodesProbeQueue() {
    if (deadNodesProbeQueue.isEmpty()) {
      return null;
    }
    return deadNodesProbeQueue.remove(0);
  }

  private synchronized boolean isLiveNodesProbeQueueFull () {
    return liveNodesProbeQueue.size() >= maxLiveNodesProbeQueueLen;
  }

  private synchronized boolean isDeadNodesProbeQueueFull () {
    return deadNodesProbeQueue.size() >= maxDeadNodesProbeQueueLen;
  }

  private void checkDeadNodes() {
    long ts = Time.monotonicNow();
    if (ts - lastDetectDeadTS > deadNodeDetectInterval) {
      Set<DatanodeInfo> datanodeInfos = getDeadNodesToDetect();
      for (DatanodeInfo datanodeInfo : datanodeInfos) {
        LOG.debug("add dead node to check " + datanodeInfo);
        if (isDeadNodesProbeQueueFull()) {
          break;
        }
        addToDeadNodesProbeQueue(datanodeInfo);
      }
      lastDetectDeadTS = ts;
    }

    state = State.IDLE;
  }

  private void checkLiveNodes() {
    long ts = Time.monotonicNow();
    if (enableProbeLiveNodes && (ts - lastDetectLiveTS > aliveNodeDetectInterval)) {
      Set<DatanodeInfo> datanodeInfos = getLiveNodesToDetect();
      if (datanodeInfos != null) {
        for (DatanodeInfo datanodeInfo : datanodeInfos) {
          LOG.debug("add live node to check " + datanodeInfo);
          if (isLiveNodesProbeQueueFull()) {
            break;
          }
          addToLiveNodesProbeQueue(datanodeInfo);
        }
      }
      lastDetectLiveTS = ts;
    }

    state = State.CHECK_DEAD;
  }

  private void idle () {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }

    state = State.CHECK_ALIVE;
  }

  private void init() {
    state = State.CHECK_DEAD;
  }

  @Override
  public void run() {
    while (true) {
      LOG.debug("state " + state);
      switch (state) {
        case INIT:
          init();
          break;
        case CHECK_ALIVE:
          checkLiveNodes();
          break;
        case CHECK_DEAD:
          checkDeadNodes();
          break;
        case IDLE:
          idle();
          break;
        case ERROR:
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
          }
          return;
        default:
          break;
      }
    }
  }

  public void probeCallBack (Probe probe, boolean success) {
    LOG.debug("probe datanode " + probe.getDatanodeInfo() + " success=" + success + " type=" + probe.getType());
    probeInProg.remove(probe.getDatanodeInfo());
    if (success) {
      if (probe.getType() == ProbeType.CHECK_DEAD) {
        LOG.info("remove the node out from dead " + probe.getDatanodeInfo());
        removeFromDead(probe.getDatanodeInfo());
      }
    } else {
      if (probe.getType() == ProbeType.CHECK_ALIVE) {
        LOG.info("add the node to dead " + probe.getDatanodeInfo());
        addToDead(probe.getDatanodeInfo());
      }
    }
  }

  public void scheduleProbe(ProbeType type) {
    LOG.debug("schedule probe type=" + type);
    DatanodeInfo datanodeInfo = null;
    if (type == ProbeType.CHECK_ALIVE) {
      while ((datanodeInfo = pollFromLiveNodesProbeQueue()) != null) {
        if (probeInProg.contains(datanodeInfo)) {
          continue;
        }
        probeInProg.put(datanodeInfo, datanodeInfo);
        Probe probe = new Probe(this, datanodeInfo, ProbeType.CHECK_ALIVE);
        probeLiveNodesThreadPool.execute(probe);
      }
    } else if (type == ProbeType.CHECK_DEAD) {
      while ((datanodeInfo = pollFromDeadNodesProbeQueue()) != null) {
        if (probeInProg.contains(datanodeInfo)) {
          continue;
        }
        probeInProg.put(datanodeInfo, datanodeInfo);
        Probe probe = new Probe(this, datanodeInfo, ProbeType.CHECK_DEAD);
        probeDeadNodesThreadPool.execute(probe);
      }
    }
  }

  class Probe implements Runnable {
    DeadNodeDetector deadNodeDetector = null;
    DatanodeInfo datanodeInfo = null;
    ProbeType type;


    public Probe(DeadNodeDetector deadNodeDetector, DatanodeInfo datanodeInfo, ProbeType type) {
      this.deadNodeDetector = deadNodeDetector;
      this.datanodeInfo = datanodeInfo;
      this.type = type;
    }

    public DatanodeInfo getDatanodeInfo() {
      return datanodeInfo;
    }

    public ProbeType getType() {
      return this.type;
    }

    @Override
    public void run() {
      LOG.debug("check node " + datanodeInfo + " type=" + this.type);
      int retries = deadNodeDetector.retries;
      while (retries > 0) {
        try {
          ClientDatanodeProtocol proxy = DFSUtil.createClientDatanodeProtocolProxy(datanodeInfo,
              deadNodeDetector.conf,
              deadNodeDetectTimeout,
              true
          );
          DatanodeLocalInfo localInfo = proxy.getDatanodeInfo();
          deadNodeDetector.probeCallBack(this, true);
          return ;
        } catch (IOException e) {
          LOG.error(e);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
          }
          retries--;
        }
      }

      deadNodeDetector.probeCallBack(this, false);
    }
  }

  public void addToDead(DatanodeInfo datanodeInfo) {
    deadNodes.put(datanodeInfo, datanodeInfo);
  }
  private void removeFromDead(DatanodeInfo datanodeInfo) {
    deadNodes.remove(datanodeInfo);
  }

  public synchronized void addNodeToDetect (DFSInputStream dfsInputStream, DatanodeInfo datanodeInfo) {
    HashSet<DatanodeInfo> datanodeInfos = DFSInputStreamNodesMap.get(dfsInputStream);
    if (datanodeInfos == null) {
      datanodeInfos = new HashSet<DatanodeInfo>();
      datanodeInfos.add(datanodeInfo);
      DFSInputStreamNodesMap.put(dfsInputStream, datanodeInfos);
    } else {
      datanodeInfos.add(datanodeInfo);
    }
  }

  public synchronized void removeNodeFromDetect(DFSInputStream dfsInputStream, DatanodeInfo datanodeInfo) {
    HashSet<DatanodeInfo> datanodeInfos = DFSInputStreamNodesMap.get(dfsInputStream);
    if (datanodeInfos != null) {
      datanodeInfos.remove(datanodeInfo);
      if (datanodeInfos.isEmpty()) {
        DFSInputStreamNodesMap.remove(dfsInputStream);
      }
    }
  }

  public synchronized Set<DatanodeInfo> getDeadNodesToDetect() {
    // remove the dead nodes who doesn't have any inputstream first
    HashSet<DatanodeInfo> newDeadNodes = new HashSet<DatanodeInfo>();
    for (HashSet<DatanodeInfo> datanodeInfos : DFSInputStreamNodesMap.values()) {
      newDeadNodes.addAll(datanodeInfos);
    }

    newDeadNodes.retainAll(deadNodes.values());

    for (DatanodeInfo datanodeInfo: deadNodes.values()) {
      if (!newDeadNodes.contains(datanodeInfo)) {
        deadNodes.remove(datanodeInfo);
      }
    }
    return newDeadNodes;
  }

  public synchronized Set<DatanodeInfo> getLiveNodesToDetect() {
    HashSet<DatanodeInfo> liveNodes = new HashSet<DatanodeInfo>();
    for (HashSet<DatanodeInfo> datanodeInfos : DFSInputStreamNodesMap.values()) {
      liveNodes.addAll(datanodeInfos);
    }

    liveNodes.removeAll(deadNodes.values());

    return liveNodes;
  }

  public boolean hasDeadNodes(DatanodeInfo datanodeInfo) {
    return deadNodes.contains(datanodeInfo);
  }

  public ConcurrentHashMap<DatanodeInfo, DatanodeInfo> getDeadNodes () {
    return deadNodes;
  }

  class ProbeScheduler implements Runnable {
    private DeadNodeDetector deadNodeDetector;
    private ProbeType type;
    public ProbeScheduler (DeadNodeDetector deadNodeDetector, ProbeType type) {
      this.deadNodeDetector = deadNodeDetector;
      this.type = type;
    }
    @Override
    public void run() {
      while(true) {
        deadNodeDetector.scheduleProbe(type);
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
        }
      }

    }
  }

}
