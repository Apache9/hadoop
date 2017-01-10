package org.apache.hadoop.hdfs.tools;

import com.xiaomi.infra.hadoop.FalconSink;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.tools.Probe;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProbeFactory {
  public enum ProbeType {
    DN_IO_LATENCY
  }

  private static final Log LOG = LogFactory.getLog(Canary.class);

  public static Probe getProbeByType (ProbeType type, Canary.ProbeManager probeManager) {
    Probe probe = null;
    switch (type) {
      case DN_IO_LATENCY:
        probe = new DataNodeLatencyProbe(probeManager);
        break;
      default:
        LOG.error("not implement the probe type");
    }
    return probe;
  }
}


class DataNodeLatencyProbe extends Thread implements Probe {
  private static final int batchedTaskNum = 1;
  private static final int defaultProbeTimeout = 20000;
  private Canary.ProbeManager probeManager = null;
  private Canary canary = null;
  private List<DatanodeInfo> subTaskList = null;
  private boolean shouldStop = false;
  private static ExecutorService threadPool = Executors.newCachedThreadPool();
  private static final Log LOG = LogFactory.getLog(Canary.class);

  public DataNodeLatencyProbe(Canary.ProbeManager probeManager) {
    this.probeManager = probeManager;
    this.canary = probeManager.getCanary();
  }

  public void startProbe() {
    threadPool.execute(this);
  }
  public void stopProbe() {
    shouldStop = true;
  }

  private void probeDNReadLatency() {
    Canary.Sink sink = canary.getSink();
    Configuration conf = canary.getConf();
    int timeout = conf.getInt("dfs.canary.probe.timeout", defaultProbeTimeout);

    for (DatanodeInfo info : subTaskList) {
      try {
        long startTime = System.currentTimeMillis();

        ClientDatanodeProtocol proxy = DFSUtil.createClientDatanodeProtocolProxy(info,conf,
            conf.getInt("dfs.canary.probe.timeout", timeout),
            true
        );
        DatanodeLocalInfo localInfo = proxy.getDatanodeInfo();
        long latency = System.currentTimeMillis() - startTime;
        LOG.info(String.format("DataNode [%s] Read Latency : %d",
                info.getHostName(), latency));
        sink.publishDataNodeLatency(info.getHostName(), Canary.Sink.OpType.READ, latency);
      } catch (IOException e) {
        if (sink.getClass().equals(FalconSink.class)) {
          ((FalconSink)sink).addFailedDatanodes(1);
        }
        // when I/O error, we set the latency to the default max
        sink.publishDataNodeLatency(info.getHostName(), Canary.Sink.OpType.READ, timeout);
        LOG.error(String.format("Get datanode [%s]:[%s] info failed", info.getHostName(), info.getInfoPort()), e);
      }

      if (sink.getClass().equals(FalconSink.class)) {
        ((FalconSink)sink).addProbedDatanodes(1);
      }
    }
  }

  public void run() {

    while (!shouldStop) {
      subTaskList = probeManager.getTasks(batchedTaskNum);
      if (subTaskList == null || subTaskList.size() == 0) {
        try {
          Thread.sleep(2000);
        } catch (Exception e) {
          LOG.info("Sleep in Canary prob is interrupted");
        }
        continue;
      }
      probeDNReadLatency();
      probeManager.completeNTasks(this, subTaskList.size());
    }
  }

  public void CompleteCallBack() {
    Canary.Sink sink = canary.getSink();

    sink.publishDNReadLatencyPercentitle();
    sink.publishDatanodeAvailability();
    sink.publishDatanodeSLAAvailability();

    if (sink.getClass().equals(FalconSink.class)) {
      ((FalconSink)sink).setFailedDatanode(0);
      ((FalconSink)sink).setProbedDatanode(0);
    }
  }
}
