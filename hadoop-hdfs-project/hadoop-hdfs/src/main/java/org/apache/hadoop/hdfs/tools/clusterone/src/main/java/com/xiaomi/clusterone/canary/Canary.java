package com.xiaomi.clusterone.canary;

/**
 * Created by xiegang1 on 17-1-17.
 */

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class Canary implements Configurable, Runnable {

  private static final long DEFAULT_INTERVAL = 6000;
  private static final long DEFAULT_AVAIL_DETECT_INTERAL = 100;
  private static final long DEFAULT_TX_DETECT_INTERAL = 3600000; // 1 hour
  private static final String DEFAULT_TEST_PATH_BASE = "/hdfs_canary/.health_monitoring_canary_";
  private static final String DEFAULT_PATH_FOR_AVAILABILITY_TEST = "hdfs_canary/.file_for_availability_test";
  private static final int DEFAULT_TEST_DATA_SIZE = 4194304; // 4MB
  private static final int DEFAULT_AVAIL_TEST_DATA_SIZE = 1024; // 1k
  private static final String NN_JMX_SUFFIX = "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";
  private static final String JN_JMX_SUFFIX = "/jmx?qry=Hadoop:service=JournalNode,name=Journal-";
  private static final boolean DEFAULT_DATANODE_LATENCY_ENABLE = true;
  private static final int DEFAULT_REPEATS = 3;
  private static final boolean DEFAULT_ENABLE_lIST_CORRUPTEDFILES = false;
  private Log LOG = null;

  private Configuration conf = null;
  private DistributedFileSystem dfs = null;
  private ArrayList<URL> jnList = null;

  //configurable variables
  private long interval = DEFAULT_INTERVAL;
  private long availDetectInterval;
  private String testfilePathBase;
  private int testfileDataSize;
  private String availTestfilePath;
  private int availTestfileDataSize;
  private int rpcTimeoutForChecks = 0;
  private Sink sink = null;

  private int repeats = 0;

  // datanodes prob
  private ProbeManager probManger = null;
  private boolean enableDatanodeLatencyCheck = false;

  private long lastTxCheckTime = 0;
  private boolean enableListCorruptedFiles = false;

  public Canary() {
    this(null);
  }

  public Canary(HdfsConfiguration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Sink getSink() {
    return sink;
  }

  public DistributedFileSystem getDFS () {
    return dfs;
  }

  @Override
  public void run() {

    try {
      prepare();
    } catch (Exception e) {
      LOG.error("Canary exited unexpectedly " + e);
      return;
    }

    int run = 0;

    while (run <= repeats) {

      long startTime = System.currentTimeMillis();
      boolean clusterIsAvailable = true;

      try {
        checkIOLatency();
        // start Datanodes prob
        if (enableDatanodeLatencyCheck) {
          startDataNodeLatencyProb();
        }
      } catch (IOException e) {
        // if got socketTimeoutException when renew lease, dfsClient will fail in the following operation
        // since canary is stateless, exit directly. supervisor will start Canary again in clean state
        if (isFSclosed(e.getMessage())) {
          if (probManger != null) {
            probManger.stopProbs();
          }
          return;
        }
        clusterIsAvailable = false;
        LOG.warn("Test IO latency failed, will start availability detect");
      }

      sink.publishAvailableStatus(clusterIsAvailable);
      if (!clusterIsAvailable) {
        checkAvailability(startTime + interval);
      } else {
        checkClusterCapacityRemaining();
        checkJNHealth();
        checkNameNodeSerivce();
        listCorruptBlocks();
      }

      while (probManger != null && !probManger.isCompleted()) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {

        }
      }

      sink.reportSummary();

      long finishTime = System.currentTimeMillis();
      run ++;

      LOG.info("Finish one turn sniff, consume(ms)=" + (finishTime - startTime) + ", interval(ms)="
          + interval + " runs= " + run);

      try {
        Thread.sleep(interval*1000);
      } catch (InterruptedException e) {

      }
    }

    return;
  }

  private boolean isFSclosed(String message) {
    return message.contains("Filesystem closed");
  }

  private void checkIOLatency() throws IOException {
    String host = null;

    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOG.error("failed to get localhost", e);
    }

    Path testFilePath = new Path(testfilePathBase + host + "_" + System.currentTimeMillis() / 1000L);

    // Test write operation
    OutputStream out = null;
    String testData = RandomStringGenerator.generate(testfileDataSize);
    try {
      long startTime = System.currentTimeMillis();
      out = dfs.create(testFilePath, false);
      out.write(testData.getBytes());
      out.close();

      sink.publishTiming(Sink.OpType.WRITE, System.currentTimeMillis() - startTime);
    } catch (IOException e) {
      sink.publishTiming(Sink.OpType.WRITE, -1);
      LOG.error("Write file failed ", e);
      if (dfs.exists(testFilePath)) {
        dfs.delete(testFilePath, false);
      }
      throw e;
    } finally {
      IOUtils.closeStream(out);
    }

    // Test read operation
    InputStream in = null;
    try {
      long startTime = System.currentTimeMillis();
      in = dfs.open(testFilePath);
      IOUtils.copyBytes(in, new IOUtils.NullOutputStream(), 4096, false);

      sink.publishTiming(Sink.OpType.READ, System.currentTimeMillis() - startTime);
    } catch (IOException e) {
      sink.publishTiming(Sink.OpType.READ, -1);
      LOG.error("Read file failed ", e);
      throw e;
    } finally {
      IOUtils.closeStream(in);
    }

    // Test delete operation
    try {
      dfs.delete((testFilePath), false);
    } catch (IOException e) {
      LOG.error("Delete file failed ", e);
      throw e;
    }
  }

  private void checkAvailability(long finishTime) {
    long startTime = System.currentTimeMillis();
    while (startTime + availDetectInterval < finishTime) {
      try {
        TestReadAvailability();
        sink.publishAvailableStatus(true);
      } catch (IOException e) {
        sink.publishAvailableStatus(false);
        LOG.error("Test Read availability failed ", e);
      }

      long endTime = System.currentTimeMillis();
      if (endTime < startTime + availDetectInterval) {
        try {
          Thread.sleep(startTime + availDetectInterval - endTime);
        } catch (InterruptedException e) {
          LOG.error("Thread sleep failed while availability test ", e);
        }
      }
      startTime = System.currentTimeMillis();
    }
  }

  private void TestReadAvailability() throws IOException {
    InputStream in = null;
    try {
      in = dfs.open(new Path(availTestfilePath));
      IOUtils.copyBytes(in, new IOUtils.NullOutputStream(), 4096, false);
    } catch (IOException e) {
      throw e;
    } finally {
      IOUtils.closeStream(in);
    }
  }

  private void checkJNHealth() {
    for (URL url : jnList) {
      String jmxJson = null;
      try {
        jmxJson = readOutput(url);
        if (LOG.isDebugEnabled()) {
          LOG.debug(jmxJson);
        }
        JSONObject jsonObj = new JSONObject(jmxJson);
        int latencyIn99 = jsonObj.getJSONArray("beans").getJSONObject(0).
            getInt("Syncs60s99thPercentileLatencyMicros");
        LOG.info("99 percents sync operation in " + latencyIn99 + " ms");
        // TODO: process JN latency
        sink.publishNodeHealth(Sink.NodeType.JOURNAL_NODE, url.getHost(), Sink.NodeState.LIVE);
      } catch (IOException e) {
        LOG.error("Get jmx from JournalNode url " + url + " failed. error: ", e);
        sink.publishNodeHealth(Sink.NodeType.JOURNAL_NODE, url.getHost(), Sink.NodeState.FAILED);
      } catch (JSONException e) {
        if (jmxJson != null)
          LOG.error("Can't parse the jmx content as JSON\n" + jmxJson);
      }
    }
  }

  private int getJNHttpPort() throws Exception {
    String httpAddr = conf.get(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY);
    if (httpAddr == null) {
      throw new Exception(String.format("Can't find configuration[%s]",
          DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY));
    }
    // Don't need to consider any format error here. The config file is used by online hadoop
    return Integer.parseInt(httpAddr.split(":")[1]);
  }

  private ArrayList<URL> getJNList() throws Exception {
    ArrayList<URL> jnList = new ArrayList<URL>();
    int httpPort = getJNHttpPort();
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);

    String journalURI = conf.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    if (journalURI == null) {
      throw new Exception(String.format("Can't find configuration[%s]",
          DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY));
    }

    String[] parts = journalURI.split("/");
    for (String jnURL : parts[2].split(";")) {
      String[] items = jnURL.split(":");
      String host = items[0];
      jnList.add(new URL("http://" + host + ":" + httpPort + JN_JMX_SUFFIX + nsId));
    }
    return jnList;
  }

  @VisibleForTesting
  void checkClusterCapacityRemaining() {
    try {
      List<DFSUtil.ConfiguredNNAddress> nns =
          DFSUtil.flattenAddressMap(DFSUtil.getNNServiceRpcAddresses(conf));

      for (DFSUtil.ConfiguredNNAddress cnn : nns) {
        InetSocketAddress isa = cnn.getAddress();
        URL nnJmxUrl = new URL(DFSUtil.getInfoServer(isa, conf,
            DFSUtil.getHttpClientScheme(conf)).toURL(), NN_JMX_SUFFIX);
        LOG.info("will access jmx url: " + nnJmxUrl);
        String jmxJson = null;
        try {
          jmxJson = readOutput(nnJmxUrl);
          JSONObject jsonObj = new JSONObject(jmxJson);
          double percentRemaining = jsonObj.getJSONArray("beans").getJSONObject(0).
              getDouble("PercentRemaining");
          LOG.info("cluster capacity ramaining in percent" + percentRemaining);
          sink.publishCapacityRemaining(percentRemaining);
          // Access one NN is enough
          break;
        } catch (IOException ioe) {
          LOG.error("Get JMX from NameNode faild, jmx url:" + nnJmxUrl, ioe);
          //sink.publishNodeHealth(Sink.NodeType.NMAE_NODE, NNHost, Sink.NodeState.FAILED);
        } catch (JSONException je) {
          if (jmxJson != null)
            LOG.error("Can't parse the jmx content as JSON\n" + jmxJson);
        }
      }
    } catch (IOException e) {
      LOG.error("check cluster remaining capacity failed", e);
    }
  }

  private long getMaxTxDelta(JSONObject jsonObj) {
    long maxTxDelta = 0;

    try {
      String journalStr = (String) jsonObj.getJSONArray("beans").getJSONObject(0).get("JournalTransactionInfo");
      JSONObject jsonObj2 = new JSONObject(journalStr);
      long lastAppliedOrWrittenTxId = jsonObj2.getLong("LastAppliedOrWrittenTxId");
      long mostRecentCheckpointTxId = jsonObj2.getLong("MostRecentCheckpointTxId");
      if (maxTxDelta < (lastAppliedOrWrittenTxId - mostRecentCheckpointTxId)) {
        maxTxDelta = lastAppliedOrWrittenTxId - mostRecentCheckpointTxId;
      }
    } catch (JSONException je) {
      if (jsonObj != null)
        LOG.error("Can't parse the jmx content as JSON when check Tx delta\n" + jsonObj);
    }

    return maxTxDelta;
  }


  private long getLiveNodes(JSONObject jsonObj) {
    long minTxid = Long.MAX_VALUE;
    long maxTxid = 0;
    try {
      String liveNodesStr = (String) jsonObj.getJSONArray("beans").getJSONObject(0).get("LiveNodes");
      JSONObject liveNodesJO = new JSONObject(liveNodesStr);
      if (liveNodesJO != null) {
        return liveNodesJO.length();
      }
    } catch (JSONException je) {
      if (jsonObj != null)
        LOG.error("Can't parse the jmx content as JSON when get live nodes\n" + jsonObj);
    }

    return 0;
  }

  private long getMaxJournalDelay(JSONObject jsonObj) {
    long minTxid = Long.MAX_VALUE;
    long maxTxid = 0;
    try {
      String journalStr = (String) jsonObj.getJSONArray("beans").getJSONObject(0).get("NameJournalStatus");
      JSONArray ja = new JSONArray(journalStr);
      String stream = ja.getJSONObject(0).getString("stream");
      if (!stream.contains("Writing segment")) {
        return -1;
      }

      String[] tmplist = stream.split(" ");
      for (int i = 2; i < tmplist.length; i ++) {
        String txidStr = null;
        if (tmplist[i-2].equals("(Written") && tmplist[i-1].equals("txid")) {
          if (tmplist[i].indexOf(')') >= 0) {
            txidStr = tmplist[i].substring(0, tmplist[i].indexOf(')'));
          } else {
            txidStr = tmplist[i];
          }

          long txid = Long.parseLong(txidStr);
          if (txid > maxTxid) {
            maxTxid = txid;
          }

          if (txid < minTxid) {
            minTxid = txid;
          }
        }
      }

    } catch (JSONException je) {
      if (jsonObj != null)
        LOG.error("Can't parse the jmx content as JSON when check journal node delay\n" + jsonObj);
    }

    return maxTxid - minTxid;
  }

  void checkNameNodeSerivce() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastTxCheckTime < DEFAULT_TX_DETECT_INTERAL) {
      return;
    }

    long maxTxDelta = 0;
    long maxJournalDelay = 0;
    long minLiveNodes = Long.MAX_VALUE;
    long maxLiveNodes = Long.MIN_VALUE;
    String ns = conf.get(DFSConfigKeys.DFS_NAMESERVICES);

    try {

      List<DFSUtil.ConfiguredNNAddress> nns =
          DFSUtil.flattenAddressMap(DFSUtil.getNNServiceRpcAddresses(conf));

      for (DFSUtil.ConfiguredNNAddress cnn : nns) {
        InetSocketAddress isa = cnn.getAddress();
        URL nnJmxUrl = new URL(DFSUtil.getInfoServer(isa, conf,
            DFSUtil.getHttpClientScheme(conf)).toURL(), NN_JMX_SUFFIX);
        LOG.info("will access jmx url: " + nnJmxUrl);
        String jmxJson = null;
        try {
          jmxJson = readOutput(nnJmxUrl);
          JSONObject jsonObj = new JSONObject(jmxJson);

          //check TxId
          long tmpMaxTxDelta = getMaxTxDelta(jsonObj);
          if (maxTxDelta < tmpMaxTxDelta) {
            maxTxDelta = tmpMaxTxDelta;
          }

          //check journal nodes
          long tmpMaxJournalDelay = getMaxJournalDelay(jsonObj);
          if (maxJournalDelay < tmpMaxJournalDelay) {
            maxJournalDelay = tmpMaxJournalDelay;
          }

          //check the livenodes
          long liveNodes = getLiveNodes(jsonObj);
          if (liveNodes < minLiveNodes) {
            minLiveNodes = liveNodes;
          }
          if (liveNodes > maxLiveNodes) {
            maxLiveNodes = liveNodes;
          }

        } catch (IOException ioe) {
          LOG.error("Get JMX from NameNode faild, jmx url:" + nnJmxUrl, ioe);
          //sink.publishNodeHealth(Sink.NodeType.NMAE_NODE, NNHost, Sink.NodeState.FAILED);
        } catch (JSONException je) {
          if (jmxJson != null)
            LOG.error("Can't parse the jmx content as JSON\n" + jmxJson);
        }
      }

      LOG.info("MaxTxDelta: " + maxTxDelta);
      sink.publishMaxTxIdDelta(ns, maxTxDelta);

      LOG.info("MaxJournalDelay: " + maxJournalDelay);
      sink.publishMaxJournalDelay(ns, maxJournalDelay);

      LOG.info("MaxLiveNodesDiff: " + (maxLiveNodes - minLiveNodes));
      sink.publishMaxLiveNodesDiff(ns, maxLiveNodes - minLiveNodes);

    } catch (IOException e) {
      LOG.error("check transaction ID failed", e);
    }

    lastTxCheckTime = System.currentTimeMillis();
  }

  private String readOutput(URL url) throws IOException {
    InputStream in = url.openConnection().getInputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, 4096, true);
    return new String(out.toByteArray(), Charsets.UTF_8);
  }

  private void listCorruptBlocks() {
    try {
      long corruptedFileNum = 0;
      CorruptFileBlockIterator iter = (CorruptFileBlockIterator)
          dfs.listCorruptFileBlocks(new Path("/"));
      while (iter.hasNext()) {
        Path path = iter.next();
        corruptedFileNum ++;
        if (enableListCorruptedFiles) {
          LOG.info("Found a corrupt file: " + path);
          sink.publishCorruptBlocks(path);
        }
        LOG.info("Corrupted files: " + corruptedFileNum);
        sink.publishCorruptedFileNum(corruptedFileNum);
      }
    } catch (IOException e) {
      LOG.error("ListCourruptFileBlocks failed ", e);
    }
  }

  private void prepare() throws Exception {
    LOG = LogFactory.getLog("[" + conf.get("dfs.nameservices") + "]");
    LOG.info("Starting Canary");
    initFromConfiguration();
    // Init kerberos
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, "dfs.canary.keytab.file", "dfs.canary.kerberos.principal");

    dfs = (DistributedFileSystem) FileSystem.get(conf);
    jnList = getJNList();
    // create test file for availability test
    Path testPath = new Path(availTestfilePath);
    if (!dfs.exists(testPath)) {
      try {
        OutputStream out = dfs.create(testPath, false);
        String testData = RandomStringGenerator.generate(availTestfileDataSize);
        out.write(testData.getBytes());
        out.close();
      } catch (IOException e) {
        if (dfs.exists(testPath)) {
          dfs.delete(testPath, false);
        }
        throw e;
      }
    }
  }

  private void initFromConfiguration() {
    conf.setInt("ipc.client.connect.timeout", conf.getInt("dfs.canary.rpc.timeout", 200));
    conf.setInt("ipc.client.connect.retry.interval", conf.getInt("dfs.canary.rpc.retry.interval", 100));
    conf.setInt("ipc.client.connect.max.retries", conf.getInt("dfs.canary.rpc.max.retries", 2));
    conf.setInt("dfs.client.socket-timeout", conf.getInt("dfs.canary.read.timeout", 500));
    conf.setInt("dfs.client.failover.max.attempts", conf.getInt("dfs.canary.failover.max.retries", 2));

    testfilePathBase = conf.get("dfs.canary.testfile.path.base", DEFAULT_TEST_PATH_BASE);
    testfileDataSize = conf.getInt("dfs.canary.testfile.data-size", DEFAULT_TEST_DATA_SIZE);
    availTestfilePath = conf.get("dfs.canary.availability.testfile",
        DEFAULT_PATH_FOR_AVAILABILITY_TEST);
    availTestfileDataSize = conf.getInt("dfs.canary.availability.testfile.data-size",
        DEFAULT_AVAIL_TEST_DATA_SIZE);
    availDetectInterval = conf.getLong("dfs.canary.availability.quick-detect.interval",
        DEFAULT_AVAIL_DETECT_INTERAL);
    rpcTimeoutForChecks = conf.getInt("dfs.canary.read.timeout", 500);
    enableDatanodeLatencyCheck = conf.getBoolean("dfs.canary.datanode.check-latency", DEFAULT_DATANODE_LATENCY_ENABLE);
    interval = conf.getLong("akka.clusterone.canary.interval", DEFAULT_INTERVAL);
    repeats = conf.getInt("akka.clusterone.worker.executor.repeats", DEFAULT_REPEATS);
    enableListCorruptedFiles = conf.getBoolean("dfs.canary.list-courruptfiles", DEFAULT_ENABLE_lIST_CORRUPTEDFILES);

    Class<? extends Sink> sinkClass =
        (Class<? extends Sink>) conf.getClass("akka.clusterone.canary.sink.class", StdOutSink.class);
    sink = ReflectionUtils.newInstance(sinkClass, conf);

    sink.init(conf);
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hadoop %s [opts]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -interval <N>  Interval between checks (sec)");
    System.exit(1);
  }

  private void startDataNodeLatencyProb() {
    if (probManger == null) {
      probManger = new ProbeManager(this);
    }
    probManger.initTasks();
    probManger.initProbs();
    probManger.startProbs();
  }


  // Generate random test data
  static class RandomStringGenerator {
    private static final int charBase = 48; // '0'
    private static final int charRange = 75; // 48~122, corresponding '0'~'z'
    private static Random rand = new Random();

    public static String generate(int length) {
      char[] text = new char[length];
      for (int i = 0; i < length; i++) {
        text[i] = (char) (rand.nextInt(charRange) + charBase);
      }
      return new String(text);
    }
  }

}

