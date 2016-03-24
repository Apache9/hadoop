package org.apache.hadoop.hdfs.tools;

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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;

public class Canary implements Tool {
  // Sink interface used by the canary to outputs information
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

    void publishNodeHealth(NodeType type, String host, NodeState state);
    void publishTiming(OpType type, long msTime);
    void publishAvailableStatus(boolean isAvailable);
    void publishCorruptBlocks(Path corruptFilePath);

    void reportSummary();
  }

  // Simple implementation of canary sink that allows to plot on
  // file or standard output timings or failures.
  public static class StdOutSink implements Sink {
    private boolean clusterAvailableStatus = true;
    private long lastSummaryTime = 0;
    private long lastStatusChangeTime = 0;
    private long unavailableTime = 0;

    StdOutSink() {
      lastStatusChangeTime = System.currentTimeMillis();
      lastSummaryTime = lastStatusChangeTime;
    }

    @Override
    public void publishNodeHealth(NodeType type, String host, NodeState state) {
      LOG.info(host + " is a " + type.name() + ", current state: " + state.name());
    }

    @Override
    public void publishTiming(OpType type, long msTime) {
      LOG.info(type.name() + " latency: " + msTime + " ms");
    }

    @Override
    public void publishAvailableStatus(boolean isAvailable){
      if (isAvailable != clusterAvailableStatus) {
        long curTime = System.currentTimeMillis();
        if (!clusterAvailableStatus) {
          unavailableTime += curTime - lastStatusChangeTime;
        }
        LOG.info("cluster become " + (isAvailable ? "available" : "unavailable"));
        clusterAvailableStatus = isAvailable;
        lastStatusChangeTime = curTime;
      } else if (!clusterAvailableStatus) {
        LOG.info("cluster is still unavailable");
      }
    }

    @Override
    public void publishCorruptBlocks(Path corruptFilePath) {
      LOG.info("Found missing blocks in file: " + corruptFilePath);
    }

    @Override
    public void reportSummary() {
      long curTime = System.currentTimeMillis();
      if (!clusterAvailableStatus) {
        unavailableTime += curTime - lastStatusChangeTime;
      }
      double unavailableRate = unavailableTime/(double)(curTime - lastSummaryTime);
      LOG.info("Available rate : "  + (1.0 -unavailableRate));
      lastStatusChangeTime = curTime;
      lastSummaryTime = curTime;
      unavailableTime = 0;
    }
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

  private static final long DEFAULT_INTERVAL = 6000;
  private static final long DEFAULT_AVAIL_DETECT_INTERAL = 100;
  private static final Log LOG = LogFactory.getLog(Canary.class);
  private static final String DEFAULT_TEST_PATH_BASE = "/hdfs_canary/.health_monitoring_canary_";
  private static final String DEFAULT_PATH_FOR_AVAILABILITY_TEST = "hdfs_canary/.file_for_availability_test";
  private static final int DEFAULT_TEST_DATA_SIZE = 4194304; // 4MB
  private static final int DEFAULT_AVAIL_TEST_DATA_SIZE = 1024; // 1k
  private static final String JMX_SUFFIX = "/jmx?qry=Hadoop:service=JournalNode,name=Journal-";

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

  public Canary() {
    this(new StdOutSink());
  }

  public Canary(Sink sink) {
    this.sink = sink;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    // Process command line args
    processArguments(args);
    prepare();

    LOG.info("interval = " + interval);
    LOG.info("availability test interval = " + availDetectInterval);

    do {
      long startTime = System.currentTimeMillis();
      boolean clusterIsAvailable = true;

      try {
        checkIOLatency();
      } catch (IOException e) {
        clusterIsAvailable = false;
        LOG.warn("Test IO latency failed, will start availability detect");
      }

      sink.publishAvailableStatus(clusterIsAvailable);
      if (!clusterIsAvailable) {
        checkAvailability(startTime + interval);
      } else {
        checkNNAndDNHealth();
        checkJNHealth();
        listCorruptBlocks();
      }

      sink.reportSummary();

      long finishTime = System.currentTimeMillis();
      LOG.info("Finish one turn sniff, consume(ms)=" + (finishTime - startTime) + ", interval(ms)="
              + interval);
      if (finishTime < startTime + interval) {
        LOG.info("will sleep for " + (startTime + interval - finishTime) + " ms" );
        Thread.sleep(startTime + interval - finishTime);
      }
    } while (interval > 0);

    return 0;
  }

  private void checkIOLatency() throws IOException {
    Path testFilePath = new Path(testfilePathBase + System.currentTimeMillis() / 1000L);

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

  private void checkNNAndDNHealth() {
    try {
      // Check datanode health
      for (DatanodeInfo info : dfs.getDataNodeStats()) {
        try {
          ClientDatanodeProtocol proxy = DFSUtil.createClientDatanodeProtocolProxy(
            NetUtils.createSocketAddr(info.getIpcAddr(false)),
            UserGroupInformation.getCurrentUser(), conf,
            NetUtils.getSocketFactory(conf, ClientDatanodeProtocol.class));
          DatanodeLocalInfo localInfo = proxy.getDatanodeInfo();
          LOG.info(String.format("DataNode report for node [%s] : %s",
                  info.getXferAddr(), localInfo.getDatanodeLocalReport()));
          sink.publishNodeHealth(Sink.NodeType.DATA_NODE, info.getXferAddr(), Sink.NodeState.LIVE);
        } catch (IOException e) {
          LOG.error("Get datanode info failed", e);
          sink.publishNodeHealth(Sink.NodeType.DATA_NODE, info.getXferAddr(), Sink.NodeState.FAILED);
        }
      }

      //check namenode health
      String nsId = DFSUtil.getNamenodeNameServiceId(conf);
      for (String nnId : DFSUtil.getNameNodeIds(conf, nsId)) {
        String NNHost = DFSUtil.getNamenodeServiceAddr(conf, nsId, nnId);
        try {
          HAServiceProtocol proxy = new NNHAServiceTarget(conf, nsId, nnId).getProxy(conf,
                  rpcTimeoutForChecks);
          /*
           * skip at first version
           * due to some kerberos issues, this call will always fail
           *
          String state = proxy.getServiceStatus().getState().toString();
          LOG.info(String.format("%s current state [%s]", NNHost, state));
          */
          sink.publishNodeHealth(Sink.NodeType.NMAE_NODE, NNHost, Sink.NodeState.LIVE);
        } catch (IOException e) {
          LOG.error("Get NameNode state faild ", e);
          sink.publishNodeHealth(Sink.NodeType.NMAE_NODE, NNHost, Sink.NodeState.FAILED);
        }
      }
    } catch (IOException e) {
      // TODO: process the failure on sink
      LOG.error("Get datanode stats failed ", e);
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
      jnList.add(new URL("http://" + host + ":" + httpPort + JMX_SUFFIX + nsId));
    }
    return jnList;
  }

  private String readOutput(URL url) throws IOException {
    InputStream in = url.openConnection().getInputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, 4096, true);
    return new String(out.toByteArray(), Charsets.UTF_8);
  }

  private void listCorruptBlocks() {
    try {
      CorruptFileBlockIterator iter = (CorruptFileBlockIterator)
              dfs.listCorruptFileBlocks(new Path("/"));
      while (iter.hasNext()) {
        Path path = iter.next();
        LOG.info("Found a corrupt file: " + path);
        sink.publishCorruptBlocks(path);
      }
    } catch (IOException e) {
      LOG.error("ListCourruptFileBlocks failed ", e);
    }
  }

  private void processArguments(String[] args) {
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (cmd.equals("-help")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-interval")) {
          // user has specified an interval for canary breaths (-interval N)
          i++;

          if (i == args.length) {
            System.err.println("-interval needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            interval = Long.parseLong(args[i]) * 1000;
          } catch (NumberFormatException e) {
            System.err.println("-interval needs a numeric value argument.");
            printUsageAndExit();
          }
        } else {
          // no options match
          System.err.println(cmd + " options is invalid.");
          printUsageAndExit();
        }
      } else {
        // illegal arguments
        System.err.println(cmd + " is illegal argument.");
        printUsageAndExit();
      }
    }
  }

  private void prepare() throws Exception {
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
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hadoop %s [opts]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -interval <N>  Interval between checks (sec)");
    System.exit(1);
  }

  public static void main(String[] argv) {
    HdfsConfiguration conf = new HdfsConfiguration();
    Class<? extends Sink> sinkClass =
            (Class<? extends Sink>) conf.getClass("dfs.canary.sink.class", StdOutSink.class);
    Sink sink = ReflectionUtils.newInstance(sinkClass, conf);

    int exitCode = 0;
    try {
      exitCode = ToolRunner.run(conf, new Canary(sink), argv);
    } catch (Exception e) {
      LOG.error("Canary exited with exception. ", e);
    }
    System.exit(exitCode);
  }
}
