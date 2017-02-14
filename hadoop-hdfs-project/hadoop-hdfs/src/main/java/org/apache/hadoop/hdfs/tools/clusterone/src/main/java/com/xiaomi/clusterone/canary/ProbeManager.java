package com.xiaomi.clusterone.canary;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by xiegang1 on 17-1-17.
 */
public class ProbeManager {
  private static final Log LOG = LogFactory.getLog(ProbeManager.class);
  private static final int maxThreads = 10;
  private List<Probe> probeList = new ArrayList<Probe>();
  private ArrayList<DatanodeInfo> taskList = new ArrayList<DatanodeInfo>();
  private Canary canary = null;
  private int currTask = 0;
  private int taskNum = 0;
  private boolean isTaskCompleted = true;
  private Configuration conf = null;
  private static final String NN_JMX_SUFFIX = "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";

  public ProbeManager (Canary canary) {
    this.canary = canary;
    conf = canary.getConf();
  }

  public Canary getCanary () {return this.canary;}

  public synchronized void completeNTasks(Probe probe, int taskNum) {
    this.taskNum -= taskNum;
    if (this.taskNum == 0) {
      this.isTaskCompleted = true;
      probe.CompleteCallBack();
    }
  }

  private String readOutput(URL url) throws IOException {
    InputStream in = url.openConnection().getInputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, 4096, true);
    return new String(out.toByteArray(), Charsets.UTF_8);
  }

  private ArrayList<String> getDeadDatanodes() {

    try {

      List<DFSUtil.ConfiguredNNAddress> nns =
          DFSUtil.flattenAddressMap(DFSUtil.getNNServiceRpcAddresses(conf));

      if (nns.size() <= 0) {
        LOG.error("could not get namenode when getting dead datanodes");
        return null;
      }
      // choose any of the nn should be OK
      DFSUtil.ConfiguredNNAddress cnn = nns.get(0);
      InetSocketAddress isa = cnn.getAddress();
      URL nnJmxUrl = new URL(DFSUtil.getInfoServer(isa, conf,
          DFSUtil.getHttpClientScheme(conf)).toURL(), NN_JMX_SUFFIX);
      String jmxJson = null;
      try {
        ArrayList<String> deadnodesList = new ArrayList<String>();
        jmxJson = readOutput(nnJmxUrl);
        org.codehaus.jettison.json.JSONObject jsonObj = new org.codehaus.jettison.json.JSONObject(jmxJson);
        String deadNodesStr = (String) jsonObj.getJSONArray("beans").getJSONObject(0).get("DeadNodes");
        org.codehaus.jettison.json.JSONObject deadNodesJO = new org.codehaus.jettison.json.JSONObject(deadNodesStr);
        if (deadNodesJO != null && deadNodesJO.length() > 0) {
          JSONArray ja = deadNodesJO.names();
          for (int i = 0; i < deadNodesJO.length(); i ++) {
            String xferaddrStr = deadNodesJO.getJSONObject(ja.getString(i)).getString("xferaddr");
            deadnodesList.add(xferaddrStr);

          }
        }
        return deadnodesList;

      } catch (IOException ioe) {
        LOG.error("Get JMX from NameNode faild, jmx url:" + nnJmxUrl, ioe);
      } catch (JSONException je) {
        if (jmxJson != null)
          LOG.error("Can't parse the jmx content as JSON\n" + jmxJson);
      }

    } catch (IOException e) {
      LOG.error("getting dead datanode failed", e);
    }

    return null;

  }

  private void removeDeadNodesFromTaskList(ArrayList<DatanodeInfo> tasklist, ArrayList<String> datanodes) {
    if (tasklist == null || datanodes == null) {
      return;
    }
    for (int i = 0; i < datanodes.size(); i ++) {
      for (int j =0 ; j < tasklist.size(); j ++) {
        if (datanodes.get(i).equals(tasklist.get(j).getXferAddr())) {
          tasklist.remove(j);
          break;
        }
      }
    }
  }

  public synchronized void initTasks () {
    // we need ensure all the nodes is probed before refresh the task list
    if (!isTaskCompleted) {
      return;
    }
    try {
      taskList = new ArrayList<DatanodeInfo> (Arrays.asList(canary.getDFS().getDataNodeStats()));
      // remove dead node
      ArrayList deadNodeList = getDeadDatanodes();
      if (deadNodeList != null && deadNodeList.size() > 0) {
        removeDeadNodesFromTaskList(taskList, deadNodeList);
      }
      isTaskCompleted = false;
      taskNum = taskList.size();
      currTask = 0;
    } catch (IOException e) {
      LOG.error("Init prob tasks failed", e);
    }

  }

  public void initProbs () {
    if (probeList.isEmpty()) {
      for (int i = 0; i < maxThreads; i ++) {
        Probe tempProbe = ProbeFactory.getProbeByType(ProbeFactory.ProbeType.DN_IO_LATENCY, this);
        probeList.add(tempProbe);
      }
    }
  }

  public synchronized List<DatanodeInfo> getTasks (int bacthedTaskNum) {
    int tasksToDispatch = bacthedTaskNum;
    List<DatanodeInfo> subTaskList = null;
    if (isTaskCompleted) {
      return null;
    }

    if (currTask + bacthedTaskNum >= taskList.size()) {
      tasksToDispatch = taskList.size() - currTask;
    }

    subTaskList = taskList.subList(currTask, currTask + tasksToDispatch);
    currTask += tasksToDispatch;

    return subTaskList;
  }

  public void startProbs () {
    for (int i = 0; i < probeList.size(); i ++) {
      probeList.get(i).startProbe();
    }
  }

  public void stopProbs () {
    for (int i = 0; i < probeList.size(); i ++) {
      probeList.get(i).stopProbe();
    }
  }

  public synchronized boolean isCompleted () {
    return this.isTaskCompleted;
  }

}