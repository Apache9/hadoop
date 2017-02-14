package com.xiaomi.clusterone;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;

import static com.xiaomi.clusterone.MasterWorkerProtocol.*;

/**
 * Created by xiegang1 on 17-1-19.
 */
public class ConfManager extends UntypedActor implements Runnable {

  private static final Log LOG = LogFactory.getLog(ConfManager.class);
  private static final String defaultGitDirStr = "./git/";
  private static final String defaultClusterConfOutputDirStr = "./conf/";
  private static long defaultUpdateInterval = 1800;

  private String gitDirStr = null;
  private String clusterConfDirStr = null;
  private String clusterConfOutputDirStr = null;
  private Thread confRefreshThr = null;
  private ActorRef master = null;
  private long updateInterval = 0; // 1800s

  private List<String> whiteList = null;
  private List<String> blackList = null;
  private Config conf = null;


  HashMap<String, SerializableHdfsConfiguration> confMap = new HashMap<String, SerializableHdfsConfiguration>();

  public ConfManager(Config conf) {
    this.conf = conf;
    loadConf();
    confRefreshThr = new Thread(this);
    confRefreshThr.setName("Thread-confRefresh");
  }

  private synchronized List<String> getWhiteList() {
    return whiteList;
  }

  private synchronized void setWhiteList(List<String> whiteList) {
    this.whiteList = whiteList;
  }

  private synchronized List<String> getBlackList() {
    return blackList;
  }

  private synchronized void setBlackList(List<String> blackList) {
    this.blackList = blackList;
  }


  private void reloadConf() {
    Config newconf = null;
    try {
      newconf = ConfigFactory.parseFile(new File("master.conf"));
    } catch (Exception e) {
      LOG.error("failed to reload the master conf", e);
    }
    this.conf = newconf;
    loadConf();
  }

  private void loadConf() {
    LOG.info("load conf of ConfManager");
    if (conf.hasPath("akka.clusterone.conf.gitdir")) {
      gitDirStr = conf.getString("akka.clusterone.conf.gitdir");
    } else {
      gitDirStr = defaultGitDirStr;
    }

    clusterConfDirStr = gitDirStr + "infra/deployment/xiaomi-config/conf/hdfs/";

    if (conf.hasPath("akka.clusterone.conf.dir")) {
      clusterConfOutputDirStr = conf.getString("akka.clusterone.conf.dir");
    } else {
      clusterConfOutputDirStr = defaultClusterConfOutputDirStr;
    }

    if (conf.hasPath("akka.clusterone.conf.updateinterval")) {
      updateInterval = conf.getLong("akka.clusterone.conf.updateinterval");
    } else {
      updateInterval = defaultUpdateInterval;
    }


    if (conf.hasPath("akka.clusterone.conf.whilelist")) {
      setWhiteList(conf.getStringList("akka.clusterone.conf.whilelist"));
    }

    if (conf.hasPath("akka.clusterone.conf.blacklist")) {
      setBlackList(conf.getStringList("akka.clusterone.conf.blacklist"));
    }
  }

  private void addConf(String nameservice, SerializableHdfsConfiguration conf) {
    confMap.put(nameservice, conf);
  }

  private void passPropertiesFromMasterToWork(SerializableHdfsConfiguration sconf) {
    if (conf == null || sconf == null) {
      return;
    }

    for (Map.Entry<String, ConfigValue> e : conf.entrySet()) {
      String property = e.getKey();
      String value = e.getValue().unwrapped().toString();
      if (property.startsWith("pass.")) {
        String tmp = property.substring(property.indexOf(".") + 1, property.length());
        LOG.info("pass the config " + tmp + " value " + value);
        sconf.set(tmp, value);
      }
    }

  }

  private synchronized boolean isInWhiteList(String cluster) {
    if (whiteList == null) {
      return false;
    }

    for (int i = 0; i < whiteList.size(); i ++) {
      if (cluster.matches(whiteList.get(i))) {
        return true;
      }
    }
    return false;
  }

  private synchronized boolean isInBlackList(String cluster) {
    if (blackList == null) {
      return false;
    }

    for (int i = 0; i < blackList.size(); i ++) {
      if (cluster.matches(blackList.get(i))) {
        return true;
      }
    }

    return false;
  }

  public void scanConfs() {
    File confdir=new File(clusterConfOutputDirStr);
    if (!confdir.isDirectory()) {
      return;
    }

    LOG.info("scan conf in " + confdir);

    File[] fList=confdir.listFiles();

    for (int j = 0; j < fList.length; j++) {
      File conffile = fList[j];
      try{
        //check the white & black list if enabled
        if (!isInWhiteList(conffile.getName())) {
          continue;
        }

        if (isInBlackList(conffile.getName())) {
          continue;
        }

        SerializableHdfsConfiguration conf = confMap.get(conffile.getName());

        if (conffile.isDirectory()) {
          continue;
        } else if (confMap.containsKey(conffile.getName())
            && isConfUpdatedAfter(conffile.getAbsolutePath(), conf.getLastModifiedTime())) {
          // update the old one
          updateConfResource(conf, conffile);
          tellMaster(new ConfUpdated(conffile.getName(), conf.getPropertiesStr(false)));
          LOG.info("updated conf for " + conf.get("dfs.nameservices"));
        } else if (!confMap.containsKey(conffile.getName())) {
          // add new one
          conf = new SerializableHdfsConfiguration();
          updateConfResource(conf, conffile);
          addConf(conffile.getName(), conf);
          tellMaster(new ConfAdded(conffile.getName(), conf.getPropertiesStr(false)));
          LOG.info("add conf for " + conf.get("dfs.nameservices"));
        }
      } catch (Exception e) {
        if (conffile != null) {
          LOG.error("fail to scan conf file: " + conffile.getAbsolutePath());
        }
      }
    }

  }

  private void updateConfResource(SerializableHdfsConfiguration conf, File conffile) throws Exception {
    LOG.info("update resource " + conffile.getName() + " modified time: " + conffile.lastModified());
    conf.addResource(conffile.toURI().toURL());
    passPropertiesFromMasterToWork(conf);
    // the defaultfs property from web is not logical one,
    // we change it when load the conf
    conf.setStrings("fs.defaultFS", "hdfs://" + conffile.getName());
    conf.getPropertiesStr(true);
    conf.setLastModifiedTime(conffile.lastModified());
  }

  private void checkoutConf() {
    File confdir = new File(clusterConfDirStr);
    String cmd = null;
    if (confdir.exists()) {
      cmd = "git --git-dir " + gitDirStr + "infra/.git" + " pull";
    } else {
      cmd = "git clone git@git.n.xiaomi.com:infra/infra.git " + gitDirStr + "infra";
    }
    LOG.info("starting git update in " + gitDirStr + " " + cmd);
    callShell(cmd);

  }

  private boolean isConfUpdatedAfter(String conffile, long currtime) {
    File file = new File(conffile);
    return file.lastModified() > currtime ? true : false;
  }

  private ArrayList<String> getNameNodeAddr(String filestr) {
    boolean foundnamenode = false;
    String port = null;
    ArrayList<String> hostports = new ArrayList<String>();
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filestr)));
      String data = null;
      while ((data = br.readLine()) != null) {

        if (data.contains("#")) {
          data = data.substring(0, data.indexOf('#'));
        }
        if (data.contains("[namenode]")) {
          foundnamenode  = true;
          continue;
        }

        if (foundnamenode) {
          if (data.contains("base_port")) {
            String tmp = data.split("=")[1].trim();
            port = tmp.substring(0, tmp.length() - 1) + "1";
            continue;
          }

          if (data.contains("host")) {
            hostports.add(data.split("=")[1].trim() + ":" + port);
            continue;
          }
        }

      }
    } catch (FileNotFoundException e) {

    } catch (IOException e) {
      e.printStackTrace();
    }

    return hostports;
  }

  private void checkAndUpdateConfs() {

    File outdir = new File(clusterConfOutputDirStr);
    if (!outdir.exists()) {
      String cmd = "mkdir -p " + clusterConfOutputDirStr;
      callShell(cmd);
    }

    File confdir = new File (clusterConfDirStr);

    for (File file: confdir.listFiles()) {
      // file is the *.cfg in source code
      if (file.isDirectory()) {
        continue;
      }

      String filename = file.getName();
      String tmp = filename.substring(0, filename.lastIndexOf("."));
      String clustername = tmp.substring(tmp.indexOf("-") + 1, tmp.length());

      if (!isInWhiteList(clustername)) {
        continue;
      }

      if (isInBlackList(clustername)) {
        continue;
      }

      SerializableHdfsConfiguration conf = confMap.get(clustername);

      if (conf != null && !isConfUpdatedAfter(file.getAbsolutePath(), conf.getLastModifiedTime())) {
        continue;
      }

      ArrayList<String> hostports = getNameNodeAddr(file.getAbsolutePath());

      String url = "http://" + hostports.get(0) + "/conf";
      String filepath = clusterConfOutputDirStr + clustername;
      downloadFile(url, filepath);

      LOG.info("starting to get conf from " + url.toString());
    }

  }

  public void callShell(String shellString) {
    try {
      Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",shellString});
      int exitValue = process.waitFor();
      InputStreamReader ir = new InputStreamReader(process.getInputStream());
      LineNumberReader input = new LineNumberReader(ir);
      String line;
      while ((line = input.readLine()) != null)
        LOG.warn(line);
      input.close();
      ir.close();
      if (0 != exitValue) {
        LOG.warn("call shell failed. error code is :" + exitValue);
      }
    } catch (Throwable e) {
      LOG.error("call shell failed. " + e);
    }
  }

  @Override
  public void run() {
    while (true) {
      checkoutConf();
      checkAndUpdateConfs();
      scanConfs();
      try {
        Thread.sleep(updateInterval * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void tellMaster(Object msg) {
    master.tell(msg, getSelf());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof StartConf) {
      master = getSender();
      confRefreshThr.start();

    } else if (message instanceof ReloadConf) {
      reloadConf();
    }
  }

  private void downloadFile(String urlPath, String path) {
    DefaultHttpClient httpclient = null;
    String content = null;
    try {
      httpclient = new DefaultHttpClient();

      HttpGet httpget = new HttpGet(urlPath);

      httpget.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,1000*30);

      HttpResponse responses = httpclient.execute(httpget);
      HttpEntity entity = responses.getEntity();
      content = EntityUtils.toString(entity);

      File file = new File(path);
      PrintStream ps = new PrintStream(new FileOutputStream(file));
      ps.println(content);


    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      httpclient.getConnectionManager().shutdown();
    }
    return;
  }

}
