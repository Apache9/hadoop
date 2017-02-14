package com.xiaomi.clusterone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import akka.actor.ActorRef;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.xiaomi.clusterone.canary.Canary;
import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.tools.*;
import org.apache.hadoop.hdfs.tools.Probe;
import org.apache.hadoop.hdfs.tools.ProbeFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.json.JSONObject;
import org.apache.hadoop.util.*;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.xiaomi.clusterone.canary.*;


public class WorkExecutor extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private static int defaultRepeats = 3;
  private Config conf;
  private Thread executorThread = null;
  private Thread monitorThread = null;
  private ActorRef worker = null;
  private Work work = null;
  private Class<? extends Runnable> jobClass = null;

  public WorkExecutor(Config conf) {
    this.conf = conf;
    if (conf.hasPath("akka.clusterone.worker.jobclass")) {
      try {
        jobClass = (Class<? extends Runnable>) Class.forName(conf.getString("akka.clusterone.worker.jobclass"));
      } catch (ClassNotFoundException e) {
        log.error("cound not find the job class " + conf.getString("akka.clusterone.worker.jobclass"));
      }
    } else {
      jobClass = DefaultJob.class;
    }
  }

  public ActorRef getWorker () {
    return worker;
  }

  public Thread getExecutorThread () {
    return executorThread;
  }

  public Work getWork () {
    return work;
  }

  private void startJob(SerializableHdfsConfiguration conf) {
    Runnable job = ReflectionUtils.newInstance(jobClass, conf);

    executorThread = new Thread(job);
    executorThread.setName("JobThread-" + work.workId);
    executorThread.start();

    Runnable mon = new ExcutorMonitor(this);
    monitorThread = new Thread(mon);
    monitorThread.setName("JobMonitorThread-" + work.workId);
    monitorThread.start();

  }
  @Override
  public void onReceive(Object message) {
    if (message instanceof Work) {
      work = (Work) message;
      Task task = work.task;
      log.info("receive message " + message);
      worker = getSender();
      try {
        SerializableHdfsConfiguration conf = new SerializableHdfsConfiguration();
        conf.updatePropertiesFromString(work.task.getPropertiesStr());
        startJob(conf);
      } catch (Exception e) {
        log.error(e.toString());
      }

    }
  }

}

class ExcutorMonitor implements Runnable {
  private static final Log LOG = LogFactory.getLog(ExcutorMonitor.class);

  private ActorRef worker = null;
  private Thread executorThread = null;
  private Work work = null;
  private ActorRef executor = null;

  public ExcutorMonitor (WorkExecutor executor) {
    this.worker = executor.getWorker();
    this.executorThread = executor.getExecutorThread();
    this.work = executor.getWork();
    this.executor = executor.getSelf();
  }

  public ActorRef getWorker() {
    return worker;
  }

  public void setWorker(ActorRef worker) {
    this.worker = worker;
  }

  public Thread getExecutorThread() {
    return executorThread;
  }

  public void setExecutorThread(Thread excutorThread) {
    this.executorThread = excutorThread;
  }


  @Override
  public void run() {
    try {
      while (executorThread.isAlive()) {
        try {
          executorThread.join(2000);
        } catch (InterruptedException e) {

        }
      }
    } catch(Exception e){
      LOG.error(e.toString());
      worker.tell(new Worker.WorkFail(work.workId), executor);
      return;
    }

    worker.tell(new Worker.WorkComplete(work.workId), executor);

  }
}
