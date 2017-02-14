package com.xiaomi.clusterone;

import java.io.Serializable;

import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Work implements Serializable {
  public final String workId;
  public final Task task;
  private final int period;
  private long lastSchduledTime;
  private transient ActorRef worker = null;

  public ActorRef getWorker() {
    return worker;
  }

  public void setWorker(ActorRef worker) {
    this.worker = worker;
  }

  public Work(String workId, Task task, int period) {
    this.workId = workId;
    this.task = task;
    this.period = period;
    this.lastSchduledTime = 0;
  }

  public void updateLastSchduledTime() {
    this.lastSchduledTime = System.currentTimeMillis() / 1000;
  }

  public void updateWork (String propertiesStr) { task.setPropertiesStr(propertiesStr);}

  public boolean isRunable() {
    return System.currentTimeMillis() / 1000 > (this.lastSchduledTime + this.period);
  }

  @Override
  public String toString() {
    return "Work{" + "workId='" + workId + '\'' + ", task=" + task + '}';
  }
}