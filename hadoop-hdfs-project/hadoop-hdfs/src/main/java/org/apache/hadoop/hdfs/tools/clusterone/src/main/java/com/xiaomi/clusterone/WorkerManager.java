package com.xiaomi.clusterone;

import akka.actor.ActorRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by xiegang1 on 17-2-4.
 */
public class WorkerManager {
  private static final Log LOG = LogFactory.getLog(WorkerManager.class);
  private HashMap<String, ConcurrentSkipListMap<String, WorkerState>> workerStates = null;
  private HashMap<String, String> workerTypes = null;
  private ArrayList<String> workers = null;

  public WorkerManager(List<String> typeList) {
    workerStates = new HashMap<String, ConcurrentSkipListMap<String, WorkerState>>();
    if (typeList != null && typeList.size() > 0) {
      for (int i = 0; i < typeList.size(); i ++) {
        workerStates.put(typeList.get(i), new ConcurrentSkipListMap<String, WorkerState>());
      }
    }

    workerTypes = new HashMap<String, String>();
    workers = new ArrayList<String>();

  }

  public synchronized String registerWorker(String workerId, WorkerState workerState) {
    String typeWithlestWorks = null;
    int smallest = Integer.MAX_VALUE;
    Iterator iter = workerStates.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      ConcurrentSkipListMap<String, WorkerState> tmp = (ConcurrentSkipListMap<String, WorkerState>)entry.getValue();
      if (tmp == null) {
        continue;
      }
      if (tmp.size() <= smallest) {
        smallest = tmp.size();
        typeWithlestWorks = (String) entry.getKey();
      }
    }

    if (typeWithlestWorks == null) {
      LOG.error("could not find cluster type");
      return null;
    }

    workerStates.get(typeWithlestWorks).put(workerId, workerState);
    workerTypes.put(workerId, typeWithlestWorks);
    workers.add(workerId);

    LOG.info("allocate Worker " + workerId + " for cluster type " + typeWithlestWorks);

    return typeWithlestWorks;

  }

  public synchronized void unRegisterWorker(String type, String workerId) {
    ConcurrentSkipListMap<String, WorkerState> workerStatelist = workerStates.get(type);
    if (workerStatelist == null) {
      LOG.warn("could not find worker " + workerId + " type " + type + " while unregister");
      return;
    }
    workerStatelist.remove(workerId);
    workerTypes.remove(workerId);
    workers.remove(workerId);

    LOG.info("Worker " + workerId + " is removed for cluster type " + type);
  }

  public synchronized boolean isWorkerRegistered (String workerId) {
    return workerTypes.containsKey(workerId);
  }

  public synchronized WorkerState getWorkerState (String workerId) {
    return workerStates.get(workerTypes.get(workerId)).get(workerId);
  }

  public synchronized String getType (String workerId) {
    return workerTypes.get(workerId);
  }

  public void notifyWorkers(String type, ActorRef sender) {
    Iterator iter = workerStates.get(type).entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      WorkerState workstate = (WorkerState)entry.getValue();
      workstate.ref.tell(MasterWorkerProtocol.WorkIsReady.getInstance(), sender);
    }
  }

  public synchronized ArrayList<WorkerState> checkTimeout(String type) {
    ArrayList<WorkerState> timeoutList = new ArrayList<WorkerState>();
    Iterator iter = workerStates.get(type).entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      String workerId = (String) entry.getKey();
      WorkerState state = (WorkerState) entry.getValue();
      if (state.getStatus().getDeadLine().isOverdue()) {
          LOG.error("Works timed out: " + state.getStatus().getWorkIds());
          unRegisterWorker(type, workerId);
          timeoutList.add(state);
      }
    }

    return timeoutList;
  }

  public synchronized  ArrayList<String> getWorkers() {
    return workers;
  }
}
