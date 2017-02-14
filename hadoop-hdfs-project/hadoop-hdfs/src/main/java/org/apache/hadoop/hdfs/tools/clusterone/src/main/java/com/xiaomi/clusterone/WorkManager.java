package com.xiaomi.clusterone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by xiegang1 on 17-2-4.
 */
public class WorkManager {
  private static final Log LOG = LogFactory.getLog(WorkManager.class);
  private HashMap<String, WorkState> works = null;
  private List<String> typeList = null;

  public WorkManager(List<String> typeList) {
    this.typeList = typeList;
    works = new HashMap<String, WorkState>();
    if (typeList != null && typeList.size() > 0) {
      for (int i = 0; i < typeList.size(); i ++) {
        works.put(typeList.get(i), new WorkState());
      }
    }
  }

  public String getWorkTypeById(String workId) {
    String prefix = workId.split("-")[0];
    for (int i = 0; i < typeList.size(); i ++) {
      if (prefix.contains(typeList.get(i))) {
        return typeList.get(i);
      }
    }
    return null;
  }

  private WorkState getWorkStateByWorkId(String workId) {
    String prefix = workId.split("-")[0];
    for (int i = 0; i < typeList.size(); i ++) {
      if (prefix.contains(typeList.get(i))) {
        return works.get(typeList.get(i));
      }
    }
    LOG.warn("could not find workState for work: " + workId);
    return null;

  }


  public WorkState updatedByWorkId(String workId, WorkDomainEvent event) {
    WorkState workState = getWorkStateByWorkId(workId);
    if (event instanceof WorkState.WorkAccepted) {
      workState.updateWorkAcceptedEvent((WorkState.WorkAccepted) event);
    } else if (event instanceof WorkState.WorkStarted) {
      workState.updateWorkStartedEvent((WorkState.WorkStarted) event);
    } else if (event instanceof WorkState.WorkCompleted) {
      workState.updateWorkCompletedEvent((WorkState.WorkCompleted) event);
    } else if (event instanceof WorkState.WorkerHeartbeat) {
      //
    } else if (event instanceof WorkState.WorkFailed) {
      workState.updateWorkFailedEvent((WorkState.WorkFailed) event);
    } else if (event instanceof WorkState.WorkerTimedOut) {
      workState.updateWorkerTimedOutEvent((WorkState.WorkerTimedOut) event);
    }
    return workState;
  }

  public WorkState updatedByType(String type, WorkDomainEvent event) {
    WorkState workState = works.get(type);
    if (event instanceof WorkState.WorkAccepted) {
      workState.updateWorkAcceptedEvent((WorkState.WorkAccepted) event);
    } else if (event instanceof WorkState.WorkStarted) {
      workState.updateWorkStartedEvent((WorkState.WorkStarted) event);
    } else if (event instanceof WorkState.WorkCompleted) {
      workState.updateWorkCompletedEvent((WorkState.WorkCompleted) event);
    } else if (event instanceof WorkState.WorkerHeartbeat) {
      //
    } else if (event instanceof WorkState.WorkFailed) {
      workState.updateWorkFailedEvent((WorkState.WorkFailed) event);
    } else if (event instanceof WorkState.WorkerTimedOut) {
      workState.updateWorkerTimedOutEvent((WorkState.WorkerTimedOut) event);
    }
    return workState;
  }

  public void updateWork (String workId, String properties) {
    WorkState workState = getWorkStateByWorkId(workId);
    Work work = workState.getWork(workId);
    if (work == null) {
      return;
    }
    work.updateWork(properties);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator iter = works.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      WorkState tmp = (WorkState)entry.getValue();
      sb.append("type: " + entry.getKey() + " ");
      sb.append(tmp.toString() + " ");
    }
    return sb.toString();
  }

  public Work nextWork(String type) {
    WorkState workState = works.get(type);
    return workState.nextWork();
  }

  public boolean hasWork(String type) {
    WorkState workState = works.get(type);
    return workState.hasWork();
  }

  public boolean isAccepted(String workId) {
    WorkState workState = getWorkStateByWorkId(workId);
    return workState.isAccepted(workId);
  }

  public boolean isInProgress(String workId) {
    WorkState workState = getWorkStateByWorkId(workId);
    return workState.isInProgress(workId);
  }

  public Work getWork(String workId) {
    WorkState workState = getWorkStateByWorkId(workId);
    return workState.getWork(workId);
  }

}
