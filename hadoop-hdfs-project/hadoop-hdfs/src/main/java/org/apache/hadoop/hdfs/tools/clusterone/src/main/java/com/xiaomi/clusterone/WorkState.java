package com.xiaomi.clusterone;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class WorkState {
  private final Set<String> acceptedWorkIds;
  private final Map<String, Work> workInProgress;
  private final ConcurrentLinkedQueue<Work> pendingWork;
  private final ConcurrentLinkedQueue<Work> waitingWork;
  private final HashMap<String, Work> works;

  public WorkState updated(WorkDomainEvent event) {
    if (event instanceof WorkAccepted) {
      updateWorkAcceptedEvent((WorkAccepted) event);
    } else if (event instanceof WorkStarted) {
      updateWorkStartedEvent((WorkStarted) event);
    } else if (event instanceof WorkCompleted) {
      updateWorkCompletedEvent((WorkCompleted) event);
    } else if (event instanceof WorkerHeartbeat) {
      //
    } else if (event instanceof WorkFailed) {
      updateWorkFailedEvent((WorkFailed) event);
    } else if (event instanceof WorkerTimedOut) {
      updateWorkerTimedOutEvent((WorkerTimedOut) event);
    }
    return this;
  }

  public WorkState() {
    workInProgress = new HashMap<String, Work>();
    acceptedWorkIds = new HashSet<String>();
    pendingWork = new ConcurrentLinkedQueue<Work>();
    waitingWork = new ConcurrentLinkedQueue<Work>();
    works = new HashMap<String, Work>();
  }

  public void updateWorkAcceptedEvent(WorkAccepted workAccepted) {
    pendingWork.add(workAccepted.work);
    acceptedWorkIds.add(workAccepted.work.workId);
    works.put(workAccepted.work.workId, workAccepted.work);
  }

  public void updateWorkStartedEvent(WorkStarted workStarted) {
    Work work = pendingWork.poll();
    if (!work.workId.equals(workStarted.workId)) {
      throw new IllegalArgumentException("WorkStarted expected workId "+work.workId+"=="+workStarted.workId);
    }
    workInProgress.put(work.workId, work);
  }

  public void updateWorkCompletedEvent(WorkCompleted workCompleted) {
    Work work = workInProgress.get(workCompleted.workId);
    workInProgress.remove(workCompleted.workId);
    waitingWork.add(work);
  }

  public void updateWorkFailedEvent(WorkFailed WorkFailed) {
    Work work = workInProgress.get(WorkFailed.workId);
    pendingWork.add(work);
    workInProgress.remove(WorkFailed.workId);
  }

  public void updateWorkerTimedOutEvent(WorkerTimedOut workerTimedOut) {
    for (String workId: workerTimedOut.workIds) {
      pendingWork.add(workInProgress.get(workId));
      workInProgress.remove(workId);
    }
  }

  public void updateWork (String workid, String properties) {
    Work work = works.get(workid);
    if (work == null) {
      return;
    }
    work.updateWork(properties);
  }

  public void test() {
    for (Work work : pendingWork) {
      System.out.println(work.task);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("All work num: ").append(acceptedWorkIds.size());
    sb.append(", pending work num: ").append(pendingWork.size());
    sb.append(", in progress work num: ").append(workInProgress.size());
    sb.append(", waiting work num: ").append(waitingWork.size());
    return sb.toString();
  }

  public Work nextWork() {
    return pendingWork.peek();
  }

  public boolean hasWork() {
    while (!waitingWork.isEmpty()) {
      Work work = waitingWork.peek();
      if (work.isRunable()) {
        pendingWork.add(waitingWork.poll());
      } else {
        break;
      }
    }
    return !pendingWork.isEmpty();
  }

  public boolean isAccepted(String workId) {
    return acceptedWorkIds.contains(workId);
  }

  public boolean isInProgress(String workId) {
    return workInProgress.containsKey(workId);
  }

  public Work getWork(String workId) {
    return workInProgress.get(workId);
  }

  public static final class WorkAccepted implements WorkDomainEvent, Serializable {
    final Work work;

    public WorkAccepted(Work work) {
      this.work = work;
    }
  }

  public static final class WorkStarted implements WorkDomainEvent, Serializable {
    final String workId;

    public WorkStarted(String workId) {
      this.workId = workId;
    }

  }

  public static final class WorkCompleted implements WorkDomainEvent, Serializable {
    final String workId;

    public WorkCompleted(String workId) {
      this.workId = workId;
    }

  }

  public static final class WorkerHeartbeat implements WorkDomainEvent, Serializable {
    final String workId;
    public WorkerHeartbeat(String workId) {
      this.workId = workId;
    }
  }

  public static final class WorkFailed implements WorkDomainEvent, Serializable {
    final String workId;

    public WorkFailed(String workId) {
      this.workId = workId;
    }
  }

  public static final class WorkerTimedOut implements WorkDomainEvent, Serializable {
    final Set<String> workIds;
    public WorkerTimedOut(Set<String> workIds) {
      this.workIds = workIds;
    }
  }
}
