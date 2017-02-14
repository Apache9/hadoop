package com.xiaomi.clusterone;

import java.util.HashSet;
import java.util.Set;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;

import com.xiaomi.clusterone.WorkState.WorkAccepted;
import com.xiaomi.clusterone.WorkState.WorkCompleted;
import com.xiaomi.clusterone.WorkState.WorkFailed;
import com.xiaomi.clusterone.WorkState.WorkStarted;
import com.xiaomi.clusterone.WorkState.WorkerHeartbeat;
import com.xiaomi.clusterone.WorkState.WorkerTimedOut;

public class WorkerState {

  public static final class WorkerStatus {
    private final Set<String> workIds;
    private Deadline deadline;
    private long requestedWorkNum = 0;
    private long ackedWorkNum = 0;

    public WorkerStatus(Deadline deadline) {
      this.workIds = new HashSet<String>();
      this.deadline = deadline;
    }

    private void addWorkId(String workId) {
      this.workIds.add(workId);
      requestedWorkNum ++;
    }

    private void removeWorkId(String workId) {
      this.workIds.remove(workId);
      ackedWorkNum++;
    }

    protected Set<String> getWorkIds() {
      return workIds;
    }

    protected Deadline getDeadLine() {
      return deadline;
    }

    @Override
    public String toString() {
      return "{running work: " + workIds.size() + ", requestedWorkNum=" + requestedWorkNum
          + ", ackedWorkNum=" + ackedWorkNum + "}";
    }
  }

  private String workerId;
  private String hostname;
  public final ActorRef ref;
  private final FiniteDuration workTimeout;
  public final WorkerStatus status;

  public WorkerState(String workerId, ActorRef ref, FiniteDuration workTimeout, WorkerStatus status) {
    this.workerId = workerId;
    this.ref = ref;
    this.hostname = ref.path().address().hostPort();
    this.workTimeout = workTimeout;
    this.status = status;
  }

  protected String getWorkerId() {
    return workerId;
  }

  public WorkerStatus getStatus() {
    return status;
  }

  public WorkerState updated(WorkDomainEvent event) {
    if (event instanceof WorkStarted) {
      String workId = ((WorkStarted)event).workId;
      status.addWorkId(workId);
      status.deadline = workTimeout.fromNow();
    } else if (event instanceof WorkAccepted) {
      //
    } else if (event instanceof WorkCompleted) {
      String workId = ((WorkCompleted)event).workId;
      status.removeWorkId(workId);
      status.deadline = workTimeout.fromNow();
    } else if (event instanceof WorkerHeartbeat) {
      status.deadline = workTimeout.fromNow();
    } else if (event instanceof WorkFailed) {
      String workId = ((WorkFailed)event).workId;
      status.removeWorkId(workId);
      status.deadline = workTimeout.fromNow();
    } else if (event instanceof WorkerTimedOut) {
      //
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !getClass().equals(o.getClass()))
      return false;

    WorkerState that = (WorkerState) o;

    if (!ref.equals(that.ref))
      return false;
    if (!status.equals(that.status))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = ref.hashCode();
    result = 31 * result + status.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "WorkerState{" + "workerId=" + workerId + "hostname=" + hostname + ", status=" + status
        + '}';
  }
}