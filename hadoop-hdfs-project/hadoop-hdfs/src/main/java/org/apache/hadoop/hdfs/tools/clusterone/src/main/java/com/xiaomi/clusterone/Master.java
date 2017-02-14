package com.xiaomi.clusterone;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.typesafe.config.Config;
import com.xiaomi.clusterone.MasterWorkerProtocol.WorkFailed;
import com.xiaomi.clusterone.WorkState.WorkAccepted;
import com.xiaomi.clusterone.WorkState.WorkCompleted;
import com.xiaomi.clusterone.WorkState.WorkStarted;
import com.xiaomi.clusterone.WorkState.WorkerHeartbeat;
import com.xiaomi.clusterone.WorkState.WorkerTimedOut;
import com.xiaomi.clusterone.WorkerState.WorkerStatus;

import static com.xiaomi.clusterone.MasterWorkerProtocol.*;

import org.apache.commons.logging.*;

public class Master extends UntypedActor {

  public static Props props(Config conf, Props confManagerProps) {
    return Props.create(Master.class, conf, confManagerProps);
  }
  private static final int defaultWorkInterval = 10;
  private static final int defaultWorkTimeOut = 300;
  private static final int defaultConfReloadTime = 300;
  private static final int defaultDistributeWorkThreshhold = 1000; // 1000ms

  private Config conf;
  private int timeout;
  private FiniteDuration workTimeout;
  private int interval = 10; //10s
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private final Scheduler scheduler;
  private final Cancellable cleanupTask;
  private final Cancellable confReloadTask;
  private FiniteDuration confReloadTimeout;
  private int confReloadTime;
  private ActorRef confManager = null;
  private List<String> typeList = null;

  private WorkerManager workerMgr = null;
  private WorkManager workMgr = null;

  private int distributeWorkThreshhod = 0;

  public Master(Config conf, Props confManagerProps) {
    this.conf = conf;

    loadConf();

    workerMgr = new WorkerManager(typeList);
    workMgr = new WorkManager(typeList);

    workTimeout = Duration.create(timeout, "seconds");
    confReloadTimeout = Duration.create(confReloadTime, "seconds");

    this.scheduler = getContext().system().scheduler();
    this.cleanupTask =
        scheduler.schedule(workTimeout.div(2), workTimeout.div(2), getSelf(), CleanupTick,
          getContext().dispatcher(), getSelf());

    confManager = getContext().watch(
        getContext().actorOf(confManagerProps, "confMgr"));
    confManager.tell(new StartConf(), getSelf());

    this.confReloadTask =
        scheduler.schedule(confReloadTimeout.div(2), confReloadTimeout.div(2), confManager, new ReloadConf(),
            getContext().dispatcher(), getSelf());
  }

  private void loadConf() {
    if (conf.hasPath("akka.clusterone.work.interval")) {
      interval = conf.getInt("akka.clusterone.work.interval");
    } else {
      interval = defaultWorkInterval;
    }
    if (conf.hasPath("akka.clusterone.work.timeout")) {
      timeout = conf.getInt("akka.clusterone.work.timeout");

    } else {
      timeout = defaultWorkTimeOut;
    }

    if (conf.hasPath("akka.clusterone.conf.reloadtime")) {
      confReloadTime = conf.getInt("akka.clusterone.conf.reloadtime");

    } else {
      confReloadTime = defaultConfReloadTime;
    }

    if (conf.hasPath("akka.clusterone.cluster.types")) {
      typeList = conf.getStringList("akka.clusterone.cluster.types");
    } else {
      typeList = new ArrayList<String>();
      typeList.add("srv");
      typeList.add("prc");
    }

    if (conf.hasPath("akka.clusterone.mast.workthreshhold")) {
      distributeWorkThreshhod = conf.getInt("akka.clusterone.master.workthreshhold");

    } else {
      distributeWorkThreshhod = defaultDistributeWorkThreshhold;
    }

  }

  @Override
  public void postStop() {
    cleanupTask.cancel();
    confReloadTask.cancel();
  }

  private void notifyWorkers(String type) {
    log.debug("Notify workers: " + type);
    if (workMgr.hasWork(type)) {
      // could pick a few random instead of all
      workerMgr.notifyWorkers(type, getSelf());
    }
  }

  public static final Object CleanupTick = new Object() {
    @Override
    public String toString() {
      return "CleanupTick";
    }
  };

  public static final class Ack implements Serializable {
    final String workId;

    public Ack(String workId) {
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "Ack{" + "workId='" + workId + '\'' + '}';
    }
  }

  private void processWorkerRequestWorkMessage (WorkerRequestsWork workerRequestsWork) {
    String workerId = workerRequestsWork.workerId;
    String type = workerMgr.getType(workerId);
    if (workMgr.hasWork(type)) {
      WorkerState state = workerMgr.getWorkerState(workerId);
      if (state != null) {
        final Work work = workMgr.nextWork(type);
        WorkStarted event = new WorkState.WorkStarted(work.workId);
        workMgr.updatedByWorkId(work.workId, event);
        log.info("Giving worker " + workerId + " work " + event.workId);
        state.updated(event);
        ActorRef worker = getSender();
        work.setWorker(worker);
        worker.tell(work, getSelf());
        work.updateLastSchduledTime();
      }
    }
  }

  private void processRegisterWorkerMessage (RegisterWorker registerWorker) {
    String workerId = registerWorker.workerId;
    String type = null;
    if (workerMgr.isWorkerRegistered(workerId)) {
      type = workerMgr.getType(workerId);
      log.info(type + " Worker heartbeat: " + workerId);
      WorkerHeartbeat event = new WorkState.WorkerHeartbeat(workerId);
      workerMgr.getWorkerState(workerId).updated(event);
    } else {
      type =workerMgr.registerWorker(workerId, new WorkerState(workerId, getSender(), workTimeout, new WorkerStatus(
          workTimeout.fromNow())));
      log.info("Worker registered: " + workerId + " type: " + type);
    }

    if (workMgr.hasWork(type)) {
      getSender().tell(WorkIsReady.getInstance(), getSelf());
    }
  }

  private void processWorkIsDoneMessage(WorkIsDone workIsDone) {
    final String workerId = workIsDone.workerId;
    final String workId = workIsDone.workId;
    WorkerState worker = workerMgr.getWorkerState(workerId);
    if (workMgr.isInProgress(workId)) {
      log.info("Work " + workId + " is done by worker " + workerId);
      WorkCompleted event = new WorkState.WorkCompleted(workId);
      worker.updated(event);
      workMgr.updatedByWorkId(workId, event);
    } else {
      log.error("Work " + workId + " not in progress, reported as done by worker " + workerId);
    }
    getSender().tell(new Ack(workId), getSelf());
  }

  private void processWorkFailedMessage(WorkFailed workFailed) {
    final String workId = workFailed.workId;
    final String workerId = workFailed.workerId;
    if (workMgr.isInProgress(workId)) {
      log.info("Work " + workId + " failed by worker " + workerId);
      WorkState.WorkFailed event = new WorkState.WorkFailed(workId);
      workMgr.updatedByWorkId(workId, event);
      workerMgr.getWorkerState(workerId).updated(event);
    } else {
      log.error("Work " + workId + " not in progress, reported as done by worker " + workerId);
    }
    getSender().tell(new Ack(workId), getSelf());
  }

  private void processWorkMessage(Work work) {
    final String workId = work.workId;
    // idempotent
    if (workMgr.isAccepted(workId)) {
      getSender().tell(new Ack(workId), getSelf());
    } else {
      WorkAccepted event = new WorkState.WorkAccepted(work);
      // Ack back to original sender
      getSender().tell(new Ack(event.work.workId), getSelf());
      workMgr.updatedByWorkId(workId, event);
      notifyWorkers(workMgr.getWorkTypeById(event.work.workId));
    }
  }

  private void processCleanupTick() {
    log.info("received cleanup tick");
    ArrayList<WorkerState> timeoutlist = null;
    for (int i = 0; i < typeList.size(); i ++) {
      String type = typeList.get(i);
      timeoutlist = workerMgr.checkTimeout(type);
      if (timeoutlist != null) {
        for (int j = 0; j < timeoutlist.size(); j++) {
          WorkerTimedOut event = new WorkState.WorkerTimedOut(timeoutlist.get(j).getStatus().getWorkIds());
          workMgr.updatedByType(type, event);
          notifyWorkers(type);
        }
      }
    }

    for (String workerId: workerMgr.getWorkers()) {
      for (String workId : workerMgr.getWorkerState(workerId).getStatus().getWorkIds()) {
        Work work = workMgr.getWork(workId);
        if (work != null && work.isRunable()) {
          getSelf().tell(new WorkFailed(workerId, work.workId), getSelf());
        }
      }
    }
    log.info("Work status: " + workMgr.toString());
  }

  private void processConfAddedMessage(ConfAdded confAdded) {
    final String cluster = confAdded.conf;
    final String properties = confAdded.propertiesStr;
    // idempotent
    addOrUpdateConf(cluster, properties);
  }

  private void processConfUpdatedMessage(ConfUpdated confUpdated) {
    final String cluster = confUpdated.conf;
    final String properties = confUpdated.propertiesStr;
    // idempotent
    addOrUpdateConf(cluster, properties);
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof RegisterWorker) {
      processRegisterWorkerMessage((RegisterWorker) message);
    } else if (message instanceof WorkerRequestsWork) {
      processWorkerRequestWorkMessage((WorkerRequestsWork) message);
    } else if (message instanceof WorkIsDone) {
      processWorkIsDoneMessage(((WorkIsDone) message));
    } else if (message instanceof WorkFailed) {
      processWorkFailedMessage((WorkFailed) message);
    } else if (message instanceof Work) {
      processWorkMessage(((Work) message));
    } else if (message == CleanupTick) {
      processCleanupTick();
    } else if (message instanceof Ack) {
      log.debug("Schedule period task: " + ((Ack)message).workId);
    } else if (message instanceof ConfAdded) {
      processConfAddedMessage(((ConfAdded) message));
    }  else if (message instanceof ConfUpdated) {
      processConfUpdatedMessage(((ConfUpdated) message));
    } else {
      unhandled(message);
    }
  }

  public void addOrUpdateConf (String workid, String propertes) {
    // workid is the cluster name
    // check if the work type is support (in the type list)
    // if not, we reject it
    if (workMgr.getWorkTypeById(workid) == null) {
      log.warning("could not find the type for work: "+ workid);
      return;
    }
    if (workMgr.isAccepted(workid)) {
      workMgr.updateWork(workid, propertes);
    } else {
      Task t = new Task(workid, 0, propertes);
      this.getSelf().tell(new Work(workid, t, interval), ActorRef.noSender());
    }
  }
}
