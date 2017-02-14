package com.xiaomi.clusterone;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import com.typesafe.config.Config;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import static akka.actor.SupervisorStrategy.Directive;
import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.stop;
import static com.xiaomi.clusterone.Master.Ack;
import static com.xiaomi.clusterone.MasterWorkerProtocol.*;

public class Worker extends UntypedActor {

  public static Props props(Config conf, ActorRef masterClient, Props workExecutorProps) {
    return Props.create(Worker.class, conf, masterClient, workExecutorProps);
  }

  private static final int defaultExecutorNum = 1;
  private static final int defaultHeartBeat = 10;

  private Config conf;
  private ActorRef masterClient;
  private int executorNum;
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private String workerId = null;
  private final Cancellable registerTask;
  private final FiniteDuration registerInterval;

  private Set<String> runningWorks = new HashSet<String>();
  private Queue<ActorRef> idleExecutors;

  private long requestedWorkNum = 0;
  private long ackedWorkNum = 0;
  private long completedWorkNum = 0;

  public Worker(Config conf, ActorRef masterClient, Props workExecutorProps) {
    this.conf = conf;
    this.masterClient = masterClient;
    this.idleExecutors = new LinkedList<ActorRef>();
    try {
      workerId = InetAddress.getLocalHost().getHostName().toString() + "_" + UUID.randomUUID().toString();
    } catch (Exception e) {
      workerId = UUID.randomUUID().toString();
    }

    if (conf.hasPath("akka.clusterone.worker.heartbeat")) {
      registerInterval =  Duration.create(conf.getInt("akka.clusterone.worker.heartbeat"), "seconds");
    } else {
      registerInterval =  Duration.create(defaultHeartBeat, "seconds");
    }

    if (conf.hasPath("akka.clusterone.worker.executor.num")) {
      executorNum = conf.getInt("akka.clusterone.worker.executor.num");
    } else {
      executorNum = defaultExecutorNum;
    }

    for (int i = 0; i < executorNum; i++) {
      this.idleExecutors.add(getContext().watch(
        getContext().actorOf(workExecutorProps, "executor-" + i)));
    }

    log.info("Worker id: {}", workerId);
    // period heartbeat
    this.registerTask =
        getContext()
            .system()
            .scheduler()
            .schedule(Duration.Zero(), registerInterval, masterClient,
              new RegisterWorker(workerId), getContext().dispatcher(), getSelf());
  }

  @Override
  public SupervisorStrategy supervisorStrategy () {
    return new OneForOneStrategy(-1, Duration.Inf(), new Function<Throwable, Directive>() {
      @Override
      public Directive apply(Throwable t) {
        log.error(t, "Worker encounter exceptions");
        if (t instanceof ActorInitializationException) return stop();
        else if (t instanceof DeathPactException) return stop();
        else if (t instanceof Exception) {
          for (String workId : runningWorks) {
            sendToMaster(new WorkFailed(workerId, workId));
          }
          log.info("restart worker");
          return restart();
        } else {
          log.info("escalate worker failure");
          return escalate();
        }
      }
    });
  }


  private void processWorkMessage (Work work) {
    requestedWorkNum ++;

    if (idleExecutors.isEmpty()) {
      sendToMaster(new WorkFailed(workerId, work.workId));
      completedWorkNum++;
      log.info("Worker has no idle excutors. Running works: {}", runningWorks.size());
    } else {
      log.info("Got work: {}, Idle executors: {}", work.task, idleExecutors.size());
      if (runningWorks.contains(work.workId)) {
        log.info("The work {} has been handling by worker {}", work.workId, this.workerId);
      } else {
        runningWorks.add(work.workId);
        idleExecutors.poll().tell(work, getSelf());
      }
      if (!idleExecutors.isEmpty()) {
        getSender().tell(new WorkerRequestsWork(workerId), getSelf());
      }
    }
  }

  private void processWorkCompleteMessage (WorkComplete workComplete) {
    String workId = workComplete.workId;
    runningWorks.remove(workId);
    idleExecutors.add(getSender());

    log.info("Work is complete. workId {}. Idle executors: {}", workId, idleExecutors.size());
    sendToMaster(new WorkIsDone(workerId, workId));
    completedWorkNum++;
    if (!idleExecutors.isEmpty()) {
      sendToMaster(new WorkerRequestsWork(workerId));
    }
    log.info("requestedWorkNum: {}, compelted task: {},  ackedWorkNum: {}", requestedWorkNum,
        completedWorkNum, ackedWorkNum);

  }

  private void processAckMessage (Ack ack) {
    ackedWorkNum++;
    log.info("requestedWorkNum: {}, compelted task: {},  ackedWorkNum: {}", requestedWorkNum,
        completedWorkNum, ackedWorkNum);
  }

  private void processWorkFailMessage (WorkFail workFail) {
    String workId = workFail.workId;
    runningWorks.remove(workId);
    idleExecutors.add(getSender());

    log.info("Work fails. workId {}. Idle executors: {}", workId, idleExecutors.size());
    sendToMaster(new WorkFailed(workerId, workId));
    if (!idleExecutors.isEmpty()) {
      sendToMaster(new WorkerRequestsWork(workerId));
    }
    log.info("requestedWorkNum: {}, compelted task: {},  ackedWorkNum: {}", requestedWorkNum,
        completedWorkNum, ackedWorkNum);
  }

  private void processWorkIsReadyMessage (WorkIsReady workIsReady) {
    if (!idleExecutors.isEmpty()) {
      sendToMaster(new WorkerRequestsWork(workerId));
    }
  }

  @Override
  public void postStop() {
    registerTask.cancel();
  }

  public void onReceive(Object message) {
    if (message instanceof WorkIsReady) {
      processWorkIsReadyMessage((WorkIsReady) message);
    } else if (message instanceof Work) {
      processWorkMessage((Work) message);
    } else if (message instanceof WorkComplete) {
      processWorkCompleteMessage((WorkComplete) message);
    } else if (message instanceof Ack) {
      processAckMessage((Ack) message);
    } else if (message instanceof WorkFail) {
      processWorkFailMessage((WorkFail) message);
    } else {
      unhandled(message);
    }
  }

  @Override
  public void unhandled(Object message) {
    log.error("Unhandled message: {}", message);
    if (message instanceof Terminated) {
      getContext().stop(getSelf());
    } else {
      super.unhandled(message);
    }
  }

  private void sendToMaster(Object msg) {
    masterClient.tell(msg, getSelf());
  }

  public static final class WorkComplete implements Serializable {
    public final String workId;

    public WorkComplete(String workId) {
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "WorkComplete{" + "workId=" + workId + '}';
    }
  }

  public static final class WorkFail implements Serializable {
    public final String workId;

    public WorkFail(String workId) {
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "WorkFail{" + "workId=" + workId + '}';
    }
  }
}
