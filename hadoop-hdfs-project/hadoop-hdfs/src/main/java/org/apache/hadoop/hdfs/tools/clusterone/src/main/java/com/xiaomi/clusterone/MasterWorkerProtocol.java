package com.xiaomi.clusterone;

import java.io.Serializable;

public abstract class MasterWorkerProtocol {

  // Messages from/to Workers

  public static final class RegisterWorker implements Serializable {
    public final String workerId;

    public RegisterWorker(String workerId) {
      this.workerId = workerId;
    }

    @Override
    public String toString() {
      return "RegisterWorker{" +
        "workerId='" + workerId + '\'' +
        '}';
    }
  }

  public static final class WorkerRequestsWork implements Serializable {
    public final String workerId;

    public WorkerRequestsWork(String workerId) {
      this.workerId = workerId;
    }

    @Override
    public String toString() {
      return "WorkerRequestsWork{" +
        "workerId='" + workerId + '\'' +
        '}';
    }
  }

  public static final class WorkIsDone implements Serializable {
    public final String workerId;
    public final String workId;

    public WorkIsDone(String workerId, String workId) {
      this.workerId = workerId;
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "WorkIsDone{" +
        "workerId='" + workerId + '\'' +
        ", workId='" + workId + '\'' +
        '}';
    }
  }

  public static final class WorkFailed implements Serializable {
    public final String workerId;
    public final String workId;

    public WorkFailed(String workerId, String workId) {
      this.workerId = workerId;
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "WorkFailed{" +
        "workerId='" + workerId + '\'' +
        ", workId='" + workId + '\'' +
        '}';
    }
  }

  // Message from / to conf manager
  public static final class ConfAdded implements Serializable {
    public final String conf;
    public final String propertiesStr;

    public ConfAdded(String conf, String propertiesStr) {
      this.conf = conf;
      this.propertiesStr = propertiesStr;
    }

    @Override
    public String toString() {
      return "ConfAdded{" +
          "conf='" + conf + '\'' +
          '}';
    }
  }

  public static final class ConfUpdated implements Serializable {
    public final String conf;
    public final String propertiesStr;

    public ConfUpdated(String conf, String propertiesStr) {
      this.conf = conf;
      this.propertiesStr = propertiesStr;
    }

    @Override
    public String toString() {
      return "ConfUpdated{" +
          "conf='" + conf + '\'' +
          '}';
    }
  }

  public static final class ConfRemoved implements Serializable {
    public final String conf;
    public ConfRemoved(String conf) {
      this.conf = conf;
    }

    @Override
    public String toString() {
      return "ConfRemoved{" +
          "conf='" + conf + '\'' +
          '}';
    }
  }

  public static final class StartConf implements Serializable {

    @Override
    public String toString() {
      return "ConfRemoved{" +
          '}';
    }
  }

  public static final class ReloadConf implements Serializable {

    @Override
    public String toString() {
      return "ReloadConf{" +
          '}';
    }
  }

  // Messages to Workers

  public static final class WorkIsReady implements Serializable {
    private static final WorkIsReady instance = new WorkIsReady();
    public static WorkIsReady getInstance() {
      return instance;
    }
  }

  public static final class WorkToStop implements Serializable {
    private static final WorkToStop instance = new WorkToStop();
    public static WorkToStop getInstance() {
      return instance;
    }
  }

}