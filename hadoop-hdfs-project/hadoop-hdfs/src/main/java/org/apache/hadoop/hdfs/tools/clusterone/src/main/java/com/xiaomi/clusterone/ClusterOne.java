package com.xiaomi.clusterone;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ClusterOne {
  private static Log LOG = LogFactory.getLog(ClusterOne.class);

  public static void main(String[] args) throws IOException {
    if (args.length == 1 && args[0].equals("master")) {
      startMaster();
    } else if (args.length == 1 && args[0].equals("worker")) {
      startWorker();
    } else {
      System.out.println("Usage: com.xiaomi.clusterone master|worker");
    }
  }

  public static void startMaster() throws IOException {
    Config conf = ConfigFactory.load("master");
    ActorSystem system = ActorSystem.create("MasterSystem", conf);
    ActorRef master = system.actorOf(Master.props(conf, Props.create(ConfManager.class, conf)), "master");

  }


  public static void startWorker() throws IOException {
      Config conf = ConfigFactory.load("worker");
      ActorSystem system = ActorSystem.create("WorkerSystem", conf);
      final ActorRef master = system.actorFor(conf.getString("akka.clusterone.master"));
      system.actorOf(Worker.props(conf, master, Props.create(WorkExecutor.class, conf)), "worker");
  }
}
