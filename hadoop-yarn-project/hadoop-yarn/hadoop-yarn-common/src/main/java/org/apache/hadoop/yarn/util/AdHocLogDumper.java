/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AdHocLogDumper {

  private static final Logger LOG =
      LoggerFactory.getLogger(AdHocLogDumper.class);

  private String name;
  private String targetFilename;
  private org.apache.logging.log4j.Level currentLogLevel;
  private org.apache.logging.log4j.core.appender.FileAppender appender;
  public static final String AD_HOC_DUMPER_APPENDER = "ad-hoc-dumper-appender";
  private static volatile boolean logFlag = false;
  private static final Object lock = new Object();

  public AdHocLogDumper(String name, String targetFilename) {
    this.name = name;
    this.targetFilename = targetFilename;
  }

  public void dumpLogs(String level, int timePeriod)
      throws YarnRuntimeException, IOException {
    synchronized (lock){
      if (logFlag) {
        LOG.info("Attempt to dump logs when appender is already running");
        throw new YarnRuntimeException("Appender is already dumping logs");
      }
      org.apache.logging.log4j.Level targetLevel = org.apache.logging.log4j.Level.toLevel(level);
      org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger)
              org.apache.logging.log4j.LogManager.getLogger(name);
      currentLogLevel = logger.getLevel();
      // make sure we can create the appender first
      File file = new File(System.getProperty("yarn.log.dir"), targetFilename);
      appender = org.apache.logging.log4j.core.appender.FileAppender.newBuilder()
              .setName(AD_HOC_DUMPER_APPENDER)
              .setLayout(org.apache.logging.log4j.core.layout.PatternLayout.newBuilder()
                      .withPattern("%d{ISO8601} %p %c: %m%n")
                      .build())
              .withFileName(file.getAbsolutePath())
              .build();
      logger.addAppender(appender);
      LOG.info("Dumping adhoc logs for " + name + " to "
          + file.getAbsolutePath() + " for " + timePeriod + " milliseconds");
      logger.setLevel(targetLevel);
      logFlag = true;

      TimerTask restoreLogLevel = new RestoreLogLevel();
      Timer restoreLogLevelTimer = new Timer();
      restoreLogLevelTimer.schedule(restoreLogLevel, timePeriod);
    }
  }

  @VisibleForTesting
  public static boolean getState() {
    return logFlag;
  }

  class RestoreLogLevel extends TimerTask {
    @Override
    public void run() {
      org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger)
              org.apache.logging.log4j.LogManager.getLogger(name);
      logger.removeAppender(appender);
      logger.setLevel(currentLogLevel);
      logFlag = false;
      LOG.info("Done dumping adhoc logs for " + name);
    }
  }
}
