/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Time;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Plugin(name = Log4jWarningErrorMetricsAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME, elementType = "appender", printObject = true)
public final class Log4jWarningErrorMetricsAppender extends AbstractAppender {

  public static final String PLUGIN_NAME = "Log4jWarningErrorMetricsAppender";

  public static final String LOG_METRICS_APPENDER = "RM_LOG_METRICS_APPENDER";
  static final int MAX_MESSAGE_SIZE = 2048;

  public static class Builder
          implements org.apache.logging.log4j.core.util.Builder<Log4jWarningErrorMetricsAppender> {

    @PluginBuilderAttribute
    private long cleanupInterval = 5L * 60;

    @PluginBuilderAttribute
    private long messageAgeLimitSeconds = 24L * 60 * 60;

    @PluginBuilderAttribute
    private int maxUniqueMessages = 250;

    public long getCleanupInterval() {
      return cleanupInterval;
    }

    public Builder setCleanupInterval(long cleanupInterval) {
      this.cleanupInterval = cleanupInterval;
      return this;
    }

    public long getMessageAgeLimitSeconds() {
      return messageAgeLimitSeconds;
    }

    public Builder setMessageAgeLimitSeconds(long messageAgeLimitSeconds) {
      this.messageAgeLimitSeconds = messageAgeLimitSeconds;
      return this;
    }

    public int getMaxUniqueMessages() {
      return maxUniqueMessages;
    }

    public Builder setMaxUniqueMessages(int maxUniqueMessages) {
      this.maxUniqueMessages = maxUniqueMessages;
      return this;
    }

    @Override
    public Log4jWarningErrorMetricsAppender build() {
      return new Log4jWarningErrorMetricsAppender(cleanupInterval, messageAgeLimitSeconds, maxUniqueMessages);
    }
  }

  @PluginBuilderFactory
  public static Builder newBuilder() {
    return new Builder();
  }

  static public class Element {
    public Long count;
    public Long timestampSeconds;

    Element(Long count, Long timestampSeconds) {
      this.count = count;
      this.timestampSeconds = timestampSeconds;
    }
  }


  static class PurgeElement implements Comparable<Log4jWarningErrorMetricsAppender.PurgeElement> {
    String message;
    Long timestamp;

    PurgeElement(String message, Long timestamp) {
      this.message = message;
      this.timestamp = timestamp;
    }

    public int compareTo(Log4jWarningErrorMetricsAppender.PurgeElement e) {
      if (e == null) {
        throw new NullPointerException("Null element passed to compareTo");
      }
      int ret = this.timestamp.compareTo(e.timestamp);
      if (ret != 0) {
        return ret;
      }
      return this.message.compareTo(e.message);
    }

    @Override
    public boolean equals(Object e) {
      if (!(e instanceof Log4jWarningErrorMetricsAppender.PurgeElement)) {
        return false;
      }
      if (e == this) {
        return true;
      }
      Log4jWarningErrorMetricsAppender.PurgeElement el = (Log4jWarningErrorMetricsAppender.PurgeElement) e;
      return (this.message.equals(el.message))
          && (this.timestamp.equals(el.timestamp));
    }

    @Override
    public int hashCode() {
      return this.timestamp.hashCode();
    }
  }


  Map<String, SortedMap<Long, Integer>> errors;
  Map<String, SortedMap<Long, Integer>> warnings;
  SortedMap<Long, Integer> errorsTimestampCount;
  SortedMap<Long, Integer> warningsTimestampCount;
  SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> errorsPurgeInformation;
  SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> warningsPurgeInformation;

  Timer cleanupTimer;
  long cleanupInterval;
  long messageAgeLimitSeconds;
  int maxUniqueMessages;

  final Object lock = new Object();

  private Log4jWarningErrorMetricsAppender(long cleanupInterval, long messageAgeLimitSeconds, int maxUniqueMessages) {
    super(LOG_METRICS_APPENDER, null, null, true, null);
    this.cleanupInterval = cleanupInterval;
    this.messageAgeLimitSeconds = messageAgeLimitSeconds;
    this.maxUniqueMessages = maxUniqueMessages;
  }

  @Override
  public void append(LogEvent event) {
    String message = event.getMessage().getFormattedMessage();
    String throwableStr = event.getThrownProxy().getExtendedStackTraceAsString();
    if (throwableStr != null && throwableStr.length() > 0) {
      message = message + "\n" + throwableStr;
      message =
          org.apache.commons.lang3.StringUtils.left(message, MAX_MESSAGE_SIZE);
    }
    int level = event.getLevel().intLevel();

    if (level == Level.WARN.intLevel() || level == Level.ERROR.intLevel()) {
      // store second level information
      long eventTimeSeconds = event.getTimeMillis() / 1000;
      Map<String, SortedMap<Long, Integer>> map;
      SortedMap<Long, Integer> timestampsCount;
      SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> purgeInformation;
      if (level == Level.WARN.intLevel()) {
        map = warnings;
        timestampsCount = warningsTimestampCount;
        purgeInformation = warningsPurgeInformation;
      } else {
        map = errors;
        timestampsCount = errorsTimestampCount;
        purgeInformation = errorsPurgeInformation;
      }
      updateMessageDetails(message, eventTimeSeconds, map, timestampsCount,
          purgeInformation);
    }
  }

  private void updateMessageDetails(String message, Long eventTimeSeconds,
      Map<String, SortedMap<Long, Integer>> map,
      SortedMap<Long, Integer> timestampsCount,
      SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> purgeInformation) {
    synchronized (lock) {
      if (map.containsKey(message)) {
        SortedMap<Long, Integer> tmp = map.get(message);
        Long lastMessageTime = tmp.lastKey();
        int value = 1;
        if (tmp.containsKey(eventTimeSeconds)) {
          value = tmp.get(eventTimeSeconds) + 1;
        }
        tmp.put(eventTimeSeconds, value);
        purgeInformation.remove(new Log4jWarningErrorMetricsAppender.PurgeElement(message, lastMessageTime));
      } else {
        SortedMap<Long, Integer> value = new TreeMap<>();
        value.put(eventTimeSeconds, 1);
        map.put(message, value);
        if (map.size() > maxUniqueMessages * 2) {
          cleanupTimer.cancel();
          cleanupTimer = new Timer();
          cleanupTimer.schedule(new ErrorAndWarningsCleanup(), 0);
        }
      }
      purgeInformation.add(new Log4jWarningErrorMetricsAppender.PurgeElement(message, eventTimeSeconds));
      int newValue = 1;
      if (timestampsCount.containsKey(eventTimeSeconds)) {
        newValue = timestampsCount.get(eventTimeSeconds) + 1;
      }
      timestampsCount.put(eventTimeSeconds, newValue);
    }
  }

  /**
   * Get the counts of errors in the time periods provided. Note that the counts
   * provided by this function may differ from the ones provided by
   * getErrorMessagesAndCounts since the message store is purged at regular
   * intervals to prevent it from growing without bounds, while the store for
   * the counts is purged less frequently.
   *
   * @param cutoffs list of timestamp cutoffs(in seconds) for which the counts are
   *                desired
   * @return list of error counts in the time periods corresponding to cutoffs
   */
  public List<Integer> getErrorCounts(List<Long> cutoffs) {
    return this.getCounts(errorsTimestampCount, cutoffs);
  }

  /**
   * Get the counts of warnings in the time periods provided. Note that the
   * counts provided by this function may differ from the ones provided by
   * getWarningMessagesAndCounts since the message store is purged at regular
   * intervals to prevent it from growing without bounds, while the store for
   * the counts is purged less frequently.
   *
   * @param cutoffs list of timestamp cutoffs(in seconds) for which the counts are
   *                desired
   * @return list of warning counts in the time periods corresponding to cutoffs
   */
  public List<Integer> getWarningCounts(List<Long> cutoffs) {
    return this.getCounts(warningsTimestampCount, cutoffs);
  }

  private List<Integer> getCounts(SortedMap<Long, Integer> map,
      List<Long> cutoffs) {
    List<Integer> ret = new ArrayList<>();
    Long largestCutoff = Collections.min(cutoffs);
    for (int i = 0; i < cutoffs.size(); ++i) {
      ret.add(0);
    }
    synchronized (lock) {
      Map<Long, Integer> submap = map.tailMap(largestCutoff);
      for (Map.Entry<Long, Integer> entry : submap.entrySet()) {
        for (int i = 0; i < cutoffs.size(); ++i) {
          if (entry.getKey() >= cutoffs.get(i)) {
            int tmp = ret.get(i);
            ret.set(i, tmp + entry.getValue());
          }
        }
      }
    }
    return ret;
  }

  /**
   * Get the errors and the number of occurrences for each of the errors for the
   * time cutoffs provided. Note that the counts provided by this function may
   * differ from the ones provided by getErrorCounts since the message store is
   * purged at regular intervals to prevent it from growing without bounds,
   * while the store for the counts is purged less frequently.
   *
   * @param cutoffs list of timestamp cutoffs(in seconds) for which the counts are
   *                desired
   * @return list of maps corresponding for each cutoff provided; each map
   * contains the error and the number of times the error occurred in
   * the time period
   */
  public List<Map<String, Log4jWarningErrorMetricsAppender.Element>>
  getErrorMessagesAndCounts(List<Long> cutoffs) {
    return this.getElementsAndCounts(errors, cutoffs, errorsPurgeInformation);
  }

  /**
   * Get the warning and the number of occurrences for each of the warnings for
   * the time cutoffs provided. Note that the counts provided by this function
   * may differ from the ones provided by getWarningCounts since the message
   * store is purged at regular intervals to prevent it from growing without
   * bounds, while the store for the counts is purged less frequently.
   *
   * @param cutoffs list of timestamp cutoffs(in seconds) for which the counts are
   *                desired
   * @return list of maps corresponding for each cutoff provided; each map
   * contains the warning and the number of times the error occurred in
   * the time period
   */
  public List<Map<String, Log4jWarningErrorMetricsAppender.Element>> getWarningMessagesAndCounts(
      List<Long> cutoffs) {
    return this.getElementsAndCounts(warnings, cutoffs, warningsPurgeInformation);
  }

  private List<Map<String, Log4jWarningErrorMetricsAppender.Element>> getElementsAndCounts(
      Map<String, SortedMap<Long, Integer>> map, List<Long> cutoffs,
      SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> purgeInformation) {
    if (purgeInformation.size() > maxUniqueMessages) {
      ErrorAndWarningsCleanup
          cleanup = new ErrorAndWarningsCleanup();
      long cutoff = Time.now() - (messageAgeLimitSeconds * 1000);
      cutoff = (cutoff / 1000);
      cleanup.cleanupMessages(map, purgeInformation, cutoff, maxUniqueMessages);
    }
    List<Map<String, Log4jWarningErrorMetricsAppender.Element>> ret = new ArrayList<>(cutoffs.size());
    for (int i = 0; i < cutoffs.size(); ++i) {
      ret.add(new HashMap<String, Log4jWarningErrorMetricsAppender.Element>());
    }
    synchronized (lock) {
      for (Map.Entry<String, SortedMap<Long, Integer>> element : map.entrySet()) {
        for (int i = 0; i < cutoffs.size(); ++i) {
          Map<String, Log4jWarningErrorMetricsAppender.Element> retMap = ret.get(i);
          SortedMap<Long, Integer> qualifyingTimes =
              element.getValue().tailMap(cutoffs.get(i));
          long count = 0;
          for (Map.Entry<Long, Integer> entry : qualifyingTimes.entrySet()) {
            count += entry.getValue();
          }
          if (!qualifyingTimes.isEmpty()) {
            retMap.put(element.getKey(),
                new Log4jWarningErrorMetricsAppender.Element(count, qualifyingTimes.lastKey()));
          }
        }
      }
    }
    return ret;
  }

  // getters and setters for log4j
  public long getCleanupInterval() {
    return cleanupInterval;
  }

  public void setCleanupInterval(long cleanupInterval) {
    this.cleanupInterval = cleanupInterval;
  }

  public long getMessageAgeLimitSeconds() {
    return messageAgeLimitSeconds;
  }

  public void setMessageAgeLimitSeconds(long messageAgeLimitSeconds) {
    this.messageAgeLimitSeconds = messageAgeLimitSeconds;
  }

  public int getMaxUniqueMessages() {
    return maxUniqueMessages;
  }


  public void setMaxUniqueMessages(int maxUniqueMessages) {
    this.maxUniqueMessages = maxUniqueMessages;
  }

  class ErrorAndWarningsCleanup extends TimerTask {

    @Override
    public void run() {
      long cutoff = Time.now() - (messageAgeLimitSeconds * 1000);
      cutoff = (cutoff / 1000);
      cleanupMessages(errors, errorsPurgeInformation, cutoff, maxUniqueMessages);
      cleanupMessages(warnings, warningsPurgeInformation, cutoff,
          maxUniqueMessages);
      cleanupCounts(errorsTimestampCount, cutoff);
      cleanupCounts(warningsTimestampCount, cutoff);
      try {
        cleanupTimer.schedule(new ErrorAndWarningsCleanup(), cleanupInterval);
      } catch (IllegalStateException ie) {
        // don't do anything since new timer is already scheduled
      }
    }

    void cleanupMessages(Map<String, SortedMap<Long, Integer>> map,
        SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> purgeInformation, long cutoff,
        int mapTargetSize) {

      Log4jWarningErrorMetricsAppender.PurgeElement el = new Log4jWarningErrorMetricsAppender.PurgeElement("", cutoff);
      synchronized (lock) {
        SortedSet<Log4jWarningErrorMetricsAppender.PurgeElement> removeSet = purgeInformation.headSet(el);
        Iterator<Log4jWarningErrorMetricsAppender.PurgeElement> it = removeSet.iterator();
        while (it.hasNext()) {
          Log4jWarningErrorMetricsAppender.PurgeElement p = it.next();
          map.remove(p.message);
          it.remove();
        }

        // don't keep more mapTargetSize keys
        if (purgeInformation.size() > mapTargetSize) {
          Object[] array = purgeInformation.toArray();
          int cutoffIndex = purgeInformation.size() - mapTargetSize;
          for (int i = 0; i < cutoffIndex; ++i) {
            Log4jWarningErrorMetricsAppender.PurgeElement p = (Log4jWarningErrorMetricsAppender.PurgeElement) array[i];
            map.remove(p.message);
            purgeInformation.remove(p);
          }
        }
      }
    }

    void cleanupCounts(SortedMap<Long, Integer> map, long cutoff) {
      synchronized (lock) {
        Iterator<Map.Entry<Long, Integer>> it = map.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Long, Integer> element = it.next();
          if (element.getKey() < cutoff) {
            it.remove();
          }
        }
      }
    }
  }

  // helper function
  public static Log4jWarningErrorMetricsAppender findAppender() {
    Iterator<Appender> iter =
        ((org.apache.logging.log4j.core.Logger) LogManager.getRootLogger()).getAppenders().values().iterator();
    while (iter.hasNext()) {
      Appender obj = iter.next();
      if (obj instanceof Log4jWarningErrorMetricsAppender) {
        return (Log4jWarningErrorMetricsAppender) obj;
      }
    }
    return null;
  }
}
