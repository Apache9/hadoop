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
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;


public class TracerLog {
  public enum TracerWarnTimeType { normal, rwPacket, rwBlock }

  public static int     MAX_ANNOTATION_COUNT = 100;
  public static String  TAG_MORE_LOGS = "more logs ......";

  private static final Log tracerLog = LogFactory.getLog(TracerLog.class.getName());
  private static DateFormat tracerDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static TraceInfo tinfo = new TraceInfo(272860771,0);
  private static ThreadLocal<TraceScope> threadLocalScope = new ThreadLocal<TraceScope>();

  private static boolean initialised    = false;
  private static boolean clientEnabled  = false;
  private static long    warnTimeNormal = 0;
  private static long    warnTimePacket = 0;
  private static long    warnTimeBlock  = 0;

  public static void initClient(Configuration conf) {
    clientEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG,
      DFSConfigKeys.DFS_CLIENT_ENABLE_TRACER_LOG_DEFAULT);
  }

  public static void initServer(Configuration conf) {
    warnTimeNormal = conf.getLong(
      DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_KEY,
      DFSConfigKeys.DFS_TRACER_WARN_TIME_NORMAL_DEFAULT);
    warnTimePacket = conf.getLong(
      DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_KEY,
      DFSConfigKeys.DFS_TRACER_WARN_TIME_RWPACKET_DEFAULT);
    warnTimeBlock = conf.getLong(
      DFSConfigKeys.DFS_TRACER_WARN_TIME_RWBLOCK_KEY,
      DFSConfigKeys.DFS_TRACER_WARN_TIME_RWBLOCK_DEFAULT);
    initialised = true;
  }

  public static long getWarnTime(TracerWarnTimeType type) {
    switch (type) {
      case rwPacket: return warnTimePacket;
      case rwBlock:  return warnTimeBlock;
      default:       return warnTimeNormal;
    }
  }

  public static boolean isEnabled() {
    return initialised;
  }

  public static void disable() {
    initialised =false;
  }

  public static boolean isClientTracing() {
    if (!Trace.isTracing() || !clientEnabled) {
      return false;
    }
    int curCount = Trace.currentSpan().getTimelineAnnotations().size();
    if (curCount < MAX_ANNOTATION_COUNT) {
      return true;
    }
    if (curCount == MAX_ANNOTATION_COUNT) {
      Trace.addTimelineAnnotation("HDFS: " + TAG_MORE_LOGS);
    }
    return false;
  }

  public static TraceScope startScope(String op, String info) {
    if (!initialised) {
      return null;
    }
    Span span = Trace.currentSpan();
    if (span != null) {
      if (span.getTraceId() != tinfo.traceId) {
        return null;
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append("op=").append(op);
    sb.append(", ").append(info);
    TraceScope scope = Trace.startSpan(sb.toString(), tinfo);
    if (scope == null) {
      tracerLog.info("Failed to startSpan: " + sb.toString());
    }
    return scope;
  }

  public static void closeScope(TraceScope scope) {
    closeScope(scope, TracerWarnTimeType.normal);
  }

  public static void closeScope(TraceScope scope, TracerWarnTimeType warnTimeType) {
    if (!initialised) {
      return;
    }
    if (scope == null) {
      return;
    }

    Span span = scope.getSpan();
    scope.close();
    long duration = span.getAccumulatedMillis();
    long warnTime = getWarnTime(warnTimeType);
    if (duration >= warnTime) {
      StringBuilder sb = new StringBuilder();
      sb.append(span.getDescription()).append(" ");
      sb.append("start=").append(tracerDateFormat.format(span.getStartTimeMillis())).append("\n");
      sb.append("TimelineAnnotations\n");

      List<TimelineAnnotation> listAnnotation = span.getTimelineAnnotations();
      long lastTime = span.getStartTimeMillis();
      for (TimelineAnnotation ta : listAnnotation) {
        sb.append("---> ").append(tracerDateFormat.format(ta.getTime())).append(" ");
        sb.append(ta.getMessage()).append(" time from last annotation: ");
        sb.append(ta.getTime()-lastTime).append(" ms\n");
        lastTime = ta.getTime();
      }
      sb.append("---> ").append(tracerDateFormat.format(span.getStopTimeMillis()));
      sb.append(" time from last annotation: ");
      sb.append(span.getStopTimeMillis()-lastTime).append(" ms\n");
      tracerLog.info(sb.toString());
    }
  }

  // Use only the position of startScope is not in same scope with closeScope.
  public static void startScopeThreadLocal(String op, String info) {
    TraceScope scope = startScope(op, info);
    threadLocalScope.set(scope);
  }

  public static void closeScopeThreadLocal() {
    TraceScope scope = threadLocalScope.get();
    closeScope(scope, TracerWarnTimeType.normal);
  }

  public static void closeScopeThreadLocal(TracerWarnTimeType warnTimeType) {
    TraceScope scope = threadLocalScope.get();
    closeScope(scope, warnTimeType);
  }
}
