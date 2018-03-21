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
import org.htrace.HTraceConfiguration;
import org.htrace.Span;
import org.htrace.SpanReceiver;
import org.htrace.TimelineAnnotation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

public class TracerLog implements SpanReceiver {
  private static final Log tracerLog = LogFactory.getLog("HTrace");
  private static DateFormat tracerDateFormat =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private static long  warnTimeNormal = 0;

  public void configure(HTraceConfiguration conf) {
    warnTimeNormal = conf.getInt(DFSConfigKeys.TRACE_WARN_TIME_KEY,
      DFSConfigKeys.TRACE_WARN_TIME_DEFAULT);
    StringBuilder builder = new StringBuilder();
    builder.append(" Configuration:\n");
    builder.append(DFSConfigKeys.TRACE_WARN_TIME_KEY).append("=").append(warnTimeNormal);
    builder.append("\n");
    tracerLog.info(builder.toString());
  }

  private String formatSpan(Span span) {
    StringBuilder sb = new StringBuilder();
    sb.append("TraceID:").append(span.getTraceId());
    sb.append(" SpanID:").append(span.getSpanId());
    sb.append(" ParentID:").append(span.getParentId());
    sb.append(" Description:").append(span.getDescription());
    sb.append("TimelineAnnotations\n");
    sb.append("---> ").append(tracerDateFormat.format(span.getStartTimeMillis()));
    sb.append(" start \n");
    List<TimelineAnnotation> listAnnotation = span.getTimelineAnnotations();
    long lastTime = span.getStartTimeMillis();
    for (TimelineAnnotation ta : listAnnotation) {
      sb.append("---> ").append(tracerDateFormat.format(ta.getTime())).append(" ");
      sb.append(ta.getMessage()).append(" time from last annotation: ");
      sb.append(ta.getTime() - lastTime).append(" ms\n");
      lastTime = ta.getTime();
    }
    sb.append("---> ").append(tracerDateFormat.format(span.getStopTimeMillis()));
    sb.append(" time from last annotation: ");
    sb.append(span.getStopTimeMillis() - lastTime).append(" ms\n");
    return sb.toString();
  }

  public void receiveSpan(Span span) {
    long duration = span.getAccumulatedMillis();
    if (duration  > warnTimeNormal) {
      tracerLog.info(formatSpan(span));
    }
  }

  public void close() {}
}
