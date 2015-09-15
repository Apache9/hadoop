/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.tracing;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.TimelineAnnotation;

import com.google.common.util.concurrent.RateLimiter;

public class LogSpanReciever implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(LogSpanReciever.class);
  protected static final Log TRACELOG = LogFactory.getLog("HTrace");

  public static final String TRACE_SPAN_WARN_TIME = "trace.span.warn.time";
  public static final int DEFAULT_TRACE_SPAN_WARN_TIME = 50;
  private static final String TRACE_LOG_REQUEST_COUNT_MAX = "trace.log.rate.max";

  private long warnTime;
  private final RateLimiter limiter;
  private long ignoredSpan;
  private DateFormat df;
  private Date date;

  public LogSpanReciever(HTraceConfiguration conf) {
    this.warnTime = conf.getInt(TRACE_SPAN_WARN_TIME, DEFAULT_TRACE_SPAN_WARN_TIME);
    int maxTraceLogCountPerSeccond = conf.getInt(TRACE_LOG_REQUEST_COUNT_MAX, Integer.MAX_VALUE);
    LOG.info("configure LogSpanReciever, warnTIme=" + warnTime
        + " (ms), maxTraceLogCountPerSeccond=" + maxTraceLogCountPerSeccond);
    this.limiter = RateLimiter.create(maxTraceLogCountPerSeccond);
    this.ignoredSpan = 0;
    this.df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    this.date = new Date();
  }

  protected String toLogString(Span span) {
    StringBuffer buf = new StringBuffer();

    buf.append("TraceId=").append(span.getTraceId()).append(", description=")
        .append(span.getDescription()).append(", consume(ms)=").append(span.getAccumulatedMillis())
        .append(", spanId=").append(span.getSpanId()).append(", parentId=")
        .append(span.getParentId()).append(", start=")
        .append(formatTime(span.getStartTimeMillis())).append("\n");

    if (span.getKVAnnotations() != null && span.getKVAnnotations().size() > 0) {
      buf.append("KVAnnotations\n");
      for (Map.Entry<byte[], byte[]> entry : span.getKVAnnotations().entrySet()) {
        buf.append("---> ").append(new String(entry.getKey())).append("=")
            .append(new String(entry.getValue())).append("\n");
      }
    }

    if (span.getTimelineAnnotations() != null && span.getTimelineAnnotations().size() > 0) {
      buf.append("TimelineAnnotations \n");
      long last = span.getStartTimeMillis();
      for (TimelineAnnotation annotation : span.getTimelineAnnotations()) {
        buf.append("---> ").append(formatTime(annotation.getTime())).append(" ")
            .append(annotation.getMessage()).append(" , time from last annotation: ")
            .append(annotation.getTime() - last).append(" ms\n");
        last = annotation.getTime();
      }
      buf.append("---> ").append(formatTime(span.getStopTimeMillis()))
          .append(" time from last annotation: ").append(span.getStopTimeMillis() - last)
          .append(" ms\n");
    }
    return buf.toString();
  }

  private String formatTime(long time) {
    date.setTime(time);
    return df.format(date);
  }

  @Override
  public void receiveSpan(Span span) {
    long duration = span.getAccumulatedMillis();
    if (duration >= warnTime) {
      if (limiter.tryAcquire(0, TimeUnit.MILLISECONDS)) {
        TRACELOG.info(toLogString(span));
      } else {
        ignoredSpan++;
        if (ignoredSpan > 100) {
          TRACELOG.info("More than 100 spans are ignored to be logged for by config: "
              + TRACE_LOG_REQUEST_COUNT_MAX);
          ignoredSpan = 0;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
  }
}
