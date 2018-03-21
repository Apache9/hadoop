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

import org.apache.hadoop.conf.Configuration;
import org.htrace.Trace;

public class TracerMgr {
  public static int     MAX_ANNOTATION_COUNT = 100;
  public static String  TAG_MORE_LOGS = "more logs ......";

  private static boolean clientEnabled  = false;

  public static void initClient(Configuration conf) {
    clientEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_CLIENT_ENABLE_TRACE_LOG,
      DFSConfigKeys.DFS_CLIENT_ENABLE_TRACE_LOG_DEFAULT);
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
}
