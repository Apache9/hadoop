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

package org.apache.hadoop.mapreduce.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MRJobConfig;

public class HeapUsageUtils {
  private static final Log LOG = LogFactory.getLog(HeapUsageUtils.class.getName());
  public static String HeapUsageCounterGroup = "HeapUsageGroup";
  public static String HeapUsageCounterName = "HeapUsageCounter";

  static public void sampleHeapUsage(Configuration conf,
      Counter counter) {
    if (conf.getBoolean(MRJobConfig.HEAP_SAMPLE_ENABLE,
        MRJobConfig.DEFAULT_HEAP_SAMPLE_ENABLE)) {
      System.gc();
      long heapUsageBytes = Runtime.getRuntime().totalMemory()
          - Runtime.getRuntime().freeMemory();
      if (LOG.isDebugEnabled()) {
        LOG.debug("previous heap: " + counter.getValue());
      }
      if (heapUsageBytes > counter.getValue()) {
        counter.setValue(heapUsageBytes);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("current heap: " + counter.getValue());
      }
    }
  }

  static public void sampleHeapUsage(Configuration conf,
      Counter counter, boolean enable) {
    if (enable) {
      sampleHeapUsage(conf, counter);
    }
  }
}
