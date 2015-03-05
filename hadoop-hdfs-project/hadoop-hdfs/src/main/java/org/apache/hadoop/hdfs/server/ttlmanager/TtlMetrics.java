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

package org.apache.hadoop.hdfs.server.ttlmanager;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * The TtlManager metrics.
 * 
 */
@Metrics(about="TtlManager metrics", context="dfs")
class TtlMetrics {
  final MetricsRegistry registry = new MetricsRegistry("TtlManager");
  
  @Metric("Number of TtlManager task scheduled since startup")
  MutableCounterLong ttlScheduledTask;
  
  @Metric("Number of failed TtlManager task since startup")
  MutableCounterLong ttlFailedTask;
  
  @Metric("TTL task duration")
  MutableStat ttlDurationInMs;
    
  @Metric("Number of files being deleted by ttl")
  MutableCounterLong filesDeletedByTTL;
  
  @Metric("Files scanned by TTL")
  MutableCounterLong filesScannedByTTL;
  
  TtlMetrics() {
  }
  
  public static TtlMetrics create() {
    TtlMetrics tmm = new TtlMetrics();
    return DefaultMetricsSystem.instance().register(tmm.getName(), null, tmm);
  }
  
  String getName() {
    return "Ttl";
  }
  
  public void shutDown() {
    DefaultMetricsSystem.shutdown();
  }
 
 public void incrTtlScheduledTask() {
   ttlScheduledTask.incr();
 }
 
 public void incrTtlFailedTask() {
   ttlFailedTask.incr();
 }
 
 public void addTtlDurationInMs(long duration) {
   ttlDurationInMs.add(duration);
 }
  
 public void incrFilesDeletedByTTL() {
   filesDeletedByTTL.incr();
 }
 
 public void incrFilesScannedByTTL() {
   filesScannedByTTL.incr();
 }
}
