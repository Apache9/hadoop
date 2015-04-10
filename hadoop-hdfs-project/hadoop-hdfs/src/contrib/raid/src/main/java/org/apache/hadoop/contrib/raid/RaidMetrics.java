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

package org.apache.hadoop.contrib.raid;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * Raid metrics
 */
@Metrics(about = "HDFS Raid Metrics", context = "dfs")
public class RaidMetrics {
  final MetricsRegistry registry = new MetricsRegistry("HdfsRaid");

  @Metric("Number of Collector task scheduled since startup")
  MutableCounterLong collectorTaskScheduled;

  @Metric("Number of Collector task scheduled without anything to do")
  MutableCounterLong idleCollectorTaskScheduled;

  @Metric("Number of failed collector task since startup")
  MutableCounterLong collectorTaskFailed;

  @Metric("Collector task duration")
  MutableStat collectorDurationInMs;

  @Metric("Number of Coder task scheduled since startup")
  MutableCounterLong coderTaskScheduled;

  @Metric("Number of failed coder task since startup")
  MutableCounterLong coderTaskFailed;

  @Metric("Coder task duration")
  MutableStat coderDurationInMs;

  @Metric("Files scanned for coder")
  MutableCounterLong filesScannedForCoder;

  @Metric("Files coded")
  MutableCounterLong filesCoded;

  @Metric("Number of failure when coding files")
  MutableCounterLong failedCoding;

  @Metric("Bytes coded")
  MutableCounterLong bytesCoded;

  @Metric("Number of Fixer task scheduled since startup")
  MutableCounterLong fixerTaskScheduled;

  @Metric("Number of failed fixer task since startup")
  MutableCounterLong fixerTaskFailed;

  @Metric("Fixer task duration")
  MutableStat fixerDurationInMs;

  @Metric("Blocks fixed")
  MutableCounterLong blocksFixed;

  @Metric("Number of failure when fixing files")
  MutableCounterLong failedFixing;

  @Metric("Number of Mover task scheduled since startup")
  MutableCounterLong moverTaskScheduled;

  @Metric("Number of failed mover task since startup")
  MutableCounterLong moverTaskFailed;

  @Metric("Mover task duration")
  MutableStat moverDurationInMs;

  @Metric("Files scanned for mover")
  MutableCounterLong filesScannedForMover;

  @Metric("Blocks moved")
  MutableCounterLong blocksMoved;

  @Metric("Number of failure when building moving map")
  MutableCounterLong failedBuildingMovingMap;

  @Metric("Number of failure when moving blocks")
  MutableCounterLong failedMoving;

  @Metric("Number of ZombieSweeper task scheduled since startup")
  MutableCounterLong zombieSweeperTaskScheduled;

  @Metric("Number of failed zombie sweeper  task since startup")
  MutableCounterLong zombieSweeperTaskFailed;

  @Metric("ZombieSweeper task duration")
  MutableStat zombieSweeperDurationInMs;

  @Metric("Zombie files sweeped")
  MutableCounterLong zombieFilesSweeped;

  @Metric("Number of failure when deleting zombie files")
  MutableCounterLong failedSweeping;

  // TBD: Add some metrics for MapReduce jobs kicked by Raid

  RaidMetrics() {
  }

  public static RaidMetrics create() {
    RaidMetrics rm = new RaidMetrics();
    return DefaultMetricsSystem.instance().register(rm.getName(), null, rm);
  }

  String getName() {
    return "HdfsRaid";
  }

  public void shutDown() {
    DefaultMetricsSystem.shutdown();
  }

  public void incrCollectorTaskScheduled() {
    collectorTaskScheduled.incr();
  }

  public void incrIdleCollectorTaskScheduled() {
    idleCollectorTaskScheduled.incr();
  }

  public void incrCollectorTaskFailed() {
    collectorTaskFailed.incr();
  }

  public void addCollectorDurationInMs(long duration) {
    collectorDurationInMs.add(duration);
  }

  public void incrCoderTaskScheduled() {
    coderTaskScheduled.incr();
  }

  public void incrCoderTaskFailed() {
    coderTaskFailed.incr();
  }

  public void addCoderDurationInMs(long duration) {
    coderDurationInMs.add(duration);
  }

  public void incrFilesScannedForCoder(long val) {
    filesScannedForCoder.incr(val);
  }

  public void incrFilesCoded(long val) {
    filesCoded.incr(val);
  }

  public void incrFailedCoding(long val) {
    failedCoding.incr(val);
  }

  public void incrBytesCoded(long val) {
    bytesCoded.incr(val);
  }

  public void incrFixerTaskScheduled() {
    fixerTaskScheduled.incr();
  }

  public void incrFixerTaskFailed() {
    fixerTaskFailed.incr();
  }

  public void addFixerDurationInMs(long duration) {
    fixerDurationInMs.add(duration);
  }

  public void incrBlocksFixed(long val) {
    blocksFixed.incr(val);
  }

  public void incrFailedFixing(long val) {
    failedFixing.incr(val);
  }

  public void incrMoverTaskScheduled() {
    moverTaskScheduled.incr();
  }

  public void incrMoverTaskFailed() {
    moverTaskFailed.incr();
  }

  public void addMoverDurationInMs(long duration) {
    moverDurationInMs.add(duration);
  }

  public void incrFilesScannedForMover(long val) {
    filesScannedForMover.incr(val);
  }

  public void incrBlocksMoved(long val) {
    blocksMoved.incr(val);
  }

  public void incrFailedBuildingMovingMap(long val) {
    failedBuildingMovingMap.incr(val);
  }

  public void incrFailedMoving(long val) {
    failedMoving.incr(val);
  }

  public void incrZombieSweeperTaskScheduled() {
    zombieSweeperTaskScheduled.incr();
  }

  public void incrZombieSweeperTaskFailed() {
    zombieSweeperTaskFailed.incr();
  }

  public void addZombieSweeperDurationInMs(long duration) {
    zombieSweeperDurationInMs.add(duration);
  }

  public void incrZombieFilesSweeped() {
    zombieFilesSweeped.incr();
  }

  public void incrFailedSweeping() {
    failedSweeping.incr();
  }
}
