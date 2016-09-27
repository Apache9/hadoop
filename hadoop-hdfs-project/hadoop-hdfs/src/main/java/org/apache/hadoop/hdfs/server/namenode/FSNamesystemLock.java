
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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * Mimics a ReentrantReadWriteLock so more sophisticated locking capabilities
 * are possible.
 */
class FSNamesystemLock implements ReadWriteLock {
  @VisibleForTesting
  protected ReentrantReadWriteLock coarseLock;
  
  /**
   * When locking the FSNS for a read that may take a long time, we take this
   * lock before taking the regular FSNS read lock. All writers also take this
   * lock before taking the FSNS write lock. Regular (short) readers do not
   * take this lock at all, instead relying solely on the synchronization of the
   * regular FSNS lock.
   * 
   * This scheme ensures that:
   * 1) In the case of normal (fast) ops, readers proceed concurrently and
   *    writers are not starved.
   * 2) In the case of long read ops, short reads are allowed to proceed
   *    concurrently during the duration of the long read.
   * 
   * See HDFS-5064 for more context.
   */
  @VisibleForTesting
  protected final ReentrantLock longReadLock = new ReentrantLock(true);
  
  private long longReadLockStart;
  private volatile long readLockStart;
  private long writeLockStart;

  FSNamesystemLock(boolean fair) {
    this.coarseLock = new ReentrantReadWriteLock(fair);
  }
  
  @Override
  public Lock readLock() {
    return coarseLock.readLock();
  }
  
  @Override
  public Lock writeLock() {
    return coarseLock.writeLock();
  }

  public Lock longReadLock() {
    return longReadLock;
  }
  
  public int getReadHoldCount() {
    return coarseLock.getReadHoldCount();
  }
  
  public int getWriteHoldCount() {
    return coarseLock.getWriteHoldCount();
  }
  
  public boolean isWriteLockedByCurrentThread() {
    return coarseLock.isWriteLockedByCurrentThread();
  }

  public void markLongReadLockStartTime() {
    if (longReadLock.getHoldCount() == 1) {
      longReadLockStart = Time.now();
    }
  }
  
  public void checkAndLogLongReadLockDuration(long logThreshold, Log log) {
    if (longReadLock.getHoldCount() == 1) {
      long duration = Time.now() - longReadLockStart;
      if (duration > logThreshold) {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Long read lock is held at ")
                  .append(longReadLockStart)
                  .append(". And released after ")
                  .append(duration)
                  .append(" milliseconds. ")
                  .append("Call stack is:\n")
                  .append(StringUtils.getStackTrace(Thread.currentThread()));
        log.info(msgBuilder.toString());
      }
    }
  }
  
  /*
   * Without a lock, it is hard to get exactly beginning and ending time of the
   * read lock. Here we use volatile variable to get a value just for reference,
   * it might not be correct.
   */
  public void markReadLockStartTime() {
    if (getReadHoldCount() == 1) {
      readLockStart = Time.now();
    }
  }

  public void checkAndLogReadLockDuration(long logThreshold, Log log) {
    if (getReadHoldCount() == 1) {
      long duration = Time.now() - readLockStart;
      if (duration > logThreshold) {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Read lock is held at ")
                  .append(readLockStart)
                  .append(". And released after ")
                  .append(duration)
                  .append(" milliseconds.");
        log.info(msgBuilder.toString());
      }
    }
  }

  public void markWriteLockStartTime() {
    if (getWriteHoldCount() == 1) {
      writeLockStart = Time.now();
    }
  }

  public void checkAndLogWriteLockDuration(long logThreshold, Log log) {
    if (getWriteHoldCount() == 1) {
      long duration = Time.now() - writeLockStart;
      if (duration > logThreshold) {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Write lock is held at ")
                  .append(writeLockStart)
                  .append(". And released after ")
                  .append(duration)
                  .append(" milliseconds. ")
                  .append("Call stack is:\n")
                  .append(StringUtils.getStackTrace(Thread.currentThread()));
        log.info(msgBuilder.toString());
      }
    }
  }
}
