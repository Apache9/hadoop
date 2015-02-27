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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

/**
 * Policy defines how a task will be executed, and how the result and failure
 * will be processed.
 */
public abstract class Policy<R> extends Configured {

  public enum Type {
    ONE_SHOT,
    PERIOD,
  }

  /** The switch of whether the policy is enabled or not */
  protected final AtomicBoolean enabled = new AtomicBoolean(true);

  protected final FileSystem fs;
  protected long delayMs = 0;
  protected long periodMs = 0;

  protected Policy(Configuration conf) throws IOException {
    this.setConf(conf);
    this.fs = FileSystem.get(conf);
  }

  /**
   * Gets the name of the policy.
   */
  public abstract String getName();

  /**
   * Checks whether the policy is enabled or not.
   */
  public boolean isEnabled() {
    return enabled.get();
  }

  /**
   * Disables the policy.
   */
  public void disable() {
    enabled.set(false);
  }

  /**
   * Enables the policy.
   */
  public void enable() {
    enabled.set(true);
  }

  /**
   * Gets the policy type{@link Type}.
   */
  public abstract Type getType();

  /**
   * Gets the delay time of the policy in milliseconds.
   */
  public long getDelayMs() {
    return delayMs;
  }

  /**
   * Gets the period of the policy in milliseconds.
   *
   * @return If the policy is scheduled periodically return the period,
   *         otherwise, return 0.
   */
  public long getPeriodMs() {
    return periodMs;
  }

  /**
   * Callback function when the policy task starts to execute.
   * @return The execution result
   */
  public abstract R onStart() throws IOException;

  /**
   * Callback function when the policy task is successfully executed.
   * @param result The task's result
   */
  public abstract void onSuccess(R result);

  /**
   * Callback function when the policy task is failed.
   * @param t The exception of the failure
   */
  public abstract void onFailure(Throwable t);
}
