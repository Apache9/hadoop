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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestTtlManager {

  private static class TaskResult {
    private String message;

    public TaskResult(String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }
  }

  private static class OneShotPolicy extends Policy<TaskResult> {

    private int successCount = 0;

    public OneShotPolicy(Configuration conf) throws IOException {
      super(conf);
    }

    public int getSuccessCount() {
      return successCount;
    }

    @Override
    public String getName() {
      return getType().name();
    }

    @Override
    public Type getType() {
      return Type.ONE_SHOT;
    }

    @Override
    public TaskResult onStart() throws IOException {
      return new TaskResult("OK");
    }

    @Override
    public void onSuccess(TaskResult result) {
      ++successCount;
      System.out.println(result.getMessage());

      // Enable the policy as needed
      if (successCount < 5) {
        enable();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      System.err.println(t.getMessage());
    }
  }

  private static class PeriodPolicy extends Policy<TaskResult> {

    private int successCount = 0;
    private long periodMs = 0;

    public PeriodPolicy(Configuration conf, long periodMs) throws IOException {
      super(conf);
      this.periodMs = periodMs;
    }

    @Override
    public long getPeriodMs() {
      return periodMs;
    }

    public int getSuccessCount() {
      return successCount;
    }

    @Override
    public String getName() {
      return getType().name();
    }

    @Override
    public Type getType() {
      return Type.PERIOD;
    }

    @Override
    public TaskResult onStart() throws IOException {
      return new TaskResult("OK");
    }

    @Override
    public void onSuccess(TaskResult result) {
      ++successCount;
      System.out.println(result.getMessage());
    }

    @Override
    public void onFailure(Throwable t) {
      System.err.println(t.getMessage());
    }
  }

  @Test
  public void testTtlManager() throws Exception {
    Configuration conf = new Configuration();
    TtlManager ttlManager = new TtlManager(conf);
    OneShotPolicy policy1 = new OneShotPolicy(conf);
    PeriodPolicy policy2 = new PeriodPolicy(conf, 1000);
    ttlManager.registerPolicy(policy1);
    ttlManager.registerPolicy(policy2);
    ttlManager.start();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
    }
    ttlManager.interrupt();
    ttlManager.join();

    Assert.assertEquals(5, policy1.getSuccessCount());
    Assert.assertTrue(policy2.getSuccessCount() > 1);
  }
}
