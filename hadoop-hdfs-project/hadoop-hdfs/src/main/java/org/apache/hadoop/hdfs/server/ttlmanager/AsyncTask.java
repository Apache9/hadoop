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

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.FutureCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * AsyncTask is the execution unit of the Worker{@link Worker}.
 */
public class AsyncTask<R> implements Callable<R>, FutureCallback<R> {

  private static final Log LOG = LogFactory.getLog(AsyncTask.class);

  private final Policy<R> policy;

  public AsyncTask(Policy policy) {
    this.policy = policy;
  }

  @Override
  public R call() throws Exception {
    LOG.info("Policy " + policy.getName() + " is started");
    return policy.onStart();
  }

  @Override
  public void onSuccess(R result) {
    LOG.info("Policy " + policy.getName() + " completed successfully");
    policy.onSuccess(result);
  }

  @Override
  public void onFailure(Throwable t) {
    LOG.error("Policy " + policy.getName() + " failed", t);
    policy.onFailure(t);
  }
}
