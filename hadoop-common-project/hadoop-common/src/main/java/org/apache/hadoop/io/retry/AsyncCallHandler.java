/*
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
package org.apache.hadoop.io.retry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handle async calls. */
@InterfaceAudience.Private
public class AsyncCallHandler {
  public static final Logger LOG = LoggerFactory.getLogger(
      AsyncCallHandler.class);

  private static final ThreadLocal<CompletableFuture<?>>
      LOWER_LAYER_ASYNC_RETURN = new ThreadLocal<>();
  private static final ThreadLocal<CompletableFuture<?>>
      ASYNC_RETURN = new ThreadLocal<>();

  /** @return the async return value from {@link AsyncCallHandler}. */
  @InterfaceStability.Unstable
  @SuppressWarnings("unchecked")
  public static <R> CompletableFuture<R> getAsyncReturn() {
    final CompletableFuture<R> ret = (CompletableFuture<R>) ASYNC_RETURN.get();
    ASYNC_RETURN.set(null);
    return ret;
  }

  /** For the lower rpc layers to set the async return value. */
  @InterfaceStability.Unstable
  public static void setLowerLayerAsyncReturn(CompletableFuture<?> asyncReturn) {
    LOWER_LAYER_ASYNC_RETURN.set(asyncReturn);
  }

  private static CompletableFuture<?> getLowerLayerAsyncReturn() {
    final CompletableFuture<?> ret = LOWER_LAYER_ASYNC_RETURN.get();
    Preconditions.checkNotNull(ret);
    LOWER_LAYER_ASYNC_RETURN.set(null);
    return ret;
  }

  /** A simple concurrent queue which keeping track the empty start time. */
  static class ConcurrentQueue<T> {
    private final Queue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicLong emptyStartTime
        = new AtomicLong(Time.monotonicNow());

    Iterator<T> iterator() {
      return queue.iterator();
    }

    /** Is the queue empty for more than the given time in millisecond? */
    boolean isEmpty(long time) {
      return Time.monotonicNow() - emptyStartTime.get() > time
          && queue.isEmpty();
    }

    void offer(T c) {
      final boolean added = queue.offer(c);
      Preconditions.checkState(added);
    }

    void checkEmpty() {
      if (queue.isEmpty()) {
        emptyStartTime.set(Time.monotonicNow());
      }
    }
  }

  /** A queue for handling async calls. */
  class AsyncCallQueue {
    private final ConcurrentQueue<AsyncCall> queue = new ConcurrentQueue<>();
    private final Processor processor = new Processor();

    void addCall(AsyncCall call) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("add " + call);
      }
      queue.offer(call);
      processor.tryStart();
    }

    long checkCalls() {
      final long startTime = Time.monotonicNow();
      long minWaitTime = Processor.MAX_WAIT_PERIOD;

      for (final Iterator<AsyncCall> i = queue.iterator(); i.hasNext();) {
        final AsyncCall c = i.next();
        if (c.isDone()) {
          i.remove(); // the call is done, remove it from the queue.
          queue.checkEmpty();
        } else {
          final Long waitTime = c.getWaitTime(startTime);
          if (waitTime != null && waitTime > 0 && waitTime < minWaitTime) {
            minWaitTime = waitTime;
          }
        }
      }
      return minWaitTime;
    }

    /** Process the async calls in the queue. */
    private class Processor {
      static final long GRACE_PERIOD = 3*1000L;
      static final long MAX_WAIT_PERIOD = 100L;

      private final AtomicReference<Thread> running = new AtomicReference<>();

      boolean isRunning(Daemon d) {
        return d == running.get();
      }

      void tryStart() {
        final Thread current = Thread.currentThread();
        if (running.compareAndSet(null, current)) {
          final Daemon daemon = new Daemon() {
            @Override
            public void run() {
              for (; isRunning(this);) {
                final long waitTime = checkCalls();
                tryStop(this);

                try {
                  synchronized (AsyncCallHandler.this) {
                    AsyncCallHandler.this.wait(waitTime);
                  }
                } catch (InterruptedException e) {
                  kill(this);
                }
              }
            }
          };

          final boolean set = running.compareAndSet(current, daemon);
          Preconditions.checkState(set);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting AsyncCallQueue.Processor " + daemon);
          }
          daemon.start();
        }
      }

      void tryStop(Daemon d) {
        if (queue.isEmpty(GRACE_PERIOD)) {
          kill(d);
        }
      }

      void kill(Daemon d) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Killing " + d);
        }
        final boolean set = running.compareAndSet(d, null);
        Preconditions.checkState(set);
      }
    }
  }

  static class AsyncCall extends RetryInvocationHandler.Call {
    private final AsyncCallHandler asyncCallHandler;
    
    private CompletableFuture<?> lowerLayerAsyncReturn;

    AsyncCall(Method method, Object[] args, boolean isRpc, int callId,
              RetryInvocationHandler<?> retryInvocationHandler,
              AsyncCallHandler asyncCallHandler) {
      super(method, args, isRpc, callId, retryInvocationHandler);

      this.asyncCallHandler = asyncCallHandler;
    }

    /** @return true if the call is done; otherwise, return false. */
    boolean isDone() {
      final CallReturn r = invokeOnce();
      LOG.debug("#{}: {}", getCallId(), r.getState());
      switch (r.getState()) {
        case RETURNED:
        case EXCEPTION:
          return true;
        case RETRY:
          invokeOnce();
          break;
        case WAIT_RETRY:
        case ASYNC_CALL_IN_PROGRESS:
        case ASYNC_INVOKED:
          // nothing to do
          break;
        default:
          throw new IllegalStateException("Unknown state: " + r.getState());
      }
      return false;
    }

    @Override
    CallReturn processWaitTimeAndRetryInfo() {
      final Long waitTime = getWaitTime(Time.monotonicNow());
      LOG.trace("#{} processRetryInfo: waitTime={}", getCallId(), waitTime);
      if (waitTime != null && waitTime > 0) {
        return CallReturn.WAIT_RETRY;
      }
      processRetryInfo();
      return CallReturn.RETRY;
    }

    @Override
    CallReturn invoke() throws Throwable {
      LOG.debug("{}.invoke {}", getClass().getSimpleName(), this);
      if (lowerLayerAsyncReturn != null) {
        // async call was submitted early, check the lower level async call
        final boolean isDone = lowerLayerAsyncReturn.isDone();
        LOG.trace("#{} invoke: lowerLayerAsyncGet.isDone()? {}",
            getCallId(), isDone);
        if (!isDone) {
          return CallReturn.ASYNC_CALL_IN_PROGRESS;
        }
        try {
          return new CallReturn(lowerLayerAsyncReturn.getNow(null));
        } finally {
          lowerLayerAsyncReturn = null;
        }
      }

      // submit a new async call
      LOG.trace("#{} invoke: ASYNC_INVOKED", getCallId());
      final boolean mode = Client.isAsynchronousMode();
      try {
        Client.setAsynchronousMode(true);
        final Object r = invokeMethod();
        // invokeMethod should set LOWER_LAYER_ASYNC_RETURN and return null.
        Preconditions.checkState(r == null);
        lowerLayerAsyncReturn = getLowerLayerAsyncReturn();

        if (getCounters().isZeros()) {
          // first async attempt, initialize
          LOG.trace("#{} invoke: initAsyncCall", getCallId());
          asyncCallHandler.initAsyncCall(this, lowerLayerAsyncReturn);
        }
        return CallReturn.ASYNC_INVOKED;
      } finally {
        Client.setAsynchronousMode(mode);
      }
    }
  }

  private final AsyncCallQueue asyncCalls = new AsyncCallQueue();
  private volatile boolean hasSuccessfulCall = false;

  AsyncCall newAsyncCall(Method method, Object[] args, boolean isRpc,
                         int callId,
                         RetryInvocationHandler<?> retryInvocationHandler) {
    return new AsyncCall(method, args, isRpc, callId,
        retryInvocationHandler, this);
  }

  boolean hasSuccessfulCall() {
    return hasSuccessfulCall;
  }

  private void initAsyncCall(final AsyncCall asyncCall,
      CompletableFuture<?> lowerLayerAsyncReturn) {
    asyncCalls.addCall(asyncCall);
    lowerLayerAsyncReturn.thenRun(() -> hasSuccessfulCall = true);
    ASYNC_RETURN.set(lowerLayerAsyncReturn);
  }

  @VisibleForTesting
  public static long getGracePeriod() {
    return AsyncCallQueue.Processor.GRACE_PERIOD;
  }
}
