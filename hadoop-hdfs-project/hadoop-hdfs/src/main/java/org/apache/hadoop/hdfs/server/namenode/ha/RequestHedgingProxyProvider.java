/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcInvocationHandler;
import org.apache.hadoop.ipc.StandbyException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.retry.MultiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FailoverProxyProvider implementation that technically does not "failover"
 * per-se. It constructs a wrapper proxy that sends the request to ALL
 * underlying proxies simultaneously. It assumes the in an HA setup, there will
 * be only one Active, and the active should respond faster than any configured
 * standbys. Once it recieve a response from any one of the configred proxies,
 * outstanding requests to other proxies are immediately cancelled.
 */
public class RequestHedgingProxyProvider<T> extends
        ConfiguredFailoverProxyProvider<T> {

  public static final Logger LOG =
      LoggerFactory.getLogger(RequestHedgingProxyProvider.class);

  class RequestHedgingInvocationHandler implements RpcInvocationHandler {

    final Map<String, ProxyInfo<T>> targetProxies;

    public RequestHedgingInvocationHandler(
            Map<String, ProxyInfo<T>> targetProxies) {
      this.targetProxies = new HashMap(targetProxies);
    }

    /**
     * Creates a Executor and invokes all proxies concurrently. This
     * implementation assumes that Clients have configured proper socket
     * timeouts, else the call can block forever.
     *
     * @param proxy
     * @param method
     * @param args
     * @return
     * @throws Throwable
     */
    @Override
    public Object
    invoke(Object proxy, final Method method, final Object[] args)
            throws Throwable {
      if (currentUsedProxy != null) {
        try {
          Object retVal = method.invoke(currentUsedProxy.proxy, args);
          LOG.debug("Invocation successful on [{}]",
              currentUsedProxy.proxyInfo);
          return retVal;
        } catch (InvocationTargetException ex) {
          Exception unwrappedException = unwrapInvocationTargetException(ex);
          logProxyException(unwrappedException, currentUsedProxy.proxyInfo);
          LOG.trace("Unsuccessful invocation on [{}]",
              currentUsedProxy.proxyInfo);
          throw unwrappedException;
        }
      }
      if (targetProxies.isEmpty()) {
        Client.clearCallId();
        if (tolerateNoNn) {
          // will trigger failover
          throw new UnknownHostException("no namenode address configured");
        } else {
          throw new RuntimeException("Could not find any configured addresses "
              + "for HedgingProxyProvider");
        }
      }
      Map<Future<Object>, ProxyInfo<T>> proxyMap = new HashMap<Future<Object>, ProxyInfo<T>>();
      int numAttempts = 0;

      ExecutorService executor = null;
      CompletionService<Object> completionService;
      try {
        final Integer cid = Client.getCallId();
        final Integer rc = Client.getRetryCount();
        executor = Executors.newFixedThreadPool(targetProxies.size());
        completionService = new ExecutorCompletionService<Object>(executor);
        for (final Map.Entry<String, ProxyInfo<T>> pEntry :
                targetProxies.entrySet()) {
          Callable<Object> c = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              if (cid != null && rc != null) {
                Client.setCallIdAndRetryCount(cid, rc);
              }
              LOG.trace("Invoking method {} on proxy {}", method,
                  pEntry.getValue().proxyInfo);
              return method.invoke(pEntry.getValue().proxy, args);
            }
          };
          proxyMap.put(completionService.submit(c), pEntry.getValue());
          numAttempts++;
        }

        Map<String, Exception> badResults = new HashMap<String, Exception>();
        while (numAttempts > 0) {
          Future<Object> callResultFuture = completionService.take();
          Object retVal = null;
          try {
            retVal = callResultFuture.get();
            currentUsedProxy = proxyMap.get(callResultFuture);
            LOG.debug("Invocation successful on [{}]",
                currentUsedProxy.proxyInfo);
            return retVal;
          } catch (ExecutionException ex) {
            Exception unwrappedException = unwrapExecutionException(ex);
            ProxyInfo<T> tProxyInfo = proxyMap.get(callResultFuture);
            logProxyException(unwrappedException, tProxyInfo.proxyInfo);
            badResults.put(tProxyInfo.proxyInfo, unwrappedException);
            LOG.trace("Unsuccessful invocation on [{}]", tProxyInfo.proxyInfo);
            numAttempts--;
          }
        }

        // At this point we should have All bad results (Exceptions)
        // Or should have returned with successful result.
        if (badResults.size() == 1) {
          throw badResults.values().iterator().next();
        } else {
          throw new MultiException(badResults);
        }
      } finally {
        if (executor != null) {
          LOG.trace("Shutting down threadpool executor");
          executor.shutdownNow();
        }
        Client.clearCallId();
      }
    }

    @Override
    public Client.ConnectionId getConnectionId() {
      if (currentUsedProxy == null) {
        return null;
      } else {
        return RPC.getConnectionIdForProxy(currentUsedProxy.proxy);
      }
    }

    @Override
    public void close() throws IOException {
      for (Map.Entry<String, ProxyInfo<T>> pEntry : targetProxies.entrySet()) {
        T proxy = pEntry.getValue().proxy;
        RPC.stopProxy(proxy);
      }
    }
  }


  private volatile ProxyInfo<T> currentUsedProxy = null;

  public RequestHedgingProxyProvider(
          Configuration conf, URI uri, Class<T> xface) {
    this(conf, uri, xface, new DefaultProxyFactory<T>());
  }

  @VisibleForTesting
  RequestHedgingProxyProvider(Configuration conf, URI uri,
                              Class<T> xface, ProxyFactory<T> factory) {
    super(conf, uri, xface, factory);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (currentUsedProxy != null) {
      return currentUsedProxy;
    }
    Map<String, ProxyInfo<T>> targetProxyInfos = new HashMap<String, ProxyInfo<T>>();
    StringBuilder combinedInfo = new StringBuilder("[");
    for (int i = 0; i < proxies.size(); i++) {
      ProxyInfo<T> pInfo = super.getProxy();
      incrementProxyIndex();
      targetProxyInfos.put(pInfo.proxyInfo, pInfo);
      combinedInfo.append(pInfo.proxyInfo).append(',');
    }
    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
        RequestHedgingInvocationHandler.class.getClassLoader(),
            new Class<?>[]{xface},
            new RequestHedgingInvocationHandler(targetProxyInfos));
    return new ProxyInfo<T>(wrappedProxy, combinedInfo.toString());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    currentUsedProxy = null;
  }

  /**
   * Check the exception returned by the proxy log a warning message if it's
   * not a StandbyException (expected exception).
   * @param ex Exception to evaluate.
   * @param proxyInfo Information of the proxy reporting the exception.
   */
  private void logProxyException(Exception ex, String proxyInfo) {
    if (isStandbyException(ex)) {
      LOG.debug("Invocation returned standby exception on [{}]", proxyInfo, ex);
    } else {
      LOG.warn("Invocation returned exception on [{}]", proxyInfo, ex);
    }
  }

  /**
   * Check if the returned exception is caused by an standby namenode.
   * @param exception Exception to check.
   * @return If the exception is caused by an standby namenode.
   */
  private boolean isStandbyException(Exception exception) {
    if (exception instanceof RemoteException) {
      return ((RemoteException) exception).unwrapRemoteException()
          instanceof StandbyException;
    }
    return false;
  }

  /**
   * Unwraps the ExecutionException. <p>
   * Example:
   * <blockquote><pre>
   * if ex is
   * ExecutionException(InvocationTargetException(SomeException))
   * returns SomeException
   * </pre></blockquote>
   *
   * @return unwrapped exception
   */
  private Exception unwrapExecutionException(ExecutionException ex) {
    if (ex != null) {
      Throwable cause = ex.getCause();
      if (cause instanceof InvocationTargetException) {
        return
            unwrapInvocationTargetException((InvocationTargetException)cause);
      }
    }
    return ex;

  }

  /**
   * Unwraps the InvocationTargetException. <p>
   * Example:
   * <blockquote><pre>
   * if ex is InvocationTargetException(SomeException)
   * returns SomeException
   * </pre></blockquote>
   *
   * @return unwrapped exception
   */
  private Exception unwrapInvocationTargetException(
      InvocationTargetException ex) {
    if (ex != null) {
      Throwable cause = ex.getCause();
      if (cause instanceof Exception) {
        return (Exception) cause;
      }
    }
    return ex;
  }
}
