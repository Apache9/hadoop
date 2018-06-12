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
package org.apache.hadoop.ha;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthMonitor.Callback;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.net.StandardSocketFactory;
import org.apache.hadoop.util.Time;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.SocketFactory;

public class TestHealthMonitor {
  private static final Log LOG = LogFactory.getLog(
      TestHealthMonitor.class);
  
  /** How many times has createProxy been called */
  private AtomicInteger createProxyCount = new AtomicInteger(0);
  private volatile boolean throwOOMEOnCreate = false;

  private HealthMonitor hm;

  private DummyHAService svc;
  
  @Before
  public void setupHM() throws InterruptedException, IOException {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    conf.setInt(CommonConfigurationKeys.HA_HM_RPC_CONNECT_MAX_RETRIES_KEY, 1);
    conf.setInt(CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY, 50);

    svc = new DummyHAService(HAServiceState.ACTIVE, null);
    hm = new HealthMonitor(conf, svc) {
      @Override
      protected HAServiceProtocol createProxy() throws IOException {
        createProxyCount.incrementAndGet();
        if (throwOOMEOnCreate) {
          throw new OutOfMemoryError("oome");
        }
        return super.createProxy();
      }
    };
    LOG.info("Starting health monitor");
    hm.start();
    
    LOG.info("Waiting for HEALTHY signal");    
    waitForState(hm, HealthMonitor.State.SERVICE_HEALTHY, 2000);
  }
  
  @Test(timeout=15000)
  public void testMonitor() throws Exception {
    LOG.info("Mocking bad health check, waiting for UNHEALTHY");
    svc.isHealthy = false;
    waitForState(hm, HealthMonitor.State.SERVICE_UNHEALTHY, 2000);
    
    LOG.info("Returning to healthy state, waiting for HEALTHY");
    svc.isHealthy = true;
    waitForState(hm, HealthMonitor.State.SERVICE_HEALTHY, 2000);

    LOG.info("Returning an IOException, as if node went down");
    // should expect many rapid retries
    int countBefore = createProxyCount.get();
    svc.actUnreachable = true;
    waitForState(hm, HealthMonitor.State.SERVICE_NOT_RESPONDING, 2000);

    // Should retry several times
    while (createProxyCount.get() < countBefore + 3) {
      Thread.sleep(10);
    }
    
    LOG.info("Returning to healthy state, waiting for HEALTHY");
    svc.actUnreachable = false;
    waitForState(hm, HealthMonitor.State.SERVICE_HEALTHY, 2000);
    
    hm.shutdown();
    hm.join();
    assertFalse(hm.isAlive());
  }

  /**
   * Test that the proper state is propagated when the health monitor
   * sees an uncaught exception in its thread.
   */
  @Test(timeout=15000)
  public void testHealthMonitorDies() throws Exception {
    LOG.info("Mocking RTE in health monitor, waiting for FAILED");
    throwOOMEOnCreate = true;
    svc.actUnreachable = true;
    waitForState(hm, HealthMonitor.State.HEALTH_MONITOR_FAILED, 2000);
    hm.shutdown();
    hm.join();
    assertFalse(hm.isAlive());
  }
  
  /**
   * Test that, if the callback throws an RTE, this will terminate the
   * health monitor and thus change its state to FAILED
   * @throws Exception
   */
  @Test(timeout=15000)
  public void testCallbackThrowsRTE() throws Exception {
    hm.addCallback(new Callback() {
      @Override
      public void enteredState(State newState) {
        throw new RuntimeException("Injected RTE");
      }
    });
    LOG.info("Mocking bad health check, waiting for UNHEALTHY");
    svc.isHealthy = false;
    waitForState(hm, HealthMonitor.State.HEALTH_MONITOR_FAILED, 2000);
  }

  private static class InjectingSocketFactory extends StandardSocketFactory {

    static final SocketFactory defaultFactory = SocketFactory.getDefault();

    static int portToInjectOn;

    static int exceptionTimes = 0;

    private void conn() {

    }

    @Override
    public Socket createSocket() throws IOException {
      Socket spy = Mockito.spy(defaultFactory.createSocket());
      // Simplify our spying job by not having to also spy on the channel
      Mockito.doReturn(null).when(spy).getChannel();
      // Throw a ConnectTimeoutException when connecting to our target "bad"
      // host.
      Mockito.doThrow(new IOException("injected " + exceptionTimes++ + " times")).when(spy)
          .connect(Mockito.argThat(new MatchesPort()), Mockito.anyInt());
      return spy;
    }

    private class MatchesPort extends BaseMatcher<SocketAddress> {
      @Override
      public boolean matches(Object arg0) {
        return ((InetSocketAddress) arg0).getPort() == portToInjectOn;
      }

      @Override
      public void describeTo(Description desc) {
        desc.appendText("matches port " + portToInjectOn);
      }
    }
  }

  private class SimpleHaService extends HAServiceTarget {
    InetSocketAddress address;

    SimpleHaService(InetSocketAddress addr) {
      address = addr;
    }

    @Override
    public InetSocketAddress getAddress() {
      return address;
    }

    @Override
    public InetSocketAddress getZKFCAddress() {
      return null;
    }

    @Override
    public NodeFencer getFencer() {
      return null;
    }

    @Override
    public void checkFencingConfigured()
        throws BadFencingConfigurationException {
    }
  }

  @Test
  public void testConnectionTimeoutRetryTimes() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_HM_RPC_CONNECT_MAX_RETRIES_KEY, 5);
    InjectingSocketFactory.portToInjectOn = 1234;
    conf.setClass(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
            InjectingSocketFactory.class, SocketFactory.class);
    SimpleHaService shs = new SimpleHaService(new InetSocketAddress("127.0.0.1", 1234));
    HealthMonitor monitor = new HealthMonitor(conf, shs);
    monitor.start();
    waitForState(monitor, State.SERVICE_NOT_RESPONDING, 10000);
    Assert.assertEquals(6, InjectingSocketFactory.exceptionTimes);
  }

  private void waitForState(HealthMonitor hm, State state, int timeout)
      throws InterruptedException {
    long st = Time.now();
    while (Time.now() - st < timeout) {
      if (hm.getHealthState() == state) {
        return;
      }
      Thread.sleep(50);
    }
    assertEquals(state, hm.getHealthState());
  }
}
