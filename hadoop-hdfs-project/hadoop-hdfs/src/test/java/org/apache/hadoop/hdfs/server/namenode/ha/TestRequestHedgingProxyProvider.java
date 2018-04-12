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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.ProxyFactory;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestRequestHedgingProxyProvider {

  private Configuration conf;
  private URI nnUri;
  private String ns;

  @BeforeClass
  public static void setupClass() throws Exception {
    //GenericTestUtils.setLogLevel(RequestHedgingProxyProvider.LOG, Level.TRACE);
  }

  @Before
  public void setup() throws URISyntaxException {
    ns = "mycluster-" + Time.monotonicNow();
    nnUri = new URI("hdfs://" + ns);
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, ns);
    conf.set(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns, "nn1,nn2");
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn1",
        "machine1.foo.bar:8020");
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn2",
        "machine2.foo.bar:8020");
  }

  @Test
  public void testHedgingWhenOneSuccess() throws Exception {
    final AtomicInteger count = new AtomicInteger(0);
    // good Mock
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        count.incrementAndGet();
        Thread.sleep(1000);// sleep so bad mock could be called.
        return new long[]{1};
      }
    });
    // bad Mock
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        count.incrementAndGet();
        throw new IOException("Bad Mock! This is Standby!");
      }
    });

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock,goodMock,goodMock,badMock));
    NamenodeProtocols proxy = provider.getProxy().proxy;
    long[] stats = proxy.getStats();
    assertTrue(count.get()==2);
    proxy.getStats();
    assertTrue(count.get()==3);
  }

  @Test
  public void testExceptionInfo() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      boolean first = true;
      @Override
      public long[] answer(InvocationOnMock invocation)
          throws Throwable {
        if (first) {
          Thread.sleep(1000);// sleep so bad mock could be called.
          first = false;
          return new long[] { 1 };
        } else {
          throw new IOException("Expected Exception Info");
        }
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation)
          throws Throwable {
        throw new IOException("Bad Mock! This is Standby!");
      }
    });

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock, goodMock));
    NamenodeProtocols proxy = provider.getProxy().proxy;
    proxy.getStats();
    try {
      proxy.getStats();
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
      assertEquals("Expected Exception Info", e.getMessage());
    }
  }

  @Test
  public void testHedgingWhenOneFails() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000);
        return new long[]{1};
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock, goodMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenOneIsSlow() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenReturn(new long[] {1});
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(10000);
        return new long[]{2};
      }
    });

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenBothFail() throws Exception {
    NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));
    NamenodeProtocols worseMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(worseMock.getStats()).thenThrow(
            new IOException("Worse mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock, worseMock));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since both namenodes throw IOException !!");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MultiException);
    }
    Mockito.verify(badMock).getStats();
    Mockito.verify(worseMock).getStats();
  }

  @Test
  public void testHedgingWhenFileNotFoundException() throws Exception {
    NamenodeProtocols active = Mockito.mock(NamenodeProtocols.class);
    Mockito
        .when(active.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(new RemoteException("java.io.FileNotFoundException",
            "File does not exist!"));

    NamenodeProtocols standby = Mockito.mock(NamenodeProtocols.class);
    Mockito
        .when(standby.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(
            new RemoteException("org.apache.hadoop.ipc.StandbyException",
            "Standby NameNode"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
            NamenodeProtocols.class, createFactory(active, standby));
    try {
      provider.getProxy().proxy.getBlockLocations("/tmp/test.file", 0L, 20L);
      Assert.fail("Should fail since the active namenode throws"
          + " FileNotFoundException!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        Exception rEx = ((RemoteException) ex).unwrapRemoteException();
        if (rEx instanceof StandbyException) {
          continue;
        }
        Assert.assertTrue(rEx instanceof FileNotFoundException);
      }
    }
    Mockito.verify(active).getBlockLocations(Matchers.anyString(),
        Matchers.anyLong(), Matchers.anyLong());
    Mockito.verify(standby).getBlockLocations(Matchers.anyString(),
        Matchers.anyLong(), Matchers.anyLong());
  }

  @Test
  public void testHedgingWhenConnectException() throws Exception {
    NamenodeProtocols active = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(active.getStats()).thenThrow(new ConnectException());

    NamenodeProtocols standby = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(standby.getStats())
        .thenThrow(
            new RemoteException("org.apache.hadoop.ipc.StandbyException",
            "Standby NameNode"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
            NamenodeProtocols.class, createFactory(active, standby));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since the active namenode throws"
          + " ConnectException!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        if (ex instanceof RemoteException) {
          Exception rEx = ((RemoteException) ex)
              .unwrapRemoteException();
          Assert.assertTrue("Unexpected RemoteException: " + rEx.getMessage(),
              rEx instanceof StandbyException);
        } else {
          Assert.assertTrue(ex instanceof ConnectException);
        }
      }
    }
    Mockito.verify(active).getStats();
    Mockito.verify(standby).getStats();
  }

  @Test
  public void testHedgingWhenConnectAndEOFException() throws Exception {
    NamenodeProtocols active = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(active.getStats()).thenThrow(new EOFException());

    NamenodeProtocols standby = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(standby.getStats()).thenThrow(new ConnectException());

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
            NamenodeProtocols.class, createFactory(active, standby));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since both active and standby namenodes throw"
          + " Exceptions!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        if (!(ex instanceof ConnectException) &&
            !(ex instanceof EOFException)) {
          Assert.fail("Unexpected Exception " + ex.getMessage());
        }
      }
    }
    Mockito.verify(active).getStats();
    Mockito.verify(standby).getStats();
  }

  private ProxyFactory<NamenodeProtocols> createFactory(
      NamenodeProtocols... protos) {
    final Iterator<NamenodeProtocols> iterator =
        Lists.newArrayList(protos).iterator();
    return new ProxyFactory<NamenodeProtocols>() {
      @Override
      public NamenodeProtocols createProxy(Configuration conf,
          InetSocketAddress nnAddr, Class<NamenodeProtocols> xface,
          UserGroupInformation ugi, boolean withRetries,
          AtomicBoolean fallbackToSimpleAuth) throws IOException {
        return iterator.next();
      }
    };
  }
}
