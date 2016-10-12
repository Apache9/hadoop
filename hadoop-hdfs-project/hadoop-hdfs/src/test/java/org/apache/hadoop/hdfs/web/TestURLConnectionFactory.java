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
package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.*;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;


public final class TestURLConnectionFactory {

  public static final Log LOG = LogFactory.getLog(TestURLConnectionFactory.class);
  @Test
  public void testConnConfiguratior() throws IOException, AuthenticationException {
    final URL u = new URL("http://localhost");
    final List<HttpURLConnection> conns = Lists.newArrayList();
    URLConnectionFactory fc = new URLConnectionFactory(new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection conn)
          throws IOException {
        Assert.assertEquals(u, conn.getURL());
        conns.add(conn);
        return conn;
      }
    });

    fc.openConnection(u);
    Assert.assertEquals(1, conns.size());
  }

  public static final int CONNECT_TIMES = 1000;
  private static class OpenConnectionThread extends Thread {
    private URL mURL;
    private URLConnectionFactory mUCF;
    public static int sSuccessCount = 0;
    public OpenConnectionThread(Configuration conf, URL url) {
      mUCF = URLConnectionFactory.newDefaultURLConnectionFactory(conf);
      mURL = url;
    }
    public void run() {
      for (int i = 0; i < CONNECT_TIMES; ++i) {
        try {
          mUCF.openConnection(mURL, true);
          ++sSuccessCount;
        }
        catch(AuthenticationException e){
          LOG.info("UnExpected - AuthenticationException caught", e);
        } catch(ConnectException e){
          //Do nothing here to speed up execution
        } catch(IOException e){
          LOG.info("UnExpected - IOException caught", e);
        }
      }
    }
  }

  @Test
  public void testForIssueHDFS7798()
      throws IOException, AuthenticationException, URISyntaxException, InterruptedException {

    //URL1 with which the openConnection() will succeed
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    URI uri = new URI(cluster.getHttpUri(0));

    //URL2 with which the openConnection() will raise java.net.ConnectException
    URL url = new URL("http://localhost");
    OpenConnectionThread t1 = new OpenConnectionThread(conf, uri.toURL());
    OpenConnectionThread t2 = new OpenConnectionThread(conf, url);
    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Assert.assertEquals(OpenConnectionThread.sSuccessCount, CONNECT_TIMES);
  }
}

