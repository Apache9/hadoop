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
package org.apache.hadoop.hdfs.protocol.datatransfer.http2;

import java.io.IOException;
import java.util.Arrays;

import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http.MetaData.Response;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;
import org.mortbay.log.Log;

public class ContinuousStreamListener extends Stream.Listener.Adapter {

  private Object gotStatus = new Object();

  private Object gotData = new Object();

  private int status = -1;

  private boolean reset = false;

  private boolean finish = false;

  private boolean data = false;

  private byte[] buf = new byte[0];

  private byte[] rBuf = new byte[0];

  @Override
  public void onReset(Stream stream, ResetFrame frame) {
    synchronized (gotStatus) {
      reset = true;
      gotStatus.notifyAll();
    }
    synchronized (gotData) {
      finish = true;
      gotData.notifyAll();
    }
  }

  @Override
  public void onHeaders(Stream stream, HeadersFrame frame) {
    synchronized (gotStatus) {
      if (reset) {
        throw new IllegalStateException("Stream already closed");
      }
      if (status != -1) {
        throw new IllegalStateException("Header already received");
      }
      MetaData meta = frame.getMetaData();
      if (!meta.isResponse()) {
        throw new IllegalStateException("Received non-response header");
      }
      status = ((Response) meta).getStatus();
      gotStatus.notifyAll();
    }
    synchronized (gotData) {
      if (frame.isEndStream()) {
        finish = true;
        Log.info("====== set finish");
        gotData.notifyAll();
      }
    }
  }

  @Override
  public void onData(Stream stream, DataFrame frame, Callback callback) {
    synchronized (gotData) {
      if (reset) {
        callback.failed(new IllegalStateException("Stream already closed"));
      }
      if (status == -1) {
        callback.failed(new IllegalStateException("Haven't received header yet"));
      }
      int bufLen = buf.length;
      int newBufLen = bufLen + frame.getData().remaining();
      buf = Arrays.copyOf(buf, newBufLen);
      frame.getData().get(buf, bufLen, frame.getData().remaining());
      data = true;
      if (frame.isEndStream()) {
        finish = true;
        Log.info("====== set finish");
      }
      gotData.notifyAll();
      callback.succeeded();
    }
  }

  public int getStatus() throws InterruptedException, IOException {
    synchronized (gotStatus) {
      while (status == -1) {
        gotStatus.wait();
      }
      if (reset) {
        throw new IOException("stream reset");
      }
      return status;
    }
  }

  public byte[] getData() throws InterruptedException, IOException {
    synchronized (gotData) {
      while (!data && !finish) {
        gotData.wait();
      }
      if (reset) {
        throw new IOException("stream reset");
      }
      rBuf = buf;
      buf = new byte[0];
      data = false;
      return rBuf;
    }
  }
}
