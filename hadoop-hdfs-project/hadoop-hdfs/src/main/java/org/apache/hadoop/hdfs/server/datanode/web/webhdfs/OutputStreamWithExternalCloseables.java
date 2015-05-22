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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.IOUtils;

class OutputStreamWithExternalCloseables extends FilterOutputStream {

  private static final Log LOG = WebHdfsHandler.LOG;

  private final Closeable[] closeables;

  OutputStreamWithExternalCloseables(OutputStream out,
      Closeable... closeables) {
    super(out);
    this.closeables = closeables;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      IOUtils.cleanup(LOG, closeables);
    }
  }

}