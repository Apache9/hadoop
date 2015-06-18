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
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import static org.apache.hadoop.hdfs.server.datanode.web.dtp.HandlerNames.DISPATCHER_HANDLER_NAME;

import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.EmbeddedStream;
import org.apache.hadoop.hdfs.protocol.datatransfer.http2.StreamHandlerInitializer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

/**
 *
 */
@InterfaceAudience.Private
public class DtpStreamHandlerInitializer implements StreamHandlerInitializer {

  static final Log LOG = LogFactory.getLog(DtpStreamHandlerInitializer.class);

  public static final int VERSION = 1;

  public static final String URL_PREFIX = "/dtp/v" + VERSION;

  public static final String OP_READ_BLOCK = "/read_block";

  private final DataNode datanode;

  private final ExecutorService executor;

  public DtpStreamHandlerInitializer(DataNode datanode, ExecutorService executor) {
    this.datanode = datanode;
    this.executor = executor;
  }

  @Override
  public void initialize(EmbeddedStream stream) {
    stream.pipeline().addLast(DISPATCHER_HANDLER_NAME,
        new DtpUrlDispatcher(datanode, executor));
  }

}
