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

import java.util.IdentityHashMap;

import io.netty.channel.ChannelPipeline;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A list of {@link StreamHandler}s.
 * @see ChannelPipeline
 */
@InterfaceAudience.Private
public class StreamPipeline {

  private static final Log LOG = DataTransferHttp2ConnectionHandler.LOG;

  private final EmbeddedStream stream;

  private final IdentityHashMap<StreamHandler, StreamHandlerContext> handler2Ctx =
      new IdentityHashMap<>();

  private final StreamHandlerContext head;

  private final StreamHandlerContext tail;

  private final StreamOutboundHandler headHandler =
      new StreamOutboundHandlerAdaptor() {

        @Override
        public void writeAndFlush(StreamHandlerContext ctx, Object msg,
            boolean endOfStream) throws Exception {
          stream.writeToParentChannel(msg, endOfStream, true);
        }

        @Override
        public void write(StreamHandlerContext ctx, Object msg,
            boolean endOfStream) throws Exception {
          stream.writeToParentChannel(msg, endOfStream, false);
        }
      };

  private final StreamHandler tailHandler = new StreamInboundHandlerAdaptor() {

    @Override
    public void streamRead(StreamHandlerContext ctx, Object msg,
        boolean endOfStream) throws Exception {
      LOG.warn("Discard inbound message " + msg
          + " that reach tail of pipeline");
    }

    @Override
    public void streamClosed(StreamHandlerContext ctx) {
    }

    @Override
    public void exceptionCaught(StreamHandlerContext ctx, Throwable cause) {
      LOG.warn("exception passed to tail of pipeline", cause);
    }
  };

  public StreamPipeline(EmbeddedStream stream) {
    this.stream = stream;
    head = new StreamHandlerContext(null, null, headHandler, stream);
    tail = new StreamHandlerContext(head, null, tailHandler, stream);
    head.next = tail;
  }

  private void addHandlerFirst0(StreamHandler handler) {
    StreamHandlerContext holder =
        new StreamHandlerContext(head, head.next, handler, stream);
    head.next.prev = holder;
    head.next = holder;
    handler2Ctx.put(handler, holder);
  }

  public void addHandlerFirst(StreamHandler... handlers) {
    for (int i = handlers.length - 1; i >= 0; i--) {
      addHandlerFirst0(handlers[i]);
    }
  }

  private void removeHandler0(StreamHandlerContext ctx) {
    ctx.next.prev = ctx.prev;
    ctx.prev.next = ctx.next;
  }

  public void removeHandler(StreamHandler handler) {
    StreamHandlerContext ctx = handler2Ctx.get(handler);
    if (ctx != null) {
      removeHandler0(ctx);
    }
  }

  /**
   * Should only be called inside event loop.
   */
  public void fireStreamRead(Object msg, boolean endOfStream) {
    head.fireStreamRead(msg, endOfStream);
  }

  /**
   * Should only be called inside event loop.
   */
  public void fireStreamClosed() {
    head.fireStreamClosed();
  }

  public void write(Object msg, boolean endOfStream) {
    tail.write(msg, endOfStream);
  }

  public void writeAndFlush(Object msg, boolean endOfStream) {
    tail.writeAndFlush(msg, endOfStream);
  }

  public void fireExceptionCaught(Throwable cause) {
    head.fireExceptionCaught(cause);
  }
}
