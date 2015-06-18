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

import io.netty.channel.ChannelPipeline;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

/**
 * A list of {@link StreamHandler}s.
 * @see ChannelPipeline
 */
@InterfaceAudience.Private
public class StreamPipeline {

  private static final Log LOG = DataTransferHttp2ConnectionHandler.LOG;

  private final EmbeddedStream stream;

  private final IdentityHashMap<StreamHandler, String> handler2Name =
      new IdentityHashMap<>();

  private final Map<String, StreamHandlerContext> name2Ctx = new HashMap<>();

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

  private long handlerNumber = 0;

  private String generateName(StreamHandler handler) {
    return (handler.getClass().getSimpleName() + (handlerNumber++));
  }

  private void checkDuplicate(String name, StreamHandler handler) {
    Preconditions.checkArgument(!handler2Name.containsKey(handler), "Handler "
        + handler + " already added");
    Preconditions.checkArgument(!name2Ctx.containsKey(name), "Handler name "
        + name + " already exists");
  }

  private void addLast0(String name, StreamHandler handler) {
    checkDuplicate(name, handler);
    StreamHandlerContext ctx =
        new StreamHandlerContext(tail.prev, tail, handler, stream);
    tail.prev.next = ctx;
    tail.prev = ctx;
    handler2Name.put(handler, name);
    name2Ctx.put(name, ctx);
  }

  public StreamPipeline addLast(StreamHandler... handlers) {
    for (StreamHandler handler : handlers) {
      addLast0(generateName(handler), handler);
    }
    return this;
  }

  public StreamPipeline addLast(String name, StreamHandler handler) {
    addLast0(name, handler);
    return this;
  }

  private void addAfter0(StreamHandlerContext baseCtx, String name,
      StreamHandler handler) {
    checkDuplicate(name, handler);
    StreamHandlerContext ctx =
        new StreamHandlerContext(baseCtx, baseCtx.next, handler, stream);
    baseCtx.next.prev = ctx;
    baseCtx.next = ctx;
    handler2Name.put(handler, name);
    name2Ctx.put(name, ctx);
  }

  public StreamPipeline addAfter(String baseName, String name,
      StreamHandler handler) {
    StreamHandlerContext baseCtx = name2Ctx.get(baseName);
    Preconditions.checkArgument(baseCtx != null, "Handler " + baseName
        + " not found");
    addAfter0(baseCtx, name, handler);
    return this;
  }

  private void remove0(StreamHandlerContext ctx) {
    ctx.next.prev = ctx.prev;
    ctx.prev.next = ctx.next;
    handler2Name.remove(ctx.handler);
    ctx.handler.streamClosed(ctx);
  }

  public void remove(StreamHandler handler) {
    String name = handler2Name.get(handler);
    if (name != null) {
      remove(name);
    }
  }

  public void remove(String name) {
    StreamHandlerContext ctx = name2Ctx.remove(name);
    if (ctx != null) {
      remove0(ctx);
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
