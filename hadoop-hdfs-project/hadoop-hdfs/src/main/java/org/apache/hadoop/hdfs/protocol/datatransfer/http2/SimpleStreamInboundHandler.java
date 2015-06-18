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

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.TypeParameterMatcher;

/**
 * {@link StreamInboundHandlerAdaptor} which allows to explicit only handle a
 * specific type of messages.
 * @see SimpleChannelInboundHandler
 */
public abstract class SimpleStreamInboundHandler<I> extends
    StreamInboundHandlerAdaptor {

  private final TypeParameterMatcher matcher;

  private final boolean autoRelease;

  /**
   * @see {@link #SimpleStreamInboundHandler(boolean)} with {@code true} as
   *      boolean parameter.
   */
  protected SimpleStreamInboundHandler() {
    this(true);
  }

  /**
   * Create a new instance which will try to detect the types to match out of
   * the type parameter of the class.
   * @param autoRelease {@code true} if handled messages should be released
   *          automatically by pass them to
   *          {@link ReferenceCountUtil#release(Object)}.
   */
  protected SimpleStreamInboundHandler(boolean autoRelease) {
    matcher =
        TypeParameterMatcher.find(this, SimpleStreamInboundHandler.class, "I");
    this.autoRelease = autoRelease;
  }

  @Override
  public void streamRead(StreamHandlerContext ctx, Object msg,
      boolean endOfStream) throws Exception {
    boolean release = true;
    try {
      if (matcher.match(msg)) {
        @SuppressWarnings("unchecked")
        I imsg = (I) msg;
        streamRead0(ctx, imsg, endOfStream);
      } else {
        release = false;
        ctx.fireStreamRead(msg, endOfStream);
      }
    } finally {
      if (autoRelease && release) {
        ReferenceCountUtil.release(msg);
      }
    }
  }

  protected abstract void streamRead0(StreamHandlerContext ctx, I msg,
      boolean endOfStream) throws Exception;

}
