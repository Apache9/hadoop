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
package org.apache.hadoop.hdfs.util;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 * A simple class for pooling direct ByteBuffers. This is necessary
 * because Direct Byte Buffers do not take up much space on the heap,
 * and hence will not trigger GCs on their own. However, they do take
 * native memory, and thus can cause high memory usage if not pooled.
 * The pooled instances are referred to only via weak references, allowing
 * them to be collected when a GC does run.
 *
 * This class only does effective pooling when many buffers will be
 * allocated at the same size. There is no attempt to reuse larger
 * buffers to satisfy smaller allocations.
 */
@InterfaceAudience.Private
public class DirectBufferPool {
  private static final Log LOG = LogFactory.getLog(DirectBufferPool.class);
  
  // Essentially implement a multimap with weak values.
  final ConcurrentMap<Integer, Queue<WeakReference<ByteBuffer>>> buffersBySize =
    new ConcurrentHashMap<Integer, Queue<WeakReference<ByteBuffer>>>();
  private AtomicLong usingMemoryBytes = new AtomicLong(0);
 
  /**
   * Allocate a direct buffer of the specified size, in bytes.
   * If a pooled buffer is available, returns that. Otherwise
   * allocates a new one.
   */
  public ByteBuffer getBuffer(int size) {
    try {
      Queue<WeakReference<ByteBuffer>> list = buffersBySize.get(size);
      if (list == null) {
        // no available buffers for this size
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        usingMemoryBytes.addAndGet(size);
        return buffer;
      }

      WeakReference<ByteBuffer> ref;
      while ((ref = list.poll()) != null) {
        ByteBuffer b = ref.get();
        if (b != null) {
          usingMemoryBytes.addAndGet(size);
          return b;
        }
      }

      ByteBuffer buffer = ByteBuffer.allocateDirect(size);
      usingMemoryBytes.addAndGet(size);
      return buffer;
    } catch (OutOfMemoryError e) {
      LOG.error("OOM when getBuffer : " + bufferPoolStateToString(), e);
      throw e;
    }
  }
  
  public String bufferPoolStateToString() {
    StringBuilder builder = new StringBuilder();
    builder.append("PooledMemoryMB=" + getPooledMemoryMB() + ", UsingMemoryMB="
        + getUsingMemoryMB() + "\n");
    for (Entry<Integer, Queue<WeakReference<ByteBuffer>>> bufferBySize : buffersBySize.entrySet()) {
      builder.append("Pooled bufferSize=" + bufferBySize.getKey() + " : count="
          + bufferBySize.getValue().size() + "\n");
    }
    return builder.toString();
  }
  
  /**
   * Return a buffer into the pool. After being returned,
   * the buffer may be recycled, so the user must not
   * continue to use it in any way.
   * @param buf the buffer to return
   */
  public void returnBuffer(ByteBuffer buf) {
    buf.clear(); // reset mark, limit, etc
    int size = buf.capacity();
    Queue<WeakReference<ByteBuffer>> list = buffersBySize.get(size);
    if (list == null) {
      list = new ConcurrentLinkedQueue<WeakReference<ByteBuffer>>();
      Queue<WeakReference<ByteBuffer>> prev = buffersBySize.putIfAbsent(size, list);
      // someone else put a queue in the map before we did
      if (prev != null) {
        list = prev;
      }
    }
    list.add(new WeakReference<ByteBuffer>(buf));
    usingMemoryBytes.addAndGet(size * -1);
  }
  
  /**
   * Return the number of available buffers of a given size.
   * This is used only for tests.
   */
  @VisibleForTesting
  int countBuffersOfSize(int size) {
    Queue<WeakReference<ByteBuffer>> list = buffersBySize.get(size);
    if (list == null) {
      return 0;
    }
    
    return list.size();
  }

  /**
   * Return the sum size of pool buffers, in MB.
   */
  public long getPooledMemoryMB() {
    long sizeInTotal = 0;
    for (Entry<Integer, Queue<WeakReference<ByteBuffer>>> entry : buffersBySize.entrySet()) {
      sizeInTotal += entry.getKey().longValue() * entry.getValue().size();
    }
    return sizeInTotal/(1024 * 1024);
  }

  /**
   * Return the currently using memory sum size in MB.
   */
  public long getUsingMemoryMB() {
    return usingMemoryBytes.get()/(1024 * 1024);
  }
}
