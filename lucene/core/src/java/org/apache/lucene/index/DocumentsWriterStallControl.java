/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Controls the health status of a {@link DocumentsWriter} sessions. This class used to block
 * incoming indexing threads if flushing significantly slower than indexing to ensure the {@link
 * DocumentsWriter}s healthiness. If flushing is significantly slower than indexing the net memory
 * used within an {@link IndexWriter} session can increase very quickly and easily exceed the JVM's
 * available memory.
 *
 * <p>To prevent OOM Errors and ensure IndexWriter's stability this class blocks incoming threads
 * from indexing once 2 x number of available {@link DocumentsWriterPerThread}s in {@link
 * DocumentsWriterPerThreadPool} is exceeded. Once flushing catches up and the number of flushing
 * DWPT is equal or lower than the number of active {@link DocumentsWriterPerThread}s threads are
 * released and can continue indexing.
 */
final class DocumentsWriterStallControl {

  // 是否停滞
  private volatile boolean stalled;

  private int numWaiting; // only with assert
  private boolean wasStalled; // only with assert

  // 注意点：这里用的是 IdentityHashMap 与 HashMap不同，HashMap使用的是 对象的hashCode 做的散列，然后再根据==判断是否相同
  // IdentityHashMap 使用的是System.identityHashCode(obj)
  // 如果把Thread当成key，两者好像没有什么不同
  private final Map<Thread, Boolean> waiting = new IdentityHashMap<>(); // only with assert

  /**
   * 更新停滞状态。 当且仅当正在刷新的 {@link DocumentsWriterPerThread} 数量大于活动数量的
   * {@link DocumentsWriterPerThread} 这个方法将 stalled 更新为 <code>true</code> ，
   * 否则他将重置 {@link DocumentsWriterStallControl} 为健康状态
   * 并且释放所有等待 {@link #waitIfStalled()} 的线程
   */
  synchronized void updateStalled(boolean stalled) {
    if (this.stalled != stalled) {
      this.stalled = stalled;
      if (stalled) {
        wasStalled = true;
      }
      notifyAll();
    }
  }

  /** 如果文档写入当前处于停滞状态，则阻塞。 */
  void waitIfStalled() {
    if (stalled) {
      synchronized (this) {
        if (stalled) { // react on the first wakeup call!
          // don't loop here, higher level logic will re-stall!
          try {
            incWaiters();
            // Defensive, in case we have a concurrency bug that fails to .notify/All our thread:
            // just wait for up to 1 second here, and let caller re-stall if it's still needed:
            wait(1000);
            decrWaiters();
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
        }
      }
    }
  }

  boolean anyStalledThreads() {
    return stalled;
  }

  private void incWaiters() {
    numWaiting++;
    assert waiting.put(Thread.currentThread(), Boolean.TRUE) == null;
    assert numWaiting > 0;
  }

  private void decrWaiters() {
    numWaiting--;
    assert waiting.remove(Thread.currentThread()) != null;
    assert numWaiting >= 0;
  }

  synchronized boolean hasBlocked() { // for tests
    return numWaiting > 0;
  }

  synchronized int getNumWaiting() { // for tests
    return numWaiting;
  }

  boolean isHealthy() { // for tests
    return !stalled; // volatile read!
  }

  synchronized boolean isThreadQueued(Thread t) { // for tests
    return waiting.containsKey(t);
  }

  synchronized boolean wasStalled() { // for tests
    return wasStalled;
  }
}
