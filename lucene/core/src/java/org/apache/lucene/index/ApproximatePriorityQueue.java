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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

/**
 * 近似优先队列, which attempts to poll items by decreasing log of the weight,
 * though exact ordering is not guaranteed. This class doesn't support null elements.
 */
final class ApproximatePriorityQueue<T> {

  // 0到63之间的索引是稀疏填充的, and indexes that are
  // greater than or equal to 64 are densely populated
  // Items close to the beginning of this list are more likely to have a
  // higher weight.
  private final List<T> slots = new ArrayList<>(Long.SIZE);

  // 一个bitset，其中的1表示在' slots '中对应的索引已被占用。
  // long 64个bit，所以可以表示64个槽位
  private long usedSlots = 0L;

  ApproximatePriorityQueue() {
    for (int i = 0; i < Long.SIZE; ++i) {
      slots.add(null);
    }
  }

  /** 在队列中添加一个包含优先级的条目。 */
  void add(T entry, long weight) {
    assert entry != null;
    // 期望的槽位是这个权重前导0的个数。
    // 比如：权重越大，项就越接近数组的开头。
    final int expectedSlot = Long.numberOfLeadingZeros(weight);

    // 如果这个槽位被占用，则寻找下一个
    // 上面的位操作相当于循环遍历槽，直到找到空闲的槽。
    final long freeSlots = ~usedSlots;
    final int destinationSlot =
        expectedSlot + Long.numberOfTrailingZeros(freeSlots >>> expectedSlot);
    assert destinationSlot >= expectedSlot;
    if (destinationSlot < Long.SIZE) {
      // 在usedSlots中新占用的slot的位置位1
      usedSlots |= 1L << destinationSlot;
      T previous = slots.set(destinationSlot, entry);
      assert previous == null;
    } else {
      slots.add(entry);
    }
  }

  /**
   * Return an entry matching the predicate. This will usually be one of the available entries that
   * have the highest weight, though this is not guaranteed. This method returns {@code null} if no
   * free entries are available.
   */
  T poll(Predicate<T> predicate) {
    // 先查看索引0到63, 这一部分是稀疏的。
    int nextSlot = 0;
    do {
      final int nextUsedSlot = nextSlot + Long.numberOfTrailingZeros(usedSlots >>> nextSlot);
      if (nextUsedSlot >= Long.SIZE) {
        break;
      }
      final T entry = slots.get(nextUsedSlot);
      if (predicate.test(entry)) {
        // 如果可以给这个拿到的dwpt上锁，说明这个dwpt没有被占用
        usedSlots &= ~(1L << nextUsedSlot);
        slots.set(nextUsedSlot, null);
        return entry;
      } else {
        nextSlot = nextUsedSlot + 1;
      }
    } while (nextSlot < Long.SIZE);

    // Then look at indexes 64.. which are densely populated.
    // Poll in descending order so that if the number of indexing threads
    // decreases, we keep using the same entry over and over again.
    // Resizing operations are also less costly on lists when items are closer
    // to the end of the list.
    for (ListIterator<T> lit = slots.listIterator(slots.size());
        lit.previousIndex() >= Long.SIZE; ) {
      final T entry = lit.previous();
      if (predicate.test(entry)) {
        lit.remove();
        return entry;
      }
    }

    // No entry matching the predicate was found.
    return null;
  }

  // Only used for assertions
  boolean contains(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return slots.contains(o);
  }

  boolean isEmpty() {
    return usedSlots == 0 && slots.size() == Long.SIZE;
  }

  boolean remove(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    int index = slots.indexOf(o);
    if (index == -1) {
      return false;
    }
    if (index >= Long.SIZE) {
      slots.remove(index);
    } else {
      usedSlots &= ~(1L << index);
      slots.set(index, null);
    }
    return true;
  }
}
