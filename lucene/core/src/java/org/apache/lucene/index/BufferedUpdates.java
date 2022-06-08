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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * 为什么有docId？代码里好像没有看见
 * 为单个段以docID,term,query的形式，缓存删除和更新。
 * 为待刷新的段保存挂起的删除和更新。
 * 一旦这些删除和更新被 push (DocumentsWriter中调用flush), 他们被转变为 {@link
 * FrozenBufferedUpdates} 实例，并且推送到 {@link BufferedUpdatesStream}.
 */

// 注意: 该类的实例可以通过DocumentWriterPerThread上的私有字段访问，
// 也可以通过DocumentsWriterDeleteQueue上的同步代码访问

class BufferedUpdates implements Accountable {

  /* Rough logic: HashMap has an array[Entry] w/ varying
  load factor (say 2 * POINTER).  Entry is object w/ Term
  key, Integer val, int hash, Entry next
  (OBJ_HEADER + 3*POINTER + INT).  Term is object w/
  String field and String text (OBJ_HEADER + 2*POINTER).
  Term's field is String (OBJ_HEADER + 4*INT + POINTER +
  OBJ_HEADER + string.length*CHAR).
  Term's text is String (OBJ_HEADER + 4*INT + POINTER +
  OBJ_HEADER + string.length*CHAR).  Integer is
  OBJ_HEADER + INT. */
  static final int BYTES_PER_DEL_TERM =
      9 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + 7 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 10 * Integer.BYTES;

  /* Rough logic: HashMap has an array[Entry] w/ varying
  load factor (say 2 * POINTER).  Entry is object w/
  Query key, Integer val, int hash, Entry next
  (OBJ_HEADER + 3*POINTER + INT).  Query we often
  undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
  static final int BYTES_PER_DEL_QUERY =
      5 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 2 * Integer.BYTES
          + 24;
  // 需要删除term的数量，重复的term，数量依然累计
  final AtomicInteger numTermDeletes = new AtomicInteger();
  final AtomicInteger numFieldUpdates = new AtomicInteger();

  // 需要删除的terms
  final Map<Term, Integer> deleteTerms =
      new HashMap<>(); // TODO cut this over to FieldUpdatesBuffer
  // 需要删除的queries
  final Map<Query, Integer> deleteQueries = new HashMap<>();

  final Map<String, FieldUpdatesBuffer> fieldUpdates = new HashMap<>();

  public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

  // 需要删除的queries占用的内存
  private final Counter bytesUsed = Counter.newCounter(true);
  final Counter fieldUpdatesBytesUsed = Counter.newCounter(true);

  // 需要删除的term占用的内存
  private final Counter termsBytesUsed = Counter.newCounter(true);

  private static final boolean VERBOSE_DELETES = false;

  long gen;

  final String segmentName;

  public BufferedUpdates(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public String toString() {
    if (VERBOSE_DELETES) {
      return ("gen=" + gen)
          + (" numTerms=" + numTermDeletes)
          + (", deleteTerms=" + deleteTerms)
          + (", deleteQueries=" + deleteQueries)
          + (", fieldUpdates=" + fieldUpdates)
          + (", bytesUsed=" + bytesUsed);
    } else {
      String s = "gen=" + gen;
      if (numTermDeletes.get() != 0) {
        s +=
            " " + numTermDeletes.get() + " deleted terms (unique count=" + deleteTerms.size() + ")";
      }
      if (deleteQueries.size() != 0) {
        s += " " + deleteQueries.size() + " deleted queries";
      }
      if (numFieldUpdates.get() != 0) {
        s += " " + numFieldUpdates.get() + " field updates";
      }
      if (bytesUsed.get() != 0) {
        s += " bytesUsed=" + bytesUsed.get();
      }

      return s;
    }
  }

  public void addQuery(Query query, int docIDUpto) {
    // 为什么deleteQueries.put不像，deleteTerms.put之前要先判断版本呢？
    Integer current = deleteQueries.put(query, docIDUpto);
    // increment bytes used only if the query wasn't added so far.
    if (current == null) {
      // 为什么删除query占用的内存是个常数？
      bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
    }
  }

  public void addTerm(Term term, int docIDUpto) {
    // Term的equals方法被重写，field和bytes都相同才相等
    Integer current = deleteTerms.get(term);
    if (current != null && docIDUpto < current) {
      // 只记录大于当前数字的新数字。 这一点很重要，
      // 因为如果多个线程几乎在同一时间替换相同的文档，
      // 那么可能会有一个拥有更高docID的线程在其他线程之前被调度。
      // 如果我们盲目地替换，我们就会错误地对两个文档进行索引。
      return;
    }

    deleteTerms.put(term, Integer.valueOf(docIDUpto));
    // 请注意，如果current != null，则意味着该term已经有一个缓存的删除，因此似乎numTermDeletes计数增多了。
    // 这种重复计数是为了照应 IndexWriterConfig.setMaxBufferedDeleteTerms。
    // 根据LUCENE-7868，setMaxBufferedDeleteTerms已经被删除了，汗……
    numTermDeletes.incrementAndGet();
    if (current == null) {
      termsBytesUsed.addAndGet(
          BYTES_PER_DEL_TERM + term.bytes.length + (Character.BYTES * term.field().length()));
    }
  }

  void addNumericUpdate(NumericDocValuesUpdate update, int docIDUpto) {
    FieldUpdatesBuffer buffer =
        fieldUpdates.computeIfAbsent(
            update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
    if (update.hasValue) {
      buffer.addUpdate(update.term, update.getValue(), docIDUpto);
    } else {
      buffer.addNoValue(update.term, docIDUpto);
    }
    numFieldUpdates.incrementAndGet();
  }

  void addBinaryUpdate(BinaryDocValuesUpdate update, int docIDUpto) {
    FieldUpdatesBuffer buffer =
        fieldUpdates.computeIfAbsent(
            update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
    if (update.hasValue) {
      buffer.addUpdate(update.term, update.getValue(), docIDUpto);
    } else {
      buffer.addNoValue(update.term, docIDUpto);
    }
    numFieldUpdates.incrementAndGet();
  }

  void clearDeleteTerms() {
    numTermDeletes.set(0);
    termsBytesUsed.addAndGet(-termsBytesUsed.get());
    deleteTerms.clear();
  }

  void clear() {
    deleteTerms.clear();
    deleteQueries.clear();
    numTermDeletes.set(0);
    numFieldUpdates.set(0);
    fieldUpdates.clear();
    bytesUsed.addAndGet(-bytesUsed.get());
    fieldUpdatesBytesUsed.addAndGet(-fieldUpdatesBytesUsed.get());
    termsBytesUsed.addAndGet(-termsBytesUsed.get());
  }

  boolean any() {
    return deleteTerms.size() > 0 || deleteQueries.size() > 0 || numFieldUpdates.get() > 0;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get() + fieldUpdatesBytesUsed.get() + termsBytesUsed.get();
  }
}
