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

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;

/**
 * {@link DocumentsWriterDeleteQueue}是一个非阻塞的链接待删除队列。
 * 与其他队列实现相比，我们只维护队列的尾部。删除队列总是在DWPT和全局删除池使用。每个DWPT和全局删除池需要维护他们自己的队列头
 * (作为每个{@link DocumentsWriterPerThread}的DeleteSlice实例)
 * DWPT和全局删除池的区别在于，DWPT在添加了第一个文档之后，就开始维护头部。因为对于
 *  The difference between the DWPT and the global pool is that the DWPT
 * starts maintaining a head once it has added its first document since for its segments private
 * deletes only the deletes after that document are relevant. The global pool instead starts
 * maintaining the head once this instance is created by taking the sentinel instance as its initial
 * head.
 *
 * <p>因为每个{@link DeleteSlice}维护自己的队列头，并且列表只有单链接，垃圾收集器会为我们清理列表。
 * 列表中所有仍然相关的节点应该被DWPT私有的{@link DeleteSlice}或全局的{@link BufferedUpdates}slice直接或间接引用。
 *
 * <p>每个DWPT以及全局删除池都维护它们的私有DeleteSlice实例。
 * 在DWPT情况下，更新一个slice相当于原子地完成文档。在同一索引会话中，片更新保证了相对于所有其他更新的“happens before”关系。
 * 当DWPT更新文档时:
 *
 * <ol>
 *   <li>consumes a document and finishes its processing
 *   <li>通过调用{@link #updateSlice(DeleteSlice)}或{@link #add(Node, DeleteSlice)}(如果文档有delTerm)
 *       来更新它的私有{@link DeleteSlice}
 *   <li>将切片中的所有删除操作应用于它的私有{@link BufferedUpdates}，并重置它
 *   <li>递增其内部文档id
 * </ol>
 *
 *  DWPT也不会应用它当前的文档删除术语，直到它更新了它的删除片，这确保了更新的一致性。
 *  如果在可以更新DeleteSlice之前更新失败，那么deleteTerm也不会添加到其私有删除和全局删除。
 * The DWPT also doesn't apply its current documents delete term until it has updated its delete
 * slice which ensures the consistency of the update. If the update fails before the DeleteSlice
 * could have been updated the deleteTerm will also not be added to its private deletes neither to
 * the global deletes.
 */
final class DocumentsWriterDeleteQueue implements Accountable, Closeable {

  // 删除队列的当前元素（最后一个删除操作）
  private volatile Node<?> tail;

  private volatile boolean closed = false;

  /**
   * 用于记录对所有先前(已经写入磁盘)段的删除。
   * 每当任何段刷新时，我们都会在新刷新的段之前将这组删除和插入操作捆绑到缓冲的更新流中。
   */
  private final DeleteSlice globalSlice;

  private final BufferedUpdates globalBufferedUpdates;

  // only acquired to update the global deletes, pkg-private for access by tests:
  final ReentrantLock globalBufferLock = new ReentrantLock();

  final long generation;

  /**
   * 生成IW返回给更改索引的调用者的序列号，显示所有操作的有效序列化。
   * Generates the sequence number that IW returns to callers changing the index, showing the
   * effective serialization of all operations.
   */
  private final AtomicLong nextSeqNo;

  private final InfoStream infoStream;

  private volatile long maxSeqNo = Long.MAX_VALUE;

  private final long startSeqNo;
  private final LongSupplier previousMaxSeqId;
  private boolean advanced;

  DocumentsWriterDeleteQueue(InfoStream infoStream) {
    // seqNo must start at 1 because some APIs negate this to also return a boolean
    this(infoStream, 0, 1, () -> 0);
  }

  private DocumentsWriterDeleteQueue(
      InfoStream infoStream, long generation, long startSeqNo, LongSupplier previousMaxSeqId) {
    this.infoStream = infoStream;
    this.globalBufferedUpdates = new BufferedUpdates("global");
    this.generation = generation;
    this.nextSeqNo = new AtomicLong(startSeqNo);
    this.startSeqNo = startSeqNo;
    this.previousMaxSeqId = previousMaxSeqId;
    long value = previousMaxSeqId.getAsLong();
    assert value <= startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
    /*
     * we use a sentinel instance as our initial tail. No slice will ever try to
     * apply this tail since the head is always omitted.
     */
    tail = new Node<>(null); // sentinel
    globalSlice = new DeleteSlice(tail);
  }

  long addDelete(Query... queries) {
    long seqNo = add(new QueryArrayNode(queries));
    tryApplyGlobalSlice();
    return seqNo;
  }

  long addDelete(Term... terms) {
    long seqNo = add(new TermArrayNode(terms));
    tryApplyGlobalSlice();
    return seqNo;
  }

  long addDocValuesUpdates(DocValuesUpdate... updates) {
    long seqNo = add(new DocValuesUpdatesNode(updates));
    tryApplyGlobalSlice();
    return seqNo;
  }

  static Node<Term> newNode(Term term) {
    return new TermNode(term);
  }

  static Node<DocValuesUpdate[]> newNode(DocValuesUpdate... updates) {
    return new DocValuesUpdatesNode(updates);
  }

  /** invariant for document update */
  long add(Node<?> deleteNode, DeleteSlice slice) {
    long seqNo = add(deleteNode);
    /*
     * this is an update request where the term is the updated documents
     * delTerm. in that case we need to guarantee that this insert is atomic
     * with regards to the given delete slice. This means if two threads try to
     * update the same document with in turn the same delTerm one of them must
     * win. By taking the node we have created for our del term as the new tail
     * it is guaranteed that if another thread adds the same right after us we
     * will apply this delete next time we update our slice and one of the two
     * competing updates wins!
     */
    slice.sliceTail = deleteNode;
    assert slice.sliceHead != slice.sliceTail : "slice head and tail must differ after add";
    tryApplyGlobalSlice(); // TODO doing this each time is not necessary maybe
    // we can do it just every n times or so?

    return seqNo;
  }

  synchronized long add(Node<?> newNode) {
    ensureOpen();
    tail.next = newNode;
    this.tail = newNode;
    return getNextSequenceNumber();
  }

  boolean anyChanges() {
    globalBufferLock.lock();
    try {
      /*
       * check if all items in the global slice were applied
       * and if the global slice is up-to-date
       * and if globalBufferedUpdates has changes
       */
      return globalBufferedUpdates.any()
          || !globalSlice.isEmpty()
          || globalSlice.sliceTail != tail
          || tail.next != null;
    } finally {
      globalBufferLock.unlock();
    }
  }

  void tryApplyGlobalSlice() {
    if (globalBufferLock.tryLock()) {
      ensureOpen();
      /*
       * The global buffer must be locked but we don't need to update them if
       * there is an update going on right now. It is sufficient to apply the
       * deletes that have been added after the current in-flight global slices
       * tail the next time we can get the lock!
       */
      try {
        if (updateSliceNoSeqNo(globalSlice)) {
          globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
        }
      } finally {
        globalBufferLock.unlock();
      }
    }
  }

  FrozenBufferedUpdates freezeGlobalBuffer(DeleteSlice callerSlice) {
    globalBufferLock.lock();
    try {
      ensureOpen();
      /*
       * Here we freeze the global buffer so we need to lock it, apply all
       * deletes in the queue and reset the global slice to let the GC prune the
       * queue.
       */
      final Node<?> currentTail = tail; // take the current tail make this local any
      // Changes after this call are applied later
      // and not relevant here
      if (callerSlice != null) {
        // Update the callers slices so we are on the same page
        callerSlice.sliceTail = currentTail;
      }
      return freezeGlobalBufferInternal(currentTail);
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * This may freeze the global buffer unless the delete queue has already been closed. If the queue
   * has been closed this method will return <code>null</code>
   */
  FrozenBufferedUpdates maybeFreezeGlobalBuffer() {
    globalBufferLock.lock();
    try {
      if (closed == false) {
        /*
         * Here we freeze the global buffer so we need to lock it, apply all
         * deletes in the queue and reset the global slice to let the GC prune the
         * queue.
         */
        return freezeGlobalBufferInternal(tail); // take the current tail make this local any
      } else {
        assert anyChanges() == false : "we are closed but have changes";
        return null;
      }
    } finally {
      globalBufferLock.unlock();
    }
  }

  private FrozenBufferedUpdates freezeGlobalBufferInternal(final Node<?> currentTail) {
    assert globalBufferLock.isHeldByCurrentThread();
    if (globalSlice.sliceTail != currentTail) {
      globalSlice.sliceTail = currentTail;
      globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
    }

    if (globalBufferedUpdates.any()) {
      final FrozenBufferedUpdates packet =
          new FrozenBufferedUpdates(infoStream, globalBufferedUpdates, null);
      globalBufferedUpdates.clear();
      return packet;
    } else {
      return null;
    }
  }

  DeleteSlice newSlice() {
    return new DeleteSlice(tail);
  }

  /** Negative result means there were new deletes since we last applied */
  synchronized long updateSlice(DeleteSlice slice) {
    ensureOpen();
    long seqNo = getNextSequenceNumber();
    if (slice.sliceTail != tail) {
      // new deletes arrived since we last checked
      slice.sliceTail = tail;
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /** Just like updateSlice, but does not assign a sequence number */
  boolean updateSliceNoSeqNo(DeleteSlice slice) {
    if (slice.sliceTail != tail) {
      // new deletes arrived since we last checked
      slice.sliceTail = tail;
      return true;
    }
    return false;
  }

  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException(
          "This " + DocumentsWriterDeleteQueue.class.getSimpleName() + " is already closed");
    }
  }

  public boolean isOpen() {
    return closed == false;
  }

  @Override
  public synchronized void close() {
    globalBufferLock.lock();
    try {
      if (anyChanges()) {
        throw new IllegalStateException("Can't close queue unless all changes are applied");
      }
      this.closed = true;
      long seqNo = nextSeqNo.get();
      assert seqNo <= maxSeqNo
          : "maxSeqNo must be greater or equal to " + seqNo + " but was " + maxSeqNo;
      nextSeqNo.set(maxSeqNo + 1);
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class DeleteSlice {
    // No need to be volatile, slices are thread captive (only accessed by one thread)!
    Node<?> sliceHead; // we don't apply this one
    Node<?> sliceTail;

    DeleteSlice(Node<?> currentTail) {
      assert currentTail != null;
      /*
       * Initially this is a 0 length slice pointing to the 'current' tail of
       * the queue. Once we update the slice we only need to assign the tail and
       * have a new slice
       */
      sliceHead = sliceTail = currentTail;
    }

    void apply(BufferedUpdates del, int docIDUpto) {
      if (sliceHead == sliceTail) {
        // 0 length slice
        return;
      }
      /*
       * When we apply a slice we take the head and get its next as our first
       * item to apply and continue until we applied the tail. If the head and
       * tail in this slice are not equal then there will be at least one more
       * non-null node in the slice!
       */
      Node<?> current = sliceHead;
      do {
        current = current.next;
        assert current != null
            : "slice property violated between the head on the tail must not be a null node";
        current.apply(del, docIDUpto);
      } while (current != sliceTail);
      reset();
    }

    void reset() {
      // Reset to a 0 length slice
      sliceHead = sliceTail;
    }

    /**
     * Returns <code>true</code> iff the given node is identical to the slices tail, otherwise
     * <code>false</code>.
     */
    boolean isTail(Node<?> node) {
      return sliceTail == node;
    }

    /**
     * Returns <code>true</code> iff the given item is identical to the item hold by the slices
     * tail, otherwise <code>false</code>.
     */
    boolean isTailItem(Object object) {
      return sliceTail.item == object;
    }

    boolean isEmpty() {
      return sliceHead == sliceTail;
    }
  }

  public int numGlobalTermDeletes() {
    return globalBufferedUpdates.numTermDeletes.get();
  }

  void clear() {
    globalBufferLock.lock();
    try {
      final Node<?> currentTail = tail;
      globalSlice.sliceHead = globalSlice.sliceTail = currentTail;
      globalBufferedUpdates.clear();
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class Node<T> {
    volatile Node<?> next;
    final T item;

    Node(T item) {
      this.item = item;
    }

    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      throw new IllegalStateException("sentinel item must never be applied");
    }

    boolean isDelete() {
      return true;
    }
  }

  private static final class TermNode extends Node<Term> {

    TermNode(Term term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addTerm(item, docIDUpto);
    }

    @Override
    public String toString() {
      return "del=" + item;
    }
  }

  private static final class QueryArrayNode extends Node<Query[]> {
    QueryArrayNode(Query[] query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Query query : item) {
        bufferedUpdates.addQuery(query, docIDUpto);
      }
    }
  }

  private static final class TermArrayNode extends Node<Term[]> {
    TermArrayNode(Term[] term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Term term : item) {
        bufferedUpdates.addTerm(term, docIDUpto);
      }
    }

    @Override
    public String toString() {
      return "dels=" + Arrays.toString(item);
    }
  }

  private static final class DocValuesUpdatesNode extends Node<DocValuesUpdate[]> {

    DocValuesUpdatesNode(DocValuesUpdate... updates) {
      super(updates);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (DocValuesUpdate update : item) {
        switch (update.type) {
          case NUMERIC:
            bufferedUpdates.addNumericUpdate((NumericDocValuesUpdate) update, docIDUpto);
            break;
          case BINARY:
            bufferedUpdates.addBinaryUpdate((BinaryDocValuesUpdate) update, docIDUpto);
            break;
          case NONE:
          case SORTED:
          case SORTED_SET:
          case SORTED_NUMERIC:
          default:
            throw new IllegalArgumentException(
                update.type + " DocValues updates not supported yet!");
        }
      }
    }

    @Override
    boolean isDelete() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("docValuesUpdates: ");
      if (item.length > 0) {
        sb.append("term=").append(item[0].term).append("; updates: [");
        for (DocValuesUpdate update : item) {
          sb.append(update.field).append(':').append(update.valueToString()).append(',');
        }
        sb.setCharAt(sb.length() - 1, ']');
      }
      return sb.toString();
    }
  }

  public int getBufferedUpdatesTermsSize() {
    final ReentrantLock lock = globalBufferLock; // Trusted final
    lock.lock();
    try {
      final Node<?> currentTail = tail;
      if (globalSlice.sliceTail != currentTail) {
        globalSlice.sliceTail = currentTail;
        globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
      }
      return globalBufferedUpdates.deleteTerms.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long ramBytesUsed() {
    return globalBufferedUpdates.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "DWDQ: [ generation: " + generation + " ]";
  }

  public long getNextSequenceNumber() {
    long seqNo = nextSeqNo.getAndIncrement();
    assert seqNo <= maxSeqNo : "seqNo=" + seqNo + " vs maxSeqNo=" + maxSeqNo;
    return seqNo;
  }

  long getLastSequenceNumber() {
    return nextSeqNo.get() - 1;
  }

  /**
   * Inserts a gap in the sequence numbers. This is used by IW during flush or commit to ensure any
   * in-flight threads get sequence numbers inside the gap
   */
  void skipSequenceNumbers(long jump) {
    nextSeqNo.addAndGet(jump);
  }

  /** Returns the maximum completed seq no for this queue. */
  long getMaxCompletedSeqNo() {
    if (startSeqNo < nextSeqNo.get()) {
      return getLastSequenceNumber();
    } else {
      // if we haven't advanced the seqNo make sure we fall back to the previous queue
      long value = previousMaxSeqId.getAsLong();
      assert value < startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
      return value;
    }
  }

  // we use a static method to get this lambda since we previously introduced a memory leak since it
  // would
  // implicitly reference this.nextSeqNo which holds on to this del queue. see LUCENE-9478 for
  // reference
  private static LongSupplier getPrevMaxSeqIdSupplier(AtomicLong nextSeqNo) {
    return () -> nextSeqNo.get() - 1;
  }

  /**
   * Advances the queue to the next queue on flush. This carries over the the generation to the next
   * queue and set the {@link #getMaxSeqNo()} based on the given maxNumPendingOps. This method can
   * only be called once, subsequently the returned queue should be used.
   *
   * @param maxNumPendingOps the max number of possible concurrent operations that will execute on
   *     this queue after it was advanced. This corresponds the the number of DWPTs that own the
   *     current queue at the moment when this queue is advanced since each these DWPTs can
   *     increment the seqId after we advanced it.
   * @return a new queue as a successor of this queue.
   */
  synchronized DocumentsWriterDeleteQueue advanceQueue(int maxNumPendingOps) {
    if (advanced) {
      throw new IllegalStateException("queue was already advanced");
    }
    advanced = true;
    long seqNo = getLastSequenceNumber() + maxNumPendingOps + 1;
    maxSeqNo = seqNo;
    return new DocumentsWriterDeleteQueue(
        infoStream,
        generation + 1,
        seqNo + 1,
        // don't pass ::getMaxCompletedSeqNo here b/c otherwise we keep an reference to this queue
        // and this will be a memory leak since the queues can't be GCed
        getPrevMaxSeqIdSupplier(nextSeqNo));
  }

  /**
   * Returns the maximum sequence number for this queue. This value will change once this queue is
   * advanced.
   */
  long getMaxSeqNo() {
    return maxSeqNo;
  }

  /** Returns <code>true</code> if it was advanced. */
  boolean isAdvanced() {
    return advanced;
  }
}
