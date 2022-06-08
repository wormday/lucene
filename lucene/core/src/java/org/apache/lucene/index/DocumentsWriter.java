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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOConsumer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/**
 * 这个类接受多个添加的文档，并直接写入段文件。
 *
 * <p>每个添加的文档被传递到索引链，索引链依次将文档处理为不同的编解码器格式 codec 格式。
 * 有些格式会立即将字节写入文件，例如 stored fields 和 term vectors,
 * 而其他格式会被索引链缓冲，只在刷新时写入。
 *
 * <p>一旦我们开启了 RAM 缓存， 添加的文档数量足够大(在这种情况下，我们按文档计数而不是按RAM使用量刷新),
 * 我们创建一个真正的段并将其刷新到Directory。
 *
 * <p>Threads:
 *
 * <p>Multiple threads are allowed into addDocument at once. There is an initial synchronized call
 * to {@link DocumentsWriterFlushControl#obtainAndLock()} which allocates a DWPT for this indexing
 * thread. 随着时间的推移，相同的线程不一定会获得相同的 DWPT。 然后在没有同步的情况下调用 DWPT 的 updateDocuments
 * (大部分的 "脏活累活" 都在这里). 一旦一个 DWPT 填满了足够的内存 或者 在内存中持有了足够的文档，
 * 就会检出(checked out) DWPT 进行刷新，并将所有更改写入该目录。 每个DWPT对应于一个正在写入的段。
 *
 * <p>当 flush 被 IndexWriter 调用时，我们从{@link DocumentsWriterPerThreadPool}中签出所有与当前
 * {@link DocumentsWriterDeleteQueue}关联的DWPTs，并将它们写入磁盘。
 * 刷新进程可以利用进入的索引线程，如果刷新不能跟上添加的新文档，甚至可以阻止它们添加文档。 除非暂停控制阻止索引线程，否则实际的索引请求都会并发地发生刷新。
 *
 * The flush process can piggy-back on incoming indexing threads or even block
 * them from adding documents if flushing can't keep up with new documents being added. Unless the
 * stall control kicks in to block indexing threads flushes are happening concurrently to actual
 * index requests.
 *
 * <p>Exceptions:
 *
 * <p>因为这个类直接更新内存中的 posting lists, 并且刷新 stored fields 和 term vectors 直接到目录中的文件,
 * there are certain limited times when an
 * exception can corrupt this state. For example, a disk full while flushing stored fields leaves
 * this file in a corrupt state. Or, an OOM exception while appending to the in-memory posting lists
 * can corrupt that posting list. We call such exceptions "aborting exceptions". In these cases we
 * must call abort() to discard all docs added since the last flush.
 *
 * <p>All other exceptions ("non-aborting exceptions") can still partially update the index
 * structures. These updates are consistent, but, they represent only a part of the document seen up
 * until the exception was hit. When this happens, we immediately mark the document as deleted so
 * that the document is always atomically ("all or none") added to the index.
 */
final class DocumentsWriter implements Closeable, Accountable {
  private final AtomicLong pendingNumDocs;

  private final FlushNotifications flushNotifications;

  private volatile boolean closed;

  private final InfoStream infoStream;

  private final LiveIndexWriterConfig config;

  private final AtomicInteger numDocsInRAM = new AtomicInteger(0);

  // TODO: cut over to BytesRefHash in BufferedDeletes
  volatile DocumentsWriterDeleteQueue deleteQueue;
  private final DocumentsWriterFlushQueue ticketQueue = new DocumentsWriterFlushQueue();
  /*
   * we preserve changes during a full flush since IW might not checkout before
   * we release all changes. NRT Readers otherwise suddenly return true from
   * isCurrent while there are actually changes currently committed. See also
   * #anyChanges() & #flushAllThreads
   */
  private volatile boolean pendingChangesInCurrentFullFlush;

  final DocumentsWriterPerThreadPool perThreadPool;
  final DocumentsWriterFlushControl flushControl;

  DocumentsWriter(
      FlushNotifications flushNotifications,
      int indexCreatedVersionMajor,
      AtomicLong pendingNumDocs,
      boolean enableTestPoints,
      Supplier<String> segmentNameSupplier,
      LiveIndexWriterConfig config,
      Directory directoryOrig,
      Directory directory,
      FieldInfos.FieldNumbers globalFieldNumberMap) {
    this.config = config;
    this.infoStream = config.getInfoStream();
    this.deleteQueue = new DocumentsWriterDeleteQueue(infoStream);
    this.perThreadPool =
        new DocumentsWriterPerThreadPool(
            () -> {
              final FieldInfos.Builder infos = new FieldInfos.Builder(globalFieldNumberMap);
              return new DocumentsWriterPerThread(
                  indexCreatedVersionMajor,
                  segmentNameSupplier.get(),
                  directoryOrig,
                  directory,
                  config,
                  deleteQueue,
                  infos,
                  pendingNumDocs,
                  enableTestPoints);
            });
    this.pendingNumDocs = pendingNumDocs;
    flushControl = new DocumentsWriterFlushControl(this, config);
    this.flushNotifications = flushNotifications;
  }

  long deleteQueries(final Query... queries) throws IOException {
    return applyDeleteOrUpdate(q -> q.addDelete(queries));
  }

  long deleteTerms(final Term... terms) throws IOException {
    return applyDeleteOrUpdate(q -> q.addDelete(terms));
  }

  long updateDocValues(DocValuesUpdate... updates) throws IOException {
    return applyDeleteOrUpdate(q -> q.addDocValuesUpdates(updates));
  }

  private synchronized long applyDeleteOrUpdate(ToLongFunction<DocumentsWriterDeleteQueue> function)
      throws IOException {
    // 这个方法是同步的，以确保我们在应用这个更新/删除时不会替换 deleteQueue
    // 否则，如果更新/删除同时进行到完全刷新，则可能会丢失更新/删除。
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    long seqNo = function.applyAsLong(deleteQueue);
    flushControl.doOnDelete();
    if (applyAllDeletes()) {
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /** 如果删除缓冲区占用堆太多, 清理他们，保存磁盘，并返回 true。 */
  private boolean applyAllDeletes() throws IOException {
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;

    if (flushControl.isFullFlush() == false
        // never apply deletes during full flush this breaks happens before relationship
        && deleteQueue.isOpen()
        // if it's closed then it's already fully applied and we have a new delete queue
        && flushControl.getAndResetApplyAllDeletes()) {
      if (ticketQueue.addDeletes(deleteQueue)) {
        flushNotifications.onDeletesApplied(); // apply deletes event forces a purge
        return true;
      }
    }
    return false;
  }

  void purgeFlushTickets(boolean forced, IOConsumer<DocumentsWriterFlushQueue.FlushTicket> consumer)
      throws IOException {
    if (forced) {
      ticketQueue.forcePurge(consumer);
    } else {
      ticketQueue.tryPurge(consumer);
    }
  }

  /** Returns how many docs are currently buffered in RAM. */
  int getNumDocs() {
    return numDocsInRAM.get();
  }

  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this DocumentsWriter is closed");
    }
  }

  /**
   * Called if we hit an exception at a bad time (when updating the index files) and must discard
   * all currently buffered docs. This resets our state, discarding any docs added since last flush.
   */
  synchronized void abort() throws IOException {
    boolean success = false;
    try {
      deleteQueue.clear();
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "abort");
      }
      for (final DocumentsWriterPerThread perThread : perThreadPool.filterAndLock(x -> true)) {
        try {
          abortDocumentsWriterPerThread(perThread);
        } finally {
          perThread.unlock();
        }
      }
      flushControl.abortPendingFlushes();
      flushControl.waitForFlush();
      assert perThreadPool.size() == 0
          : "There are still active DWPT in the pool: " + perThreadPool.size();
      success = true;
    } finally {
      if (success) {
        assert flushControl.getFlushingBytes() == 0
            : "flushingBytes has unexpected value 0 != " + flushControl.getFlushingBytes();
        assert flushControl.netBytes() == 0
            : "netBytes has unexpected value 0 != " + flushControl.netBytes();
      }
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "done abort success=" + success);
      }
    }
  }

  final boolean flushOneDWPT() throws IOException {
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "startFlushOneDWPT");
    }
    // first check if there is one pending
    DocumentsWriterPerThread documentsWriterPerThread = flushControl.nextPendingFlush();
    if (documentsWriterPerThread == null) {
      documentsWriterPerThread = flushControl.checkoutLargestNonPendingWriter();
    }
    if (documentsWriterPerThread != null) {
      return doFlush(documentsWriterPerThread);
    }
    return false; // we didn't flush anything here
  }

  /**
   * Locks all currently active DWPT and aborts them. The returned Closeable should be closed once
   * the locks for the aborted DWPTs can be released.
   */
  synchronized Closeable lockAndAbortAll() throws IOException {
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "lockAndAbortAll");
    }
    // Make sure we move all pending tickets into the flush queue:
    ticketQueue.forcePurge(
        ticket -> {
          if (ticket.getFlushedSegment() != null) {
            pendingNumDocs.addAndGet(-ticket.getFlushedSegment().segmentInfo.info.maxDoc());
          }
        });
    List<DocumentsWriterPerThread> writers = new ArrayList<>();
    AtomicBoolean released = new AtomicBoolean(false);
    final Closeable release =
        () -> {
          // we return this closure to unlock all writers once done
          // or if hit an exception below in the try block.
          // we can't assign this later otherwise the ref can't be final
          if (released.compareAndSet(false, true)) { // only once
            if (infoStream.isEnabled("DW")) {
              infoStream.message("DW", "unlockAllAbortedThread");
            }
            perThreadPool.unlockNewWriters();
            for (DocumentsWriterPerThread writer : writers) {
              writer.unlock();
            }
          }
        };
    try {
      deleteQueue.clear();
      perThreadPool.lockNewWriters();
      writers.addAll(perThreadPool.filterAndLock(x -> true));
      for (final DocumentsWriterPerThread perThread : writers) {
        assert perThread.isHeldByCurrentThread();
        abortDocumentsWriterPerThread(perThread);
      }
      deleteQueue.clear();

      // jump over any possible in flight ops:
      deleteQueue.skipSequenceNumbers(perThreadPool.size() + 1);

      flushControl.abortPendingFlushes();
      flushControl.waitForFlush();
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "finished lockAndAbortAll success=true");
      }
      return release;
    } catch (Throwable t) {
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "finished lockAndAbortAll success=false");
      }
      try {
        // if something happens here we unlock all states again
        release.close();
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }

  /** Returns how many documents were aborted. */
  private void abortDocumentsWriterPerThread(final DocumentsWriterPerThread perThread)
      throws IOException {
    assert perThread.isHeldByCurrentThread();
    try {
      subtractFlushedNumDocs(perThread.getNumDocsInRAM());
      perThread.abort();
    } finally {
      flushControl.doOnAbort(perThread);
    }
  }

  /** returns the maximum sequence number for all previously completed operations */
  long getMaxCompletedSequenceNumber() {
    return deleteQueue.getMaxCompletedSeqNo();
  }

  boolean anyChanges() {
    /*
     * 更改要么在 DWPT 中，要么在删除队列中。
     * 然而如果我们现在刷新删除和dwpt there
     * could be a window where all changes are in the ticket queue
     * before they are published to the IW. ie we need to check if the
     * ticket queue has any tickets.
     */
    boolean anyChanges =
        numDocsInRAM.get() != 0
            || anyDeletions()
            || ticketQueue.hasTickets()
            || pendingChangesInCurrentFullFlush;
    if (infoStream.isEnabled("DW") && anyChanges) {
      infoStream.message(
          "DW",
          "anyChanges? numDocsInRam="
              + numDocsInRAM.get()
              + " deletes="
              + anyDeletions()
              + " hasTickets:"
              + ticketQueue.hasTickets()
              + " pendingChangesInFullFlush: "
              + pendingChangesInCurrentFullFlush);
    }
    return anyChanges;
  }

  int getBufferedDeleteTermsSize() {
    return deleteQueue.getBufferedUpdatesTermsSize();
  }

  // for testing
  int getNumBufferedDeleteTerms() {
    return deleteQueue.numGlobalTermDeletes();
  }

  boolean anyDeletions() {
    return deleteQueue.anyChanges();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    IOUtils.close(flushControl, perThreadPool);
  }

  private boolean preUpdate() throws IOException {
    ensureOpen();
    boolean hasEvents = false;
    // 修改stalled的状态的地方很多，但是判断这个状态的，只有这个地方
    while (flushControl.anyStalledThreads()
        || (flushControl.numQueuedFlushes() > 0 && config.checkPendingFlushOnUpdate)) {
      // Help out flushing any queued DWPTs so we can un-stall:
      // 如果可能的话，尝试在获取挂起的线程
      DocumentsWriterPerThread flushingDWPT;
      while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
        // Don't push the delete here since the update could fail!
        hasEvents |= doFlush(flushingDWPT);
      }
      // 如果flush速度远低于index速度，这里会被阻塞一会儿
      flushControl.waitIfStalled(); // block if stalled
    }
    return hasEvents;
  }

  private boolean postUpdate(DocumentsWriterPerThread flushingDWPT, boolean hasEvents)
      throws IOException {
    hasEvents |= applyAllDeletes();
    if (flushingDWPT != null) {
      hasEvents |= doFlush(flushingDWPT);
    } else if (config.checkPendingFlushOnUpdate) {
      final DocumentsWriterPerThread nextPendingFlush = flushControl.nextPendingFlush();
      if (nextPendingFlush != null) {
        hasEvents |= doFlush(nextPendingFlush);
      }
    }

    return hasEvents;
  }

  long updateDocuments(
      final Iterable<? extends Iterable<? extends IndexableField>> docs,
      final DocumentsWriterDeleteQueue.Node<?> delNode)
      throws IOException {
    boolean hasEvents = preUpdate();
    final DocumentsWriterPerThread dwpt = flushControl.obtainAndLock();
    final DocumentsWriterPerThread flushingDWPT;
    long seqNo;

    try {
      // This must happen after we've pulled the DWPT because IW.close
      // waits for all DWPT to be released:
      ensureOpen();
      try {
        seqNo =
            dwpt.updateDocuments(docs, delNode, flushNotifications, numDocsInRAM::incrementAndGet);
      } finally {
        if (dwpt.isAborted()) {
          flushControl.doOnAbort(dwpt);
        }
      }
      final boolean isUpdate = delNode != null && delNode.isDelete();
      flushingDWPT = flushControl.doAfterDocument(dwpt, isUpdate);
    } finally {
      if (dwpt.isFlushPending() || dwpt.isAborted()) {
        dwpt.unlock();
      } else {
        perThreadPool.marksAsFreeAndUnlock(dwpt);
      }
      assert dwpt.isHeldByCurrentThread() == false : "we didn't release the dwpt even on abort";
    }

    if (postUpdate(flushingDWPT, hasEvents)) {
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /**
   * 主动flush跟自动flush都是执行DWPT的doFlush( )，但是DWPT的来源是不一样的
   */
  private boolean  doFlush(DocumentsWriterPerThread flushingDWPT) throws IOException {
    boolean hasEvents = false;
    while (flushingDWPT != null) {
      assert flushingDWPT.hasFlushed() == false;
      hasEvents = true;
      boolean success = false;
      // ticket是 frozenUpdates 和 segment 的封装
      DocumentsWriterFlushQueue.FlushTicket ticket = null;
      try {
        assert currentFullFlushDelQueue == null
                || flushingDWPT.deleteQueue == currentFullFlushDelQueue
            : "expected: "
                + currentFullFlushDelQueue
                + "but was: "
                + flushingDWPT.deleteQueue
                + " "
                + flushControl.isFullFlush();
        /*
         * 因为使用了DWPT刷新过程是并发的多个DWPT可以同时刷新， we must maintain the order of the
         * flushes before we can apply the flushed segment and the frozen global
         * deletes it is buffering. The reason for this is that the global
         * deletes mark a certain point in time where we took a DWPT out of
         * rotation and freeze the global deletes.
         *
         * 比如：刷新'A'启动并冻结全局删除，然后刷新'B'启动并冻结自'A'启动以来发生的所有删除。
         * 如果“B”在“A”之前完成，我们需要等待“A”完成，否则被“B”冻结的删除不会应用于“A”，
         * 我们可能会错过删除“A”中的文档。
         */
        try {
          assert assertTicketQueueModification(flushingDWPT.deleteQueue);
          // 每次刷新都按照获取ticketQueue锁的顺序分配一个票据
          // 内部逻辑为，根据DWPT的 DocumentsWriterDeleteQueue 实例 deleteQueue
          // 生成 FrozenBufferedUpdates 进一步生成 FlushTicket
          // 为什么 FrozenBufferedUpdates 分别要生成全局和DWPT两份数据？
          ticket = ticketQueue.addFlushTicket(flushingDWPT);
          final int flushingDocsInRam = flushingDWPT.getNumDocsInRAM();
          boolean dwptSuccess = false;
          try {
            // 无锁并发刷新逻辑
            final FlushedSegment newSegment = flushingDWPT.flush(flushNotifications);
            // 为什么前边生成了ticket，然后生成newSegment，才把ticket补充上newSegment信息
            // 而不是直接生成ticket的时候，就补充上newSegment
            // 这里的addSegment并不是给ticketQueue添加segment,而是给ticket添加segment
            ticketQueue.addSegment(ticket, newSegment);
            dwptSuccess = true;
          } finally {
            subtractFlushedNumDocs(flushingDocsInRam);
            if (flushingDWPT.pendingFilesToDelete().isEmpty() == false) {
              Set<String> files = flushingDWPT.pendingFilesToDelete();
              flushNotifications.deleteUnusedFiles(files);
              hasEvents = true;
            }
            if (dwptSuccess == false) {
              flushNotifications.flushFailed(flushingDWPT.getSegmentInfo());
              hasEvents = true;
            }
          }
          // flush was successful once we reached this point - new seg. has been assigned to the
          // ticket!
          success = true;
        } finally {
          if (!success && ticket != null) {
            // In the case of a failure make sure we are making progress and
            // apply all the deletes since the segment flush failed since the flush
            // ticket could hold global deletes see FlushTicket#canPublish()
            ticketQueue.markTicketFailed(ticket);
          }
        }
        /*
         * Now we are done and try to flush the ticket queue if the head of the
         * queue has already finished the flush.
         */
        if (ticketQueue.getTicketCount() >= perThreadPool.size()) {
          // This means there is a backlog: the one
          // thread in innerPurge can't keep up with all
          // other threads flushing segments.  In this case
          // we forcefully stall the producers.
          flushNotifications.onTicketBacklog();
          break;
        }
      } finally {
        flushControl.doAfterFlush(flushingDWPT);
      }

      flushingDWPT = flushControl.nextPendingFlush();
    }

    if (hasEvents) {
      flushNotifications.afterSegmentsFlushed();
    }

    // If deletes alone are consuming > 1/2 our RAM
    // buffer, force them all to apply now. This is to
    // prevent too-frequent flushing of a long tail of
    // tiny segments:
    final double ramBufferSizeMB = config.getRAMBufferSizeMB();
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH
        && flushControl.getDeleteBytesUsed() > (1024 * 1024 * ramBufferSizeMB / 2)) {
      hasEvents = true;
      if (applyAllDeletes() == false) {
        if (infoStream.isEnabled("DW")) {
          infoStream.message(
              "DW",
              String.format(
                  Locale.ROOT,
                  "force apply deletes after flush bytesUsed=%.1f MB vs ramBuffer=%.1f MB",
                  flushControl.getDeleteBytesUsed() / (1024. * 1024.),
                  ramBufferSizeMB));
        }
        flushNotifications.onDeletesApplied();
      }
    }

    return hasEvents;
  }

  synchronized long getNextSequenceNumber() {
    // this must be synced otherwise the delete queue might change concurrently
    return deleteQueue.getNextSequenceNumber();
  }

  synchronized void resetDeleteQueue(DocumentsWriterDeleteQueue newQueue) {
    assert deleteQueue.isAdvanced();
    assert newQueue.isAdvanced() == false;
    assert deleteQueue.getLastSequenceNumber() <= newQueue.getLastSequenceNumber();
    assert deleteQueue.getMaxSeqNo() <= newQueue.getLastSequenceNumber()
        : "maxSeqNo: " + deleteQueue.getMaxSeqNo() + " vs. " + newQueue.getLastSequenceNumber();
    deleteQueue = newQueue;
  }

  interface FlushNotifications { // TODO maybe we find a better name for this?

    /**
     * Called when files were written to disk that are not used anymore. It's the implementation's
     * responsibility to clean these files up
     */
    void deleteUnusedFiles(Collection<String> files);

    /** Called when a segment failed to flush. */
    void flushFailed(SegmentInfo info);

    /** Called after one or more segments were flushed to disk. */
    void afterSegmentsFlushed() throws IOException;

    /**
     * Should be called if a flush or an indexing operation caused a tragic / unrecoverable event.
     */
    void onTragicEvent(Throwable event, String message);

    /** Called once deletes have been applied either after a flush or on a deletes call */
    void onDeletesApplied();

    /**
     * Called once the DocumentsWriter ticket queue has a backlog. This means there is an inner
     * thread that tries to publish flushed segments but can't keep up with the other threads
     * flushing new segments. This likely requires other thread to forcefully purge the buffer to
     * help publishing. This can't be done in-place since we might hold index writer locks when this
     * is called. The caller must ensure that the purge happens without an index writer lock being
     * held.
     *
     * @see DocumentsWriter#purgeFlushTickets(boolean, IOConsumer)
     */
    void onTicketBacklog();
  }

  void subtractFlushedNumDocs(int numFlushed) {
    int oldValue = numDocsInRAM.get();
    while (numDocsInRAM.compareAndSet(oldValue, oldValue - numFlushed) == false) {
      oldValue = numDocsInRAM.get();
    }
    assert numDocsInRAM.get() >= 0;
  }

  // for asserts
  private volatile DocumentsWriterDeleteQueue currentFullFlushDelQueue = null;

  // for asserts
  private synchronized boolean setFlushingDeleteQueue(DocumentsWriterDeleteQueue session) {
    assert currentFullFlushDelQueue == null || currentFullFlushDelQueue.isOpen() == false
        : "Can not replace a full flush queue if the queue is not closed";
    currentFullFlushDelQueue = session;
    return true;
  }

  private boolean assertTicketQueueModification(DocumentsWriterDeleteQueue deleteQueue) {
    // assign it then we don't need to sync on DW
    DocumentsWriterDeleteQueue currentFullFlushDelQueue = this.currentFullFlushDelQueue;
    assert currentFullFlushDelQueue == null || currentFullFlushDelQueue == deleteQueue
        : "only modifications from the current flushing queue are permitted while doing a full flush";
    return true;
  }

  /*
   * FlushAllThreads通过IW的fullFlushLock同步， 刷新所有线程分为两个阶段。
   * 调用者必须确保(in try/finally) finishFlush 必须在这个方法之后调用  that finishFlush
   * 以释放 DWFlushControl 中的 flush lock。
   *
   * 添加/更新文档跟执行主动flush是并行操作，当某个线程执行主动flush后，
   * 随后其他线程获得的新生成的DWPT 不能在这次的flush作用范围内，
   * 又因为DWPT的构造函数的其中一个参数就是deleteQueue，
   * 故可通过判断DWPT对象中持有的deleteQueue对象来判断它是否在此次的flush的作用范围内。
   *
   * 故当某个线程执行了主动flush后，保存当前的全局变量deleteQueue为flushingQueue，
   * 然后生成一个新的删除队列newQueue更新为全局变量deleteQueue，
   * 其他线程新生成的DWPT则会使用新的deleteQueue，
   * 所以任意时刻最多只能存在两个deleteQueue，
   * 一个是正在主动flush使用的用于区分flush作用域的flushingQueue，
   * 另一个则是下一次flush需要用到的newQueue。
   *
   */
  long flushAllThreads() throws IOException {
    final DocumentsWriterDeleteQueue flushingDeleteQueue;
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "startFullFlush");
    }

    long seqNo;
    synchronized (this) {
      pendingChangesInCurrentFullFlush = anyChanges();
      flushingDeleteQueue = deleteQueue;
      // 切换到新的删除队列，这必须在flushControl中同步，
      /* Cutover to a new delete queue.  This must be synced on the flush control
       * otherwise a new DWPT could sneak into the loop with an already flushing
       * delete queue */
      seqNo = flushControl.markForFullFlush(); // 在FlushControl同步的替换 this.deleteQueue
      assert setFlushingDeleteQueue(flushingDeleteQueue);
    }
    assert currentFullFlushDelQueue != null;
    assert currentFullFlushDelQueue != deleteQueue;

    boolean anythingFlushed = false;
    try {
      DocumentsWriterPerThread flushingDWPT;
      // 找出需要flush的dwpt
      while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
        // 针对dwpt逐个执行doFlush逻辑
        anythingFlushed |= doFlush(flushingDWPT);
      }
      // If a concurrent flush is still in flight wait for it
      flushControl.waitForFlush();
      if (anythingFlushed == false
          && flushingDeleteQueue.anyChanges()) { // apply deletes if we did not flush any document
        if (infoStream.isEnabled("DW")) {
          infoStream.message(
              "DW", Thread.currentThread().getName() + ": flush naked frozen global deletes");
        }
        assert assertTicketQueueModification(flushingDeleteQueue);
        ticketQueue.addDeletes(flushingDeleteQueue);
      }
      // we can't assert that we don't have any tickets in teh queue since we might add a
      // DocumentsWriterDeleteQueue
      // concurrently if we have very small ram buffers this happens quite frequently
      assert !flushingDeleteQueue.anyChanges();
    } finally {
      assert flushingDeleteQueue == currentFullFlushDelQueue;
      flushingDeleteQueue
          .close(); // all DWPT have been processed and this queue has been fully flushed to the
      // ticket-queue
    }
    if (anythingFlushed) {
      return -seqNo;
    } else {
      return seqNo;
    }
  }

  void finishFullFlush(boolean success) throws IOException {
    try {
      if (infoStream.isEnabled("DW")) {
        infoStream.message(
            "DW", Thread.currentThread().getName() + " finishFullFlush success=" + success);
      }
      assert setFlushingDeleteQueue(null);
      if (success) {
        // Release the flush lock
        flushControl.finishFullFlush();
      } else {
        flushControl.abortFullFlushes();
      }
    } finally {
      pendingChangesInCurrentFullFlush = false;
      applyAllDeletes(); // make sure we do execute this since we block applying deletes during full
      // flush
    }
  }

  @Override
  public long ramBytesUsed() {
    return flushControl.ramBytesUsed();
  }

  /**
   * Returns the number of bytes currently being flushed
   *
   * <p>This is a subset of the value returned by {@link #ramBytesUsed()}
   */
  long getFlushingBytes() {
    return flushControl.getFlushingBytes();
  }
}
