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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.store.Directory;

/**
 * An {@link IndexDeletionPolicy} that wraps any other {@link IndexDeletionPolicy} and adds the
 * ability to hold and later release snapshots of an index. While a snapshot is held, the {@link
 * IndexWriter} will not remove any files associated with it even if the index is otherwise being
 * actively, arbitrarily changed. Because we wrap another arbitrary {@link IndexDeletionPolicy},
 * this gives you the freedom to continue using whatever {@link IndexDeletionPolicy} you would
 * normally want to use with your index.
 *
 * <p>这个类在内存中维护所有快照, 因此信息不会持久保存，也不能抵御系统故障. 如果持久保存非常重要，
 * 你可以使用 {@link PersistentSnapshotDeletionPolicy}.
 *
 * @lucene.experimental
 */
public class SnapshotDeletionPolicy extends IndexDeletionPolicy {

  /** Records how many snapshots are held against each commit generation */
  protected final Map<Long, Integer> refCounts = new HashMap<>();

  /** 用于 map 代和索引提交. */
  protected final Map<Long, IndexCommit> indexCommits = new HashMap<>();

  /** Wrapped {@link IndexDeletionPolicy} */
  private final IndexDeletionPolicy primary;

  /** Most recently committed {@link IndexCommit}. */
  protected IndexCommit lastCommit;

  /** Used to detect misuse */
  private boolean initCalled;

  /** 唯一的构造函数 {@link IndexDeletionPolicy} 用于包裹. */
  public SnapshotDeletionPolicy(IndexDeletionPolicy primary) {
    this.primary = primary;
  }

  @Override
  public synchronized void onCommit(List<? extends IndexCommit> commits) throws IOException {
    primary.onCommit(wrapCommits(commits));
    lastCommit = commits.get(commits.size() - 1);
  }

  @Override
  public synchronized void onInit(List<? extends IndexCommit> commits) throws IOException {
    initCalled = true;
    primary.onInit(wrapCommits(commits));
    for (IndexCommit commit : commits) {
      if (refCounts.containsKey(commit.getGeneration())) {
        indexCommits.put(commit.getGeneration(), commit);
      }
    }
    if (!commits.isEmpty()) {
      lastCommit = commits.get(commits.size() - 1);
    }
  }

  /**
   * Release a snapshotted commit.
   *
   * @param commit the commit previously returned by {@link #snapshot}
   */
  public synchronized void release(IndexCommit commit) throws IOException {
    long gen = commit.getGeneration();
    releaseGen(gen);
  }

  /** Release a snapshot by generation. */
  protected void releaseGen(long gen) throws IOException {
    if (!initCalled) {
      throw new IllegalStateException(
          "this instance is not being used by IndexWriter; be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
    }
    Integer refCount = refCounts.get(gen);
    if (refCount == null) {
      throw new IllegalArgumentException("commit gen=" + gen + " is not currently snapshotted");
    }
    int refCountInt = refCount.intValue();
    assert refCountInt > 0;
    refCountInt--;
    if (refCountInt == 0) {
      refCounts.remove(gen);
      indexCommits.remove(gen);
    } else {
      refCounts.put(gen, refCountInt);
    }
  }

  /** Increments the refCount for this {@link IndexCommit}. */
  protected synchronized void incRef(IndexCommit ic) {
    long gen = ic.getGeneration();
    Integer refCount = refCounts.get(gen);
    int refCountInt;
    if (refCount == null) {
      indexCommits.put(gen, lastCommit);
      refCountInt = 0;
    } else {
      refCountInt = refCount.intValue();
    }
    refCounts.put(gen, refCountInt + 1);
  }

  /**
   * 快照并返回最后一个提交. 一旦一个提交‘被快照’，他就被保护，不被删除 (尽管配置了 {@link IndexDeletionPolicy})。
   * 快照可以通过调用 {@link #release(IndexCommit)}
   * 然后调用 {@link IndexWriter#deleteUnusedFiles()} 进行删除。
   *
   * <p><b>NOTE:</b> 当快照被保存时，它引用的文件不会被删除， 这会消耗索引中的额外磁盘空间。 如果你在一个特别糟糕的时间生成快照
   * (比如说恰巧在调用 forceMerge 之前) 那么在最坏的情况下，这可能会消耗总索引大小的1倍，直到你释放这个快照。
   *
   * @throws IllegalStateException 如果这个索引还没有任何提交
   * @return 被快照的那个 {@link IndexCommit}。
   */
  public synchronized IndexCommit snapshot() throws IOException {
    if (!initCalled) {
      throw new IllegalStateException(
          "this instance is not being used by IndexWriter; be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
    }
    if (lastCommit == null) {
      // No commit yet, eg this is a new IndexWriter:
      throw new IllegalStateException("No index commit to snapshot");
    }
    incRef(lastCommit);
    return lastCommit;
  }

  /** Returns all IndexCommits held by at least one snapshot. */
  public synchronized List<IndexCommit> getSnapshots() {
    return new ArrayList<>(indexCommits.values());
  }

  /** Returns the total number of snapshots currently held. */
  public synchronized int getSnapshotCount() {
    int total = 0;
    for (Integer refCount : refCounts.values()) {
      total += refCount.intValue();
    }

    return total;
  }

  /**
   * Retrieve an {@link IndexCommit} from its generation; returns null if this IndexCommit is not
   * currently snapshotted
   */
  public synchronized IndexCommit getIndexCommit(long gen) {
    return indexCommits.get(gen);
  }

  /** 将每个 {@link IndexCommit} 包装成 {@link SnapshotCommitPoint}。 */
  private List<IndexCommit> wrapCommits(List<? extends IndexCommit> commits) {
    List<IndexCommit> wrappedCommits = new ArrayList<>(commits.size());
    for (IndexCommit ic : commits) {
      wrappedCommits.add(new SnapshotCommitPoint(ic));
    }
    return wrappedCommits;
  }

  /** 包装一个提供的 {@link IndexCommit} 防止它被删除。 */
  private class SnapshotCommitPoint extends IndexCommit {

    /** 阻止被删除的 {@link IndexCommit}。 */
    protected IndexCommit cp;

    /** 为提供的 {@link IndexCommit} 创建一个包装它的 {@code SnapshotCommitPoint}。 */
    protected SnapshotCommitPoint(IndexCommit cp) {
      this.cp = cp;
    }

    @Override
    public String toString() {
      return "SnapshotDeletionPolicy.SnapshotCommitPoint(" + cp + ")";
    }

    @Override
    public void delete() {
      synchronized (SnapshotDeletionPolicy.this) {
        // Suppress the delete request if this commit point is
        // currently snapshotted.
        if (!refCounts.containsKey(cp.getGeneration())) {
          cp.delete();
        }
      }
    }

    @Override
    public Directory getDirectory() {
      return cp.getDirectory();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
      return cp.getFileNames();
    }

    @Override
    public long getGeneration() {
      return cp.getGeneration();
    }

    @Override
    public String getSegmentsFileName() {
      return cp.getSegmentsFileName();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
      return cp.getUserData();
    }

    @Override
    public boolean isDeleted() {
      return cp.isDeleted();
    }

    @Override
    public int getSegmentCount() {
      return cp.getSegmentCount();
    }
  }
}
