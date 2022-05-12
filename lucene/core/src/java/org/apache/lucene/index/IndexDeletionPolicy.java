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
import java.util.List;

/**
 * Expert: policy for deletion of stale {@link IndexCommit index commits}.
 *
 * <p>Implement this interface, and set it on {@link
 * IndexWriterConfig#setIndexDeletionPolicy(IndexDeletionPolicy)} to customize when older {@link
 * IndexCommit point-in-time commits} are deleted from the index directory.
 *
 * <p>默认的删除策略为 {@link KeepOnlyLastCommitDeletionPolicy}, 当新的提交产生是，总是删除旧的提交 (这与 2.2 之前的策略一致).
 *
 * <p>One expected use case for this (它最初被创建的原因) is to work around
 * problems with an index directory accessed via filesystems like NFS because NFS does not provide
 * the "delete on last close" semantics that Lucene's "point in time" search normally relies on. By
 * implementing a custom deletion policy, such as "提交只有在过期超过X分钟后才会被删除  ",
 * you can give your readers time to refresh to the new commit before
 * {@link IndexWriter} removes the old commits. 注意，这样做将增加索引的存储需求 .
 * 查看 <a target="top" href="http://issues.apache.org/jira/browse/LUCENE-710">LUCENE-710</a> 了解详情.
 */
public abstract class IndexDeletionPolicy {

  /** 唯一的构造函数，通常由子类的构造函数调用。 */
  protected IndexDeletionPolicy() {}

  /**
   * 第一次实例化 writer 时调用这个方法，以便给策略一个机会来删除旧的提交点。
   *
   * <p>The writer locates all index commits present in the index directory and calls this method.
   * The policy may choose to delete some of the commit points, doing so by calling method {@link
   * IndexCommit#delete delete()} of {@link IndexCommit}.
   *
   * <p><u>Note:</u> 最后的 CommitPoint 是最新的一个, 即 "front index state"。
   * 注意不要删除它，除非您确实知道自己在做什么, 除非你能够承担丢失索引内容的损失。
   *
   * @param commits List of current {@link IndexCommit point-in-time commits}, sorted by age (the
   *     0th one is the oldest commit). Note that for a new index this method is invoked with an
   *     empty list.
   */
  public abstract void onInit(List<? extends IndexCommit> commits) throws IOException;

  /**
   * writer 完成一个提交后调用这个方法，以便给策略一个机会来删除旧的提交点。
   *
   * <p>The policy may now choose to delete old commit points by calling method {@link
   * IndexCommit#delete delete()} of {@link IndexCommit}.
   *
   * <p>This method is only called when {@link IndexWriter#commit} or {@link IndexWriter#close} is
   * called, or possibly not at all if the {@link IndexWriter#rollback} is called.
   *
   * <p><u>Note:</u> 最后的 CommitPoint 是最新的一个, 即 "front index state"。
   * 注意不要删除它，除非您确实知道自己在做什么, 除非你能够承担丢失索引内容的损失。
   *
   * @param commits List of {@link IndexCommit}, sorted by age (the 0th one is the oldest commit).
   */
  public abstract void onCommit(List<? extends IndexCommit> commits) throws IOException;
}
