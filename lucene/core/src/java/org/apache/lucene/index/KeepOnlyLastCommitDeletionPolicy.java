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

import java.util.List;

/**
 * 这个 {@link IndexDeletionPolicy} 的实现，仅保存最新的提交，
 * 当一个新提交产生，先前的所有提交都会立即删除。这是默认的删除策略。
 */
public final class KeepOnlyLastCommitDeletionPolicy extends IndexDeletionPolicy {

  /** 唯一的构造函数。 */
  public KeepOnlyLastCommitDeletionPolicy() {}

  /** 删除所有的提交，除了最新的那一个。 */
  @Override
  public void onInit(List<? extends IndexCommit> commits) {
    // Note that commits.size() should normally be 1:
    onCommit(commits);
  }

  /** 删除所有的提交，除了最新的那一个。 */
  @Override
  public void onCommit(List<? extends IndexCommit> commits) {
    // Note that commits.size() should normally be 2 (if not
    // called by onInit above):
    int size = commits.size();
    for (int i = 0; i < size - 1; i++) {
      commits.get(i).delete();
    }
  }
}
