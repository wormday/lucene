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
 * {@link IndexDeletionPolicy} 保存所有索引的提交, 从不删除他们。
 *  这个类是一个单例对象，可以通过引用 {@link #INSTANCE} 进行访问。
 */
public final class NoDeletionPolicy extends IndexDeletionPolicy {

  /** 这个类的单例对象。 */
  public static final IndexDeletionPolicy INSTANCE = new NoDeletionPolicy();

  private NoDeletionPolicy() {
    // keep private to avoid instantiation
  }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) {}

  @Override
  public void onInit(List<? extends IndexCommit> commits) {}
}
