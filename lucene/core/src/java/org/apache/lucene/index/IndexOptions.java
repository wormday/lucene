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

/**
 * 控制 posting list 中存储那些信息。
 *
 * @lucene.experimental
 */
public enum IndexOptions {
  // NOTE: order is important here; FieldInfo uses this
  // order to merge two conflicting IndexOptions (always
  // "downgrades" by picking the lowest).
  /** Not indexed */
  NONE,

  /**
   * 只有文档被索引: 忽略词频、位置信息。 在该字段上查询短语(Phrase)和其他位置信息将抛出异常。
   * and scoring will behave as if any term in the document appears only once.
   */
  DOCS,

  /**
   * 只有文档和词频被索引: 位置信息被忽略。 支持正常的算分。
   * except Phrase and other positional queries will throw an exception.
   */
  DOCS_AND_FREQS,

  /**
   * 索引文档、词频、位置信息。 这是全文搜索的典型默认值  :
   * full scoring is enabled and positional queries are supported.
   */
  DOCS_AND_FREQS_AND_POSITIONS,

  /**
   * Indexes documents, frequencies, positions and offsets. Character offsets are encoded alongside
   * the positions.
   */
  DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
}
