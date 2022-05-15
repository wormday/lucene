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
package org.apache.lucene.document;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

/**
 * 一个索引但不分词的字段: 整个字符串按照单个词索引。
 * 比如它可以用于国家 'country' 或者 'id' 字段。 如果你打算在这个字段上排序，
 * 在你的文档上再加上一个 {@link SortedDocValuesField} 。
 */
public final class StringField extends Field {

  /** Indexed, not tokenized, omits norms, indexes DOCS_ONLY, not stored. */
  public static final FieldType TYPE_NOT_STORED = new FieldType();

  /** Indexed, not tokenized, omits norms, indexes DOCS_ONLY, stored */
  public static final FieldType TYPE_STORED = new FieldType();

  static {
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_NOT_STORED.setTokenized(false);
    TYPE_NOT_STORED.freeze();

    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_STORED.setStored(true);
    TYPE_STORED.setTokenized(false);
    TYPE_STORED.freeze();
  }

  /**
   * 创建一个新的文本型字符串字段, 索引时将提供的字符串作为单个词。
   *
   * @param name 字段名称
   * @param value 字符串值
   * @param stored 如果内容需要被存储，传入 Store.YES
   * @throws IllegalArgumentException 如果字段名和值为空
   */
  public StringField(String name, String value, Store stored) {
    super(name, value, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
  }

  /**
   * 创建一个新的二进制型字符串字段, 索引时将提供的二进制值 (BytesRef) 作为单个词。
   *
   * @param name 字段名称
   * @param value BytesRef value. The provided value is not cloned so you must not change it until
   *     the document(s) holding it have been indexed.
   * @param stored 如果内容需要被存储，传入 Store.YES
   * @throws IllegalArgumentException 如果字段名和值为空
   */
  public StringField(String name, BytesRef value, Store stored) {
    super(name, value, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
  }
}
