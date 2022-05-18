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

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.BytesRef;

// TODO: how to handle versioning here...?

/**
 * 表示用于索引的单个字段。 IndexWriter 把 Iterable&lt;IndexableField&gt; 当做一个文档。
 *
 * @lucene.experimental
 */
public interface IndexableField {

  /** 字段名称 */
  public String name();

  /** {@link IndexableFieldType} describing the properties of this field. */
  public IndexableFieldType fieldType();

  /**
   * 创建用于索引该字段的 TokenStream。 如果有的话, 将使用给定的分词器生成 TokenStreams.
   *
   * @param analyzer 分词器 用于创建 TokenStreams
   * @param reuse TokenStream for a previous instance of this field <b>name</b>. This allows custom
   *     field types (like StringField and NumericField) that do not use the analyzer to still have
   *     good performance. Note: 传入的类型可能并不合适, 比如你给同名字段使用了多种不同的字段类型
   *     因此，检查是实现的责任.
   * @return TokenStream value for indexing the document. Should always return a non-null value if
   *     the field is to be indexed
   */
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse);

  /** Non-null if this field has a binary value */
  public BytesRef binaryValue();

  /** Non-null if this field has a string value */
  public String stringValue();

  /** Non-null if this field has a string value */
  default CharSequence getCharSequenceValue() {
    return stringValue();
  }

  /** Non-null if this field has a Reader value */
  public Reader readerValue();

  /** Non-null if this field has a numeric value */
  public Number numericValue();
}
