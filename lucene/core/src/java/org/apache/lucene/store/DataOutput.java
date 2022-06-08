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
package org.apache.lucene.store;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Abstract base class for performing write operations of Lucene's low-level data types.
 *
 * <p>{@code DataOutput} may only be used from one thread, because it is not thread safe (it keeps
 * internal state like file position).
 */
public abstract class DataOutput {

  /**
   * Writes a single byte.
   *
   * <p>The most primitive data type is an eight-bit byte. Files are accessed as sequences of bytes.
   * All other data types are defined as sequences of bytes, so file formats are byte-order
   * independent.
   *
   * @see IndexInput#readByte()
   */
  public abstract void writeByte(byte b) throws IOException;

  /**
   * Writes an array of bytes.
   *
   * @param b the bytes to write
   * @param length the number of bytes to write
   * @see DataInput#readBytes(byte[],int,int)
   */
  public void writeBytes(byte[] b, int length) throws IOException {
    writeBytes(b, 0, length);
  }

  /**
   * Writes an array of bytes.
   *
   * @param b the bytes to write
   * @param offset the offset in the byte array
   * @param length the number of bytes to write
   * @see DataInput#readBytes(byte[],int,int)
   */
  public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

  /**
   * 使用四个字节写入一个int (LE byte order).
   * Little-Endian就是低位字节排放在内存的低地址端，高位字节排放在内存的高地址端
   *
   * @see DataInput#readInt()
   * @see BitUtil#VH_LE_INT
   */
  public void writeInt(int i) throws IOException {
    writeByte((byte) i);
    writeByte((byte) (i >> 8));
    writeByte((byte) (i >> 16));
    writeByte((byte) (i >> 24));
  }

  /**
   * Writes a short as two bytes (LE byte order).
   *
   * @see DataInput#readShort()
   * @see BitUtil#VH_LE_SHORT
   */
  public void writeShort(short i) throws IOException {
    writeByte((byte) i);
    writeByte((byte) (i >> 8));
  }

  /**
   * 以变长格式写入整型数。写入1到5个字节。较小的值占用更少的字节。支持负数，但应避免使用负数。
   * 0(0x0) - 127(0x7f) 1个字节
   * 128(0x80) - 16383(0x3fff) 2个字节
   * 16384(0x4000) - 2097151(0x1fffff) 3个字节
   * 2097152(0x200000) - 268435455(0x0fffffff) 4个字节
   * 268435456(0x10000000) - 2147483647(0x7fffffff) 5字节
   * VByte是一种定义为正整数的变长格式，其中每个字节的最高位表示是否还有更多字节需要读取（0表示当前字节是最后一个字节，1表示后边还有字节）
   * 低阶的7位将整数值从低到高位被依次添加。
   *
   * <p>VByte Encoding Example
   *
   * <table class="padding2" style="border-spacing: 0px; border-collapse: separate; border: 0">
   * <caption>variable length encoding examples</caption>
   * <tr style="vertical-align: top">
   *   <th style="text-align:left">Value</th>
   *   <th style="text-align:left">Byte 1</th>
   *   <th style="text-align:left">Byte 2</th>
   *   <th style="text-align:left">Byte 3</th>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>0</td>
   *   <td><code>00000000</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>1</td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>2</td>
   *   <td><code>00000010</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr>
   *   <td style="vertical-align: top">...</td>
   *   <td></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>127</td>
   *   <td><code>01111111</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>128</td>
   *   <td><code>10000000</code></td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>129</td>
   *   <td><code>10000001</code></td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>130</td>
   *   <td><code>10000010</code></td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   * </tr>
   * <tr>
   *   <td style="vertical-align: top">...</td>
   *   <td></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>16,383</td>
   *   <td><code>11111111</code></td>
   *   <td><code>01111111</code></td>
   *   <td></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>16,384</td>
   *   <td><code>10000000</code></td>
   *   <td><code>10000000</code></td>
   *   <td><code>00000001</code></td>
   * </tr>
   * <tr style="vertical-align: bottom">
   *   <td>16,385</td>
   *   <td><code>10000001</code></td>
   *   <td><code>10000000</code></td>
   *   <td><code>00000001</code></td>
   * </tr>
   * <tr>
   *   <td style="vertical-align: top">...</td>
   *   <td ></td>
   *   <td ></td>
   *   <td ></td>
   * </tr>
   * </table>
   *
   * <p>This provides compression while still being efficient to decode.
   *
   * <p>This provides compression while still being efficient to decode.
   *
   * @param i 较小的值占用更少的字节。 支持负数，但应避免使用负数。
   * @throws IOException If there is an I/O error writing to the underlying medium.
   * @see DataInput#readVInt()
   */
  public final void writeVInt(int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      // 如果i除了后七位仍然包含1
      // 截取后7位，然后第一位置1，保存这个字节
      writeByte((byte) ((i & 0x7F) | 0x80));
      // 右移7位
      i >>>= 7;
    }
    // 最高位的字节写在最后
    writeByte((byte) i);
  }

  /**
   * Write a {@link BitUtil#zigZagEncode(int) zig-zag}-encoded {@link #writeVInt(int)
   * variable-length} integer. This is typically useful to write small signed ints and is equivalent
   * to calling <code>writeVInt(BitUtil.zigZagEncode(i))</code>.
   *
   * @see DataInput#readZInt()
   */
  public final void writeZInt(int i) throws IOException {
    writeVInt(BitUtil.zigZagEncode(i));
  }

  /**
   * Writes a long as eight bytes (LE byte order).
   *
   * @see DataInput#readLong()
   * @see BitUtil#VH_LE_LONG
   */
  public void writeLong(long i) throws IOException {
    writeInt((int) i);
    writeInt((int) (i >> 32));
  }

  /**
   * Writes an long in a variable-length format. Writes between one and nine bytes. Smaller values
   * take fewer bytes. Negative numbers are not supported.
   *
   * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
   *
   * @see DataInput#readVLong()
   */
  public final void writeVLong(long i) throws IOException {
    if (i < 0) {
      throw new IllegalArgumentException("cannot write negative vLong (got: " + i + ")");
    }
    writeSignedVLong(i);
  }

  // write a potentially negative vLong
  private void writeSignedVLong(long i) throws IOException {
    while ((i & ~0x7FL) != 0L) {
      writeByte((byte) ((i & 0x7FL) | 0x80L));
      i >>>= 7;
    }
    writeByte((byte) i);
  }

  /**
   * Write a {@link BitUtil#zigZagEncode(long) zig-zag}-encoded {@link #writeVLong(long)
   * variable-length} long. Writes between one and ten bytes. This is typically useful to write
   * small signed ints.
   *
   * @see DataInput#readZLong()
   */
  public final void writeZLong(long i) throws IOException {
    writeSignedVLong(BitUtil.zigZagEncode(i));
  }

  /**
   * 写入字符串
   *
   * <p>已UTF-8编码写入字符串。 最开始是 {@link #writeVInt VInt}格式的长度，然后是字节.
   *
   * @see DataInput#readString()
   */
  public void writeString(String s) throws IOException {
    final BytesRef utf8Result = new BytesRef(s);
    writeVInt(utf8Result.length);
    writeBytes(utf8Result.bytes, utf8Result.offset, utf8Result.length);
  }

  private static int COPY_BUFFER_SIZE = 16384;
  private byte[] copyBuffer;

  /** Copy numBytes bytes from input to ourself. */
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    assert numBytes >= 0 : "numBytes=" + numBytes;
    long left = numBytes;
    if (copyBuffer == null) copyBuffer = new byte[COPY_BUFFER_SIZE];
    while (left > 0) {
      final int toCopy;
      if (left > COPY_BUFFER_SIZE) toCopy = COPY_BUFFER_SIZE;
      else toCopy = (int) left;
      input.readBytes(copyBuffer, 0, toCopy);
      writeBytes(copyBuffer, 0, toCopy);
      left -= toCopy;
    }
  }

  /**
   * Writes a String map.
   *
   * <p>First the size is written as an {@link #writeVInt(int) vInt}, followed by each key-value
   * pair written as two consecutive {@link #writeString(String) String}s.
   *
   * @param map Input map.
   * @throws NullPointerException if {@code map} is null.
   */
  public void writeMapOfStrings(Map<String, String> map) throws IOException {
    writeVInt(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      writeString(entry.getKey());
      writeString(entry.getValue());
    }
  }

  /**
   * Writes a String set.
   *
   * <p>First the size is written as an {@link #writeVInt(int) vInt}, followed by each value written
   * as a {@link #writeString(String) String}.
   *
   * @param set Input set.
   * @throws NullPointerException if {@code set} is null.
   */
  public void writeSetOfStrings(Set<String> set) throws IOException {
    writeVInt(set.size());
    for (String value : set) {
      writeString(value);
    }
  }
}
