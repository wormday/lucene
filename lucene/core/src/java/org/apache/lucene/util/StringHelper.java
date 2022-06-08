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
package org.apache.lucene.util;

import java.io.DataInputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * Methods for manipulating strings.
 *
 * @lucene.internal
 */
public abstract class StringHelper {

  /**
   * Compares two {@link BytesRef}, element by element, and returns the number of elements common to
   * both arrays (from the start of each). This method assumes currentTerm comes after priorTerm.
   *
   * @param priorTerm The first {@link BytesRef} to compare
   * @param currentTerm The second {@link BytesRef} to compare
   * @return The number of common elements (from the start of each).
   */
  public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm) {
    int mismatch =
        Arrays.mismatch(
            priorTerm.bytes,
            priorTerm.offset,
            priorTerm.offset + priorTerm.length,
            currentTerm.bytes,
            currentTerm.offset,
            currentTerm.offset + currentTerm.length);
    if (mismatch < 0) {
      throw new IllegalArgumentException(
          "terms out of order: priorTerm=" + priorTerm + ",currentTerm=" + currentTerm);
    }
    return mismatch;
  }

  /**
   * Returns the length of {@code currentTerm} needed for use as a sort key. so that {@link
   * BytesRef#compareTo(BytesRef)} still returns the same result. This method assumes currentTerm
   * comes after priorTerm.
   */
  public static int sortKeyLength(final BytesRef priorTerm, final BytesRef currentTerm) {
    return bytesDifference(priorTerm, currentTerm) + 1;
  }

  private StringHelper() {}

  /**
   * Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>false
   * </code>.
   *
   * @param ref the {@code byte[]} to test
   * @param prefix the expected prefix
   * @return Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>
   *     false</code>.
   */
  public static boolean startsWith(byte[] ref, BytesRef prefix) {
    // not long enough to start with the prefix
    if (ref.length < prefix.length) {
      return false;
    }
    return Arrays.equals(
        ref, 0, prefix.length, prefix.bytes, prefix.offset, prefix.offset + prefix.length);
  }

  /**
   * Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>false
   * </code>.
   *
   * @param ref the {@link BytesRef} to test
   * @param prefix the expected prefix
   * @return Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>
   *     false</code>.
   */
  public static boolean startsWith(BytesRef ref, BytesRef prefix) {
    // not long enough to start with the prefix
    if (ref.length < prefix.length) {
      return false;
    }
    return Arrays.equals(
        ref.bytes,
        ref.offset,
        ref.offset + prefix.length,
        prefix.bytes,
        prefix.offset,
        prefix.offset + prefix.length);
  }

  /**
   * Returns <code>true</code> iff the ref ends with the given suffix. Otherwise <code>false</code>.
   *
   * @param ref the {@link BytesRef} to test
   * @param suffix the expected suffix
   * @return Returns <code>true</code> iff the ref ends with the given suffix. Otherwise <code>false
   *     </code>.
   */
  public static boolean endsWith(BytesRef ref, BytesRef suffix) {
    int startAt = ref.length - suffix.length;
    // not long enough to start with the suffix
    if (startAt < 0) {
      return false;
    }
    return Arrays.equals(
        ref.bytes,
        ref.offset + startAt,
        ref.offset + startAt + suffix.length,
        suffix.bytes,
        suffix.offset,
        suffix.offset + suffix.length);
  }

  /** Pass this as the seed to {@link #murmurhash3_x86_32}. */

  // Poached from Guava: set a different salt/seed
  // for each JVM instance, to frustrate hash key collision
  // denial of service attacks, and to catch any places that
  // somehow rely on hash function/order across JVM
  // instances:
  public static final int GOOD_FAST_HASH_SEED;

  static {
    String prop = System.getProperty("tests.seed");
    if (prop != null) {
      // So if there is a test failure that relied on hash
      // order, we remain reproducible based on the test seed:
      GOOD_FAST_HASH_SEED = prop.hashCode();
    } else {
      GOOD_FAST_HASH_SEED = (int) System.currentTimeMillis();
    }
  }

  /**
   * Returns the MurmurHash3_x86_32 hash. Original source/tests at
   * https://github.com/yonik/java_util/
   */
  @SuppressWarnings("fallthrough")
  public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc); // round down to 4 byte block

    for (int i = offset; i < roundedEnd; i += 4) {
      // little endian load order
      int k1 = (int) BitUtil.VH_LE_INT.get(data, i);
      k1 *= c1;
      k1 = Integer.rotateLeft(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = Integer.rotateLeft(h1, 13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch (len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  public static int murmurhash3_x86_32(BytesRef bytes, int seed) {
    return murmurhash3_x86_32(bytes.bytes, bytes.offset, bytes.length, seed);
  }

  // Holds 128 bit unsigned value:
  private static BigInteger nextId;
  private static final BigInteger mask128;
  private static final Object idLock = new Object();

  static {
    // 128 bit unsigned mask
    byte[] maskBytes128 = new byte[16];
    // 128位全部置1
    Arrays.fill(maskBytes128, (byte) 0xff);
    // maskBytes128 标识绝对值，1标识符号位，在这里会添加一个全0字节
    mask128 = new BigInteger(1, maskBytes128);

    String prop = System.getProperty("tests.seed");

    // State for xorshift128:
    long x0;
    long x1;

    if (prop != null) {
      // So if there is a test failure that somehow relied on this id,
      // we remain reproducible based on the test seed:
      if (prop.length() > 8) {
        prop = prop.substring(prop.length() - 8);
      }
      x0 = Long.parseLong(prop, 16);
      x1 = x0;
    } else {
      // seed from /dev/urandom, if its available
      try (DataInputStream is =
          new DataInputStream(Files.newInputStream(Paths.get("/dev/urandom")))) {
        x0 = is.readLong();
        x1 = is.readLong();
      } catch (
          @SuppressWarnings("unused")
          Exception unavailable) {
        // may not be available on this platform
        // fall back to lower quality randomness from 3 different sources:
        x0 = System.nanoTime();
        x1 = (long) StringHelper.class.hashCode() << 32;

        StringBuilder sb = new StringBuilder();
        // Properties can vary across JVM instances:
        try {
          Properties p = System.getProperties();
          for (String s : p.stringPropertyNames()) {
            sb.append(s);
            sb.append(p.getProperty(s));
          }
          // StringHelper的hashCode放在高32位（每次运行都不同），系统参数的hashCode 放在低32位
          x1 |= sb.toString().hashCode();
        } catch (
            @SuppressWarnings("unused")
            SecurityException notallowed) {
          // getting Properties requires wildcard read-write: may not be allowed
          x1 |= StringBuffer.class.hashCode();
        }
      }
    }
    // 使用几个xorshift128的迭代来分散种子，
    // 以防多个Lucene实例在“附近”启动相同的nanoTime，
    // 因为我们使用++ (mod 2^128)作为完整的周期周期:
    // 为什么abc选择23,17,26有什么规则？
    for (int i = 0; i < 10; i++) {
      long s1 = x0;
      long s0 = x1;
      x0 = s0;
      s1 ^= s1 << 23; // a
      x1 = s1 ^ s0 ^ (s1 >>> 17) ^ (s0 >>> 26); // b, c
    }

    // 64-bit unsigned mask
    byte[] maskBytes64 = new byte[8];
    Arrays.fill(maskBytes64, (byte) 0xff);
    BigInteger mask64 = new BigInteger(1, maskBytes64);

    // First make unsigned versions of x0, x1:
    BigInteger unsignedX0 = BigInteger.valueOf(x0).and(mask64);
    BigInteger unsignedX1 = BigInteger.valueOf(x1).and(mask64);

    // Concatentate bits of x0 and x1, as unsigned 128 bit integer:
    nextId = unsignedX0.shiftLeft(64).or(unsignedX1);
  }

  /** length in bytes of an ID */
  public static final int ID_LENGTH = 16;

  /** 生成一个非加密的全局惟一id。 */
  public static byte[] randomId() {

    // 注意:我们在这里不使用Java的UUID.randomUUID()实现，因为
    //  * 对于我们来说它有点过了：它试图加密，然而我们不关心是否有人能猜出id
    //  * 它使用SecureRandom，在Linux上，当熵收集滞后时，它很容易花费很长时间(我看到仅运行一个Lucene测试就需要10秒)。
    //  * 它会因为版本和变体而丢失一些(6)位，不清楚这对period有什么影响，而我们在这里使用的简单的++ (mod 2^128)保证有完整的period。

    byte[] bits;
    synchronized (idLock) {
      // 注意：nextId类型为BigInteger,通过一个数组可以处理任意大的整数，原生的最大是long
      bits = nextId.toByteArray();
      nextId = nextId.add(BigInteger.ONE).and(mask128);
    }
    // toByteArray()总是返回一个符号位，所以它可能需要额外的字节(总是0)
    // toByteArray() always returns a sign bit, so it may require an extra byte (always zero)
    if (bits.length > ID_LENGTH) {
      assert bits.length == ID_LENGTH + 1;
      assert bits[0] == 0;
      return ArrayUtil.copyOfSubArray(bits, 1, bits.length);
    } else {
      byte[] result = new byte[ID_LENGTH];
      System.arraycopy(bits, 0, result, result.length - bits.length, bits.length);
      return result;
    }
  }

  /**
   * Helper method to render an ID as a string, for debugging
   *
   * <p>Returns the string {@code (null)} if the id is null. Otherwise, returns a string
   * representation for debugging. Never throws an exception. The returned string may indicate if
   * the id is definitely invalid.
   */
  public static String idToString(byte[] id) {
    if (id == null) {
      return "(null)";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(new BigInteger(1, id).toString(Character.MAX_RADIX));
      if (id.length != ID_LENGTH) {
        sb.append(" (INVALID FORMAT)");
      }
      return sb.toString();
    }
  }

  /**
   * Just converts each int in the incoming {@link IntsRef} to each byte in the returned {@link
   * BytesRef}, throwing {@code IllegalArgumentException} if any int value is out of bounds for a
   * byte.
   */
  public static BytesRef intsRefToBytesRef(IntsRef ints) {
    byte[] bytes = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) {
      int x = ints.ints[ints.offset + i];
      if (x < 0 || x > 255) {
        throw new IllegalArgumentException(
            "int at pos=" + i + " with value=" + x + " is out-of-bounds for byte");
      }
      bytes[i] = (byte) x;
    }

    return new BytesRef(bytes);
  }
}
