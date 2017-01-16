/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mp.pi.digit;

import java.math.BigInteger;
import java.util.Random;

public class Util {
  public static final Random ran = new Random();

  static String toString(int[] x) {
    if (x == null) {
      return "null";
    } else if (x.length == 0) {
      return "<empty>";
    } else {
      final StringBuilder b = new StringBuilder("[");
      b.append(x[0]);
      for(int i = 1; i < x.length; i++) {
        b.append(", ").append(x[i]);
      }
      return b.append("]").toString();
    }
  }

  static String toString(int[] x, int length, int M) {
    final int hexPerDigit = (M+3) >> 2;
    final StringBuilder b = new StringBuilder();
    for(int i = length - 1; i >= 0; i--) {
      b.append(String.format(" %0" + hexPerDigit + "X", x[i]));
    }
    return b.toString();
  }

  static long toLong(int[] x, int length, int M) {
    long a = 0;
    for(int i = 0; i < length; i++) {
      a |= ((long)x[i] << (M*i));
    }
    return a;
  }

  static BigInteger toBigInteger(int[] x, int bitsPerDigit) {
    final int digitsPerLong = (59/bitsPerDigit) + 1;
    BigInteger c = BigInteger.ZERO;
    for(int i = 0; i < x.length; ) {
      long a = 0;
      for(int j = 0; i < x.length && j < digitsPerLong; j++) {
        a |= ((long)x[i] << (bitsPerDigit*j));
        i++;
      }
      c = c.add(BigInteger.valueOf(a).shiftLeft(bitsPerDigit*digitsPerLong*((i - 1)/digitsPerLong)));
    }
    return c;
  }

  static String toHexString(int[] x, int bitsPerDigit) {
    final int digitsPerLong = (59/bitsPerDigit) + 1;
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < x.length; ) {
      long a = 0;
      int j = 0;
      for(; i < x.length && j < digitsPerLong; j++) {
        a |= ((long)x[i] << (bitsPerDigit*j));
        i++;
      }
      b.insert(0, String.format((i == x.length? "%": "%0" + ((bitsPerDigit*j + 3) >> 2)) + "X ", a));
    }
    return b.toString();
  }

  static String toHexString(byte[] bytes, final int digitsPerGroup,
      final String groupSeparator) {
    if (digitsPerGroup <= 0 || (digitsPerGroup & 1) != 0) {
      throw new IllegalArgumentException(
          "digitsPerGroup <= 0 || (digitsPerGroup & 1) != 0, digitsPerGroup="
          + digitsPerGroup);
    }
    final StringBuilder b = new StringBuilder();
    int r = 0;
    for(int i = bytes.length - 1; i >= 0; i--) {
      b.append(String.format("%2X", bytes[i]));
      if ((r += 2) == digitsPerGroup && i >= 0) {
        r = 0;
        b.append(groupSeparator);
      }
    }
    return b.toString();
  }

  static final String[] BIT_STRINGS = new String[16];
  static {
    for(int i = 0; i < BIT_STRINGS.length; i++)
      BIT_STRINGS[i] = ((i & 8) == 0? "0": "1")
              + ((i & 4) == 0? "0": "1")
              + ((i & 2) == 0? "0": "1")
              + ((i & 1) == 0? "0": "1");
  }
  static String toBitString(byte[] bytes, final int digitsPerGroup,
      final String groupSeparator) {
    if (digitsPerGroup <= 0 || (digitsPerGroup & 7) != 0) {
      throw new IllegalArgumentException(
          "digitsPerGroup <= 0 || (digitsPerGroup & 1) != 0, digitsPerGroup="
          + digitsPerGroup);
    }
    final StringBuilder b = new StringBuilder();
    int r = 0;
    for(int i = bytes.length - 1; i >= 0; i--) {
      b.append(BIT_STRINGS[bytes[i] >>> 4]) 
       .append(BIT_STRINGS[bytes[i] & 0xF]);
      if ((r += 8) == digitsPerGroup && i >= 0) {
        r = 0;
        b.append(groupSeparator);
      }
    }
    return b.toString();
  }
  static String toBitString(long[] integers, final int digitsPerGroup,
      final String groupSeparator) {
    if (digitsPerGroup <= 0 || (digitsPerGroup & 3) != 0) {
      throw new IllegalArgumentException(
          "digitsPerGroup <= 0 || (digitsPerGroup & 1) != 0, digitsPerGroup="
          + digitsPerGroup);
    }
    final StringBuilder b = new StringBuilder();
    int r = 0;
    for(int i = integers.length - 1; i >= 0; i--) {
      for(int j = 28; j >= 0; j -= 4) {
        b.append(BIT_STRINGS[(int)(integers[i] >>> j) & 0xF]);
        if ((r += 4) == digitsPerGroup && i >= 0) {
          r = 0;
          b.append(groupSeparator);
        }
      }
    }
    return b.toString();
  }

  static byte[] reverse(byte[] bytes) {
    for(int i = 0, j = bytes.length - 1; i < j; i++, j--) {
      final byte tmp = bytes[i];
      bytes[i] = bytes[j];
      bytes[j] = tmp;
    }
    return bytes;
  }
  
  static long[] byte2long(byte[] bytes) {
    final long[] a = new long[((bytes.length - 1) >> 2) + 1];
    int j = a.length - 1;
    for(int i = bytes.length - 1; i >= 0; j--) {
      for(int k = 7; i >= 0 && k >= 0; i--, k--)
        a[j] |= ((int)bytes[i]) << (k << 3);
    }
    return a;
  }
}
