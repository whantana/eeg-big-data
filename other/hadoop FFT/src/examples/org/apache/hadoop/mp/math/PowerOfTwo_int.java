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
package org.apache.hadoop.mp.math;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

public enum PowerOfTwo_int implements Print.Brief, Print.Detail,
    DataSerializable<Void>, StringSerializable {
  TWO_00(0), TWO_01(1), TWO_02(2), TWO_03(3), TWO_04(4),
  TWO_05(5), TWO_06(6), TWO_07(7), TWO_08(8), TWO_09(9),
  TWO_10(10), TWO_11(11), TWO_12(12), TWO_13(13), TWO_14(14),
  TWO_15(15), TWO_16(16), TWO_17(17), TWO_18(18), TWO_19(19),
  TWO_20(20), TWO_21(21), TWO_22(22), TWO_23(23), TWO_24(24),
  TWO_25(25), TWO_26(26), TWO_27(27), TWO_28(28), TWO_29(29),
  TWO_30(30);

  /** value is a power of 2 */
  public final int value;
  /** 2^exponent = value */
  public final int exponent;
  /** mask = value - 1 */
  public final int mask;
  /** reverseMask = ~mask */
  public final int reverseMask;
  
  private PowerOfTwo_int(final int e) {
    exponent = e;
    value = 1 << exponent;
    mask = value - 1;
    reverseMask = ~mask;
  }

  /** Is n a multiple of this? */
  public void checkIsMultiple(final int n) {
    if ((n & mask) != 0)
      throw new IllegalArgumentException("(n & mask) != 0, n="
          + n + ", this=" + this);
  }
  
  /** Is n a multiple of this? */
  public int toMultipleOf(final int n) {
    return n + ((value - (n & mask)) & mask);
  }
  
  @Override
  public String toBrief() {return "2^" + exponent;}
  @Override
  public String serialize() {return toBrief() + " (=" + value + ")";}
  @Override
  public String toString() {return serialize();}
  @Override
  public void printDetail(final String firstlineprefix) {
    Print.print(firstlineprefix);
    Print.print(this);
    Print.println(String.format(", mask=%08X, reverseMask=%08X", mask, reverseMask));
  }

  public static final StringSerializable.ValueOf<PowerOfTwo_int> VALUE_OF_STR
      = new StringSerializable.ValueOf<PowerOfTwo_int>() {
    @Override
    public PowerOfTwo_int valueOf(final String s) {
      final int i = s.indexOf("2^") + 2;
      final int j = s.indexOf(" (=", i);
      return values()[Integer.parseInt(s.substring(i, j))];    
    }
  };

  @Override
  public Void serialize(DataOutput out) throws IOException {
    out.writeInt(ordinal());
    return null;
  }

  public static PowerOfTwo_int valueOf(DataInput in) throws IOException {
    return values()[in.readInt()];    
  }

  public static PowerOfTwo_int valueOf(final int n) {
    if (n <= 0)
      throw new IllegalArgumentException(n + " = n < 0");
    final int b = Integer.bitCount(n);
    if (b != 1)
      throw new IllegalArgumentException(b + " = Integer.bitCount(n) != 1");
    return values()[Integer.numberOfTrailingZeros(n)];
  }
}
