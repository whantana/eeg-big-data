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
package org.apache.hadoop.mp.pi.math;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.hadoop.mp.util.Interval;
import org.apache.hadoop.mp.util.Statistics;

public class Mod1Fraction_LongArray_NotWorking extends Mod1Fraction {
  private static final Statistics stat = new Statistics(Mod1Fraction_LongArray_NotWorking.class.getSimpleName());
  public static Statistics getStatistics() {return stat;}

  public static class Factory extends Mod1Fraction.Factory {
    @Override
    public String getName() {
      return Mod1Fraction_LongArray_NotWorking.class.getSimpleName();
    }

    @Override
    public Mod1Fraction_LongArray_NotWorking zero() {
      return new Mod1Fraction_LongArray_NotWorking(1 + (int)((getPrecision() - 1)/BIT_PER_VALUE));
    }

    @Override
    public Mod1Fraction valueOf(DataInput in) throws IOException {
      final Mod1Fraction_LongArray_NotWorking n = new Mod1Fraction_LongArray_NotWorking(in.readInt());
      for(int i = in.readInt(); i < n.values.length; i++)
        n.values[i] = in.readLong();
      return n;
    }

    @Override
    public Mod1Fraction valueOf(String s) {
      try {
        if (s.charAt(0) != '[' || s.charAt(s.length() - 1) != ']')
          throw new IllegalArgumentException("s.charAt(0) != '[' || s.charAt(s.length() - 1) != ']'");

        int i = 1;
        int j = s.indexOf(":");
        final Mod1Fraction_LongArray_NotWorking n = new Mod1Fraction_LongArray_NotWorking(
            Integer.parseInt(s.substring(i, j)));
        
        for(int k = 0; k < n.values.length; k++) {
          i = j + 1;
          j = s.indexOf(" ", i);
          n.values[k] = Long.parseLong(s.substring(i, j), 16);
        }
        return n;
      } catch(RuntimeException e) {
        throw new RuntimeException("s = ***" + s + "***", e);
      }
    }
  }

  private static int BIT_PER_VALUE = Long.SIZE;
  //---------------------------------------------------------------------------
  private final long[] values;

  Mod1Fraction_LongArray_NotWorking(final int len) {
    values = new long[len];
  }
  
  Mod1Fraction_LongArray_NotWorking init(final long numerator, final long denominator, final long shift) {
    /*
    if (stat != null)
      stat.countExecutionPoint("Mod1Fraction_IntArray.init");
*/
    Arrays.fill(values, 0);

    final long sq = shift >>> 5;
    if (sq >= values.length)
      return this;

    final int sr;
    final long d;
    if (denominator == 1) {
      sr = 0;
      d = 1L << (shift & 0x1FL);
    } else {
      sr = (int)(shift & 0x1FL);
      d = denominator;
    }

    if (SMALL_DENOMINATORS.contains(d))
      return initSmall(numerator, d, (int)sq, sr);
    else {
      initLarge(numerator, d, (int)sq, sr);
      /*
      JavaUtil.out.println("\ninitLarge(numerator = " + numerator + ", denominator = " + denominator + ", shift = " + shift + ")"); 
      JavaUtil.out.println("  this     = " + this); 
      JavaUtil.out.println("  expected = " + initApfloatTL.get().initApfloat(numerator, denominator, shift));
      */
      return this;
    }
  }

  private static Interval SMALL_DENOMINATORS = new Interval(1L, 1L << 31);
  private static Interval LARGE_DENOMINATORS = new Interval(SMALL_DENOMINATORS.end, 1L << 62);

  final long[][] quotients = new long[9][16];
  final long[][] remainers = new long[quotients.length][quotients[0].length];

  /** Bit 61 to 96 */
  private void initQR(final long d) {
    long q = (1L << 60)/d;
    long r = (1L << 60) - q*d;

    for(int i = 0; i < quotients.length; i++) {
      final long unitQ = q;
      final long unitR = r;
      quotients[i][0] = 0;
      remainers[i][0] = 0;

      for(int j = 1; j < quotients[i].length; j++) {
        quotients[i][j] = q;
        remainers[i][j] = r;
        q += unitQ;
        r += unitR;
        if (r >= d) {q++;  r -= d;}
      }
    }
  }

  /** d is a {@link #LARGE_DENOMINATORS} */
  Mod1Fraction_LongArray_NotWorking initLarge(long n, final long d, final int beginindex, final int rightshift) {
    if (beginindex >= values.length)
      return this;
/*
    if (stat != null)
      stat.countExecutionPoint("Mod1Fraction_IntArray.initLarge");
*/
    if (!LARGE_DENOMINATORS.contains(d))
      throw new IllegalArgumentException("d = " + d + " is not in " + LARGE_DENOMINATORS);

    initQR(d);
    if (rightshift == 0)
      for(int i = beginindex; i < values.length && n != 0; i++) {
        long high = n >>> 24;
        n <<= 36;
        n >>>= 4;
        long q = n/d;
        long r = n - q*d;
        for(int j = 0; j < quotients.length; j++) {
          high >>= 4;
          final int bits = (int)(high & 0xF);
          q += quotients[j][bits];
          r += remainers[j][bits];
          if (r >= d) {q++;  r -= d;}
        }
        n = r;

        values[i] = (int)q;
      }
    else {
      final int leftshift = Integer.SIZE - rightshift;
      int prev = 0;
      for(int i = beginindex; i < values.length && n != 0; i++) {
        long high = n >>> 24;
        n <<= 36;
        n >>>= 4;
        long q = n/d;
        long r = n - q*d;
        for(int j = 0; j < quotients.length; j++) {
          high >>= 4;
          final int bits = (int)(high & 0xF);
          q += quotients[j][bits];
          r += remainers[j][bits];
          if (r >= d) {q++;  r -= d;}
        }
        n = r;

        values[i] = prev;
        prev = (int)q;
        values[i] |= prev >>> rightshift;
        prev <<= leftshift;
      }
    }
    return this;
  }

  /** d is a {@link #SMALL_DENOMINATORS} */
  Mod1Fraction_LongArray_NotWorking initSmall(long n, final long d, final int beginindex, final int rightshift) {
    /*
    if (stat != null)
      stat.countExecutionPoint("Mod1Fraction_IntArray.initSmall");
*/
    if (!SMALL_DENOMINATORS.contains(d))
      throw new IllegalArgumentException("d = " + d + " is not in " + SMALL_DENOMINATORS);

    if (rightshift == 0)
      for(int i = beginindex; i < values.length && n != 0; i++) {
        n <<= 32;
        final long q = n/d;
        values[i] = (int)q;
        n -= q*d;
      }
    else {
      final int leftshift = Integer.SIZE - rightshift;
      int prev = 0;
      for(int i = beginindex; i < values.length && n != 0; i++) {
        n <<= 32;
        final long q = n/d;
        n -= q*d;

        values[i] = prev;
        prev = (int)q;
        values[i] |= prev >>> rightshift;
        prev <<= leftshift;
      }
    }
    /*
    JavaUtil.out.println("\nnumerator = " + numerator + ", denominator = " + denominator + ", shift = " + shift); 
    JavaUtil.out.println("  this     = " + this); 
    JavaUtil.out.println("  expected = " + newInstance_Apfloat(len, numerator,  denominator, shift));
    */ 
    return this;
  }

  @Override
  public Mod1Fraction clone() {
    final Mod1Fraction_LongArray_NotWorking f = new Mod1Fraction_LongArray_NotWorking(values.length);
    System.arraycopy(values, 0, f.values, 0, values.length);
    return f;
  }

  @Override
  public Mod1Fraction_LongArray_NotWorking addMod1Equal(Mod1Fraction that) {
    return addMod1Equal(that, 0);
  }
  private Mod1Fraction_LongArray_NotWorking addMod1Equal(Mod1Fraction that, final long beginindex) {
    if (beginindex >= values.length)
      return this;

    final long[] a = ((Mod1Fraction_LongArray_NotWorking)that).values;
    long carry = 0;
    int i = values.length - 1;
    for(; i >= (int)beginindex; i--) {
      carry += values[i] & 0xFFFFFFFFL;
      carry += a[i] & 0xFFFFFFFFL;
      values[i] = (int)carry;
      carry >>= 32;
    }
    for(; carry != 0 && i >= 0; i--) {
      carry += values[i] & 0xFFFFFFFFL;
      values[i] = (int)carry;
      carry >>= 32;
    }
    return this;
  }

  @Override
  public Mod1Fraction_LongArray_NotWorking subtractMod1Equal(Mod1Fraction that) {
    final long[] a = ((Mod1Fraction_LongArray_NotWorking)that).values;
    long borrow = 0;
    for(int i = values.length - 1; i >= 0; i--) {
      borrow += values[i] & 0xFFFFFFFFL;
      borrow -= a[i] & 0xFFFFFFFFL;
      values[i] = (int)borrow;
      borrow >>= 32;
    }
    return this;
  }

  private final ThreadLocal<Mod1Fraction_LongArray_NotWorking> tmp = new ThreadLocal<Mod1Fraction_LongArray_NotWorking>() {
    @Override
    protected Mod1Fraction_LongArray_NotWorking initialValue() {
      return new Mod1Fraction_LongArray_NotWorking(values.length);
    }
  };

  @Override
  public Mod1Fraction_LongArray_NotWorking addFractionMod1Equal(long numerator, long denominator) {
    return addMod1Equal(tmp.get().init(numerator, denominator, 0));
  }

  /**
   * s += 1/(n 2^e)
   * s += 1.0 / (n << e);
   * if (s >= 1) s--;
   */
  @Override
  public Mod1Fraction_LongArray_NotWorking addShiftFractionMod1Equal(long n, long e) {
    return addMod1Equal(tmp.get().init(1, n, e), e >>> 5);
  }

  @Override
  public void printHex(int partsPerLine, PrintStream out) {
    out.print("[" + values.length + ":");
    for(int i = 0; i < values.length; i++) {
      if (partsPerLine > 0 && i % partsPerLine == 0) {
        out.println();
        out.print("  ");
      }
      out.print(String.format("%08X ", values[i]));
    }
  }

  private final int MAX_STRING_ITEM = 100;
  @Override
  public String toHexString(int partPerLine) {
    final boolean iscompact = partPerLine == 0;
    final StringBuilder b = new StringBuilder(); 
    b.append("[").append(values.length).append(":");

    int i = 0;
    if (iscompact) {
      //skip zeros
      for(; i < values.length && values[i] == 0; i++);
      if (i > 0)
        b.append(" <00000000 x " + i + "> ");
    }

    final int n = iscompact && MAX_STRING_ITEM+i < values.length?
        MAX_STRING_ITEM+i: values.length;
    for(; i < n; i++) {
      if (partPerLine > 0 && i % partPerLine == 0)
        b.append("\n  ");
      b.append(String.format("%08X ", values[i]));
    }
    if (i < values.length)
      b.append("... ");
    return b.append("]").toString();
  }

  @Override
  public Void serialize(DataOutput out) throws IOException {
    out.writeInt(values.length);
    int i = 0;
    for(; i < values.length && values[i] == 0; i++);
    out.writeInt(i);
    for(; i < values.length; i++)
      out.writeLong(values[i]);
    return null;
  }
}
