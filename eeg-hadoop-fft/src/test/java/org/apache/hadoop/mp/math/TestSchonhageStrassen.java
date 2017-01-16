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

import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.mp.math.BrentSalamin;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.junit.Assert;
import org.junit.Test;

public class TestSchonhageStrassen {
  @Test
  public void testSerialization() {
    for(int bpd = 3; bpd <= 5; bpd++) {
      final Zahlen Z = Zahlen.FACTORY.valueOf(1 << bpd, 4096, 1024);
      Print.println("testSerialization");
      for(int d = 4; d < 10; d++) {
        for(int bpe = 4; bpe < 10; bpe++) {
          final PowerOfTwo_int D = PowerOfTwo_int.valueOf(1 << d);
          final PowerOfTwo_int bitsPerElement = PowerOfTwo_int.valueOf(1 << bpe);
          final int ss_exponent = Z.bitsPerDigit.toMultipleOf((D.exponent >> 1) + (bitsPerElement.value << 1));
          final int modulusExponent = D.toMultipleOf(ss_exponent << 1) >> 1;
          final SchonhageStrassen schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(
              modulusExponent, bitsPerElement, D, Z);

          final String str = schonhagestrassen.serialize();
          final SchonhageStrassen ss = SchonhageStrassen.FACTORY.valueOf(str, Z);
          Assert.assertTrue(schonhagestrassen == ss);
        }
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  static class SuitableNumber {
    final int n;
    final int v;
    final int k;
    
    SuitableNumber(final int v, final int k) {
      this.n = v << k;
      this.v = v;
      this.k = k;
    }

    SuitableNumber nextN() {
      return new SuitableNumber(v/2 + 1, (k-1)/2 + 1);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + ": " + n + " = " + v + "*2^" + k;
    }
  }

  static class Average implements Comparable<Average> {
    final String name;
    final double avg;

    Average(String name, long[] values) {
      this.name = name;
      long sum = 0;
      for(long v : values)
        sum += v;
      avg = sum / (double)values.length;
    }
    
    @Override
    public int compareTo(final Average that) {
      final double d = this.avg - that.avg;
      return d > 0? 1: d < 0? -1: 0;
    }

    @Override
    public String toString() {
      return Parse.millis2String(avg, 2) + ": " + name;
    }
  }

  @Test
  public void testMultiplication() throws FileNotFoundException {
    final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
//    Print.initLogFile("TestMultiplication");
    final int trials = 10;
    timer.tick("testMultiplication: trial=" + trials);
    final Map<String, long[]> t = new TreeMap<String, long[]>();
    for(int i = 0; i < trials; i++) {
      //runTestClassicalMultiplication();
      Print.beginIndentation("\n\n\n*** TEST " + i);
      for(Map.Entry<String, Long> d : runTestMultiplication(new SuitableNumber(16, 15)).entrySet()) {
        long[] a = t.get(d.getKey());
        if (a == null) {
          a = new long[trials];
          t.put(d.getKey(), a);
        }
        a[i] = d.getValue();
      }
      timer.tick("DONE");
      Print.endIndentation();
    }

    final List<Average> a = new ArrayList<Average>();
    for(Map.Entry<String, long[]> e : t.entrySet())
      a.add(new Average(e.getKey(), e.getValue()));
    Collections.sort(a);
    
    timer.tick("DONE ALL");
    Print.println("Average Running Time = " + Parse.list2string(a));
  }

  private Map<String, Long> runTestMultiplication(final SuitableNumber n) {
    final JavaUtil.Timer timer = new JavaUtil.Timer(false, false);
    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(BrentSalamin.class.getSimpleName(), 2, null);

    final PowerOfTwo_int D = PowerOfTwo_int.valueOf(1 << 10);
    Print.println("D                = " + D);
    final PowerOfTwo_int bitsPerElement = PowerOfTwo_int.valueOf(1 << 10);
    Print.println("bitsPerElement   = " + bitsPerElement);
    
    final Zahlen Z = Zahlen.FACTORY.valueOf(32, 4096, 1024);
    Print.beginIndentation(Z);

    final Random RANDOM = JavaUtil.newRandom();
    final int digitsPerOperand = (D.value >> 1)*(bitsPerElement.value >> Z.bitsPerDigit.exponent);
    final Zahlen.Element x = Z.random(digitsPerOperand, RANDOM);
    Print.println("  x = " + x.toBrief());
    final Zahlen.Element y = Z.random(digitsPerOperand, RANDOM);
    Print.println("  y = " + y.toBrief());
    timer.tick("Random Integers");
    Print.endIndentation();

    //Fast Fourier Transform
    final int modulusExponent = Math.max(D.value, bitsPerElement.value << 3) >> 1;
    Print.println("modulusExponent      = " + modulusExponent);
    final SchonhageStrassen ss = SchonhageStrassen.FACTORY.valueOf(
        modulusExponent, bitsPerElement, D, Z);
    SchonhageStrassen.PRINT_LEVEL = Print.Level.VERBOSE;
    Print.beginIndentation(ss);

    final BigInteger xx = x.toBigInteger();
    Print.println("x = " + Parse.bigInteger2Brief(xx));
    final BigInteger yy = y.toBigInteger();
    Print.println("y = " + Parse.bigInteger2Brief(yy));
    timer.tick();
    final BigInteger expected = xx.multiply(yy);
    timer.tick("BigInteger.multiply(..)");
    Print.println("expected = " + Parse.bigInteger2Brief(expected));

    final Map<String, Long> durations = new TreeMap<String, Long>();
//    for(int i = 0; i < ss.algorithms.length; i++) {
//      final SchonhageStrassen.FastFourierTransform fft = ss.algorithms[i];
    final SchonhageStrassen.FastFourierTransform fft = ss.parallel;
      timer.tick();
      final Zahlen.Element z = ss.multiplyEquals(Z.newElement().set(x), y, fft, workers);
      final String name = fft.getClass().getSimpleName();
      final long ms = timer.tick(name);
      durations.put(name, ms); 
      Print.println("z        = " + z.toBrief());
  
      //BigInteger
      final BigInteger zz = z.toBigInteger();
      Print.println("z        = " + Parse.bigInteger2Brief(zz));
      Assert.assertEquals(expected, zz);
//    }
    /*
    //Apint
    final Apint apx = new Apint(xx);
    final Apint apy = new Apint(yy);
    timer.tick();
    final Apint apz = apx.multiply(apy);
    timer.tick("Apint.multiply(..)");
    final BigInteger apzz = apz.toBigInteger();
    Print.println("apzz    = " + toBrief(apzz));
    assertEquals(expected, apzz);
*/
    Print.endIndentation();
    return durations;
  }

  static void runTestMultiplication(final int d, final int bitsPerDigit, final int orderOfTwo) {

    /*

    final long Y = ran.nextLong() & long_mask;

    final int D_half = fft.D >> 1;
    final int[] x = new int[fft.D];
    final int[] y = new int[fft.D];
    Printer.println("");
    Printer.println(String.format("  X =%20d = %16X", X, X));
    Printer.println(String.format("  x =%20d =", toLong(x, D_half, bitsPerDigit)) + toString(x, D_half, bitsPerDigit));
    Printer.println(String.format("  Y =%20d = %16X", Y, Y));
    Printer.println(String.format("  y =%20d =", toLong(y, D_half, bitsPerDigit)) + toString(y, D_half, bitsPerDigit));


    //Schonhage-Strassen
    final int[] z = fft.schonhageStrassen(x, y);
    final BigInteger zzz = toBigInteger(z, bitsPerDigit);
    Printer.println("  z        = " + zzz + " = " + toLongString(z, bitsPerDigit));

    final BigInteger expected = LongLong.multiplication(new LongLong(), X, Y).toBigInteger();
    Printer.println("  expected = " + expected + " = " + expected.toString(16).toUpperCase());
    assertEquals(expected, zzz);
    */
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

  static String toLongString(int[] x, int bitsPerDigit) {
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
}
