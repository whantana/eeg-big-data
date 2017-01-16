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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.Random;

import org.apache.hadoop.mp.math.BrentSalamin;
import org.apache.hadoop.mp.math.MathUtil;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;
import org.junit.Assert;
import org.junit.Test;

public class TestZahlen {
  private static final Random RANDOM = JavaUtil.newRandom();
  static final PrintStream out = System.out;

  static class CheckPlusEqual {
    final Zahlen.Element e;
    long value = 0;
    
    CheckPlusEqual(final Zahlen Z) {e = Z.newElement();}

    void checkPlusEqual(long singleDigit) {
      for(; singleDigit > Integer.MAX_VALUE; ) {
        e.plusEqual(Integer.MAX_VALUE);
        singleDigit -= Integer.MAX_VALUE;
      }
      for(; singleDigit < Integer.MIN_VALUE; ) {
        e.plusEqual(Integer.MIN_VALUE);
        singleDigit -= Integer.MIN_VALUE;
      }
      e.plusEqual((int)singleDigit);
      value += singleDigit;
      out.println("e " + (singleDigit > 0? "+ " + singleDigit: "- " + -singleDigit)
          + " = " + e + ", value=" + value);
      Assert.assertEquals(value, e.toBigInteger().longValue());
    }

    Zahlen.Element checkPlusEqual(Zahlen.Element x) {
      e.plusEqual(x);
      final long xValue = x.toBigInteger().longValue();
      value += xValue;
      out.println("e " + (xValue > 0? "+ " + xValue: "- " + -xValue)
          + " = " + e + ", value=" + value);
      Assert.assertEquals(value, e.toBigInteger().longValue());
      return e;
    }
  }

  @Test
  public void testPlusEqualElement() {
    final Zahlen Z = Zahlen.FACTORY.valueOf(8, 4, 1024);
    final CheckPlusEqual cpe = new CheckPlusEqual(Z);
    final Zahlen.Element x = Z.newElement();
    x.plusEqual(1);
    cpe.checkPlusEqual(1);
    cpe.checkPlusEqual(x);

    x.plusEqual((int)Z.digitLimit.mask >>> 1);
    cpe.checkPlusEqual(x);

    x.plusEqual(-1);
    cpe.checkPlusEqual(x);
    cpe.checkPlusEqual(x);
    
    
    final int n = 16;
    final Zahlen.Element y = Z.random(n, new Random());
    out.println("y = " + y);
    int d = y.difference(n, 3);
    out.println("d = " + d + ", x = " + y);
  }

  @Test
  public void testPlusEqualSingleDigit() {
    final Zahlen Z = Zahlen.FACTORY.valueOf(8, 4, 1024);
    final CheckPlusEqual cpe = new CheckPlusEqual(Z);

    for(int i = 0; i < 2; i++)
      cpe.checkPlusEqual(1);
    for(int i = 0; i < 4; i++)
      cpe.checkPlusEqual(-1);
    for(int i = 0; i < 5; i++)
      cpe.checkPlusEqual(1);

    cpe.checkPlusEqual(Z.digitLimit.value - 1);

    for(int i = 0; i < 5; i++)
      cpe.checkPlusEqual(-1);
    for(int i = 0; i < 5; i++)
      cpe.checkPlusEqual(1);
    
    for(int i = 0; i < 3; i++) {
      cpe.checkPlusEqual(Z.digitLimit.value - 2);
      cpe.checkPlusEqual(2);
    }

    for(int i = 0; i < 2; i++)
      cpe.checkPlusEqual(-1);

    for(int i = 0; i < 10; i++) {
      cpe.checkPlusEqual(-(Z.digitLimit.value >> 1));
    }
  }
  
  @Test
  public void testSerialization() throws IOException {
    for(int bpd = 3; bpd <= 5; bpd++) {
      final Zahlen Z = Zahlen.FACTORY.valueOf(1 << bpd, 4, 1024);
      Assert.assertTrue(Z == Zahlen.FACTORY.valueOf(Z.serialize()));
  
      {
        final ByteArrayOutputStream ba = new ByteArrayOutputStream();
        final DataOutputStream baout = new DataOutputStream(ba);
        Z.serialize(baout);
        baout.flush();
  
        final DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.toByteArray()));
        final Zahlen z = Zahlen.FACTORY.valueOf(in);
        out.println("z = " + z);
        Assert.assertTrue(Z == z);
      }
  
  
      runTestSerialization(Z.newElement());
      for(int e = 1; e < 10; e++)
        for(int i = 0; i < 5; i++)
          runTestSerialization(Z.random((1 << e) + i - 2, RANDOM));
    }
  }
  
  private void runTestSerialization(Zahlen.Element x) throws IOException {
    final ByteArrayOutputStream ba = new ByteArrayOutputStream();
    final DataOutputStream baout = new DataOutputStream(ba);
    
    x.serialize(baout);
    baout.flush();

    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.toByteArray()));
    final Zahlen.Element e = Zahlen.ELEMENT_FACTORY.valueOf(in);
    Assert.assertTrue(x.get() == e.get());
    Assert.assertEquals(x, e);
  }

  @Test
  public void testSqrt() {
    final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(BrentSalamin.class.getSimpleName(), 2, timer);

    final Zahlen Z = Zahlen.FACTORY.valueOf(8, 4, 1024);
    for(long n = 0; n < 1000; n++) {
      final long sqrt = MathUtil.sqrt_long(n);
      final long r = n - sqrt*sqrt;
      //Print.println("n = " + n + " = " + sqrt + "^2 + " + r);
      Assert.assertTrue(r >= 0);
      Assert.assertTrue((sqrt + 1)*(sqrt + 1) > n);
    }


    final Zahlen.Element n = Z.newElement();
    for(long i = 0; i < 100000; i++) {
      //Print.println("n    = " + n);
      Zahlen.Element r = Z.newElement().set(n);
      Zahlen.Element sqrt = r.sqrtRemainderEqual(workers);
      //Print.println("sqrt = " + sqrt);
      //Print.println("r    = " + r);
      Assert.assertTrue(r.isNonNegative());
      sqrt.plusEqual(1).multiplyEqual(sqrt, workers);
      Assert.assertTrue(sqrt.compareMagnitudeTo(n) > 0);

      n.plusEqual(1);
    }
  }

  @Test
  public void testMultiplication() {
    final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
    final JavaUtil.Timer t = new JavaUtil.Timer(false, false);
    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(BrentSalamin.class.getSimpleName(), 2, null);

    final Zahlen Z = Zahlen.FACTORY.valueOf(16, 1 << 12, 1 << 12);
    for(int i = 0; i < 16; i++) {
      Print.println();
      final int d = 1 << i;
      t.tick("i=" + i + ", d=" + d);
      final Zahlen.Element x = Z.random(d, RANDOM);
      final Zahlen.Element y = Z.random(d, RANDOM);
      final Zahlen.Element y_copy = Z.newElement().set(y);
      t.tick("ramdom");
      final BigInteger expected = x.toBigInteger().multiply(y.toBigInteger());
      t.tick("BigInteger");
      x.multiplyEqual(y, workers);
      t.tick("Zahlen");
      Assert.assertEquals(expected, x.toBigInteger());
      Assert.assertEquals(y_copy, y);
      y.multiplyEqual(Z.newElement().plusEqual(1).shiftLeftEqual_bits(Z.digitsPerArray.value), workers);
      y_copy.shiftLeftEqual_bits(Z.digitsPerArray.value);
      Assert.assertEquals(y_copy, y);

      Print.println("product = " + x.toBrief());
      timer.tick("done iteration " + i);
      Print.printMemoryInfo();
      
    }
  }

  @Test
  public void testShiftAsMultiplication() {
    final Zahlen Z = Zahlen.FACTORY.valueOf(8, 4, 1 << 12);

//    {
//      for(int i = 10; i < 100; i++) {
//        Print.println("************* i=" + i);
//        final Zahlen.Element x = Z.newElement();
//        final Zahlen.Element y = Z.newElement().plusEqual(1).shiftLeftEqual_digits(i);
//        Print.println("x = " + x.toBrief());
//        Print.println("y = " + y.toBrief());
//        x.plusEqual(y);
//        Print.println("x = " + x.toBrief());
//        Assert.assertEquals(y, x);
//      }
//    }
    
    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(TestZahlen.class.getSimpleName(), 2, null);
    for(int i = 1; i < 100; i++) {
      final Zahlen.Element x = Z.random(8, RANDOM);
      final Zahlen.Element y = Z.newElement().plusEqual(1).shiftLeftEqual_bits(i);
//      final Zahlen.Element y = Z.random(4, RANDOM).shiftLeftEqual_bits(1);
      Print.println("************* i=" + i);
      Print.println("x = " + x);
      Print.println("y = " + y);
  
      final Zahlen.Element y_copy = Z.newElement().set(y);
      final BigInteger expected = x.toBigInteger().multiply(y.toBigInteger());
      final int max = x.numberOfDigits() > y.numberOfDigits()? x.numberOfDigits(): y.numberOfDigits();
      x.multiplyEqual_SchonhageStrassen(y, max, workers);
  
      Assert.assertEquals(expected, x.toBigInteger());
      Assert.assertEquals(y_copy, y);
  
      y.multiplyEqual(Z.newElement().plusEqual(1).shiftLeftEqual_bits(Z.digitsPerArray.value), workers);
      y_copy.shiftLeftEqual_bits(Z.digitsPerArray.value);
      Assert.assertEquals(y_copy, y);
  
      Print.println("product = " + x);
      Print.printMemoryInfo();
    }
  }

  @Test
  public void testKaratsuba() {
    final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(BrentSalamin.class.getSimpleName(), 2, null);
    final JavaUtil.Timer t = new JavaUtil.Timer(false, false);
    final Zahlen Z = Zahlen.FACTORY.valueOf(32, 1 << 3, 1 << 12);
    for(int i = 1; i < 5; i++) {
      Print.println();
      final int d = 1 << i;
      t.tick("i=" + i + ", d=" + d + " **********************");
      Zahlen.Element x = Z.random(d, RANDOM);
      final Zahlen.Element y = Z.random(d, RANDOM);
      final Zahlen.Element y_copy = Z.newElement().set(y);
      t.tick("ramdom");
      final BigInteger expected = x.toBigInteger().multiply(y.toBigInteger());
      final long tBigInteger = t.tick("BigInteger");
      final long tKaratsuba;
      {
        final int max = x.numberOfDigits() > y.numberOfDigits()? x.numberOfDigits(): y.numberOfDigits();
        x.multiplyEqual_Karatsuba(y, max, workers);
        tKaratsuba = t.tick("Karatsuba");
        Assert.assertEquals(expected, x.toBigInteger());
        Assert.assertEquals(y_copy, y);
      }

      Print.println("product = " + x.toBrief());
      if (tKaratsuba < tBigInteger) {
        Print.println("!!!");
        Print.println("!!! Karatsuba");
        Print.println("!!!");
      }

      {
        x = Z.newElement().plusEqual(1).shiftLeftEqual_bits(Z.digitsPerArray.value);
        final int max = x.numberOfDigits() > y.numberOfDigits()? x.numberOfDigits(): y.numberOfDigits();
        y.multiplyEqual_Karatsuba(x, max, workers);
        y_copy.shiftLeftEqual_bits(Z.digitsPerArray.value);
        Assert.assertEquals(y_copy, y);
      }

      timer.tick("done iteration " + i);
      Print.printMemoryInfo();
    }
  }
}
