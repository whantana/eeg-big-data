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
import java.math.BigDecimal;
import java.util.Random;

import org.apache.hadoop.mp.math.BrentSalamin;
import org.apache.hadoop.mp.math.FixedPointFraction;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;

public class TestFixedPointFraction extends junit.framework.TestCase {
  private static final Random RANDOM = JavaUtil.newRandom();
  final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
  final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(BrentSalamin.class.getSimpleName(), 2, timer);
  
  static final BigDecimal TWO = BigDecimal.valueOf(2);
  static final BigDecimal THREE = BigDecimal.valueOf(3);
  
  public void testFixedPointFraction() throws IOException {
    final Zahlen Z = Zahlen.FACTORY.valueOf(8, 2, 1024);
    final FixedPointFraction R = FixedPointFraction.FACTORY.valueOf(8, Z);
    Print.println("R = " + R);
    
    final FixedPointFraction.Element x = R.newElement();
    Print.println("x = " + x);
    assertEquals(BigDecimal.ZERO, x.toBigDecimal());

    final FixedPointFraction.Element y = R.valueOf(1);
    y.printDetail("y");
    assertTrue(y.isOne());
    assertEquals(BigDecimal.ONE, y.toBigDecimal());
    final FixedPointFraction.Element one = R.newElement().set(y);

    y.plusEqual(y);
    Print.println("y = " + y);
    assertFalse(y.isOne());
    assertEquals(TWO, y.toBigDecimal());
    final FixedPointFraction.Element two = R.newElement().set(y);
    
    Print.println("x += y = " + x.plusEqual(y));
    assertFalse(x.isOne());
    assertEquals(TWO, x.toBigDecimal());

    y.plusEqual(one);
    Print.println("y = " + y);
    assertFalse(y.isOne());
    assertEquals(THREE, y.toBigDecimal());
    
    final BigDecimal DIGIT = BigDecimal.valueOf(Z.digitLimit.value);
    x.shiftRightEqual_bits(Z.digitsPerArray.value << Z.bitsPerDigit.exponent);
    BigDecimal expected = TWO.divide(DIGIT.pow(Z.digitsPerArray.value));
    Print.println("x >>= " + Z.bitsPerDigit.value + " = " + x);
    assertFalse(x.isOne());
    assertEquals(expected, x.toBigDecimal());

    x.plusEqual(y);
    expected = expected.add(THREE);
    x.printDetail("x += y = ");
    assertFalse(x.isOne());
    assertEquals(expected, x.toBigDecimal());
    
    x.multiplyEqual(x, workers);
    x.printDetail("x *= x = ");
    assertFalse(x.isOne());
    expected = expected.multiply(expected);
    assertEquals(expected, x.toBigDecimal());
   
    y.multiplyEqual(y, workers);
    Print.println("y = " + y);
    assertFalse(y.isOne());
    assertEquals(THREE.multiply(THREE), y.toBigDecimal());

    y.multiplyEqual(y, workers);
    Print.println("y = " + y);
    assertFalse(y.isOne());
    assertEquals(BigDecimal.valueOf(81), y.toBigDecimal());

    x.set(y).reciprocalEqual(workers);
    Print.println("x = 1/y = " + x.toBigDecimal());

    Print.println("two = " + two);
    y.multiplyEqual(two, workers);
    Print.println("y = " + y);
    assertFalse(y.isOne());
    assertEquals(BigDecimal.valueOf(162), y.toBigDecimal());

    x.set(y);
    Print.println("x = y = " + x.toBigDecimal());
    y.reciprocalEqual(workers);
    Print.println("y = 1/y = " + y.toBigDecimal());
    
    x.multiplyEqual(y, workers);
    Print.println("x *= y = " + x.toBigDecimal());

    y.set(R.valueOf(4));
    Print.println("y = " + y.toBigDecimal());
    x.set(y).reciprocalEqual(workers);
    Print.println("x = 1/y = " + x.toBigDecimal());
  }
  
  public void testSqrt() throws IOException {
    final Zahlen Z = Zahlen.FACTORY.valueOf(8, 4, 1024);
    final FixedPointFraction R = FixedPointFraction.FACTORY.valueOf(8, Z);
    Print.println("R = " + R);
    
    {//Zahlen
      final Zahlen.Element a = Z.newElement();
      Print.println("a = " + a + " = " + a.toBigInteger());
      a.plusEqual(16);
      Print.println("a = " + a + " = " + a.toBigInteger());
      a.approximateSqrtReciprocalEqual(workers);
      Print.println("a = " + a + " = " + a.toBigInteger());
    }
    

    final FixedPointFraction.Element x = R.newElement();
    Print.println("x = " + x);
    assertEquals(BigDecimal.ZERO, x.toBigDecimal());

    final FixedPointFraction.Element a = R.valueOf(1);
    assertTrue(a.isOne());
    assertEquals(BigDecimal.ONE, a.toBigDecimal());

    final FixedPointFraction.Element one = R.valueOf(1);
    a.plusEqual(one);
    Print.println("z = " + a.toBigDecimal());
    assertFalse(a.isOne());
    assertEquals(TWO, a.toBigDecimal());
 
    a.negateEqual();
    a.plusEqual(one);
    a.plusEqual(one);
    
    final FixedPointFraction.Element y = R.newElement();
    for(int i = 1; i < 10; i++) {
      a.plusEqual(one);
      Print.println("========================================================");
      Print.println("a = " + a + " = " + a.toBigDecimal());
      Print.println();
  
      x.set(a).sqrtReciprocalEqual(workers);
      y.set(x);
      
      Print.println("x = 1/sqrt(a) = " + x.toBigDecimal());
      x.printDetail("              = ");
      x.multiplyEqual(x, workers);
      Print.println("x^2 = " + x.toBigDecimal());
      x.printDetail("    = ");
      x.multiplyEqual(a, workers);
      Print.println("ax^2 = " + assertOne(x.toBigDecimal()));

      Print.println("--------------------------------------------------------");
      x.set(y);
      Print.println("x = " + x + " = " + x.toBigDecimal());
      Print.println();
      y.sqrtReciprocalEqual(workers);

      Print.println("y = 1/sqrt(x) = " + y.toBigDecimal());
      y.printDetail("              = ");
      y.multiplyEqual(y, workers);
      Print.println("y^2 = " + y.toBigDecimal());
      y.printDetail("    = ");
      y.multiplyEqual(x, workers);
      Print.println("xy^2 = " + assertOne(y.toBigDecimal()));
    }
  }
  
  static final BigDecimal DELTA = BigDecimal.valueOf(0.5).pow(20);
  static BigDecimal assertOne(final BigDecimal x) {
    final BigDecimal delta = x.subtract(BigDecimal.ONE).abs();
    assertTrue(delta.compareTo(DELTA) <= 0);
    return x;
  }

  public void testSerialization() throws IOException {
    final Zahlen Z = Zahlen.FACTORY.valueOf(16, 2, 1024);
    final FixedPointFraction R = FixedPointFraction.FACTORY.valueOf(16, Z);
    assertTrue(R == FixedPointFraction.FACTORY.valueOf(R.serialize()));

    {
      final ByteArrayOutputStream ba = new ByteArrayOutputStream();
      final DataOutputStream baout = new DataOutputStream(ba);
      R.serialize(baout);
      baout.flush();

      final DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.toByteArray()));
      final FixedPointFraction r = FixedPointFraction.FACTORY.valueOf(in);
      //Print.println("r = " + r);
      assertTrue(R == r);
    }


    runTestSerialization(R.newElement());
    for(int i = 1; i < 30; i+=2) {
      runTestSerialization(R.random(i, RANDOM));
    }
  }
  
  private void runTestSerialization(FixedPointFraction.Element x) throws IOException {
    //Print.println("x = ");
    //x.printDetail();
    final ByteArrayOutputStream ba = new ByteArrayOutputStream();
    final DataOutputStream baout = new DataOutputStream(ba);
    
    x.serialize(baout);
    baout.flush();

    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.toByteArray()));
    final FixedPointFraction.Element e = FixedPointFraction.ELEMENT_FACTORY.valueOf(in);
    assertTrue(x.get() == e.get());
    assertEquals(x, e);
  }
}
