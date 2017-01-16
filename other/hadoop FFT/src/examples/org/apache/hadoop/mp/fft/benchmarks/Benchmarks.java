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
package org.apache.hadoop.mp.fft.benchmarks;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;

import org.apache.hadoop.mp.gmp.GmpMultiplier;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;

public class Benchmarks {
  public static final String DESCRIPTION = "Benchmarks: multiplication";
  private static final Random RANDOM = JavaUtil.newRandom();

  public static void multiplicationBench() throws IOException {
    final JavaUtil.Timer t = new JavaUtil.Timer(false, false);
    Print.printSystemInfo();

    final Zahlen Z = Zahlen.FACTORY.valueOf(32, 1 << 13, 1 << 10);
    Print.println(Z);

    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(
        Benchmarks.class.getSimpleName(), 2, null);

    for(int i = 4; i < 32; i++) {
      Print.println();
      final int d = 1 << i;
      t.tick("i=" + i + ", d=" + d);
      final Results r = new Results(d + " digits");

      final Zahlen.Element x = Z.random(d, RANDOM);
      final Zahlen.Element x2 = Z.newElement().set(x);
      final Zahlen.Element x3 = Z.newElement().set(x);
//      final Zahlen.Element x4 = Z.newElement().set(x);
      final Zahlen.Element y = Z.random(d, RANDOM);
      Print.println("x  = " + x.toBrief());
      Print.println("y  = " + y.toBrief());

      final int max_nDigits = x.numberOfDigits() > y.numberOfDigits()? x.numberOfDigits(): y.numberOfDigits();
      t.tick("ramdom");
      final BigInteger xx = x.toBigInteger();
      final BigInteger yy = y.toBigInteger();
      t.tick("toBigInteger");

      final Zahlen.Element x1 = GmpMultiplier.get().multiply(x, y);
      r.add("gmp", t.tick());

      x.multiplyEqual(y, workers);
      r.add("multiplyEqual", t.tick());

      x2.multiplyEqual_Karatsuba(y, max_nDigits, workers);
      r.add("Karatsuba", t.tick());
      
      x3.multiplyEqual_SchonhageStrassen(y, max_nDigits, workers);
      r.add("SchonhageStrassen", t.tick());

//      x4.multiplyEqual_Apfloat(y);
//      r.add("Apfloat", t.tick());
      
      final BigInteger expected = xx.multiply(yy);
      r.add("BigInteger", t.tick());
      
      Print.println("x  = " + x.toBrief());
      Print.println("x1 = " + x1.toBrief());
      Print.println("x2 = " + x2.toBrief());
//      Print.println("x3 = " + x3.toBrief());
//      Print.println("x4 = " + x4.toBrief());
      Print.println(r);
      Print.println("========================================================");
      /*
      if (d > Zahlen.BIG_INTEGER_THRESHOLD) {
        if (tBigInteger < tBest) {
          Print.println("!!!");
          Print.println("!!! tBigInteger = " + tBigInteger + " < tBest = " + tBest);
          Print.println("!!!");
        }
      } else if (d <= Zahlen.KARATSUBA_THRESHOLD  && tKaratsuba < tBest) {
        Print.println("!!!");
        Print.println("!!! tKaratsuba = " + tKaratsuba + " < tBest = " + tBest);
        Print.println("!!!");
      }
      if (d <= Zahlen.KARATSUBA_THRESHOLD  && tSchonhageStrassen < tBest) {
        Print.println("!!!");
        Print.println("!!! tSchonhageStrassen = " + tSchonhageStrassen + " < tBest = " + tBest);
        Print.println("!!!");
      }*/
      
      if (!x.toBigInteger().equals(expected))
        throw new ArithmeticException("!x.equals(expected)");
      if (!x1.equals(x))
        throw new ArithmeticException("!x1.equals(x)");
      if (!x2.equals(x))
        throw new ArithmeticException("!x2.equals(x)");
      if (!x3.equals(x))
        throw new ArithmeticException("!x3.equals(x)");
//      if (!x4.equals(x)) {
//        Print.printStackTrace(new ArithmeticException("!x4.equals(x)"));
//        System.exit(1);
//      }
      t.tick("verification");
      
      Print.printMemoryInfo();
    }
  }

  public static void main(String[] args) throws IOException {
//    final ApfloatContext ctx = ApfloatContext.getContext();
//    ctx.setDefaultRadix(16);
//    ctx.setMaxMemoryBlockSize(1L << 24);
//    ctx.setMemoryTreshold(1 << 16);
//    ctx.setSharedMemoryTreshold(1 << 16);
//    ctx.setBlockSize(1 << 16);
//    ctx.setCleanupAtExit(true);
    

    Print.initLogFile(Benchmarks.class.getSimpleName());
    multiplicationBench();
  }
}
