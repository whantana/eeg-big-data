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
package org.apache.hadoop.mp.fft;

import java.util.Random;

import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;

public class TestDistFft extends junit.framework.TestCase {
  public void testParallelFft() {
	// Timer
    final JavaUtil.Timer timer = new JavaUtil.Timer(false, false);
    
    // D : Power of two integer
    final PowerOfTwo_int D = PowerOfTwo_int.valueOf(1 << 4);
    Print.println("D                = " + D); // 16

    // bits per element
    final PowerOfTwo_int bitsPerElement = PowerOfTwo_int.valueOf(1 << 4);
    Print.println("bitsPerElement   = " + bitsPerElement); // 16
    
    // 16 bits per digit
    // 2^12 digits per array
    // 2^10 arrays 
    final int bitsPerDigit = 16;
    final Zahlen Z = Zahlen.FACTORY.valueOf(bitsPerDigit, 4096, 1024);

    final int modulusExponent = Math.max(D.value, bitsPerElement.value << 3) >> 1;
    Print.println("modulusExponent  = " + modulusExponent);
    final SchonhageStrassen schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(
        modulusExponent, bitsPerElement, D, Z);

    final PowerOfTwo_int J = PowerOfTwo_int.values()[D.exponent >> 1];
    final PowerOfTwo_int K = PowerOfTwo_int.values()[D.exponent - J.exponent];
    final String dir = "test-" + Parse.currentTime2String();
    final DistFft parameters = new DistFft(schonhagestrassen, false, J, K, dir);
    parameters.printDetail("");

    //setup input
    final Zahlen.Element x = Z.random(JavaUtil.toInt(schonhagestrassen.digitsPerOperand()), new Random());
    timer.tick("x = " + x.toBrief());
    final Zahlen.Element[] a = x.split(schonhagestrassen.bitsPerElement.value, D.value);

    final Zahlen.Element[][] reduce = new Zahlen.Element[J.value][K.value];

    //inner
    for(int k0 = 0; k0 < K.value; k0++) {
      final Zahlen.Element[] t = new Zahlen.Element[J.value];
      for(int k1 = 0; k1 < J.value; k1++) {
        final int i = (k1 << K.exponent) + k0;
        t[k1] = Z.newElement().set(a[i]);
      }
      parameters.dft(t, null);
      parameters.scaleMultiplication(k0, t, null);
      for(int j0 = 0; j0 < J.value; j0++) {
        reduce[j0][k0] = t[j0];
      }
    }
    
    //outer
    final Zahlen.Element[] b = new Zahlen.Element[D.value];
    for(int j0 = 0; j0 < J.value; j0++) {
      final Zahlen.Element[] t = reduce[j0];
      parameters.dft(t, null);
      for(int j1 = 0; j1 < K.value; j1++) {
        final int i = (j1 << J.exponent) + j0;
        b[i] = t[j1];
      }
    }
    
    //verify output
    schonhagestrassen.parallel.fft(a, null);
    //Print.print("a", a);
    //Print.print("b", b);
    for(int i = 0; i < J.value; i++) {
      if (!b[i].equals(a[i])) {
        Print.println("b[" + i + "] = ");
        b[i].print(10);
        Print.println("a[" + i + "] = ");
        a[i].print(10);
        Print.println("!b[j1].equals(a[i])");
        throw new RuntimeException("!b[j1].equals(a[i])");
      }
    }
    Print.print("DONE");
  }
}
