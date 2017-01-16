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

import org.apache.hadoop.mp.util.Print;


public class TestMod1Fraction extends junit.framework.TestCase {
  public void testBasic() throws Exception {
    final Mod1Fraction_IntArray.Factory f = new Mod1Fraction_IntArray.Factory();
    Print.println("0 = " + f.zero());
    
    final Mod1Fraction_IntArray x = new Mod1Fraction_IntArray(4);
    final Mod1Fraction_IntArray sum = f.zero();
    final Mod1Fraction_IntArray sub = f.zero();
    for(int i = 1; i < 10; i++) {
      x.init(1, i, 0);
      Print.println("\n1/" + i + " = " + x);
      sum.addMod1Equal(x);
      Print.println("sum = " + sum);
      sub.subtractMod1Equal(x);
      Print.println("sub = " + sub);
    }
  }
  public void testSummation() throws Exception {
    Mod1Fraction.setPrecision(1000);
    for(int i = 0; i < 10; i++) {
      final long n = (1L << 31) + (i - 5)*20;
      final Summation sigma = new Summation(true, n, 20, 100+i, -20, 0);
      Print.println("sigma           = " + sigma);
      Print.println("sigma.compute() = " + sigma.compute(null));
    }
  }
}
