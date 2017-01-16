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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BrentSalamin extends Configured implements Tool {
  public static final String DESCRIPTION = "Brent Salamin algorithm";

  final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
  final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(BrentSalamin.class.getSimpleName(), 2, null);

  final Zahlen Z = Zahlen.FACTORY.valueOf(32, 1 << 12, 1 << 10);
  final FixedPointFraction R = FixedPointFraction.FACTORY.valueOf((1 << 16) - Z.digitsPerArray.value, Z);

  final FixedPointFraction.Element a = R.valueOf(1);
  final FixedPointFraction.Element b = R.valueOf(2).sqrtReciprocalEqual(workers);
  final FixedPointFraction.Element next_a = R.valueOf(1).plusEqual(b).shiftRightEqual_bits(1);
  final FixedPointFraction.Element t = R.valueOf(1).shiftRightEqual_bits(2);
  int e = 0;
  final FixedPointFraction.Element pi = R.newElement();
  
  private boolean computepi = false;
  
  synchronized void next() {
    computepi = false;

    //b_{n+1} = sqrt(a_n b_n)
    b.multiplyEqual(a, workers);
    final FixedPointFraction.Element tmp = R.newElement().set(b);
    b.sqrtReciprocalEqual(workers).multiplyEqual(tmp, workers);

    //t_{n+1} = t_n - 2^e(a_{n+1} - a_n)^2
    tmp.set(next_a);
    tmp.negateEqual().plusEqual(a);
    tmp.multiplyEqual(tmp, workers).shiftLeftEqual_bits(e);
    t.plusEqual(tmp.negateEqual());

    //a_{n+1}
    a.set(next_a);

    //a_{n+2} = (a_{n+1} + b_{n+1})/2
    next_a.plusEqual(b).shiftRightEqual_bits(1);
    
    //e = e + 1
    e++;
  }
  
  synchronized void computePi() {
    if (!computepi) {
      //pi = a_{n+2}^2 / t_{n+1}
      final FixedPointFraction.Element tmp = R.newElement().set(t).reciprocalEqual(workers);
      pi.set(next_a).multiplyEqual(next_a, workers).multiplyEqual(tmp, workers);
      computepi = true;
    }
  }

  @Override
  public String toString() {
    return "\n  a = " + a.toBrief()
         + "\n  b = " + b.toBrief()
         + "\n  t = " + t.toBrief()
         + "\n  e = " + e
         + "\npi  = " + pi.toBrief();
  }

  @Override
  public int run(String[] args) throws Exception {
    final int nIterations = 20;
    Print.println("nIterations = " + nIterations);

//    Print.isOutEnabled.set(false);
//    Print.println(this);
//    Print.isOutEnabled.set(true);

    for(int i = 0; i < nIterations; i++) {
      timer.tick("i = " + i);
      next();
      Print.printMemoryInfo();

//      Print.println(this);
    }

    computePi();
    Print.isOutEnabled.set(false);
    Print.println();
    pi.printDetail("pi");
    Print.isOutEnabled.set(true);

    timer.tick("DONE");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Print.initLogFile(BrentSalamin.class.getSimpleName());
    Print.printSystemInfo();
    System.exit(ToolRunner.run(null, new BrentSalamin(), args));
  }
}
