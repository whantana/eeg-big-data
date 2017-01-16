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

import java.io.FileNotFoundException;
import java.util.Random;

import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.HadoopUtil;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.util.ToolRunner;


public class DistFftTest extends HadoopUtil.RunnerBase {
  protected DistFftTest() throws FileNotFoundException {}

  @Override
  public int run(String[] args) throws Exception {
    final PowerOfTwo_int D = PowerOfTwo_int.valueOf(1 << 4);
    Print.println("D                = " + D);
    final PowerOfTwo_int bitsPerElement = PowerOfTwo_int.valueOf(1 << 4);
    Print.println("bitsPerElement   = " + bitsPerElement);
    final int bitsPerDigit = 16;
    final Zahlen Z = Zahlen.FACTORY.valueOf(bitsPerDigit, 4096, 1024);
    final int modulusExponent = Math.max(D.value, bitsPerElement.value << 3) >> 1;
    Print.println("modulusExponent  = " + modulusExponent);
    final SchonhageStrassen schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(
        modulusExponent, bitsPerElement, D, Z);

    final PowerOfTwo_int K = PowerOfTwo_int.values()[schonhagestrassen.D.exponent >> 1];
    final PowerOfTwo_int J = PowerOfTwo_int.values()[schonhagestrassen.D.exponent - K.exponent];
    final String dir = DistFft.NAME + "_" + Parse.currentTime2String();
    final DistFft parameters = new DistFft(schonhagestrassen, false, J, K, dir);
    final Function.Variable a = Function.Variable.valueOf("a");
    final FunctionDescriptor vars = new FunctionDescriptor(a, Function.Variable.valueOf("b"), J, K.value);
    parameters.printDetail("");
    Print.println("vars = " + vars);

    //setup input
    final Zahlen.Element x = Z.random(JavaUtil.toInt(schonhagestrassen.digitsPerOperand()), new Random());
    timer.tick("x = " + x.toBrief());
    final Zahlen.Element[] A = x.split(schonhagestrassen.bitsPerElement.value, D.value);
    Print.print("A", A);

    for(int k0 = 0; k0 < K.value; k0++) {
      FunctionDescriptor.write(a, A, k0, K, J.value, parameters.dir, getConf());
    }
    
    //submit a job
    final DistFft.DftJob job = parameters.new DftJob(a, getConf());
    job.submit();
    
    //verify output
    schonhagestrassen.parallel.fft(A, null);
    Print.print("dft(a)", A);

    final Zahlen.Element[] b = new Zahlen.Element[A.length];
    for(int j0 = 0; j0 < J.value; j0++) {
      final Zahlen.Element[] t = vars.readOutput(j0, parameters.dir, getConf());
      for(int i = 0; i < t.length; i++)
        b[(i << J.exponent) + j0] = t[i];
    }
    Print.print("b", b);
    
    for(int i = 0; i < A.length; i++)
      if (!b[i].equals(A[i])) {
        Print.println("b[" + i + "] = ");
        b[i].print(10);
        Print.println("a[" + i + "] = ");
        A[i].print(10);
        Print.println("!b[i].equals(a[i])");
        throw new RuntimeException("!b[i].equals(a[i])");
      }

    timer.tick("DONE");
      return 0;
    }
 
  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistFftTest(), args));
  }
}