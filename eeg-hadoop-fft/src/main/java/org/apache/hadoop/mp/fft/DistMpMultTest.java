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
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenDescriptor;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.PowerOfTwo_long;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.HadoopUtil;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.util.ToolRunner;


public class DistMpMultTest extends HadoopUtil.RunnerBase {
  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;

  private static final Random RANDOM = JavaUtil.newRandom();

  static enum Mode {PRINT, GEN, MULT}

  public static class Parameter {
    public final PowerOfTwo_long numDigits;
    public final Zahlen largeZ;
    public final SchonhageStrassen schonhagestrassen;
    public final PowerOfTwo_int K, J;

    public Parameter(final int e) {
      this.numDigits = PowerOfTwo_long.values()[e];
      
      final PowerOfTwo_int digitsPerArray = PowerOfTwo_int.values()[12];
      final int nae = numDigits.exponent - digitsPerArray.exponent + 1;
      final PowerOfTwo_int numArrays = PowerOfTwo_int.values()[nae < 0? 0: nae];
      largeZ = Zahlen.FACTORY.valueOf(32, digitsPerArray.value, numArrays.value);

      final SchonhageStrassen ss = SchonhageStrassen.FACTORY.valueOf(numDigits.value, largeZ);
      final int highest = Integer.highestOneBit(ss.modulusExponent);
      final int bitsPerElement = highest == ss.modulusExponent? (ss.modulusExponent << 1): (highest << 2);
      final int smallnumarray = bitsPerElement >> (largeZ.bitsPerDigit.exponent + digitsPerArray.exponent - 2);
      final Zahlen smallZ = Zahlen.FACTORY.valueOf(largeZ.bitsPerDigit.value, largeZ.digitsPerArray.value,
          smallnumarray > 0? smallnumarray: 1);

      schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(numDigits.value, smallZ);

      K = PowerOfTwo_int.values()[schonhagestrassen.D.exponent >> 1];
      J = PowerOfTwo_int.values()[schonhagestrassen.D.exponent - K.exponent];

      Print.println();
      Print.println("numDigits        = " + numDigits + " (e=" + e + ")");
      Print.println("digitsPerOperand = " + schonhagestrassen.digitsPerOperand());
      Print.println("J                = " + J);
      Print.println("K                = " + K);
      Print.println();
    }
  }

  private static void createInput(Function.Variable var, Parameter parameters,
      final String dir, final Configuration conf) throws IOException {
//    final Zahlen.Element element = largeZ.random(schonhagestrassen.digitsPerOperand(), RANDOM);

    final Zahlen smallZ = parameters.schonhagestrassen.Z;
    final int digitsPerElement = parameters.schonhagestrassen.bitsPerElement.value >> smallZ.bitsPerDigit.exponent;
    final Zahlen.Element[] elements = new Zahlen.Element[parameters.schonhagestrassen.D.value];

    final int halfD = parameters.schonhagestrassen.D.value >> 1;
    for(int k0 = 0; k0 < parameters.K.value; k0++) {
      for(int k1 = 0; k1 < parameters.J.value; k1++) {
        final int i = (k1 << parameters.K.exponent) + k0;
        elements[i] = i < halfD? smallZ.random(digitsPerElement, RANDOM): smallZ.newElement();
      }
      if (k0 % 100 == 0) {
        Print.println("k0 = " + k0);
      }
      FunctionDescriptor.write(var, elements, k0, parameters.K, parameters.J.value, dir, conf);
      for(int k1 = 0; k1 < parameters.J.value; k1++) {
        final int i = (k1 << parameters.K.exponent) + k0;
        elements[i].reclaim();
        elements[i] = null;
      }
    }
  }

  DistMpMultTest() throws FileNotFoundException {
    DistMpMult.printVersions();
  }

  @Override
  public int run(String[] args) throws Exception {
    final Function.Variable a = Function.Variable.valueOf("a");
    final Function.Variable b = Function.Variable.valueOf("b");
    return run(args, "c", a, b);
  }

  int run(String[] args, String product, Function.Variable... vars) throws Exception {
    Print.print("args", args);

    //parse arguments
    final int e;
    final Mode mode;
    final String dir;
    {
      int i = 0;
      e = Parse.string2integer(args[i++]);
      mode = Mode.valueOf(args[i++].toUpperCase());
  
      if (mode == Mode.GEN) {
        final String fname = logfile.getName();
        dir = fname.substring(0, fname.indexOf(".log"));
      } else {
        dir = args[i++];
      }
      Print.println("dir              = " + dir);
    }

    final Parameter parameters = new Parameter(e);
    if (mode == Mode.PRINT) {
      return 0;
    }

    //setup inputs
    if (mode == Mode.GEN) {
      for(int i = 0; i < vars.length; i++) {
        createInput(vars[i], parameters, dir, getConf());
        timer.tick("random " + vars[i]);
        Print.printMemoryInfo();
      }
      timer.tick(mode);
    }

    //start verifier
    final Verifier v = e > 20? null: new Verifier(parameters, dir, vars);
    if (v != null)
      v.start();

    //run {@link DistMpMult}
    final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(
        getClass().getSimpleName(), 2, null);
    final ZahlenDescriptor p = DistMpMult.multiply(Function.Variable.valueOf(product),
        parameters.schonhagestrassen.digitsPerOperand(),
        parameters.schonhagestrassen.Z, parameters.largeZ,
        timer, workers, dir, getConf(), vars);
    Print.println(product + " = " + p);
    timer.tick(DistMpMult.class.getSimpleName());

    Print.printMemoryInfo();

    if (v != null) {
      final Zahlen.Element computed = ZahlenDescriptor.read(p.output,
          parameters.schonhagestrassen, parameters.largeZ, dir, getConf());
      Print.println("computed = " + computed.toBrief());

      v.join();
      if (!computed.equals(v.expected)) {
        Print.println("ERROR: computed != expected");
        Print.isOutEnabled.set(false);
        v.expected.printDetail("expected ");
        computed.printDetail("computed ");
        Print.isOutEnabled.set(true);
        throw new RuntimeException("!computed.equals(expected)");
      }
    }

    timer.tick("DONE (dir = " + dir + ")");
    return 0;
  }
  
  private class Verifier extends Thread {
    final Function.Variable[] vars;
    final Parameter parameters;
    final String dir;
    Zahlen.Element expected;
    
    Verifier(final Parameter parameters, final String dir, final Function.Variable... vars) {
      this.vars = vars;
      this.parameters = parameters;
      this.dir = dir;
    }

    @Override
    public void run() {
      Print.println(getClass().getSimpleName() + ": STARTED");
      try {
        final Zahlen.Element[] elements = new Zahlen.Element[vars.length];
        for(int i = 0; i < elements.length; i++) {
          elements[i] = ZahlenDescriptor.read(vars[i],
            parameters.schonhagestrassen, parameters.largeZ, dir, getConf());
        }
        
        if (elements.length == 1) {
          elements[0].multiplyEqual(elements[0], null);
        } else {
          elements[0].multiplyEqual(elements[1], null);
          elements[1].reclaim();
        }

        expected = elements[0];
        Print.println(getClass().getSimpleName() + ": expected = " + expected.toBrief());
      } catch(IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistMpMultTest(), args));
  }
}