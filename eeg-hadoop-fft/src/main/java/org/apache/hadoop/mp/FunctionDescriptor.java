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
package org.apache.hadoop.mp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mp.Function.Variable;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;

/**
 * Describe a function.
 */
public class FunctionDescriptor extends ZahlenDescriptor {
  /////////////////////////////////////////////////////////////////////////////
  private static final String PREFIX = FunctionDescriptor.class.getSimpleName();
  private static final String PROPERTY_INPUTS = PREFIX + ".inputs";

  public final Function input;
  
  public FunctionDescriptor(final Function input, final Variable output,
      final PowerOfTwo_int numParts, final int elementsPerPart) {
    super(output, numParts, elementsPerPart);
    this.input = input;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(input=" + input
    + ", output=" + output + ")";
  }

  public String functionString() {
    return input.toString();
  }

  @Override
  public void serialize(final Configuration conf) {
    super.serialize(conf);
    conf.set(PROPERTY_INPUTS, input.serialize());
  }

  public static FunctionDescriptor valueOf(final Configuration conf) {
    final ZahlenDescriptor z = ZahlenDescriptor.valueOf(conf);
    final Function input = Function.PARSER.valueOf(conf.get(PROPERTY_INPUTS));
    return new FunctionDescriptor(input, z.output, z.numParts, z.elementsPerPart);
  }

  @Override
  public Void serialize(DataOutput out) throws IOException {
    super.serialize(out);
    input.serialize(out);
    return null;
  }
  
  public static FunctionDescriptor valueOf(DataInput in) throws IOException {
    final ZahlenDescriptor z = ZahlenDescriptor.valueOf(in);
    final Function input = Function.PARSER.valueOf(in);
    return new FunctionDescriptor(input, z.output, z.numParts, z.elementsPerPart);
  }

  public static Zahlen.Element evaluateLocal(
      final Variable result, final Function f,
      final SchonhageStrassen schonhagestrassen, final Zahlen largeZ,
      final JavaUtil.WorkGroup workers,
      final String dir, final Configuration conf) throws Exception {
    final Map<Variable, Zahlen.Element> m = read(
        new TreeMap<Variable, Zahlen.Element>(),
        f, schonhagestrassen, largeZ, dir, conf);
    final Zahlen.Element r = f.evaluate(m, workers);
    for(Zahlen.Element e : m.values()) {
      e.reclaim();
    }
    if (result != null) {
      write(result, r, schonhagestrassen, dir, conf);
    }
    return r;
  }

  /** Read and evaluate a function. */
  public static class Evaluater {
    private static final int BATCH_SIZE = 64;

    private final int offset;
    private final PowerOfTwo_int step;
    private final int numElements;
    
    private final SchonhageStrassen schonhagestrassen;

    private final String dir;
    private final Configuration conf;
      
    public Evaluater(final int offset, final PowerOfTwo_int step, final int numElements,
        final SchonhageStrassen schonhagestrassen,
        final String dir, final Configuration conf) {
      this.offset = offset;
      this.step = step;
      this.numElements = numElements;
      
      this.schonhagestrassen = schonhagestrassen;

      this.dir = dir;
      this.conf = conf;
    }
    
    public Zahlen.Element[] evaluate(final Function f, final JavaUtil.Timer timer,
        final JavaUtil.WorkGroup workers) throws IOException {
      final Zahlen.Element[] z = new Zahlen.Element[numElements];
      final List<Variable> variables = f.getVariables();
      
      //print required space
      final long available = Runtime.getRuntime().maxMemory() - (1L << 28);
      final long required = (z.length >> 3) * ((long)schonhagestrassen.modulusExponent);
      final int todisk_threshold = 0;//required < available? 0: z.length - 2*BATCH_SIZE;
      Print.println(              "variables.size() = " + variables.size());
      Print.println(String.format("available        = %.3f GB (= "
          + Parse.long2string(available) + " B)",
          available/(double)(1 << 30)));
      Print.println(String.format("required         = %.3f GB (= "
          + Parse.long2string(required) + " B)",
          required/(double)(1 << 30)));
      Print.println(              "todisk_threshold = " + todisk_threshold);

      final List<VariableReader> readers = new ArrayList<VariableReader>();
      final Runner[] runners = new Runner[z.length];
      try {
        //open files
        for(Variable var: variables) {
          readers.add(new VariableReader(var));
        }
        timer.tick("readers.size() = " + readers.size());

        //evaluate
        for(int i = 0; i < z.length;) {
          for(int j = 0; i < z.length && j < BATCH_SIZE; j++) {
            runners[i] = new Runner(f, i, readNext(readers), i < todisk_threshold);
            workers.submit("i=" + i, runners[i]);
            i++;
          }
          workers.waitUntil(16);
          timer.tick("evaluating " + f + ", i=" + i);
          if ((i & 0xFF) == 0) {
            Print.printMemoryInfo();
          }
        }
      } finally {
        for(VariableReader r : readers) {
          if (r.in != null) {
            try {
              r.in.close();
            } catch(IOException ioe) {
              Print.println("Close failed(r.var=" + r.var + "): " + ioe);
            }
          }
        }
      }
      workers.waitUntilZero();

      for(int i = 0; i < z.length; i++) {
        z[i] = runners[i].getResult();
      }
      return z;
    }
    
    private static final Map<Variable, Zahlen.Element> readNext(
        final List<VariableReader> readers) throws IOException {
      final Map<Variable, Zahlen.Element> m = new TreeMap<Variable, Zahlen.Element>();
      for(VariableReader r: readers) {
        final Zahlen.Element e = r.in.readNext();
        m.put(r.var, e);
      }
      return m;
    }
    
    private class VariableReader {
      final Variable var;
      final ZahlenDescriptor.Reader in;
      
      VariableReader(final Variable var) throws IOException {
        this.var = var;
        this.in = new Reader(var, offset, step, numElements, dir, conf);
      }
    }
    
    private class Runner implements Runnable {
      final Function f;
      final int index;
      final Map<Variable, Zahlen.Element> m;
      final boolean todisk;
      private Zahlen.Element result = null;
      private File tmpfile = null;
      
      Runner(final Function f, final int index,
          final Map<Variable, Zahlen.Element> m,
          final boolean todisk) {
        this.f = f;
        this.index = index;
        this.m = m;
        this.todisk = todisk;
      }

      Zahlen.Element getResult() {
        if (result == null) {
          for(int i = 0; i < 3; i++) {
            try {
              final DataInputStream in = new DataInputStream(new FileInputStream(tmpfile));
              try {
                result = ZahlenSerialization.E.valueOf(in);
              } finally {
                in.close();
              }
            } catch (IOException ioe) {
              Print.println("Got an exception when reading from " + tmpfile);
              Print.printStackTrace(ioe);
            }
          }
        }
        return result;
      }

      @Override
      public void run() {
        result = f.ssEvaluate(m, schonhagestrassen);
        m.clear();

        if (todisk) {
          try {
            tmpfile = File.createTempFile(String.format("z%04d-", index), ".writable");
            final DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpfile));
            try {
              new ZahlenSerialization.E().set(result).write(out);
            } finally {
              out.close();
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          result.reclaim();
          result = null;
//          Print.println("Wrote result " + index + " of " + f + " to " + tmpfile);
        }
      }
    }
  }
}