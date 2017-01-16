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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mp.DistMpBase;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenDescriptor;
import org.apache.hadoop.mp.ZahlenSerialization;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenOutputFormat;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.PowerOfTwo_long;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.sum.DistCarrying;
import org.apache.hadoop.mp.sum.DistCompSum;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;

public class DistMpMult {
  public static final String VERSION = "20110307";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  private static final String PREFIX = DistMpMult.class.getSimpleName();
  public static final String NAME = PREFIX.toLowerCase();
  public static final String DESCRIPTION = "Distributed Multi-precision Multiplication";

  static final AtomicBoolean printversions = new AtomicBoolean(false);

  public static void printVersions() {
    if (!printversions.getAndSet(true)) {
      Print.println();
      Print.println(DistFft.NAME + ".VERSION = " + DistFft.VERSION);
      Print.println(NAME + ".VERSION = " + VERSION);
      Print.println(NAME + ".LOCAL_THRESHOLD = " + LOCAL_THRESHOLD);
      Print.printSystemInfo();
    }
  }

  public static final PowerOfTwo_long LOCAL_THRESHOLD = PowerOfTwo_long.values()[16];

  /** Compute product = x*y with DistFft */
  public static ZahlenDescriptor multiply(final Function.Variable product,
      final long numDigits, final Zahlen smallZ, final Zahlen largeZ,
      final JavaUtil.Timer timer, final JavaUtil.WorkGroup workers,
      final String dir, final Configuration conf,
      final Function... x) throws Exception {
    final SchonhageStrassen schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(numDigits, smallZ);
    final PowerOfTwo_int K = PowerOfTwo_int.values()[schonhagestrassen.D.exponent >> 1];
    final PowerOfTwo_int J = PowerOfTwo_int.values()[schonhagestrassen.D.exponent - K.exponent];

    if (numDigits <= LOCAL_THRESHOLD.value) {
      return multiply_local(product, schonhagestrassen, largeZ, J, K, timer, workers, dir, conf, x);
    } else {
      return multiply_mapreduce(product, schonhagestrassen, J, K, timer, dir, conf, x);
    }
  }
  
  private static ZahlenDescriptor multiply_local(final Function.Variable product,
      final SchonhageStrassen schonhagestrassen, final Zahlen largeZ,
      final PowerOfTwo_int J, final PowerOfTwo_int K,
      final JavaUtil.Timer timer, final JavaUtil.WorkGroup workers,
      final String dir, final Configuration conf,
      final Function... x) throws Exception {
    if (x.length != 1 && x.length != 2) {
      throw new IllegalArgumentException("x.length != 1 && x.length != 2, x.length=" + x.length);
    }
    if (IS_VERBOSE)
      Print.beginIndentation("multiply_local: " + product + "; x = " + Arrays.asList(x));
    final Function f = x.length == 1?
        new Function.Square(x[0]):
        new Function.Multiplication(x[0], x[1]);

    FunctionDescriptor.evaluateLocal(product, f, schonhagestrassen, largeZ, workers, dir, conf);
    final ZahlenDescriptor p = new ZahlenDescriptor(product, J, K.value);
    if (IS_VERBOSE)
      Print.endIndentation("multiply_local: return " + p);
    if (timer != null) {
      timer.tick("multiply_local, product=" + product);
    }
    return p;
  }

  private static ZahlenDescriptor multiply_mapreduce(final Function.Variable product,
      final SchonhageStrassen schonhagestrassen,
      final PowerOfTwo_int J, final PowerOfTwo_int K,
      final JavaUtil.Timer timer, final String dir, final Configuration conf,
      final Function... x) throws Exception {
    if (x.length != 1 && x.length != 2) {
      throw new IllegalArgumentException("x.length != 1 && x.length != 2, x.length=" + x.length);
    }
    if (IS_VERBOSE)
      Print.beginIndentation("multiply_mapreduce: " + product + "; x = " + Arrays.asList(x));

    final DistFft forward = new DistFft(schonhagestrassen, false, J, K, dir);
    if (IS_VERBOSE) forward.printDetail("forward");

    final long digitsPerOperand = schonhagestrassen.digitsPerOperand();
    if (IS_VERBOSE) Print.println("digitsPerOperand = " + digitsPerOperand);

    //submit forward-FFT jobs
    final DistFft.DftJob[] X = new DistFft.DftJob[x.length];
    for(int i = 0; i < x.length; i++) {
      X[i] = forward.new DftJob(x[i], conf);
    }
    final boolean[] submittedX = new boolean[x.length];
    for(int i = 0; i < x.length; i++) {
      submittedX[i] = submit("forward " + x[i], forward, X[i], false, timer);
    }
    for(int i = 0; i < x.length; i++) {
      wait4job("forward " + x[i], X[i], submittedX[i], null);
    }

    //create function
    final Function fun = x.length == 1?
        new Function.Square(X[0].descriptor.output):
        new Function.Multiplication(
          X[0].descriptor.output, X[1].descriptor.output);

    //submit the backward-FFT job
    final DistFft backward = new DistFft(schonhagestrassen, true, J, K, dir);
    final Function.Variable cdr = Function.Variable.valueOf(product.getName() + "'");

    final DistFft.DftJob B = backward.new DftJob(cdr, fun, conf);
    ZahlenOutputFormat.setSplit(schonhagestrassen.digitsPerElement(), 3, B.job.getConfiguration());
    submit("backward", backward, B, true, timer);

    //submit the summation job
    final String sumdir = dir + Path.SEPARATOR + cdr.getName();
    final DistCompSum sum = new DistCompSum(schonhagestrassen, J, K, sumdir);
    final Function.Variable[] parts = ZahlenSerialization.ZahlenOutputFormat.toSplitVariables(cdr, 3);
    final Function.Variable remainder = Function.Variable.valueOf(product.getName() + "''");
    final DistCompSum.SumJob S = sum.new SumJob(remainder, conf, parts);
    submit("summation", sum, S, true, timer);

    //submit the carrying job
    final DistCarrying carry = new DistCarrying(schonhagestrassen, J, K, sumdir);
    final DistCarrying.MpJob C = carry.new MpJob(remainder, product, conf);
    submit("carrying", carry, C, true, timer);
 
    //done
    rename(product, sumdir, dir, conf);
    final ZahlenDescriptor p = new ZahlenDescriptor(product, C.descriptor.numParts, C.descriptor.elementsPerPart);
    if (IS_VERBOSE)
      Print.endIndentation("multiply_mapreduce: returns " + p);
    return p;
  }
  
  static void rename(final Function.Variable var, final String srcdir, final String dir,
      final Configuration conf) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    final Path src = new Path(srcdir, var.getName());
    final Path dst = new Path(dir, var.getName());
    final boolean b = fs.rename(src, dst);
    if (!b) {
      throw new IOException("Failed to rename from " + src + " to " + dst);
    }
  }
  
  static boolean submit(final String name, final DistMpBase mp,
      final DistMpBase.MpJob J, final boolean wait, final JavaUtil.Timer timer
      ) throws IOException, InterruptedException, ClassNotFoundException {
    if (IS_VERBOSE)
      Print.println(name + ": " + mp);
    final boolean submitted = J.submit();
    if (IS_VERBOSE)
      Print.println((submitted? "SUBMIT": "SKIPPED") + " JOB: " + J.jobname);
    if (wait)
      wait4job(name, J, submitted, timer);
    return submitted;
  }
  static void wait4job(final String name, final DistMpBase.MpJob J,
      final boolean submitted, final JavaUtil.Timer timer
      ) throws IOException, InterruptedException, ClassNotFoundException {
    if (submitted) {
      J.wait4job(true);
    }
    if (timer != null) {
      timer.tick(name + ", " + J.jobname);
      if (IS_VERBOSE)
        Print.println("-----------------------------------------------------");
    }
  }
}