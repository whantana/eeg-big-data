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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.math.FixedPointFraction;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;

public class DistRecip {
  public static final String VERSION = "20100819";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  private static final String PREFIX = DistRecip.class.getSimpleName();
  public static final String NAME = PREFIX.toLowerCase();
  public static final String DESCRIPTION = "Compute reciprocal by Newton's method";

  static final AtomicBoolean printversions = new AtomicBoolean(false);

  public static void printVersions() {
    if (!printversions.getAndSet(true)) {
      Print.println(NAME + ".VERSION = " + VERSION);
      Print.printSystemInfo();
    }
  }


  /** Compute recip = 1/x */
  public static FunctionDescriptor approximateReciprocal(final Function.Variable recip, final Function x,
      final long numDigits, final Zahlen smallZ, final Zahlen largeZ, final FixedPointFraction R,
      final JavaUtil.WorkGroup workers,
      final String dir, final Configuration conf) throws Exception {

    final SchonhageStrassen schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(numDigits, smallZ);
    if (numDigits < DistMpMult.LOCAL_THRESHOLD.value) {
      final Zahlen.Element xx = FunctionDescriptor.evaluateLocal(null, x, schonhagestrassen, largeZ, workers, dir, conf);
      final FixedPointFraction.Element r = R.valueOf(xx).reciprocalEqual(workers); 
      FunctionDescriptor.write(recip, r.getFraction(), schonhagestrassen, dir, conf);
    }

      
    return null;
  }
}