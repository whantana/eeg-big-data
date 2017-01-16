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
package org.apache.hadoop.mp.newton;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenDescriptor;
import org.apache.hadoop.mp.fft.DistFft;
import org.apache.hadoop.mp.fft.DistMpMult;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.PowerOfTwo_long;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;

public class DistMpSqrt {
  public static final String VERSION = "20110308";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  private static final String PREFIX = DistMpMult.class.getSimpleName();
  public static final String NAME = PREFIX.toLowerCase();
  public static final String DESCRIPTION = "Distributed Multi-precision Sqrt";

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

  private static ZahlenDescriptor sqrt_local(final Function.Variable out,
      final SchonhageStrassen schonhagestrassen, final Zahlen largeZ,
      final PowerOfTwo_int J, final PowerOfTwo_int K,
      final JavaUtil.Timer timer, final JavaUtil.WorkGroup workers,
      final String dir, final Configuration conf,
      final Function x) throws Exception {
    final Function f = new Function.Sqrt(x);
    if (IS_VERBOSE)
      Print.beginIndentation("sqrt_local: " + out + " = " + f);

    FunctionDescriptor.evaluateLocal(out, f, schonhagestrassen, largeZ, workers, dir, conf);
    final ZahlenDescriptor p = new ZahlenDescriptor(out, J, K.value);
    if (IS_VERBOSE)
      Print.endIndentation("sqrt_local: return " + p);
    if (timer != null) {
      timer.tick("sqrt_local: " + out);
    }
    return p;
  }
}
