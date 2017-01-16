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
package org.apache.hadoop.mp.sum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mp.DistMpBase;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.util.Print;

public class DistCompSum extends DistComponentwiseOp {
  public static final String VERSION = "20110125";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  public static final String NAME = DistCompSum.class.getSimpleName().toLowerCase();
  public static final String DESCRIPTION = "Distributed Multi-precision Summation";
  
  public DistCompSum(SchonhageStrassen schonhagestrassen, PowerOfTwo_int J,
      PowerOfTwo_int K, String dir) {
    super(schonhagestrassen, J, K, dir);
  }

  private FunctionDescriptor toFunctionDescriptor(final Function.Variable output,
      final Function... summands) throws IOException {
    if (summands.length < 2) {
      throw new IllegalArgumentException(summands.length + " = summands.length < 2");
    }
    Function sum = new Function.Addition(summands[0], summands[1]);
    for(int i = 2; i < summands.length; i++) {
      sum = new Function.Addition(sum, summands[i]);
    }
    return new FunctionDescriptor(sum, output, J, K.value);
  }

  public class SumJob extends MpJob {
    public SumJob(final Function.Variable output, final Configuration conf,
        final Function... f) throws IOException {
      super(toFunctionDescriptor(output, f), conf);
    }
  }
}