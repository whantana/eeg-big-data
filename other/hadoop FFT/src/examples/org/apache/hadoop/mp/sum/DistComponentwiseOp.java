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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mp.DistMpBase;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenInputFormat;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;

public class DistComponentwiseOp extends DistMpBase {
  public static final String VERSION = "20110128";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  public static final String NAME = DistComponentwiseOp.class.getSimpleName().toLowerCase();
  public static final String DESCRIPTION = "Distributed Componentwise Operation";
  
  public DistComponentwiseOp(SchonhageStrassen schonhagestrassen,
      PowerOfTwo_int J, PowerOfTwo_int K, String dir) {
    super(schonhagestrassen, J, K, dir);
  }

  public static class ComponentwiseMapper
      extends Mapper<IntWritable, FunctionDescriptor, IntWritable, CarryDifference.Remainder.W> {
    protected void map(final IntWritable index, final FunctionDescriptor fun,
        final Context context) throws IOException, InterruptedException {
      //initialize
      final DistMpBase.TaskHelper helper = new DistMpBase.TaskHelper(context,
          NAME + ".VERSION = " + VERSION);

      final Configuration conf = context.getConfiguration();
      final DistMpBase parameters = DistMpBase.valueOf(conf);
      parameters.printDetail(getClass().getSimpleName());
      Print.println("fun = " + fun);

      final int k = index.get();
      helper.show("init: k=" + k);

      //evaluate inputs
      final int nWorkers = conf.getInt(N_WORKERS_CONF_KEY, 2);
      final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(
          getClass().getSimpleName(), nWorkers, null);
      final FunctionDescriptor.Evaluater evaluater = new FunctionDescriptor.Evaluater(
          k, fun.numParts, fun.elementsPerPart, parameters.schonhagestrassen,
          parameters.dir, conf); 
      final Zahlen.Element[] r = evaluater.evaluate(fun.input, helper.timer, workers);
      helper.show("evaluating inputs: r.length = " + r.length);

      //compute carries, differences, remainders
      final int m = fun.input.getVariables().size();
      final int exponent_in_digits = parameters.schonhagestrassen.digitsPerElement();
      final CarryDifference.Remainder[] cdr = new CarryDifference.Remainder[r.length];
      for(int i = 0; i < r.length; i++) {
        final String s = r[i].toBrief();
        final Zahlen.Element c = r[i].divideRemainderEqual(exponent_in_digits);
        final int d = r[i].difference(exponent_in_digits, m);
        try {
          cdr[i] = new CarryDifference.Remainder(r[i], c.toInt(), d);
        } catch(IllegalArgumentException iae) {
          throw new RuntimeException("r[" + i + "]\n= " + s
              + "\nr[" + i + "].divideRemainderEqual(" + exponent_in_digits
              + ")\n= " + r[i].toBrief()
              + "\nc\n= " + c.toBrief(), iae);
        }
      }
      helper.show("carry-difference-remainder: k=" + k);

      //write output
      final IntWritable key = new IntWritable();
      final CarryDifference.Remainder.W w = new CarryDifference.Remainder.W();
      for(int j = 0; j < r.length; j++) {
        if (j < 10 || j + 10 >= r.length)
          Print.println("r[" + j + "]=" + r[j].toBrief());
        else if (j == 10)
          Print.println("...");

        key.set((j << fun.numParts.exponent) + k);
        w.set(cdr[j]);
        context.write(key, w);
      }
      helper.show(getClass().getSimpleName());
    }
  }

  @Override
  protected Job newJob(final Configuration conf) throws IOException {
    final Job job = super.newJob(conf);

    // setup mapper
    job.setMapperClass(ComponentwiseMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(CarryDifference.Remainder.W.class);

    // setup input
    job.setInputFormatClass(ZahlenInputFormat.class);
    job.setOutputFormatClass(CarryDifference.CdOutputFormat.class);
    return job; 
  }

}