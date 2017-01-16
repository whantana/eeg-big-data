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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mp.DistMpBase;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenSerialization;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenInputFormat;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenOutputFormat;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;


public class DistFft extends DistMpBase {
  public static final String VERSION = "20110124";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  public static final String NAME = DistFft.class.getSimpleName().toLowerCase();
  public static final String DESCRIPTION = "Distributed Fast Fourier Transform";
  
  private static final String PROPERTY_INVERSE = NAME + ".inverse";

  final boolean inverse;

  DistFft(final SchonhageStrassen schonhagestrassen,
      final boolean inverse, final PowerOfTwo_int J, final PowerOfTwo_int K,
      final String dir) {
    super(schonhagestrassen, J, K, dir);
    this.inverse = inverse;
  }

  @Override
  public String toString() {
    final String s = super.toString();
    return s.substring(0, s.length() - 1) + ", inverse=" + inverse + ")";
  }
  @Override
  public void printDetail(final String name) {
    Print.beginIndentation(NAME + ": " + name);
    Print.println("inverse           = " + inverse);
    super.printDetail("");
    Print.endIndentation();
  }
  
  @Override
  public void serialize(final Configuration conf) {
    super.serialize(conf);
    conf.setBoolean(PROPERTY_INVERSE, inverse);
  }
  
  public static DistFft valueOf(final Configuration conf) {
    final DistMpBase p = DistMpBase.valueOf(conf);
    final boolean inverse = conf.getBoolean(PROPERTY_INVERSE, false);
    return new DistFft(p.schonhagestrassen, inverse, p.J, p.K, p.dir);
  }

  String dft(final Zahlen.Element[] a, final JavaUtil.WorkGroup workers) {
    final SchonhageStrassen.FastFourierTransform algorithm = SchonhageStrassen.FACTORY.valueOf(
        a.length, schonhagestrassen).parallel;
    if (inverse)
      algorithm.fft_inverse(a, workers);
    else
      algorithm.fft(a, workers);
    return algorithm.getClass().getSimpleName();
  }
  
  void scaleMultiplication(final int k0, final Zahlen.Element[] a,
      final JavaUtil.WorkGroup workers) {
    Print.println("scaleMultiplication: k0=" + k0 + ", a.length=" + a.length);
    for(int j0 = 0; j0 < a.length; j0++) {
      int index = j0*k0;
      if (index > 0 && inverse)
        index = schonhagestrassen.D.value - index;
//        Print.println("j0=" + j0 + ", index=" + index);
      
      if (workers == null) {
        schonhagestrassen.multiplyEqual(a[j0], index);
      } else {
        final Zahlen.Element z = a[j0];
        final int i = index;
        workers.submit("multiplyEqual(a[" + j0 + "], index=" + i + ")", new Runnable() {
          @Override
          public void run() {
            schonhagestrassen.multiplyEqual(z, i);
          }
        });
      }
    }
    if (workers != null)
      workers.waitUntilZero();
  }

  /** The job computes g = DFT(f). */
  public class DftJob extends MpJob {
    DftJob(final Function f, final Configuration conf) throws IOException {
      // The job computes g = DFT(f).
      this(Function.Variable.valueOf(f.getName() + "'"), f, conf);
    }

    DftJob(final Function.Variable g, final Function f,
        final Configuration conf) throws IOException {
      super(inverse? new FunctionDescriptor(f, g, K, J.value):
                     new FunctionDescriptor(f, g, J, K.value), conf);
    }
  }

  public static class FftMapper
      extends Mapper<IntWritable, FunctionDescriptor, IntWritable, ZahlenSerialization.Part.W> {
    protected void map(final IntWritable index, final FunctionDescriptor fun,
        final Context context) throws IOException, InterruptedException {
      //initialize
      final DistMpBase.TaskHelper helper = new DistMpBase.TaskHelper(context,
          NAME + ".VERSION = " + VERSION);

      final Configuration conf = context.getConfiguration();
      final DistFft parameters = DistFft.valueOf(conf);
      parameters.printDetail(getClass().getSimpleName());
      Print.println("fun = " + fun);

      final int k0 = index.get();
      helper.show("init: k0=" + k0);

      //evaluate inputs
      final int nWorkers = conf.getInt(N_WORKERS_CONF_KEY, 2);
      final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(
          getClass().getSimpleName(), nWorkers, null);
      final FunctionDescriptor.Evaluater evaluater = new FunctionDescriptor.Evaluater(
          k0, parameters.K, parameters.J.value, parameters.schonhagestrassen,
          parameters.dir, conf); 
      final Zahlen.Element[] a = evaluater.evaluate(fun.input, helper.timer, workers);
      helper.show("evaluating inputs: a.length = " + a.length);

      //fft
      final String algorithm = parameters.dft(a, workers);
      helper.show("fft: " + algorithm);

      parameters.scaleMultiplication(k0, a, workers);
      helper.show("scaleMultiplication");

      //write output
      final ZahlenSerialization.Part.W w = new ZahlenSerialization.Part.W();
      for(int j0 = 0; j0 < a.length; j0++) {
        if (j0 < 10 || j0 + 10 >= a.length)
          Print.println("a[" + j0 + "]=" + a[j0].toBrief());
        else if (j0 == 10)
          Print.println("...");

        w.set(new ZahlenSerialization.Part(k0, a[j0]));
        context.write(new IntWritable(j0), w);
      }
      helper.show(getClass().getSimpleName());
    }
  }

  public static class FftReducer
      extends Reducer<IntWritable, ZahlenSerialization.Part.W, IntWritable, ZahlenSerialization.E> {
    @Override
    protected final void reduce(final IntWritable index,
        final Iterable<ZahlenSerialization.Part.W> writables,
        final Context context) throws IOException, InterruptedException {
      //initialize
      final DistMpBase.TaskHelper helper = new DistMpBase.TaskHelper(context,
          NAME + ".VERSION = " + VERSION);

      final DistFft parameters = DistFft.valueOf(context.getConfiguration());
      parameters.printDetail(getClass().getSimpleName());

      final int j0 = index.get();
      helper.show("init: j0=" + j0);

      //read inputs
      final Zahlen.Element[] a = new Zahlen.Element[parameters.K.value];
      for(ZahlenSerialization.Part.W w : writables) {
        final ZahlenSerialization.Part p = w.get();
        a[p.index] = p.get(0);
      }
      helper.show("read inputs: a.length=" + a.length);

      //fft
      final int nWorkers = context.getConfiguration().getInt(N_WORKERS_CONF_KEY, 2);
      final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(
          getClass().getSimpleName(), nWorkers, null);
      final String algorithm = parameters.dft(a, workers);
      helper.show(algorithm);

      if (parameters.inverse) {
        parameters.schonhagestrassen.normalize(a);
        helper.show("normalize");

        if (PRINT_LEVEL.is(Print.Level.TRACE))
          Print.print("normalize", a);
      }

      //write output
      DistMpBase.TaskHelper.write(a, j0, parameters.J, parameters.K.value, context);
      helper.show(getClass().getSimpleName());
    }
  }

  @Override
  protected Job newJob(final Configuration conf) throws IOException {
    final Job job = super.newJob(conf);

    // setup mapper
    job.setMapperClass(FftMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(ZahlenSerialization.Part.W.class);

    // setup partitioner
    job.setPartitionerClass(DistMpBase.IndexPartitioner.class);

    // setup reducer
    job.setReducerClass(FftReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(ZahlenSerialization.E.class);
    job.setNumReduceTasks(J.value);

    // setup input
    job.setInputFormatClass(ZahlenInputFormat.class);
    job.setOutputFormatClass(ZahlenOutputFormat.class);
    return job; 
  }

  @Override
  protected String jobName(final FunctionDescriptor f) {
    return dir + ": " + f.output + " = dft"
        + (inverse? "^-1(": "(")
        + f.functionString() + ")";
  }
}