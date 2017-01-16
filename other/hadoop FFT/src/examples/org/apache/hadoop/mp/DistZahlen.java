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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenOutputFormat;
import org.apache.hadoop.mp.fft.DistMpMultTest;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.HadoopUtil;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.ConfSerializable;
import org.apache.hadoop.util.ToolRunner;


public class DistZahlen extends HadoopUtil.RunnerBase {
  protected DistZahlen() throws FileNotFoundException {}

  public static final String VERSION = "20100904";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  public static final String NAME = DistZahlen.class.getSimpleName().toLowerCase();
  public static final String DESCRIPTION = "Gerenate a Zahlen";
  
  static final AtomicBoolean printversions = new AtomicBoolean(false);  
  public static void printVersions() {
    if (!printversions.getAndSet(true)) {
      Print.println(NAME + ".VERSION = " + VERSION);
      Print.printSystemInfo();
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class Parameter implements ConfSerializable, Print.Detail {
    private static final String PREFIX = DistZahlen.class.getSimpleName()
                                       + "." + Parameter.class.getSimpleName();
    private static final String PROPERTY_ZAHLEN = PREFIX + "." + Zahlen.class.getSimpleName();
    private static final String PROPERTY_NUM_ELEMENTS = PREFIX + ".numElements";
    private static final String PROPERTY_DIGITS_PER_ELEMENT = PREFIX + ".digitsPerElement";
    private static final String PROPERTY_DIR = PREFIX + ".dir";

    final Zahlen smallZ;
    final ZahlenDescriptor var;
    final int numElements;
    final int digitsPerElement;
    final String dir;

    Parameter(final Zahlen smallZ, final ZahlenDescriptor var,
        final int numElements, final int digitsPerElement, final String dir) {
      this.smallZ = smallZ;
      this.var = var;
      this.numElements = numElements;
      this.digitsPerElement = digitsPerElement;
      this.dir = dir;
    }
    
    Parameter(final ZahlenDescriptor vars, final DistMpMultTest.Parameter mult,
        final String dir) {
      this(mult.schonhagestrassen.Z, vars, mult.K.value << (mult.J.exponent - 1),
          mult.schonhagestrassen.digitsPerElement(), dir);
    }

    @Override
    public String toString() {
      return DistZahlen.class.getSimpleName() + "." + Parameter.class.getSimpleName()
          + "(var=" + var
          + ", numElements=" + numElements
          + ", digitsPerElement=" + digitsPerElement
          + ")";
    }
    
    @Override
    public void serialize(final Configuration conf) {
      conf.set(PROPERTY_ZAHLEN, smallZ.serialize());
      var.serialize(conf);
      conf.setInt(PROPERTY_NUM_ELEMENTS, numElements);
      conf.setInt(PROPERTY_DIGITS_PER_ELEMENT, digitsPerElement);
      conf.set(PROPERTY_DIR, dir);
    }
    
    static Parameter valueOf(final Configuration conf) {
      final Zahlen smallZ = Zahlen.FACTORY.valueOf(conf.get(PROPERTY_ZAHLEN));
      final ZahlenDescriptor var = ZahlenDescriptor.valueOf(conf);
      final int numElements = conf.getInt(PROPERTY_NUM_ELEMENTS, -1);
      final int digitsPerElement = conf.getInt(PROPERTY_DIGITS_PER_ELEMENT, -1);
      final String dir = conf.get(PROPERTY_DIR);
      return new Parameter(smallZ, var, numElements, digitsPerElement, dir);
    }

    public void printDetail(final String name) {
      Print.beginIndentation(PREFIX + ": " + name);
      Print.println("smallZ           = " + smallZ);
      Print.println("vars             = " + var);
      Print.println("numElements      = " + numElements);
      Print.println("digitsPerElement = " + digitsPerElement);
      Print.println("dir              = " + dir);
      Print.endIndentation();
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class GenMapper
      extends Mapper<IntWritable, NullWritable, IntWritable, ZahlenSerialization.E> {
    protected void map(final IntWritable index, final NullWritable nullwritable,
        final Context context) throws IOException, InterruptedException {
      final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
      int step = 0;
      Print.printSystemInfo();

      final Configuration conf = context.getConfiguration();
      final Parameter parameters = Parameter.valueOf(conf);
      parameters.printDetail(getClass().getSimpleName());

      final int offset = index.get();
      show("offset = " + offset, context, timer, ++step);

      final Random r = JavaUtil.newRandom();
      final IntWritable key = new IntWritable();
      final ZahlenSerialization.E value = new ZahlenSerialization.E();

      for(int j = 0; j < parameters.var.elementsPerPart; j++) {
        final int i = (j << parameters.var.numParts.exponent) + offset;

        final Zahlen.Element z = i < parameters.numElements?
            parameters.smallZ.random(parameters.digitsPerElement, r):
            parameters.smallZ.newElement();
  
        if (j < 10 || j + 10 >= parameters.var.elementsPerPart)
          Print.println("j=" + j + ", i=" + i + ") z=" + z.toBrief());
        else if (j == 10)
          Print.println("...");

        key.set(i);
        value.set(z);
        context.write(key, value);
        z.reclaim();
      }
      show("END", context, timer, ++step);
    }

    static void show(final String message, final Context context,
        final JavaUtil.Timer timer, final int step) {
      timer.tick(message);
      Print.printMemoryInfo();
      context.setStatus(step + ") " + message);
      context.progress();
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  static Job newJob(final Configuration conf) throws IOException {
//  final Cluster cluster = new Cluster(JobTracker.getAddress(conf), conf);
    final Job job = new Job(conf);
    final Configuration jobconf = job.getConfiguration();
    job.setJarByClass(DistZahlen.class);

    // do not use speculative execution
    //jobconf.setBoolean(JobContext.MAP_SPECULATIVE, false);
    //jobconf.setBoolean(JobContext.REDUCE_SPECULATIVE, false);
    jobconf.setBoolean("mapred.map.tasks.speculative.execution", true);
    jobconf.setBoolean("mapred.reduce.tasks.speculative.execution", true);

    // setup mapper
    job.setMapperClass(GenMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(ZahlenSerialization.E.class);
    jobconf.setBoolean("mapred.compress.map.output", false);

    // setup reducer
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(ZahlenSerialization.E.class);

    // setup input/output
    job.setInputFormatClass(IndexInputFormat.class);
    job.setOutputFormatClass(ZahlenOutputFormat.class);
    return job; 
  }

  /** Setup a job with the parameters. */
  static String setupJob(final Job job, final Parameter parameters,
      final String dir) throws IOException {
    final Configuration jobconf = job.getConfiguration();
    parameters.serialize(jobconf);
    IndexInputFormat.setNumParts(parameters.var.numParts.value, jobconf);

    final Path p = new Path(dir);
    ZahlenOutputFormat.setOutputPath(job, p);

    final String name = NAME + ": " + p.getName();
    job.setJobName(name);
    return name; 
  }

  @Override
  public int run(String[] args) throws Exception {
    Print.print("args", args);

    int i = 0;
    final int e = Parse.string2integer(args[i++]);
    final String dir = args[i++];

    final DistMpMultTest.Parameter mult = new DistMpMultTest.Parameter(e);
    final Function.Variable output = Function.Variable.valueOf(new Path(dir).getName());
    final ZahlenDescriptor d = new ZahlenDescriptor(output, mult.J, mult.K.value);
    final Parameter parameters = new Parameter(d, mult, dir);
    parameters.printDetail("main");

    final Job job = newJob(getConf());
    setupJob(job, parameters, dir);
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistZahlen(), args));
  }
}