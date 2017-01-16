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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenOutputFormat;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.ConfSerializable;


public class DistMpBase implements ConfSerializable, Print.Detail {
  public static final String VERSION = "20110124";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  public static final String NAME = DistMpBase.class.getSimpleName().toLowerCase();
  public static final String N_WORKERS_CONF_KEY = NAME + ".nWorkers";

  private static final String PROPERTY_ZAHLEN = NAME + "." + Zahlen.class.getSimpleName();
  private static final String PROPERTY_SCHONHAGE_STRASSEN = NAME + "." + SchonhageStrassen.class.getSimpleName();
  private static final String PROPERTY_J = NAME + ".J";
  private static final String PROPERTY_K = NAME + ".K";
  private static final String PROPERTY_DIR = NAME + ".dir";

  public final SchonhageStrassen schonhagestrassen;
  public final PowerOfTwo_int J, K;
  public final String dir;

  public DistMpBase(final SchonhageStrassen schonhagestrassen,
      final PowerOfTwo_int J, final PowerOfTwo_int K,
      final String dir) {
    this.schonhagestrassen = schonhagestrassen;
    this.J = J;
    this.K = K;
    this.dir = dir;
  }

  public static DistMpBase valueOf(final Configuration conf) {
    final Zahlen Z = Zahlen.FACTORY.valueOf(conf.get(PROPERTY_ZAHLEN));
    final SchonhageStrassen schonhagestrassen = SchonhageStrassen.FACTORY.valueOf(
        conf.get(PROPERTY_SCHONHAGE_STRASSEN), Z);
    final PowerOfTwo_int J = PowerOfTwo_int.VALUE_OF_STR.valueOf(conf.get(PROPERTY_J));
    final PowerOfTwo_int K = PowerOfTwo_int.VALUE_OF_STR.valueOf(conf.get(PROPERTY_K));
    final String dir = conf.get(PROPERTY_DIR);
    return new DistMpBase(schonhagestrassen, J, K, dir);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(J=" + J + ", K=" + K + ", dir=" + dir + ")";
  }

  @Override
  public void printDetail(final String name) {
    Print.beginIndentation(NAME + ": " + name);
    Print.println("Z                 = " + schonhagestrassen.Z);
    Print.println("schonhagestrassen = " + schonhagestrassen);
    Print.println("J                 = " + J);
    Print.println("K                 = " + K);
    Print.println("dir               = " + dir);
    Print.endIndentation();
  }
  
  @Override
  public void serialize(final Configuration conf) {
    conf.set(PROPERTY_ZAHLEN, schonhagestrassen.Z.serialize());
    conf.set(PROPERTY_SCHONHAGE_STRASSEN, schonhagestrassen.serialize());
    conf.set(PROPERTY_J, J.serialize());
    conf.set(PROPERTY_K, K.serialize());
    conf.set(PROPERTY_DIR, dir);
  }
  
  public static class TaskHelper {
    public final TaskInputOutputContext<?,?,?,?> context;
    public final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
    private int step = 0;
    
    public TaskHelper(final TaskInputOutputContext<?,?,?,?> context, final String message) {
      this.context = context;
      Print.println("JobName      : " + context.getJobName());
      Print.println("TaskAttemptID: " + context.getTaskAttemptID());
      Print.printSystemInfo();
    }

    public void show(final String message) {
      final String s = ++step + ") DONE " + message;
      timer.tick(s);
      Print.printMemoryInfo();
      context.setStatus(s);
      context.progress();
    }

    public static void write(final Zahlen.Element[] a, 
        final int offset, final PowerOfTwo_int step, final int numElements,
        TaskInputOutputContext<?,?,IntWritable,ZahlenSerialization.E> context
        ) throws IOException, InterruptedException {
      final IntWritable key = new IntWritable();
      final ZahlenSerialization.E value = new ZahlenSerialization.E();
      for(int j = 0; j < numElements; j++) {
        final int i = (j << step.exponent) + offset;
        if (j < 10 || j + 10 >= numElements)
          Print.println("i=" + i + ") a[" + j + "]=" + a[j].toBrief());
        else if (j == 10)
          Print.println("...");
    
        key.set(i);
        value.set(a[j]);
        context.write(key, value);
      }
    }
  }

  /** Use the index as the partition. */
  public static class IndexPartitioner<T> extends Partitioner<IntWritable, T> {
    /** @return the index as the partition. */
    @Override
    public int getPartition(IntWritable index, T value, int numPartitions) {
      return index.get();
    }
  }

  public class MpJob {
    public final FunctionDescriptor descriptor;
    public final Job job;
    public final String jobname;
    
    public MpJob(final Function input, final Function.Variable output,
        final Configuration conf) throws IOException {
      this(new FunctionDescriptor(input, output, J, K.value), conf);
    }

    public MpJob(final FunctionDescriptor descriptor,
        final Configuration conf) throws IOException {
      this.descriptor = descriptor;
      if (IS_VERBOSE) {
        Print.println("descriptor = " + descriptor);
      }

      //initialize a job
      job = newJob(conf);

      final Configuration jobconf = job.getConfiguration();
      serialize(jobconf);
      descriptor.serialize(jobconf);

      FileOutputFormat.setOutputPath(job, descriptor.getOutputPath(dir));

      jobname = jobName(descriptor);
      job.setJobName(jobname);
    }

    /** @return whether the job is submitted. */
    public boolean submit() throws IOException, InterruptedException, ClassNotFoundException {
      final FileSystem fs = FileSystem.get(job.getConfiguration());
      final Path outdir = ZahlenOutputFormat.getOutputPath(job);
      final boolean b = fs.exists(outdir);
      if (b) {
        Print.println("outdir=" + outdir + " already exists.  Skipping job " + jobname);
      } else {
        job.submit();
      }
      return !b;
    }
    
    public void wait4job(final boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
      if (!job.waitForCompletion(verbose)) {
        throw new IOException(jobname + " job failed.");
      }
    }
  }

  protected Job newJob(final Configuration conf) throws IOException {
    //final Cluster cluster = new Cluster(JobTracker.getAddress(conf), conf);
    final Job job = new Job(conf);
    final Configuration jobconf = job.getConfiguration();
    job.setJarByClass(getClass());
    job.setNumReduceTasks(0);
    
    // disable task timeout
    //jobconf.setLong(JobContext.TASK_TIMEOUT, 0);
    jobconf.setLong("mapred.task.timeout", 0);
    
    // use speculative execution
    jobconf.setBoolean("mapred.map.tasks.speculative.execution", true);
    jobconf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
    
    jobconf.setBoolean("mapred.compress.map.output", false);
    return job; 
  }

  protected String jobName(final FunctionDescriptor f) {
    return dir + ": " + f.output + " = " + f.functionString();
  }
}