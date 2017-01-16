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
package org.apache.hadoop.mp.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mp.util.serialization.ConfSerializable;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.ValueOf;

/** Abstract machine for job execution. */
public abstract class Machine {
  protected static final Log LOG = LogFactory.getLog(Machine.class);
  static final Random RANDOM = new Random();;

  private static final String PREFIX = Machine.class.getName();
  private static final String N_PARTS =  PREFIX + ".nParts";
  private static final String N_THREADS = PREFIX + ".nThreads";

  /** Initialize a job */
  abstract void submit(Job job)
      throws IOException, InterruptedException, ClassNotFoundException;

  /** The number of "machine core", e.g. #parts*#threads. */
  public abstract int getCore();
  
  /** {@inheritDoc} */
  public String toString() {return getClass().getSimpleName();}

  /** Create a job */
  public static Job createJob(final String name, final Class<?> clazz,
      final ConfSerializable sigma,
      final Configuration conf) throws IOException {
//    final Cluster cluster = new Cluster(JobTracker.getAddress(conf), conf);
    final Job job = new Job(conf);
    final Configuration jobconf = job.getConfiguration();
    job.setJobName(name);
    job.setJarByClass(clazz);
    job.setOutputFormatClass(WritableOutputFormat.class);

    sigma.serialize(jobconf);

    // disable task timeout
    //jobconf.setLong(JobContext.TASK_TIMEOUT, 0);
    jobconf.setLong("mapred.task.timeout", 0);

    // do not use speculative execution
    //jobconf.setBoolean(JobContext.MAP_SPECULATIVE, false);
    //jobconf.setBoolean(JobContext.REDUCE_SPECULATIVE, false);
    jobconf.setBoolean("mapred.map.tasks.speculative.execution", false);
    jobconf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    return job; 
  }

  private static abstract class Runner<C, R> extends Thread {
    final C part;
    R result;

    private Runner(C part) {this.part = part;}

    @Override
    public abstract void run();
  }

  /** Compute sigma */
  private static <C extends MachineComputable<C, R>,
                  R extends MachineComputable.Result<R>>
      void compute(final C sigma, final int nThreads,
                   final TaskInputOutputContext<?, ?, NullWritable, R> context
      ) throws IOException, InterruptedException {
    final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
    final String s = "sigma = " + sigma;
    timer.tick(s);
    context.setStatus(s);
   
    final List<C> parts = sigma.partition(nThreads);

    final List<Runner<C, R>> runners = new ArrayList<Runner<C, R>>(parts.size());
    final AtomicInteger count = new AtomicInteger(0);
    
    final int base = nThreads/parts.size();
    final int remainder = nThreads%parts.size();

    //compute each part
    for(int i = 0; i < parts.size(); i++) {
      final int t = base + (i < remainder? 1: 0);
      final Runner<C, R> r = new Runner<C, R>(parts.get(i)) {
        @Override
        public void run() {
          timer.tick("START (thread=" + t + ")" + part);
          result = part.compute(t, timer);

          synchronized(count) {
            final String s = "DONE(" + count.incrementAndGet() + "/" + parts.size() + ") " + part;
            timer.tick(s);
            context.setStatus(s);
          }
        }
      }; 
      runners.add(r);
      r.start();
    }

    //wait for each runner
    for(Runner<C, R> r : runners)
      r.join();

    final List<R> results = new ArrayList<R>();
    for(Runner<C, R> r : runners) {
      if (r.result != null)
        results.add(r.result);
      else
        timer.tick("The result is null for " + r.part);
    }
    if (results.size() < parts.size())
      timer.tick("results.size() = " + results.size() + " < parts.size() = " + parts.size());

    //combine results
    final List<R> combined = JavaUtil.combine(results);
    timer.tick("combined.size() = " + combined.size());
    for(int i = 0; i < combined.size(); i++) {
      final R r = combined.get(i);
      timer.tick(i + ") " + r);
      context.write(NullWritable.get(), r);
    }
    Print.printMemoryInfo();
  }
  /////////////////////////////////////////////////////////////////////////////
  /** An output format using {@link org.apache.hadoop.io.Writable}. */
  public static class WritableOutputFormat<V extends Writable>
      extends FileOutputFormat<NullWritable, V> {
    public WritableOutputFormat() {}

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext job)
        throws IOException, InterruptedException {
      final Configuration conf = job.getConfiguration();
      final Path f = getDefaultWorkFile(job, ".writable");
      final DataOutputStream out = f.getFileSystem(conf).create(f, false);

      return new RecordWriter<NullWritable, V>() {
        @Override
        public synchronized void write(NullWritable key, V value) throws IOException {
          value.write(out);
        }

        @Override
        public synchronized void close(TaskAttemptContext context)
            throws IOException {
          out.close();
        }
      };
    }
  }

  /** Split for the computations */
  public static abstract class SplitBase<C extends Writable>
      extends InputSplit implements Container<C>, Writable, DataSerializable.ValueOf<C> {
    private final static String[] EMPTY = {};

    protected C c;

    protected SplitBase() {}
    protected SplitBase(C c) {this.c = c;}
    @Override
    public C get() {return c;}
    @Override
    public long getLength() {return 1;}
    @Override
    public String[] getLocations() {return EMPTY;}
    @Override
    public final void readFields(DataInput in) throws IOException {c = valueOf(in);}
    @Override
    public final void write(DataOutput out) throws IOException {c.write(out);}
  }
  /////////////////////////////////////////////////////////////////////////////
  /** An abstract InputFormat for the jobs */
  protected static abstract class InputFormatBase<C extends MachineComputable<C, ?>,
                                                  S extends SplitBase<C>>
      extends InputFormat<NullWritable, C>
      implements ValueOf<InputSplit, C, RuntimeException> {
    /** Specify how to read the records */
    @Override
    public final RecordReader<NullWritable, C> createRecordReader(
        InputSplit generic, TaskAttemptContext context) {
      @SuppressWarnings("unchecked")
      final S split = (S)generic;

      //return a record reader
      return new RecordReader<NullWritable, C>() {
        private boolean done = false;

        /** {@inheritDoc} */
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {}
        /** {@inheritDoc} */
        @Override
        public boolean nextKeyValue() {return !done ? done = true : false;}
        /** {@inheritDoc} */
        @Override
        public NullWritable getCurrentKey() {return NullWritable.get();}
        /** {@inheritDoc} */
        @Override
        public C getCurrentValue() {return split.get();}
        /** {@inheritDoc} */
        @Override
        public float getProgress() {return done? 1f: 0f;}
        /** {@inheritDoc} */
        @Override
        public void close() {}
      };
    }

    protected abstract ConfSerializable.ValueOf<C> getConfValueOf();
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class Null extends Machine {
    private static final Null INSTANCE = new Null();

    public static Null parse(String specification) {
      return "n".equals(specification)? INSTANCE: null;
    }

    private Null() {}

    @Override
    public int getCore() {return 1;}

    @Override
    void submit(Job job) {
      JavaUtil.sleepsecond(60, getClass().getSimpleName() + ".submit(..)");
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static abstract class SingleSideBase extends Machine {
    /** Number of parts per job */
    public final int nParts;
    /** Number of threads per part */
    final int nThreads;

    SingleSideBase(final int nParts, final int nThreads) {
      this.nParts = nParts;
      this.nThreads = nThreads;

      if (nParts <= 0) {
        throw new IllegalArgumentException("nParts = " + nParts + " <= 0");
      } else if (nThreads <= 0) {
        throw new IllegalArgumentException("nThreads = " + nThreads + " <= 0");
      }
    }

    @Override
    public int getCore() {
      return nParts*nThreads;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + "(p" + nParts + ",t" + nThreads + ")";
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static abstract class MapSideBase<C extends MachineComputable<C, R>,
                                           R extends MachineComputable.Result<R>>
      extends SingleSideBase {
    public MapSideBase(final int nParts, final int nThreads) {
      super(nParts, nThreads);
    }

    protected abstract Class<? extends MapperBase<C, R>> getMapperClass();

    protected abstract Class<R> getResultClass();

    protected abstract Class<? extends PartitionInputFormatBase<C, ?>> getPartitionInputFormatClass();

    protected final void submit(Job job)
        throws IOException, InterruptedException, ClassNotFoundException {
      final Configuration jobconf = job.getConfiguration();
      jobconf.setInt(N_PARTS, nParts);
      jobconf.setInt(N_THREADS, nThreads);

      // setup mapper
      job.setMapperClass(getMapperClass());
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(getResultClass());

      // zero reducer
      job.setNumReduceTasks(0);

      // setup input
      job.setInputFormatClass(getPartitionInputFormatClass());

      job.submit();
    }

    protected static abstract class PartitionInputFormatBase<C extends MachineComputable<C, ?>, S extends SplitBase<C>>
        extends InputFormatBase<C, S> {
      /** Partitions the summation into parts and then return them as splits */
      @Override
      public final List<InputSplit> getSplits(JobContext context) {
        //read sigma from conf
        final Configuration conf = context.getConfiguration();
        final C sigma = getConfValueOf().valueOf(conf); 
        final int nParts = conf.getInt(Machine.N_PARTS, 0);
        //LOG.info("sigma  = " + sigma);
        //LOG.info("nParts = " + nParts);

        //create splits
        final List<C> parts = sigma.partition(nParts);
        //LOG.info("parts.size() = " + parts.size());

        final List<InputSplit> splits = new ArrayList<InputSplit>();
        for(C p : parts)
          splits.add(valueOf(p));
        //LOG.info("splits.size() = " + splits.size());
        return splits;
      }
    }
    
    /** A mapper which computes sums */
    protected static abstract class MapperBase<C extends MachineComputable<C, R>, R extends MachineComputable.Result<R>>
        extends Mapper<NullWritable, C, NullWritable, R> {
      protected void init(final Context context) {}

      @Override
      protected final void map(NullWritable nw, C sigma, final Context context
          ) throws IOException, InterruptedException {
        init(context);
        final int nThreads = context.getConfiguration().getInt(N_THREADS, 1);
        Machine.compute(sigma, nThreads, context);
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static abstract class ReduceSideBase<C extends MachineComputable<C, R>,
                                              R extends MachineComputable.Result<R>>
      extends SingleSideBase {
    public ReduceSideBase(final int nParts, final int nThreads) {
      super(nParts, nThreads);
    }

    protected abstract Class<? extends PartitionMapperBase<C>> getPartitionMapperClass();

    protected abstract Class<C> getComputationClass();

    protected abstract Class<R> getResultClass();

    protected abstract Class<? extends ReducerBase<C, R>> getReducerClass();
    
    protected abstract Class<? extends SingletonInputFormatBase<C, ?>> getSingletonInputFormatClass();

    protected abstract Class<? extends IndexPartitionerBase<C>> getIndexPartitionerClass();

    /** {@inheritDoc} */
    @Override
    final void submit(Job job)
        throws IOException, InterruptedException, ClassNotFoundException {
      final Configuration jobconf = job.getConfiguration();
      jobconf.setInt(N_PARTS, nParts);
      jobconf.setInt(N_THREADS, nThreads);

      // setup mapper
      job.setMapperClass(getPartitionMapperClass());
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(getComputationClass());

      // setup partitioner
      job.setPartitionerClass(getIndexPartitionerClass());

      // setup reducer
      job.setReducerClass(getReducerClass());
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(getResultClass());
      job.setNumReduceTasks(nParts);

      // setup input
      job.setInputFormatClass(getSingletonInputFormatClass());

      job.submit();
    }

    /** An InputFormat which returns a single summation. */
    protected static abstract class SingletonInputFormatBase<C extends MachineComputable<C, ?>, S extends SplitBase<C>>
        extends InputFormatBase<C, S> {
      /** @return a list containing a single split of summation */
      @Override
      public List<InputSplit> getSplits(JobContext context) {
        //read sigma from conf
        final Configuration conf = context.getConfiguration();
        final C sigma = getConfValueOf().valueOf(conf); 
  
        //create splits
        final List<InputSplit> splits = new ArrayList<InputSplit>(1);
        splits.add(valueOf(sigma));
        return splits;
      }
    }

    /** A {@link org.apache.hadoop.mapreduce.Mapper} which partitions a computation */
    protected static abstract class PartitionMapperBase<C extends MachineComputable<C, ?>>
        extends Mapper<NullWritable, C, IntWritable, C> {
      /** Partitions sigma into parts */
      @Override
      protected final void map(NullWritable nw, C sigma, final Context context
          ) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final int nParts = conf.getInt(Machine.N_PARTS, 0);
        final List<C> parts = sigma.partition(nParts);
        for(int i = 0; i < parts.size(); ++i) {
          final C p = parts.get(i);
          context.write(new IntWritable(i), p);
          LOG.info("parts.get(" + i + ") = " + p);
        }
      }
    }

    /** Use the index for partitioning. */
    protected static class IndexPartitionerBase<C> extends Partitioner<IntWritable, C> {
      /** @return the index as the partition. */
      @Override
      public int getPartition(IntWritable index, C value, int numPartitions) {
        return index.get();
      }
    }

    /** A {@link org.apache.hadoop.mapreduce.Reducer} which does the computation */
    protected static class ReducerBase<C extends MachineComputable<C, R>, R extends MachineComputable.Result<R>>
        extends Reducer<IntWritable, C, NullWritable, R> {
      protected void init(final Context context) {}

      @Override
      protected final void reduce(IntWritable index, Iterable<C> computations, Context context
          ) throws IOException, InterruptedException {
        LOG.info("index=" + index);
        init(context);
        final int nThreads = context.getConfiguration().getInt(N_THREADS, 1);
        for(C sigma : computations)
          compute(sigma, nThreads, context);
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * A machine which chooses Machine in runtime according to the cluster status
   */
  public static abstract class MixBase<C extends MachineComputable<C, R>,
                                       R extends MachineComputable.Result<R>>
      extends Machine {
    protected JobClient jobclient;
    protected int consecutiveMap = 0;
    protected int consecutiveReduce = 0;
    //private Cluster cluster;
    
    protected final MapSideBase<C, R> mapSide;
    protected final ReduceSideBase<C, R> reduceSide;
    
    protected MixBase(final MapSideBase<C, R> mapSide, final ReduceSideBase<C, R> reduceSide) {
      this.mapSide = mapSide;
      this.reduceSide = reduceSide;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void submit(Job job)
        throws IOException, InterruptedException, ClassNotFoundException {
      final Configuration conf = job.getConfiguration();
      chooseMachine(conf).submit(job);
    }

    protected abstract int availableMap(final int remainingMap);
    protected abstract int availableReduce(final int remainingReduce);
    
    /**
     * Choose a Machine in runtime according to the cluster status.
     */
    synchronized Machine chooseMachine(Configuration conf) throws IOException {
      if (jobclient == null)
        jobclient = new JobClient(JobTracker.getAddress(conf), conf);
      /*
      if (cluster == null)
        cluster = new Cluster(JobTracker.getAddress(conf), conf);
        */

      try {
        for(;; Thread.sleep(2000)) {
          //get remaining, available
//          final ClusterMetrics status = cluster.getClusterStatus();
          final ClusterStatus status = jobclient.getClusterStatus();
          final int remaining_m = status.getMaxMapTasks() - status.getMapTasks();
          final int remaining_r = status.getMaxReduceTasks() - status.getReduceTasks();
          final int available_m = availableMap(remaining_m);
          final int available_r = availableReduce(remaining_r);
          
          if (available_m >= mapSide.nParts || available_r >= reduceSide.nParts) {
            //choose machine
            final Machine value;
            if (consecutiveMap >= 5)
              value = reduceSide;
            else if (consecutiveReduce >= 10)
              value = mapSide;
            else if (available_m < mapSide.nParts)
              value = reduceSide;
            else if (available_r < reduceSide.nParts)
              value = mapSide;
            else {
              //favor ReduceSide machine
              final int a = available_m/mapSide.nParts - 1;
              final int b = available_r/reduceSide.nParts + 1;
              value = RANDOM.nextInt(a + b) < a? mapSide: reduceSide;
            }

            //update consecutive m, r 
            if (value == mapSide)
              consecutiveMap++;
            else
              consecutiveMap = 0;
            if (value == reduceSide)
              consecutiveReduce++;
            else
              consecutiveReduce = 0;
            Print.println("  " + this + " is " + value
                + " (remaining: m=" + remaining_m + ", r=" + remaining_r
                + "; available: m=" + available_m + ", r=" + available_r
                + "; consecutive: m=" + consecutiveMap + ", r=" + consecutiveReduce + ")");
            return value;
          }
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }    
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * Alternates Map-side and Reduce-side Machines in a regular pattern.
   */
  public static abstract class AlternationBase<C extends MachineComputable<C, R>,
                                               R extends MachineComputable.Result<R>>
      extends MixBase<C, R> {
    private final char[] pattern;
    private int curr = -1;
    
    protected AlternationBase(
        final MapSideBase<C, R> mapSide, final ReduceSideBase<C, R> reduceSide,
        final String pattern) {
      super(mapSide, reduceSide);
      this.pattern = new char[pattern.length()];
      for(int i = 0; i < this.pattern.length; i++) {
        final char c = pattern.charAt(i);
        if (c != 'm' && c != 'r')
          throw new IllegalArgumentException("c != 'm' && c != 'r', i="
              + i + ", pattern=" + pattern);
        else
          this.pattern[i] = c; 
      }
    }

    /**
     * Choose a Machine in runtime according to the pattern.
     */
    synchronized Machine chooseMachine(Configuration conf) throws IOException {
      if (++curr == pattern.length)
        curr = 0;
      return pattern[curr] == 'm'? mapSide: reduceSide;
    }
  }
}
