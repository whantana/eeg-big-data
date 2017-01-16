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
package org.apache.hadoop.mp.pi;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mp.pi.math.Mod1Fraction;
import org.apache.hadoop.mp.pi.math.Summation;
import org.apache.hadoop.mp.util.HadoopUtil;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Machine;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.ConfSerializable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main class for computing sums using map/reduce jobs.
 * A sum is partitioned into jobs.
 * A job may be executed on the map-side or on the reduce-side.
 * A map-side job has multiple maps and zero reducer.
 * A reduce-side job has one map and multiple reducers.
 * Depending on the clusters status in runtime,
 * a mix-type job may be executed on either side.
 */
public final class DistSum extends Configured implements Tool {
  private static final String NAME = DistSum.class.getSimpleName();
  private static final String JOB_SEPARATION = NAME + ".job.separation.seconds";
  private static final String PRECISION = NAME + ".precision";

  final Mod1Fraction value = Mod1Fraction.zero();
  private long duration = 0;

  String getDurationString() {
    return duration + "ms = " + Parse.millis2String(duration);
  }
  /////////////////////////////////////////////////////////////////////////////
  /** DistSum job parameters */
  static final class Parameters {
    static final String[] DESCRIPTIONS = {
        "<precision> The precision.",
        "<nWorkers>  The number of workers.",
        "<nJobs>     The number of jobs per sum.",
        "<machine>   Machine specification"
        + "\n    " + MapSide.class.getSimpleName() + "    : m100t3"
        + "\n    " + ReduceSide.class.getSimpleName() + " : r50t2"
        + "\n    " + Mix.class.getSimpleName() + "        : x-m200t1-r100t2-5"
        + "\n    " + Alternation.class.getSimpleName() + ": a-m200t1-r100t2-mrr"
        + "\n    " + Machine.Null.class.getSimpleName() + "       : n"
        ,
        "<remoteDir> Remote directory for storing results.",
        "<tmpDir>    Tmp directory for submitting jobs.",
    };
    static final int COUNT = DESCRIPTIONS.length;

    static String getDescription() {
      final StringBuilder description = new StringBuilder();
      for(String s: DESCRIPTIONS)
        description.append("\n  ").append(s);
      return "" + description;      
    }

    /** The precision */
    final long precision;
    /** Number of workers */
    final int nWorkers;
    /** Number of jobs */
    final int nJobs;
    /** The machine used in the computation */
    final Machine machine;
    /** The machine specification */
    final String specification;
    /** The remote job directory */
    final String remoteDir;
    /** The remote job directory */
    final String tmpDir;
  
    private Parameters(long precision, int nWorkers, int nJobs,
        Machine machine, String specification, String remoteDir, String tmpDir) {
      this.precision = precision;

      this.nWorkers = nWorkers;
      this.nJobs = nJobs;
      this.machine = machine;
      this.specification = specification;
      this.remoteDir = remoteDir;
      this.tmpDir = tmpDir;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "\nprecision = " + Parse.long2string(precision)
           + "\nnWorkers  = " + nWorkers
           + "\nnJobs     = " + nJobs
           + "\nmachine   = " + machine + ", " + specification
           + "\nremoteDir = " + remoteDir
           + "\ntmpDir    = " + tmpDir;
    }

    /** Parse parameters */
    static Parameters parse(String[] args, int i) {
      if (args.length - i < COUNT)
        throw new IllegalArgumentException("args.length - i < COUNT = "
            + COUNT + ", args.length="
            + args.length + ", i=" + i + ", args=" + Arrays.asList(args));
      
      final long precision = Parse.string2long(args[i++]);
      final int nWorkers = Integer.parseInt(args[i++]);
      final int nJobs = Integer.parseInt(args[i++]);
      final String specification = args[i++];
      final String remoteDir = args[i++];
      final String tmpDir = args[i++];

      if (precision < 64) {
        throw new IllegalArgumentException("precision = " + precision + " < 64");
      } else if (nWorkers <= 0) {
        throw new IllegalArgumentException("nWorkers = " + nWorkers + " <= 0");
      } else if (nJobs <= 0) {
        throw new IllegalArgumentException("nJobs = " + nJobs + " <= 0");
      }
      
      final Machine m = parseMachine(specification);
      Mod1Fraction.setPrecision(precision);
      return new Parameters(precision, nWorkers, nJobs, m, specification,
          remoteDir, tmpDir);
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /** Compute sigma */
  static void setPrecision(Configuration conf) {
    final long precision = conf.getLong(PRECISION, 1000);
    Mod1Fraction.setPrecision(precision);
  }

  /** Split for the summations */
  public static final class SummationSplit extends Machine.SplitBase<SummationWritable> {
    public SummationSplit() {}
    private SummationSplit(SummationWritable sigma) {super(sigma);}

    @Override
    public SummationWritable valueOf(DataInput in) throws IOException {
      return SummationWritable.IO_VALUE_OF.valueOf(in);
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * A machine which does computation on the map side.
   */
  public static final class MapSide extends Machine.MapSideBase<SummationWritable, TaskResult> {
    static MapSide parse(String specification) {
      //e.g. m100t3
      if (specification.charAt(0) != 'm') {
        return null;
      } else {
        final int i = specification.indexOf('t');
        try {
          final int nParts = Integer.parseInt(specification.substring(1, i));
          final int nThreads = Integer.parseInt(specification.substring(i + 1));
          return new MapSide(nParts, nThreads);
        } catch(RuntimeException e) {
          throw new RuntimeException(
              "specification=" + specification + ", i=" + i, e);
        }
      }
    }

    MapSide(final int nParts, final int nThreads) {
      super(nParts, nThreads);
    }

    @Override
    protected Class<? extends MapperBase<SummationWritable, TaskResult>> getMapperClass() {
      return SummingMapper.class;
    }

    @Override
    protected Class<TaskResult> getResultClass() {
      return TaskResult.class;
    }

    @Override
    protected Class<? extends PartitionInputFormatBase<SummationWritable, ?>> getPartitionInputFormatClass() {
      return PartitionInputFormat.class;
    }

    /** An InputFormat which partitions a summation */
    public static final class PartitionInputFormat extends PartitionInputFormatBase<SummationWritable, SummationSplit> {
      @Override
      protected ConfSerializable.ValueOf<SummationWritable> getConfValueOf() {
        return SummationWritable.CONF_VALUE_OF;
      }

      @Override
      public InputSplit valueOf(SummationWritable p) throws RuntimeException {
        return new SummationSplit(p);
      }
    }
  
    /** A mapper which computes sums */
    public static final class SummingMapper extends MapperBase<SummationWritable, TaskResult> {
      @Override
      protected void init(final Context context) {
        DistSum.setPrecision(context.getConfiguration());
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * A machine which does computation on the reduce side.
   */
  public static final class ReduceSide extends Machine.ReduceSideBase<SummationWritable, TaskResult> {
    static ReduceSide parse(String specification) {
      //e.g. r100t3
      if (specification.charAt(0) != 'r') {
        return null;
      } else {
        final int i = specification.indexOf('t');
        try {
          final int nParts = Integer.parseInt(specification.substring(1, i));
          final int nThreads = Integer.parseInt(specification.substring(i + 1));
          return new ReduceSide(nParts, nThreads);
        } catch(RuntimeException e) {
          throw new RuntimeException(
              "specification=" + specification + ", i=" + i, e);
        }
      }
    }

    ReduceSide(final int nParts, final int nThreads) {
      super(nParts, nThreads);
    }

    @Override
    protected Class<SummationWritable> getComputationClass() {
      return SummationWritable.class;
    }

    @Override
    protected Class<PartitionMapper> getPartitionMapperClass() {
      return PartitionMapper.class;
    }

    @Override
    protected Class<SummingReducer> getReducerClass() {
      return SummingReducer.class;
    }

    @Override
    protected Class<TaskResult> getResultClass() {
      return TaskResult.class;
    }

    @Override
    protected Class<SingletonInputFormat> getSingletonInputFormatClass() {
      return SingletonInputFormat.class;
    }

    @Override
    protected Class<IndexPartitioner> getIndexPartitionerClass() {
      return IndexPartitioner.class;
    }

    /** An InputFormat which returns a single summation. */
    public static final class SingletonInputFormat
        extends SingletonInputFormatBase<SummationWritable, SummationSplit> {
      @Override
      protected ConfSerializable.ValueOf<SummationWritable> getConfValueOf() {
        return SummationWritable.CONF_VALUE_OF;
      }

      @Override
      public InputSplit valueOf(SummationWritable p) throws RuntimeException {
        return new SummationSplit(p);
      }
    }

    public static final class PartitionMapper extends PartitionMapperBase<SummationWritable> {
    }

    /** Use the index for partitioning. */
    public static final class IndexPartitioner extends IndexPartitionerBase<SummationWritable> {
    }

    /** A Reducer which computes sums */
    public static final class SummingReducer extends ReducerBase<SummationWritable, TaskResult> {
      @Override
      protected void init(final Context context) {
        DistSum.setPrecision(context.getConfiguration());
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  static Machine parseMachine(String specification) {
    Machine m = Machine.Null.parse(specification);
    if (m == null)
      m = MapSide.parse(specification);
    if (m == null)
      m = ReduceSide.parse(specification);
    if (m == null)
      m = Mix.parse(specification);
    if (m == null)
      m = Alternation.parse(specification);
    if (m == null)
      throw new IllegalArgumentException("specification=" + specification);
    return m;
  }

  private static class Mix extends Machine.MixBase<SummationWritable, TaskResult> {
    static Mix parse(String specification) {
      //e.g. x-m200t1-r100t3-5
      if (specification.charAt(0) != 'x') {
        return null;
      } else {
        int i = 2;
        int j = specification.indexOf('-', i+1);
        try {
          final MapSide m = MapSide.parse(specification.substring(i, j));

          i = j + 1;
          j = specification.indexOf('-', i+1);
          final ReduceSide r = ReduceSide.parse(specification.substring(i, j));

          final int ratio = Integer.parseInt(specification.substring(j+1));
          return new Mix(m, r, ratio);
        } catch(RuntimeException e) {
          throw new RuntimeException(
              "specification=" + specification + ", i=" + i + ", j=" + j, e);
        }
      }
    }

    final int nCore;
    final int reservedMap;
    final int reservedReduce;
    
    Mix(MapSide mapside, ReduceSide reduceside, final int ratio) {
      super(mapside, reduceside);
      this.nCore = (mapside.getCore() + reduceside.getCore()) >>> 1;
      this.reservedMap = mapside.nParts * ratio;
      this.reservedReduce = reduceside.nParts * ratio;
    }

    @Override
    public int getCore() {
      return nCore;
    }

    @Override
    protected int availableMap(int remainingMap) {
      final int d = remainingMap - reservedMap;
      return d > 0? d: 0;
    }

    @Override
    protected int availableReduce(int remainingReduce) {
      final int d = remainingReduce - reservedReduce;
      return d > 0? d: 0;
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  private static class Alternation extends Machine.AlternationBase<SummationWritable, TaskResult> {
    static Alternation parse(String specification) {
      //e.g. a-m200t1-r100t3-mrr
      if (specification.charAt(0) != 'a') {
        return null;
      } else {
        int i = specification.indexOf('-') + 1;
        int j = specification.indexOf('-', i);
        try {
          final MapSide m = MapSide.parse(specification.substring(i, j));
          
          i = j + 1;
          j = specification.indexOf('-', i);
          final ReduceSide r = ReduceSide.parse(specification.substring(i, j));

          return new Alternation(m, r, specification.substring(j+1));
        } catch(RuntimeException e) {
          throw new RuntimeException(
              "specification=" + specification + ", i=" + i + ", j=" + j, e);
        }
      }
    }

    final int nCore;
    
    Alternation(MapSide mapside, ReduceSide reduceside, String pattern) {
      super(mapside, reduceside, pattern);
      this.nCore = (mapside.getCore() + reduceside.getCore()) >>> 1;
    }

    @Override
    public int getCore() {return nCore;}
    @Override
    protected int availableMap(int remainingMap) {return remainingMap;}
    @Override
    protected int availableReduce(int remainingReduce) {return remainingReduce;}
  }
  /////////////////////////////////////////////////////////////////////////////
  final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
  final String starttime = Parse.time2String(timer.getStart());

  final JavaUtil.WorkGroup workgroup = new JavaUtil.WorkGroup(
      NAME + ".OutputProcessor", 1, timer);

  private Parameters parameters;

  /** Get Parameters */
  Parameters getParameters() {return parameters;}
  /** Set Parameters */
  void setParameters(Parameters p) {parameters = p;}

  /** Create a job */
  private Job createJob(String name, Summation sigma) throws IOException {
    final Job job = Machine.createJob(parameters.tmpDir + "/" + name,
        DistSum.class, new SummationWritable(sigma), getConf());
    job.getConfiguration().setLong(PRECISION, parameters.precision);
    return job; 
  }

  /**
   * Start a job to compute sigma.
   * If sigma == null, process existing output.
   */
  private void compute(final String name, final Summation sigma
      ) throws IOException {
    final long steps = sigma.E.getSteps();
    if (steps == 0) {
      timer.tick(sigma + " has zero steps.");
      return;
    } else if (parameters.machine instanceof Machine.Null) {
      timer.tick("name=" + name + ", sigma=" + sigma);
      return;
    }

    //setup remote directory
    final FileSystem fs = FileSystem.get(getConf());
    final Path dir = fs.makeQualified(new Path(parameters.tmpDir, name));
    HadoopUtil.createDirectory(fs, dir, true);
    final Path joboutdir = new Path(dir, "out");

    //setup a job
    final Job job = createJob(name, sigma);
    FileOutputFormat.setOutputPath(job, joboutdir);

    //start a map/reduce job
    final String startmessage = "steps/cores = "
        + steps + "/" + parameters.machine.getCore()
        + " = " + Parse.long2string(steps/parameters.machine.getCore());
    final long sleeptime = 1000L * job.getConfiguration().getInt(JOB_SEPARATION, 10);
    HadoopUtil.runJob(name, job, parameters.machine, startmessage, sleeptime, timer);

    //submit output for processing
    submitJobOutput(fs, dir, name, joboutdir, true);
  }

  void submitJobOutput(final FileSystem fs, final Path dir,
      final String jobname, final Path joboutdir, final boolean addValue
      ) throws IOException {
    workgroup.submit(jobname, new Runnable() {
      @Override
      public void run() {
        //combine and rewrite results
        final TaskResult combined;
        try {
          combined = HadoopUtil.processJobOutputs(fs,
              parameters.remoteDir, jobname, joboutdir, TaskResult.FACTORY);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        try {
          fs.delete(dir, true);
        } catch (IOException e) {
          Print.printStackTrace(e);
        }

        if (addValue) {
          Print.println(taskResult2string(jobname, combined));
          add(combined);
        }
      }
    });
  }

  /** Process existing job outputs. */
  void processExistingJobOutputs(final Path dir, FileSystem fs
      ) throws IOException {
    final FileStatus[] statuses = fs.listStatus(dir);
    if (statuses != null && statuses.length > 0) {
      Print.println("\nCheck existing job outputs from " + dir + " ...");

      for(FileStatus s : statuses) {
        if (s.isDir()) {
          final Path p = s.getPath();
          final Path joboutdir = new Path(p, "out");
          if (fs.exists(joboutdir)) {
            submitJobOutput(fs, p, p.getName(), joboutdir, false);
          }
        }
      }
    }
  }
  
  void add(TaskResult r) {
    synchronized(value) {
      duration += r.getDuration();
      if (r.get().ispositive)
        value.addMod1Equal(r.getValue());
      else
        value.subtractMod1Equal(r.getValue());
    }
  }

  /** Convert a TaskResult to a String */
  public static String taskResult2string(String name, TaskResult result) {
    return NAME + " " + name + "> " + result;
  }

  /** Callable computation */
  class Computation implements Callable<Computation> {
    private final int index;
    private final String name;
    private final Summation sigma;

    Computation(int index, String name, Summation sigma) {
      this.index = index;
      this.name = name;
      this.sigma = sigma;
    }

    /** @return The job name */
    String getJobName() {return String.format("%s.job%04d-" + starttime, name, index);}

    /** {@inheritDoc} */
    @Override
    public String toString() {return getJobName() + sigma;}

    /** Start the computation */
    @Override
    public Computation call() {
      for(int i = 3; i > 0; ) {
        try {
          compute(getJobName(), sigma);
          i = 0;
        } catch(Exception e) {
          Print.println("ERROR: Got an exception from " + getJobName());
          Print.printStackTrace(e);
          if (--i > 0) {
            Print.println("Retry " + getJobName() + " " + i + " more times.");
          }
        }
      }
      return this;
    }
  }

  /** Partition sigma and execute the computations. */
  private boolean execute(String name, Summation sigma,  boolean isplus) {
    final List<Summation> summations = sigma.partition(parameters.nJobs);
    final List<Computation> computations = new ArrayList<Computation>(); 
    for(int i = 0; i < summations.size(); i++)
      computations.add(new Computation(i, name, summations.get(i)));
    try {
      JavaUtil.execute(parameters.nWorkers, computations, timer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    workgroup.waitUntilZero();

    final List<Summation> combined = JavaUtil.combine(summations);
    return combined.size() == 1 && combined.get(0).equals(sigma);
  }

  static final String[] ADDITIONAL_PARAMETER = {
    "<name>      The name.",
    "<sigma>     The summation.",
  };

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    //parse arguments
    if (args.length != Parameters.COUNT + ADDITIONAL_PARAMETER.length)
      return HadoopUtil.printUsage(args, getClass().getName()
          + Parse.description2brief(ADDITIONAL_PARAMETER, Parameters.DESCRIPTIONS)
          + Parse.description(ADDITIONAL_PARAMETER, Parameters.DESCRIPTIONS),
          new IllegalArgumentException("args.length != Parameters.COUNT (="
              + Parameters.COUNT + ") + ADDITIONAL_PARAMETER.length (="
              + ADDITIONAL_PARAMETER.length + ")"));

    int i = 0;
    final String name = args[i++];
    final Summation sigma = Summation.valueOf(args[i++]);
    setParameters(Parameters.parse(args, i));

    Print.println();
    Print.println("name  = " + name);
    Print.println("sigma = " + sigma);
    Print.println(parameters);
    Print.println();

    //run jobs
    final int r;
    if (execute(name, sigma, true)) {
      timer.tick("\n\nDONE\n\nsigma=" + sigma + ", value=" + value);
      r = 0;
    } else {
      timer.tick("\n\nDONE WITH ERROR\n\nsigma=" + sigma);
      r = 1;
    }
    Print.println("cpu time = " + getDurationString());
    return r;
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistSum(), args));
  }
}