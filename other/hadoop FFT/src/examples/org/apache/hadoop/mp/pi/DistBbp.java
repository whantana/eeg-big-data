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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mp.pi.math.Bellard;
import org.apache.hadoop.mp.pi.math.Mod1Fraction;
import org.apache.hadoop.mp.pi.math.Summation;
import org.apache.hadoop.mp.util.HadoopUtil;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map/reduce program that uses a BBP-type method to compute exact 
 * binary digits of Pi.
 * This program is designed for computing the n th bit of Pi,
 * for large n, say n >= 10^8.
 * For computing lower bits of Pi, consider using bbp.
 *
 * The actually computation is done by DistSum jobs.
 * The steps for launching the jobs are:
 * 
 * (1) Initialize parameters.
 * (2) Create a list of sums.
 * (3) Read computed values from the given local directory.
 * (4) Remove the computed values from the sums.
 * (5) Partition the remaining sums into computation jobs.
 * (6) Submit the computation jobs to a cluster and then wait for the results.
 * (7) Write job outputs to the given local directory.
 * (8) Combine the job outputs and print the Pi bits.
 */
public final class DistBbp extends Configured implements Tool {
  public static final String VERSION = "20100731";
  private static final String NAME = DistBbp.class.getSimpleName();
  public static final String DESCRIPTION
      = "A map/reduce program that uses a BBP-type formula to compute exact bits of Pi.";

  private static final String PROCESS_EXISTING_JOB_OUTPUTS = NAME + ".processExistingJobOutputs";
  private static final String BELLARD = NAME + ".bellard";

  static final AtomicBoolean printversions = new AtomicBoolean(false);  
  public static void printVersions() {
    if (!printversions.getAndSet(true)) {
      Print.println(NAME + ".VERSION = " + VERSION);
      Print.printSystemInfo();
    }
  }

  static final String[] ADDITIONAL_PARAMETER = {
    "<b>         The number of bits to skip, i.e. compute the (b+1)th position."
  };

  /** {@inheritDoc} */
  public int run(String[] args) throws Exception {

    //parse arguments
    if (args.length != DistSum.Parameters.COUNT + ADDITIONAL_PARAMETER.length)
      return HadoopUtil.printUsage(args, getClass().getName()
          + Parse.description2brief(ADDITIONAL_PARAMETER, DistSum.Parameters.DESCRIPTIONS)
          + Parse.description(ADDITIONAL_PARAMETER, DistSum.Parameters.DESCRIPTIONS),
          new IllegalArgumentException("args.length != Parameters.COUNT (="
              + DistSum.Parameters.COUNT + ") + ADDITIONAL_PARAMETER.length (="
              + ADDITIONAL_PARAMETER.length + ")"));

    int i = 0;
    final long b = Parse.string2long(args[i++]);
    final DistSum.Parameters parameters = DistSum.Parameters.parse(args, i);
    if (b < 0)
      throw new IllegalArgumentException("b = " + b + " < 0");

    //init log file
    final String filename = "b" + b + "-p" + parameters.precision;
    Print.initLogFile(filename);

    //print version, parameters
    final DistSum distsum = new DistSum();
    printVersions();
    distsum.setConf(getConf());
    distsum.setParameters(parameters);
    printBitSkipped(b);
    Print.println(parameters);
    Print.println();

    //read existing
    final FileSystem fs = FileSystem.get(getConf());
    final Path remotedir = fs.makeQualified(new Path(parameters.remoteDir));
    final Path tmpdir = fs.makeQualified(new Path(parameters.tmpDir));
    final boolean pejo = getConf().getBoolean(PROCESS_EXISTING_JOB_OUTPUTS, true);
    Print.println("\n" + PROCESS_EXISTING_JOB_OUTPUTS + " = " + pejo);
    if (pejo) {
      distsum.processExistingJobOutputs(remotedir, fs);
      distsum.processExistingJobOutputs(tmpdir, fs);
      distsum.workgroup.waitUntilZero();
    }
    
    final FileStatus[] statuses = fs.listStatus(remotedir);
    final Map<Bellard.Parameter, Bellard.Sum> sums = new TreeMap<Bellard.Parameter, Bellard.Sum>();
    Print.println("\nRead existing results from " + remotedir + " ...");
    final String bellard = getConf().get(BELLARD);
    Print.println(BELLARD + " = " + bellard);
    for(Bellard.Parameter p : Bellard.parse(bellard)) {
      final List<TaskResult> existings = HadoopUtil.readExistingResults(p,
          Bellard.FACTORY, TaskResult.FACTORY, statuses, fs);
      for(TaskResult r : existings) {
        distsum.add(r);
      }

      //initialize sums
      final Bellard.Sum s = Bellard.getSum(b, p, parameters.nJobs, existings);
      if (s != null) {
        sums.put(p, s);
      } else if (existings.size() == 1) {
        HadoopUtil.rewriteCompletedResult(p, existings.get(0),
            Bellard.FACTORY, statuses, remotedir, fs);
      }
    }
    
    //execute the computations
    execute(distsum, sums);

    //compute Pi from the sums 
    final Mod1Fraction pi = distsum.value;
    final Path pifile = new Path(remotedir, filename);
    HadoopUtil.write(pi, pifile, fs);

    printBitSkipped(b);
    pi.print(Print.log);
    Print.println("\nCPU time = " + distsum.getDurationString());
    Print.println("END " + new Date(System.currentTimeMillis()));
    Print.closeFileOutput();
    pi.print(Print.out);
    return 0;
  }

  /** Execute computations */
  private static void execute(DistSum distsum,
      final Map<Bellard.Parameter, Bellard.Sum> sums) throws Exception {
    final List<DistSum.Computation> computations = new ArrayList<DistSum.Computation>();
    int i = 0;
    for(Map.Entry<Bellard.Parameter, Bellard.Sum> e : sums.entrySet()) {
      final String name = e.getKey().toString();
      for(Summation s : e.getValue())
        computations.add(distsum.new Computation(i++, name, s));
    }

    if (computations.isEmpty())
      Print.println("No computation");
    else {
      JavaUtil.execute(distsum.getParameters().nWorkers, computations, distsum.timer);
      distsum.workgroup.waitUntilZero();
    }
  }

  /** Print a "bits skipped" message. */
  static void printBitSkipped(final long b) {
    Print.println();
    Print.println("b = " + Parse.long2string(b)
        + " (" + (b < 2? "bit": "bits") + " skipped)");
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistBbp(), args));
  }
}
