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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mp.pi.math.Mod1Fraction;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Utility methods */
public class HadoopUtil {
  /** Print usage messages */
  public static int printUsage(String[] args, String usage, Exception e) {
    if (e != null)
      Print.printStackTrace(e);

    Print.println("args.length = " + args.length);
    Print.println("args        = " + Arrays.asList(args).toString().replace(", ", ",\n  "));
    Print.println();
    Print.println("Usage: java " + usage);
    Print.println();

    ToolRunner.printGenericCommandUsage(Print.out);
    return -1;
  }

  private static final Semaphore JOB_SEMAPHORE = new Semaphore(1);
  private static final AtomicInteger nSubmittedJobs = new AtomicInteger(0);

  /** Run a job. */
  public static void runJob(String name, Job job, Machine machine, String startmessage,
      final long sleeptime, final JavaUtil.Timer timer) {
    JOB_SEMAPHORE.acquireUninterruptibly();
    final long starttime = timer.tick(name + "> starting, " + startmessage
        + ", ++nSubmittedJobs=" + nSubmittedJobs.incrementAndGet());
    try {
      try {
        //submit a job
        machine.submit(job);
        
        // Separate jobs
        if (sleeptime > 0) {
          Print.println(name + "> sleep(" + Parse.millis2String(sleeptime) + ")");
          for(long ms = sleeptime; ms > 0 && !job.isComplete(); ) {
            Thread.sleep(ms < 10000L? ms: 10000L);
            
            ms -= 10000L;
            if ((sleeptime - ms) % 60000L == 0) {
              Print.println(name + "> remaining sleep time="
                  + Parse.millis2String(ms));
                  /*
              Printer.println(name + "> JobID=" + job.getJobID()
                  + String.format(", mapProgress=%1.3f, reduceProgress=%1.3f",
                      job.mapProgress(), job.reduceProgress()));
                      */
            }
          }
        }
      } finally {
        JOB_SEMAPHORE.release();
      }
  
      if (!job.waitForCompletion(false))
        throw new RuntimeException(name + " failed.");
    } catch(Exception e) {
      throw e instanceof RuntimeException? (RuntimeException)e: new RuntimeException(e);
    } finally {
      timer.tick(name + "> timetaken=" + Parse.millis2String(timer.tick() - starttime)
          +", --nSubmittedJobs=" + nSubmittedJobs.decrementAndGet());
    }
  }

  /** Rewrite job outputs */
  public static <T extends MachineComputable.Result<T>> T processJobOutputs(
      FileSystem fs, String remotedir, String jobname, Path joboutdir,
      DataSerializable.ValueOf<T> iovalueof) throws IOException {
    //read and combine results
    T t = null;
    final FileStatus[] statuses = fs.listStatus(joboutdir);
    int w = 0;
    if (statuses != null && statuses.length > 0) {
      Arrays.sort(statuses);
      Print.println("> found " + statuses.length + " items in " + joboutdir);
      for(FileStatus status : statuses) {
        if (status.getPath().getName().startsWith("part-")) {
          final DataInputStream in = fs.open(status.getPath());
          final T value;
          try {
            value = iovalueof.valueOf(in);
          } finally {
            in.close();
          }
  
          if (t == null)
            t = value;
          else {
            final T c = t.combine(value);
            if (c == null) {
              final Path outfile = new Path(remotedir, jobname + "." + w + ".writable");
              Print.println("> write the result to " + outfile);
              write(t, outfile, fs);
              w++;
              
              t = value;
            } else 
              t = c;
          }
        }
      }
  
      //write combined output
      if (t == null) {
        if (w == 0)
          Print.println("> no result is found.");
      } else {
        final Path outfile = new Path(remotedir,
            jobname + (w == 0? "": "." + w) + ".writable");
        Print.println("> write the result to " + outfile);
        write(t, outfile, fs);
      }
    }
    return t;
  }

  /** Write the result to an output file */
  public static <T extends MachineComputable.Result<T>> void write(
      T r, Path outfile, FileSystem fs) throws IOException {
    final DataOutputStream out = fs.create(outfile);
    try {
      r.write(out);
    } finally {
      out.close();
    }
  }

  /** Rewrite completed results */
  public static <K, R extends MachineComputable.Result<R>> void rewriteCompletedResult(
      final K key, final R result, final StringSerializable.ValueOf<K> keyVO,
      final FileStatus[] statuses, Path dir, FileSystem fs) throws IOException {
    if (statuses != null && statuses.length > 0) {
      final Path sub = new Path(dir, key.toString());
      if (createDirectory(fs, sub, false)) {
        Print.println("  " + sub + " already exists, skipping.");
      } else {
        Print.println(key + ": Move completed results from " + dir + " to " + sub);
        int count = 0;
        for(FileStatus s : statuses) {
          final String name = s.getPath().getName();
          if (key.equals(keyVO.valueOf(name))) {
            final Path dest = new Path(sub, name);
            fs.rename(s.getPath(), dest);
            Print.println("    Renamed " + s.getPath() + " to " + dest);
            count++;
          }
        }
        final Path p = new Path(dir, key + ".writable");
        write(result, p, fs);
        Print.println("    count=" + count);
        Print.println("  Wrote to " + p + ", result="+ result);
      }        
    }
  }

  /** Read existing results. */
  public static <K, R extends MachineComputable.Result<R>> List<R> readExistingResults(
      final K key, StringSerializable.ValueOf<K> keyVO, DataSerializable.ValueOf<R> valueVO,
      final FileStatus[] statuses, FileSystem fs) throws IOException {
    List<R> values = new ArrayList<R>();
    if (statuses != null && statuses.length > 0) {
      int count = 0;
      for(FileStatus s : statuses) {
        if (!s.isDir()) {
          final String name = s.getPath().getName();
          if (key.equals(keyVO.valueOf(name))) { 
            //read file
            DataInputStream in = null;
            R r = null;
            try {
              in = fs.open(s.getPath());
              r = valueVO.valueOf(in);
            } finally {
              Print.println("  " + name + " => " + r);
              if (in != null) {
                in.close();
              }
            }
  
            //put to the map
            if (r != null) {
              values.add(r);
              if (values.size() > 1) {
                values = JavaUtil.combine(values);
              }
              count++;
            }
          }
        }
      }
      Print.println(key + ": #(existing results) = " + count);
    }
    if (values.size() > 0) {
      values = JavaUtil.combine(values);
      //combine results
      Print.println(key + ": Combined: size=" + values.size() + ", [");
      for(int i = 0; i < values.size(); i++) {
        Print.println("  " + i + ": " + values.get(i));
      }
      Print.println("]");
    }
    return values;
  }

  /**
   * Create a directory.
   * @return true if the directory already exists.
   */
  public static boolean createDirectory(FileSystem fs, Path dir,
      boolean deleteIfExists) throws IOException {
    final boolean exists = fs.exists(dir);
    if (exists) {
      if (deleteIfExists) {
        fs.delete(dir, true);
      } else {
        return true;
      }
    }
    if (!fs.mkdirs(dir)) {
      throw new IOException("Cannot create working directory " + dir);
    }
    fs.setPermission(dir, new FsPermission((short)0777));
    return exists;
  }

  /** Write Mod1Fraction to a file. */
  public static void write(Mod1Fraction v, Path p, FileSystem fs 
      ) throws IOException{
    Print.println("Write to " + p + " ...");
    final DataOutputStream out = fs.create(p, true);
    v.serialize(out);
    out.close();
  }

  public static void array2confEntries(final Configuration conf, final String name, final String... values) {
    conf.setInt(name + ".length", values.length);
    for(int i = 0; i < values.length; i++)
      conf.set(name + "." + i, values[i]);
  }

  public static String[] confEntries2array(final Configuration conf, final String name) {
    final String[] values = new String[conf.getInt(name + ".length", 0)];
    for(int i = 0; i < values.length; i++)
      values[i] = conf.get(name + "." + i);
    return values;
  }
  /////////////////////////////////////////////////////////////////////////////
  public static abstract class RunnerBase extends Configured implements Tool {
    protected final JavaUtil.Timer timer = new JavaUtil.Timer(true, true);
    protected final File logfile; 
    protected RunnerBase() throws FileNotFoundException {
      logfile = Print.initLogFile(getClass().getSimpleName());
    }
  }
}