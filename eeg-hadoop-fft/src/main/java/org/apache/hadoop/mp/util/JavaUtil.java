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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


/** Utility methods */
public class JavaUtil {
  /** Timer */
  public static class Timer {
    private final boolean isAccumulative;
    private final long start = System.currentTimeMillis();
    private long previous = start;
  
    /** Timer constructor
     * @param isAccumulative  Is accumulating the time duration?
     */
    public Timer(final boolean isAccumulative, final boolean printStartMessage) {
      this.isAccumulative = isAccumulative;
      if (printStartMessage) {
        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        final int last = stack.length - 1;
        Print.beginIndentation("STARTUP " + new Date(start));
        Print.println("Started at " + stack[last]);
        Print.println("Printed at " + stack[Math.min(2, last)]);
        Print.endIndentation();
      }
    }
    
    public long getStart() {return start;}
  
    /** Same as tick(null). */
    public long tick() {return tick(null);}

    /**
     * Tick
     * @param mess Output message.  No output if it is null.
     * @return delta
     */
    public synchronized long tick(final Object mess) {
      final long t = System.currentTimeMillis();
      final long delta = t - (isAccumulative? start: previous);
      if (mess != null) {
        final String s = mess instanceof String? (String)mess: mess.toString();
        Print.println(String.format("%15dms (=%-15s: %s", delta, Parse.millis2String(delta) + ")", s));
      }
      previous = t;
      return delta;
    }
  }

  /** Execute the callables by a number of threads */
  public static <T, E extends Callable<T>> void execute(final int nThreads,
      List<E> callables, Timer timer
      ) throws InterruptedException, ExecutionException {
    final int n = callables.size();
    timer.tick("EXECUTOR: " + n + " computation(s)");

    final ExecutorService executor = Executors.newFixedThreadPool(nThreads); 
    final List<Future<T>> futures = new ArrayList<Future<T>>(n);
    for(int i = 0; i < n; i++) {
      futures.add(executor.submit(callables.get(i)));
    }

    long sleeptime = Math.min(n*100L, 300*1000L);
    for(int done = 0; done < n; ) {
      Thread.sleep(sleeptime);

      int count = 0;
      for(Future<T> f : futures) {
        if (f.isDone())
          count++;
      }
      if (count > done) {
        done = count;
        sleeptime = Math.min((n-done)*100L + 1000L, 300*1000L);
        final long ms = timer.tick();
        final double total = (ms/(double)done)*n;
        final double remaining = total - ms;
        Print.println("EXECUTOR: " + done + "/" + n
            + " done, estimated time: remaining=" + Parse.millis2String(remaining, 2)
            + ", total=" + Parse.millis2String(total, 2)
        		+ ", sleep=" + Parse.millis2String(sleeptime));
      }
    }
  }
  
  public static class WorkGroup {
    private final String name;
    private final Timer timer;
    private final AtomicInteger count = new AtomicInteger(0);
    private final ExecutorService executor;
    
    public WorkGroup(final String name, final int nWorkers, final Timer timer) {
      this.name = name;
      this.timer = timer;
      this.executor = Executors.newFixedThreadPool(nWorkers);
      Print.println(getClass().getSimpleName() + ": Created " + name
          + " with " + nWorkers + " threads.");
    }

    public synchronized void submit(final String task, final Runnable r) {
      final int increment = count.incrementAndGet();
      if (timer != null)
        timer.tick("++" + name + ".count=" + increment + " for " + task);
      executor.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          try {
            r.run();
          } catch(Throwable t) {
            Print.print("In " + WorkGroup.class.getSimpleName() + " " + name
                + ", " + task + " threw ");
            Print.printStackTrace(t);
            throw new Exception(t);
          } finally {
            final int decrement = count.decrementAndGet();
            if (timer != null)
              timer.tick("--" + name + ".count=" + decrement + " for " + task);
          }
          return null;
        }
      });
    }

    private static final int SLEEP_TIME = 100;
    private static final int SLEEP_COUNT = 60000/SLEEP_TIME;

    public void waitUntilZero() {waitUntil(0);}

    public void waitUntil(final int n) {
      int c;
      for(int j = 1; (c = count.get()) > n; j = (j + 1) % SLEEP_COUNT) {
        if (j == 0) {
          if (timer != null)
            timer.tick("wait4Zero " + name + ".count=" + c);
        }
        sleepms(SLEEP_TIME, null);
      }
    }
  }
  
  public static Random newRandom(long seed) {
    Print.println("seed = " + seed + "L");
    return new Random(seed);
  }

  public static Random newRandom() {
    final Random r = new Random();
    final long seed = r.nextLong();
    Print.println("seed = " + seed + "L");
    r.setSeed(seed);
    return r;
  }

  /** Combine a list of items. */
  public static <T extends Combinable<T>> List<T> combine(Collection<T> items) {
    final List<T> sorted = new ArrayList<T>(items);
    if (sorted.size() <= 1)
      return sorted;

    Collections.sort(sorted);
    //println("sorted", "", sorted);

    final List<T> combined = new ArrayList<T>(items.size());
    T prev = sorted.get(0);
    for(int i = 1; i < sorted.size(); i++) {
      final T curr = sorted.get(i);
      final T c = curr.combine(prev);

      if (c != null)
        prev = c;
      else {
        combined.add(prev);
        prev = curr;
      }
    }
    combined.add(prev);
    return combined;
  }

  public static int toInt(final long n) {
    if (n > Integer.MAX_VALUE)
      throw new ArithmeticException(n + " = n > Integer.MAX_VALUE = " + Integer.MAX_VALUE);
    if (n < Integer.MIN_VALUE)
      throw new ArithmeticException(n + " = n > Integer.MIN_VALUE = " + Integer.MIN_VALUE);
    return (int)n;
  }

  public static byte toByte(int n) {
    if (n > Byte.MAX_VALUE)
      throw new ArithmeticException(n + " = n > Byte.MAX_VALUE = " + Byte.MAX_VALUE);
    if (n < Byte.MIN_VALUE)
      throw new ArithmeticException(n + " = n < Byte.MIN_VALUE = " + Byte.MIN_VALUE);
    return (byte)n;
  }

  /** Check local directory. */
  public static void checkDirectory(final File dir) {
    if (!dir.exists())
      if (!dir.mkdirs())
        throw new IllegalArgumentException("!dir.mkdirs(), dir=" + dir);
    if (!dir.isDirectory())
      throw new IllegalArgumentException("dir (=" + dir + ") is not a directory.");
  }

  /** Create a writer of a local file. */
  public static File createFile(final File dir,
      final String prefix, final String suffix) throws FileNotFoundException {
    if (dir != null)
      checkDirectory(dir);

    for(;;) {
      final File f = new File(dir, prefix + "-" + Parse.currentTime2String() + suffix);
      if (!f.exists()) {
        Print.println("Create file " + f);
        return f;
      }

      try {Thread.sleep(10);} catch (InterruptedException e) {}
    }
  }

  public static void sleepms(final long ms, final String message) {
    if (message != null)
      Print.println(message + ": sleep " + Parse.millis2String(ms));
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Print.printStackTrace(e);
    }
  }

  public static void sleepsecond(final long s, final String message) {
    sleepms(s * 1000, message);
  }

  public static void main(String[] args) {
    Print.printSystemInfo();
    
    final long a = 1L << 53;
    final double d = 1.0/a;
    final double e = 2*d;
    final double x  = 1;
    final double y = x + d;
    final double z = x + e;
    Print.println("a = " + a);
    Print.println("d = " + d);
    Print.println("e = " + e);
    Print.println("x = " + x);
    Print.println("y = " + y);
    Print.println("z = " + z);
    
    {
      double macheps = 1.0;
      for(; 1.0 + (macheps/2.0) != 1.0; macheps /= 2.0);
      Print.println( "Calculated Machine epsilon: " + macheps );
    }
  }
}