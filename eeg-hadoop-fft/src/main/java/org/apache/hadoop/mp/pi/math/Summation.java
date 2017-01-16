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
package org.apache.hadoop.mp.pi.math;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mp.pi.DistBbp;
import org.apache.hadoop.mp.pi.TaskResult;
import org.apache.hadoop.mp.util.Combinable;
import org.apache.hadoop.mp.util.Container;
import org.apache.hadoop.mp.util.JavaUtil.Timer;

/** Represent the summation \sum \frac{2^e \mod n}{n}. */
public class Summation implements Container<Summation>, Combinable<Summation> {
  public final boolean ispositive;
  /** Variable n in the summation. */
  public final ArithmeticProgression N;
  /** Variable e in the summation. */
  public final ArithmeticProgression E;

  /** Constructor */
  public Summation(boolean ispositive, ArithmeticProgression N, ArithmeticProgression E) {
    if (N.getSteps() != E.getSteps()) {
      throw new IllegalArgumentException("N.getSteps() != E.getSteps(),"
          + "\n  N.getSteps()=" + N.getSteps() + ", N=" + N
          + "\n  E.getSteps()=" + E.getSteps() + ", E=" + E);
    }
    this.ispositive = ispositive;
    this.N = N;
    this.E = E;
  }

  /** Constructor */
  Summation(boolean ispositive, long valueN, long deltaN, 
            long valueE, long deltaE, long limitE) {
    this(ispositive, valueN, deltaN, valueN - deltaN*((valueE - limitE)/deltaE),
         valueE, deltaE, limitE);
  }

  /** Constructor */
  Summation(boolean ispositive, long valueN, long deltaN, long limitN, 
            long valueE, long deltaE, long limitE) {
    this(ispositive, new ArithmeticProgression('n', valueN, deltaN, limitN),
         new ArithmeticProgression('e', valueE, deltaE, limitE));
  }

  /** {@inheritDoc} */
  @Override
  public Summation get() {return this;}

  /** Return the number of steps of this summation */
  long getSteps() {return E.getSteps();}
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return (ispositive? "+[": "-[") + N + "; " + E + "]"; 
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (obj != null && obj instanceof Summation) {
      final Summation that = (Summation)obj;
      return this.N.equals(that.N) && this.E.equals(that.E);
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /** Covert a String to a Summation. */
  public static Summation valueOf(final String s) {
    final boolean ispositive = s.charAt(0) == '+';
    int i = 2;
    int j = s.indexOf("; ", i);
    if (j < 0)
      throw new IllegalArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
    final ArithmeticProgression N = ArithmeticProgression.valueOf(s.substring(i, j));

    i = j + 2;
    j = s.indexOf("]", i);
    if (j < 0)
      throw new IllegalArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
    final ArithmeticProgression E = ArithmeticProgression.valueOf(s.substring(i, j));
    return new Summation(ispositive, N, E);
  }

  /** Compute the value of the summation. */
  public Mod1Fraction compute(Timer timer) {
    return new Computer(timer).run();
  }

  /** Compute the value of the summation. */
  public TaskResult compute(int nThreads, Timer timer) {
    DistBbp.printVersions();

    final ThreadMXBean b = ManagementFactory.getThreadMXBean();
    final long start = b.getCurrentThreadCpuTime();
    final Mod1Fraction value = compute(timer);
    final long duration = b.getCurrentThreadCpuTime() - start;
    return new TaskResult(this, value, (duration + 500000)/1000000);
  }


  static final long SUB_STEPS = 8;

  private class Computer {
    private static final long MAX_MODULAR = 1L << 32;

    private long n = N.value;
    private long e = E.value;
    private final long ntail;
    private final Mod1Fraction s = Mod1Fraction.zero();

    final Timer timer;

    private Computer(Timer timer) {
      this.timer = timer;

      if (E.limit >= 0) {
        ntail = N.limit;
      } else if (e <= 0) {
        ntail = n;
      } else {
        final long edelta = -E.delta;
        final long r = e % edelta;
        final long q = e / edelta;
        final long nlimit =  n + (r == 0?q: q+1)* N.delta;
        ntail = N.limit < nlimit? N.limit: nlimit;
      }
    }

    /** Compute the value of the summation. */
    public Mod1Fraction run() {
      compute_modular();
      compute_montgomery();
      compute_tail();
      return s;
    }

    void println(String mess) {
      if (timer != null) {
        timer.tick(mess);
      }
    }

    /** Compute the value using {@link org.apache.hadoop.mp.pi.math.Modular#mod(long, long)}. */
    void compute_modular() {
      final long nlimit = ntail < MAX_MODULAR? ntail: MAX_MODULAR;
      if (n < nlimit) {
        final long substepsize = (nlimit - n)/(N.delta*SUB_STEPS);
        println("compute_modular: n=" + n + ", nlimit=" + nlimit
            + ", substepsize=" + substepsize + ", " + Summation.this);
  
        for(long i = 0; i < SUB_STEPS; ) {
          for(long j = 0; j < substepsize; j++) {
            s.addFractionMod1Equal(Modular.mod(e, n), n);
            n += N.delta;
            e += E.delta;
          }
          
          i++;
          println("compute_modular: sub-step=" + i + "/" + SUB_STEPS
              + ", n=" + n + ", nlimit=" + nlimit + ", " + Summation.this);
        }
        for(; n < nlimit; ) {
          s.addFractionMod1Equal(Modular.mod(e, n), n);
          n += N.delta;
          e += E.delta;
        }
      }
    }
  
    private final Montgomery montgomery = new Montgomery();
    /** Compute the value using {@link org.apache.hadoop.mp.pi.math.Montgomery#mod(long)}. */
    void compute_montgomery() {
      if (n < ntail) {
        final long substepsize = (ntail - n)/(N.delta*SUB_STEPS);
        println("compute_montgomery: n=" + n + ", ntail=" + ntail
            + ", substepsize=" + substepsize + ", " + Summation.this);
  
        for(long i = 0; i < SUB_STEPS; ) {
          for(long j = 0; j < substepsize; j++) {
            s.addFractionMod1Equal(montgomery.set(n).mod(e), n);
            n += N.delta;
            e += E.delta;
          }
          
          i++;
          println("compute_montgomery: sub-step=" + i + "/" + SUB_STEPS
              + ", n=" + n + ", ntail=" + ntail + ", " + Summation.this);
        }
        for(; n < ntail; ) {
          s.addFractionMod1Equal(montgomery.set(n).mod(e), n);
          n += N.delta;
          e += E.delta;
        }
      }
    }

    //2^{E}(1/N) < 1/2^p
    // 2^{E + p} < N
    //     E + p < log n < log N
    //
    //log n >= Long.SIZE - 1 - Long.numberOfLeadingZeros(n)
    //
    //Therefore, set E = Long.SIZE - 1 - Long.numberOfLeadingZeros(n) - p
    void compute_tail() {
      if (n < N.limit) {
        final long substepsize = (N.limit - n)/(N.delta*SUB_STEPS);
        println("compute_tail: n=" + n + ", N.limit=" + N.limit
            + ", substepsize=" + substepsize + ", " + Summation.this);
  
        for(long i = 0; i < SUB_STEPS; ) {
          for(long j = 0; j < substepsize; j++) {
            s.addShiftFractionMod1Equal(n, -e);
            n += N.delta;
            e += E.delta;
          }
  
          i++;
          println("compute_tail: sub-step=" + i + "/" + SUB_STEPS
              + ", n=" + n + ", N.limit=" + N.limit + ", " + Summation.this);
        }
        for(; n < N.limit; ) {
          s.addShiftFractionMod1Equal(n, -e);
          n += N.delta;
          e += E.delta;
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(Summation that) {
    final int de = this.E.compareTo(that.E);
    if (de != 0) return de;
    return this.N.compareTo(that.N);
  }

  /** {@inheritDoc} */
  @Override
  public Summation combine(Summation that) {
    if (this.ispositive != that.ispositive)
      throw new IllegalArgumentException("this.ispositive != that.ispositive"
          + ",\n  this=" + this
          + ",\n  that=" + that);
    if (this.N.delta != that.N.delta || this.E.delta != that.E.delta) 
      throw new IllegalArgumentException(
          "this.N.delta != that.N.delta || this.E.delta != that.E.delta"
          + ",\n  this=" + this
          + ",\n  that=" + that);
    if (this.E.limit == that.E.value && this.N.limit == that.N.value) {
      final Summation s = new Summation(ispositive,
          new ArithmeticProgression(N.symbol, N.value, N.delta, that.N.limit),
          new ArithmeticProgression(E.symbol, E.value, E.delta, that.E.limit));
      return s;
    }
    return null;
  }

  /** Find the remaining terms. */
  public <T extends Container<Summation>> List<Summation> remainingTerms(List<T> sorted) {
    final List<Summation> results = new ArrayList<Summation>();
    Summation remaining = this;

    if (sorted != null)
      for(Container<Summation> c : sorted) {
        final Summation sigma = c.get();
        if (!remaining.contains(sigma))
          throw new IllegalArgumentException("!remaining.contains(s),"
              + "\n  remaining = " + remaining
              + "\n  s         = " + sigma          
              + "\n  this      = " + this
              + "\n  sorted    = " + sorted);

        final Summation s = new Summation(sigma.ispositive,
            sigma.N.limit, N.delta, remaining.N.limit,
            sigma.E.limit, E.delta, remaining.E.limit);
        if (s.getSteps() > 0)
          results.add(s);
        remaining = new Summation(sigma.ispositive,
            remaining.N.value, N.delta, sigma.N.value,
            remaining.E.value, E.delta, sigma.E.value);
      }

    if (remaining.getSteps() > 0)
      results.add(remaining);
  
    return results;
  }

  /** Does this contains that? */
  public boolean contains(Summation that) {
    return this.ispositive == that.ispositive
        && this.N.contains(that.N) && this.E.contains(that.E);    
  }

  /** Partition the summation. */
  public List<Summation> partition(final int nParts) {
    final List<Summation> parts = new ArrayList<Summation>();
    final long steps = (E.limit - E.value)/E.delta + 1;

    long prevN = N.value;
    long prevE = E.value;

    for(int i = 1; i < nParts; i++) {
      final long k = (i * steps)/nParts;

      final long currN = N.skip(k);
      final long currE = E.skip(k);
      if (currN != prevN || currE != prevE)
        parts.add(new Summation(ispositive,
            new ArithmeticProgression(N.symbol, prevN, N.delta, currN),
            new ArithmeticProgression(E.symbol, prevE, E.delta, currE)));

      prevN = currN;
      prevE = currE;
    }

    
    if (N.limit != prevN || E.limit != prevE)
      parts.add(new Summation(ispositive,
          new ArithmeticProgression(N.symbol, prevN, N.delta, N.limit),
          new ArithmeticProgression(E.symbol, prevE, E.delta, E.limit)));
    return parts;
  }
}