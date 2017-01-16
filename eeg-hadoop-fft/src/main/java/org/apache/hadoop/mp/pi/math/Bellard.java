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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.mp.util.Container;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

/**
 * Bellard's BBP-type Pi formula
 * 1/2^6 \sum_{n=0}^\infty (-1)^n/2^{10n}
 * (-2^5/(4n+1) -1/(4n+3) +2^8/(10n+1) -2^6/(10n+3) -2^2/(10n+5)
 *  -2^2/(10n+7) +1/(10n+9))
 *  
 * References:
 *
 * [1] David H. Bailey, Peter B. Borwein and Simon Plouffe.  On the Rapid
 *     Computation of Various Polylogarithmic Constants.
 *     Math. Comp., 66:903-913, 1996.
 *     
 * [2] Fabrice Bellard.  A new formula to compute the n'th binary digit of pi,
 *     1997.  Available at http://fabrice.bellard.free.fr/pi .
 */
public final class Bellard implements StringSerializable.ValueOf<Bellard.Parameter> {
  public static Bellard FACTORY = new Bellard();

  private Bellard() {}

  @Override
  public Parameter valueOf(String s) {
    String t = s.trim();
    if (t.charAt(0) == 'P')
      t = t.substring(1);
    final String[] parts = t.split("\\D+");
    if (parts.length >= 2) {
      final String name = "P" + parts[0] + "_" + parts[1];  
      for(Parameter p : Parameter.values())
        if (p.name().equals(name))
          return p;
    }
    return null;
  }
  /////////////////////////////////////////////////////////////////////////////
  /** Parameters for the sums */
  public enum Parameter {
    // \sum_{k=0}^\infty (-1)^{k+1}( 2^{d-10k-1}/(4k+1) + 2^{d-10k-6}/(4k+3) )
    P8_1(false, 1, 8, -1),
    P8_3(false, 3, 8, -6),
    P8_5(P8_1),
    P8_7(P8_3),

    /*
     *   2^d\sum_{k=0}^\infty (-1)^k( 2^{ 2-10k} / (10k + 1)
     *                               -2^{  -10k} / (10k + 3)
     *                               -2^{-4-10k} / (10k + 5)
     *                               -2^{-4-10k} / (10k + 7)
     *                               +2^{-6-10k} / (10k + 9) )
     */
    P20_21(true , 1, 20,  2),
    P20_3(false, 3, 20,  0),
    P20_5(false, 5, 20, -4),
    P20_7(false, 7, 20, -4),
    P20_9(true , 9, 20, -6),
    P20_11(P20_21),
    P20_13(P20_3),
    P20_15(P20_5),
    P20_17(P20_7),
    P20_19(P20_9);
    
    public final boolean isplus;
    final long j;
    final int deltaN;
    final int deltaE;
    final int offsetE;      

    private Parameter(boolean isplus, long j, int deltaN, int offsetE) {
      this.isplus = isplus;
      this.j = j;
      this.deltaN = deltaN;
      this.deltaE = -20;
      this.offsetE = offsetE;        
    }

    private Parameter(Parameter p) {
      this.isplus = !p.isplus;
      this.j = p.j + (p.deltaN >> 1);
      this.deltaN = p.deltaN;
      this.deltaE = p.deltaE;
      this.offsetE = p.offsetE + (p.deltaE >> 1);
    }
  }

  static Summation createSummation(long b, Parameter p) {
    if (b < 0)
      throw new IllegalArgumentException("b = " + b + " < 0");
    final long i = p.j == 1 && p.offsetE >= 0? 1 : 0;
    final long e = b + i*p.deltaE + p.offsetE;
    final long n = i*p.deltaN + p.j;

    /*
     * Given precision p, what are the values of N and E?
     * 2^{E}(1/N) < 1/2^p
     * 2^{E + p} < N
     * E + p < log n' < log N, where n' is some value of n
     * 
     * log x >= Long.SIZE - 1 - Long.numberOfLeadingZeros(x)
     * 
     * Therefore, set E = Long.SIZE - 1 - Long.numberOfLeadingZeros(n') - p
     */
    final long ntmp = n - p.deltaN*(e/p.deltaE);
    final long elimit = Long.SIZE - 1 - Long.numberOfLeadingZeros(ntmp) - Mod1Fraction.getPrecision();

    return new Summation(p.isplus, n, p.deltaN, e, p.deltaE, elimit);
  }
  /////////////////////////////////////////////////////////////////////////////
  public static Parameter[] parse(String s) {
    if (s == null)
      return Parameter.values();
    else {
      final List<Parameter> a = new ArrayList<Parameter>();
      for(final StringTokenizer t = new StringTokenizer(s, ", "); t.hasMoreTokens();) {
        final Parameter p = FACTORY.valueOf(t.nextToken());
        if (p != null)
          a.add(p);
      }
      return a.toArray(new Parameter[a.size()]);
    }
  }

  /** The sums in the Bellard's formula */
  public static class Sum implements Container<Summation>, Iterable<Summation> {
    private final String name;
    private final Summation sigma;
    private final Summation[] parts;

    /** Constructor */
    private <T extends Container<Summation>> Sum(long b, Parameter p, int nParts, List<T> existing) {
      if (nParts < 1)
        throw new IllegalArgumentException("nParts = " + nParts + " < 1");

      this.name = p.toString();
      this.sigma = createSummation(b, p);
      this.parts = partition(sigma, nParts, existing);
    }

    private static <T extends Container<Summation>> Summation[] partition(
        Summation sigma, int nParts, List<T> existing) {
      final List<Summation> parts = new ArrayList<Summation>();
      if (existing == null || existing.isEmpty())
        parts.addAll(sigma.partition(nParts));
      else {
        final long stepsPerPart = sigma.getSteps()/nParts;
        final List<Summation> remaining = sigma.remainingTerms(existing);

        for(Summation s : remaining) {
          final int n = (int)((s.getSteps() - 1)/stepsPerPart) + 1;
          parts.addAll(s.partition(n));
        }
        Collections.sort(parts);
      }
      return parts.toArray(new Summation[parts.size()]);
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
      return name + ": " + sigma + ", parts.length=" + parts.length;
    }

    /** {@inheritDoc} */
    @Override
    public Summation get() {
      return sigma;
    }
/*
    /** The sum tail 
    private class Tail {
      private long n;
      private long e;

      private Tail(long n, long e) {
        if (e > 0) {
          final long edelta = -sigma.E.delta;
          long q = e / edelta;
          long r = e % edelta;
          if (r == 0) {
            e = 0;
            n += q * sigma.N.delta;
          } else {
            e = edelta - r;
            n += (q + 1)*sigma.N.delta;
          }
        } else if (e < 0)
          e = -e; 

        this.n = n;
        this.e = e;
      }

      //2^{p-E} < N
      //=>  p - E < log N
      //=>  p - e + s(d_e) < log(n + s(d_n))
      //
      //Therefore, check  p - e + s(d_e) < log n
      private Mod1Fraction compute() {
        //Util.out.println("Tail: e=" + e + ", n=" + n);
    
        final long p = Mod1Fraction.getPrecision();
        for(final Mod1Fraction s = Mod1Fraction.zero();;) {
          if (e > p || p - e <= Long.SIZE - 1 - Long.numberOfLeadingZeros(n))
            return s;

          s.addShiftFractionMod1Equal(n, e);
          n += sigma.N.delta;
          e -= sigma.E.delta;
        }
      }
    }
*/
    /** {@inheritDoc} */
    @Override
    public Iterator<Summation> iterator() {
      return new Iterator<Summation>() {
        private int i = 0;

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {return i < parts.length;}
        /** {@inheritDoc} */
        @Override
        public Summation next() {return parts[i++];}
        /** Unsupported */
        @Override
        public void remove() {throw new UnsupportedOperationException();}
      };
    }
  }

  /** Get the sums for the Bellard formula. */
  public static <T extends Container<Summation>> Sum getSum(final long b,
      final Parameter p, final int partsPerSum, List<T> existings) {
    final Sum s = new Sum(b, p, partsPerSum, existings);
    if (s.parts.length == 0) {
      Print.println("DONE: " + s);
      return null;
    } else {
      Print.println("ADD : " + s);
      return s;
    }
  }

  /** Compute bits of Pi in the local machine. */
  public static Mod1Fraction computePi(final long b, final long precision,
      final JavaUtil.Timer t) {
    Mod1Fraction.setPrecision(precision);

    final Mod1Fraction pi = Mod1Fraction.zero();
    for(Parameter p : Parameter.values()) {
      final Summation sigma = createSummation(b, p);
      final Mod1Fraction value = sigma.compute(t);
      if (p.isplus)
        pi.addMod1Equal(value);
      else
        pi.subtractMod1Equal(value);
      if (t != null)
        t.tick(p + ": sigma=" + sigma + ", value=" + value);
    }
    return pi;
  }

  /** main */
  public static void main(String[] args) throws IOException {
    final Mod1Fraction.Factory[] factories = {
        new Mod1Fraction_IntArray.Factory(),
        //new Mod1Fraction_Apfloat.Factory()
    };

    Print.initLogFile(Bellard.class.getSimpleName());
    /*
    for(long p = 1000; ; p *= 10) {
      final long b = 0;
      final long precision = p + 1000;
    */
    //for(long b = 100; ; b *= 10) {
    {
      int  b = 2152301;
      final long precision = 128;

      Print.println("\n\nb = " + Parse.long2string(b)
          + ", precision = " + Parse.long2string(precision));

      for(Mod1Fraction.Factory f : factories) {
        Mod1Fraction.FACTORY = f;
        final String name = Mod1Fraction.FACTORY.getName();
        Print.println("\n" + name);
      
        final JavaUtil.Timer t = new JavaUtil.Timer(false, true);
        t.tick("START: " + name);
        final Mod1Fraction pi = computePi(b, precision, null);
        final long duration = t.tick("DONE");
        Print.log.println("duration = " + duration + " = " + Parse.millis2String(duration));
        pi.print(Print.log);
      }
      Print.println(Mod1Fraction_IntArray.getStatistics());
    }
    

    /*
    computePi(t, 1);
    computePi(t, 2);
    computePi(t, 3);
    computePi(t, 4);

    Util.printBitSkipped(1008);
    computePi(t, 1008);
    computePi(t, 1012);

    {
      long b = 10;
      for(int i = 0; i < 3; i++) {
        Util.printBitSkipped(b);
        computePi(t, b - 4);
        computePi(t, b);
        computePi(t, b + 4);
        b *= 10;
      }
    }
    */
/*
    {
      final long b = 0;
      final long check = 1280;
      long p = 100;
      for(int i = 0; i < 10; i++) {
        final long duration = t.tick(computePi(new JavaUtil.Timer(false), b, p));
        JavaUtil.out.println("b = " + b + ", p = " + p
            + ", duration = " + JavaUtil.millis2String(duration));
        
        final long last = Math.max(((b + p - 1)/check - 1)*check, 0L);
        t.tick("last = " + last + ", check = " + check + ", " + computePi(null, last, 2*check));
        p *= 10;
      }
    }
    */
  }
}
