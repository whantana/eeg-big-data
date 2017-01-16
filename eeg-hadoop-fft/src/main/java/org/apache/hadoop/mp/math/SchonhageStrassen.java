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
package org.apache.hadoop.mp.math;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.mp.math.Zahlen.Element;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.StringSerializable;


/**
 * Schonhage-Strassen Integer Multiplication
 */
public class SchonhageStrassen implements StringSerializable {
  static final String VERSION = "20110123";
  static Print.Level PRINT_LEVEL = Print.Level.INFO;
  static final JavaUtil.Timer timer = new JavaUtil.Timer(false, false);
  
  public final Zahlen Z;
  /** modulus = 2^m + 1 */
  public final int modulusExponent;

  public final PowerOfTwo_int bitsPerElement;

  /** Dimension */
  public final PowerOfTwo_int D;
  final RootOfUnity D_inverse;

  final RootOfUnity[] zeta;

  private SchonhageStrassen(final Factory.Key k) {
    Print.println("NEW: " + SchonhageStrassen.class.getSimpleName() + k);

    this.modulusExponent = k.modulusExponent;
    this.D = k.D;
    
    final int orderOfTwo = modulusExponent << 1;
    this.D_inverse = new RootOfUnity(orderOfTwo - D.exponent, false, modulusExponent);
    this.bitsPerElement = k.bitsPerElement;
    this.Z = k.Z;

    zeta = new RootOfUnity[D.value];
    zeta[0] = new RootOfUnity(0, false, modulusExponent);
    final int z = orderOfTwo >> D.exponent;
    zeta[1] = z == modulusExponent?
        new RootOfUnity(0, true, modulusExponent): new RootOfUnity(z, false, modulusExponent);
    for(int i = 2; i < zeta.length; i++) {
      zeta[i] = new RootOfUnity(zeta[i - 1].shift + zeta[1].shift,
                                zeta[i - 1].negation, modulusExponent);
    }

    validate();
  }

  private void validate() {
    // require: modulus > D M^2, where M is the digit limit.
    if (modulusExponent < D.exponent + (bitsPerElement.value << 1)) {
      Print.print("this = " + this);
      throw new IllegalArgumentException(
          "m < D.exponent + 2*bitsPerElement,\n  m=" + modulusExponent
          + ", D=" + D + ", bitsPerElement=" + bitsPerElement);
    }
    // require: D | orderOfTwo
    final int orderOfTwo = modulusExponent << 1;
    if ((orderOfTwo & D.mask) != 0)
      throw new IllegalArgumentException();
    // require: digitsPerElement < digitsPerArray
//    final int digitsPerElement = bitsPerElement.value >> Z.bitsPerDigit.exponent;
//    if (digitsPerElement > Z.digitsPerArray.value)
//      throw new IllegalArgumentException(digitsPerElement
//          + " = digitsPerElement > Z.digitsPerArray = " + Z.digitsPerArray);
  }

  public long digitsPerOperand() {
    //digitsPerOperand = (D.value >> 1)*(bitsPerElement/bitsPerDigit);
    final int left = bitsPerElement.exponent - Z.bitsPerDigit.exponent - 1;
    return left > 0? (long)D.value << left: D.value >> -left;
  }

  public int digitsPerElement() {
    return bitsPerElement.value >> Z.bitsPerDigit.exponent;
  }

  public static final Factory FACTORY = new Factory();
  public static class Factory {
    private static class Key {
      final Zahlen Z;
      final int modulusExponent;
      final PowerOfTwo_int bitsPerElement;
      final PowerOfTwo_int D;
      
      private Key(final int modulusExponent, final PowerOfTwo_int bitsPerElement,
          final PowerOfTwo_int D, final Zahlen Z) {
        this.modulusExponent = modulusExponent;
        this.bitsPerElement = bitsPerElement;
        this.D = D;
        this.Z = Z;
      }
      
      @Override
      public String toString() {
        return "[modulas=2^" + modulusExponent + " + 1"
            + ", D=" + D
            + ", bitsPerElement=" + bitsPerElement
            + ", Z=" + Z.toBrief() + "]";
      }

      @Override
      public int hashCode() {
        return modulusExponent ^ bitsPerElement.value ^ D.value;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj)
          return true;
        else if (obj instanceof Key) {
          final Key that = (Key)obj;
          return this.Z == that.Z
              && this.D == that.D
              && this.modulusExponent == that.modulusExponent
              && this.bitsPerElement == that.bitsPerElement;
        }
        return false;
      }
    }
    private final Map<Key, SchonhageStrassen> cache = new HashMap<Key, SchonhageStrassen>();

    private SchonhageStrassen valueOf(final Key k) {
      SchonhageStrassen ss = cache.get(k);
      if (ss == null)
        cache.put(k, ss = new SchonhageStrassen(k));
      return ss;
    }

    /** For serialization */
    public SchonhageStrassen valueOf(final int modulusExponent,
        final PowerOfTwo_int bitsPerElement, final PowerOfTwo_int D, final Zahlen Z) {
      return valueOf(new Key(modulusExponent, bitsPerElement, D, Z));
    }

    public SchonhageStrassen valueOf(final String s, final Zahlen Z) {
      int i = 0;
      int j = s.indexOf(", ", i);
      final PowerOfTwo_int D = PowerOfTwo_int.valueOf(
          Parse.parseIntVariable("D", s.substring(i, j)));

      i = j + 2;
      j = s.indexOf(", ", i);
      final PowerOfTwo_int bitsPerElement = PowerOfTwo_int.valueOf(
          Parse.parseIntVariable("bitsPerElement", s.substring(i, j)));

      i = j + 2;
      final int modulusExponent = Parse.parseIntVariable("modulusExponent", s.substring(i));

      return FACTORY.valueOf(modulusExponent, bitsPerElement, D, Z);
    }

    /** For Map/Reduce tasks */
    public SchonhageStrassen valueOf(final int D, final SchonhageStrassen ss) {
      return valueOf(new Key(ss.modulusExponent,
          ss.bitsPerElement, PowerOfTwo_int.valueOf(D), ss.Z));
    }

    private Set<PowerOfTwo_long> PRINTED_NUM_DIGITS = new TreeSet<PowerOfTwo_long>();
    /** For initial use */
    public SchonhageStrassen valueOf(final long numDigits, final Zahlen Z) {
      final long highest = Long.highestOneBit(numDigits);
      final PowerOfTwo_long digitsPerOperand = PowerOfTwo_long.valueOf(
          highest == numDigits? numDigits: (highest << 1));

      final int D_exponent = (digitsPerOperand.exponent + Z.bitsPerDigit.exponent + 1) >> 1;
      final PowerOfTwo_int D = PowerOfTwo_int.values()[D_exponent < 4? 4: (D_exponent >> 1) << 1];

      final PowerOfTwo_int bitsPerElement;
      {
        final int e = D.exponent - 1 - Z.bitsPerDigit.exponent;
        final int bpe = (int)(e > 0? digitsPerOperand.value >> e: digitsPerOperand.value << -e);
        bitsPerElement = PowerOfTwo_int.valueOf(bpe);
      }

      final int ss_exponent = Z.bitsPerDigit.toMultipleOf((D.exponent >> 1) + (bitsPerElement.value << 1));
      final int modulusExponent = D.toMultipleOf(ss_exponent << 1) >> 1;

      if (!PRINTED_NUM_DIGITS.contains(digitsPerOperand)) {
        PRINTED_NUM_DIGITS.add(digitsPerOperand);
        Print.beginIndentation("SchonhageStrassen.Factory.valueOf(numDigits=" + numDigits + "): ");
        Print.println("digitsPerOperand = " + digitsPerOperand + " (highest=" + highest + ")");
        Print.println("bitsPerElement   = " + bitsPerElement);
        Print.println("D                = " + D); 
        Print.println("modulusExponent  = " + modulusExponent + " (ss_e=" + ss_exponent + ")");
        
        final PowerOfTwo_long N = PowerOfTwo_long.valueOf(digitsPerOperand.value << (Z.bitsPerDigit.exponent + 1));
        final double efficiency = ((N.value >> (D.exponent - 1)) + D.exponent)*1.0/modulusExponent;
        Print.println("efficiency       = " + efficiency + " (N=" + N + ", n=" + modulusExponent + ")");
        Print.endIndentation("");
      }
      return valueOf(new Key(modulusExponent, bitsPerElement, D, Z));
    }
    
    public static void main(final String[] args) {
      final Zahlen Z = Zahlen.FACTORY.valueOf(32, 1024, 1024); 
      for(int i = 10; i < 31; i++) {
        FACTORY.valueOf(1 << i, Z);
      }
    }
  }

  @Override
  public String serialize() {
    return "D=" + D.value
         + ", bitsPerElement=" + bitsPerElement.value
         + ", modulusExponent=" + modulusExponent;
  }

  /**
   * Digit scrambling 
   */
  static void scramble(Zahlen.Element[] x) {
    final int n = x.length;
    int j = 0;
    for(int i = 0; i < n - 1; i++) {
      if (i < j) {
        final Zahlen.Element tmp = x[i];
        x[i] = x[j];
        x[j] = tmp;
      }
      int k = n >> 1;
      for(; k <= j; k >>= 1) j -= k;
      j += k;
    }
  }

  /** x += y */
  public void plusEqual(final Zahlen.Element x, final int y) {
    x.plusEqual(y).ssModEqual(modulusExponent);
  }

  /** x += y */
  public void plusEqual(final Zahlen.Element x, final Zahlen.Element y) {
    x.plusEqual(y).ssModEqual(modulusExponent);
  }

  /** x *= zeta^i */
  public void multiplyEqual(final Zahlen.Element x, final int i) {
    x.ssMultiplyEqual(zeta[i], modulusExponent);
  }

  /** x *= y */
  public void multiplyEqual(final Zahlen.Element x, final Zahlen.Element y) {
    x.multiplyEqual(y, null).ssModEqual(modulusExponent);
  }

  /** x[i] *= y[i] */
  public void componentwiseMultiplications(
      final Zahlen.Element[] x, final Zahlen.Element[] y,
      final JavaUtil.WorkGroup workers) {
    if (workers == null) {
      for(int i = 0; i < x.length; i++) {
        x[i].multiplyEqual(y[i], null).ssModEqual(modulusExponent);
      }
    } else {
      final String postfix = "] mod (2^" + modulusExponent + " + 1)";
      for(int i = 0; i < x.length; i++) {
        final int index = i;
        workers.submit("x[" + i + "] *= y[" + i + postfix, new Runnable() {
          @Override
          public void run() {
            x[index].multiplyEqual(y[index], null).ssModEqual(modulusExponent);
          }
        });
      }
      workers.waitUntilZero();
    }
  }

  /** Schonhage-Strassen Integer Multiplication */
  public Zahlen.Element multiplyEquals(final Zahlen.Element x, final Zahlen.Element y,
      final FastFourierTransform algorithm, final JavaUtil.WorkGroup workers) {
    if (PRINT_LEVEL.is(Print.Level.VERBOSE)) {
      timer.tick("multiply(x.numberOfBits()=" + x.numberOfBits_long()
          + ", y.numberOfBits()=" + y.numberOfBits_long() + ")");
    }

    //split
    final Zahlen.Element[] X = x.split(bitsPerElement.value, D.value);
    if (PRINT_LEVEL.is(Print.Level.TRACE))
      Print.print("X", X);
    final Zahlen.Element[] Y = y.split(bitsPerElement.value, D.value);
    if (PRINT_LEVEL.is(Print.Level.TRACE))
      Print.print("Y", Y);

    //x = DFT(x)
    algorithm.fft(X, workers);
    if (PRINT_LEVEL.is(Print.Level.VERBOSE))
      timer.tick(algorithm.getClass().getSimpleName() + ".dft X.length=" + X.length);
    if (PRINT_LEVEL.is(Print.Level.TRACE))
      Print.print("DFT(X)", X);

    //y = DFT(y)
    algorithm.fft(Y, workers);
    if (PRINT_LEVEL.is(Print.Level.VERBOSE))
      timer.tick(algorithm.getClass().getSimpleName() + ".dft Y.length=" + Y.length);
    if (PRINT_LEVEL.is(Print.Level.TRACE))
      Print.print("DFT(Y)", Y);

    //x *= y
    long d = 0;
    long b = 0;
    for(int i = 0; i < X.length; i++) {
      d += X[i].numberOfDigits() + Y[i].numberOfDigits();
      b += X[i].numberOfBits_long() + Y[i].numberOfBits_long();
    }
    componentwiseMultiplications(X, Y, workers);
    if (PRINT_LEVEL.is(Print.Level.VERBOSE))
      timer.tick("Componentwise-Multiplications: "
          + X.length + " operations, average "
          + String.format("%.2f digits, %.2f bits", d/2.0/X.length, b/2.0/X.length));
    if (PRINT_LEVEL.is(Print.Level.TRACE))
      Print.print("DFT(X)*DFT(Y)", X);

    //DFT^{-1}(x)
    algorithm.fft_inverse(X, workers);
    if (PRINT_LEVEL.is(Print.Level.VERBOSE))
      timer.tick(algorithm.getClass().getSimpleName() + ".dft_inverse, X.length=" + X.length);
    normalize(X);
    if (PRINT_LEVEL.is(Print.Level.VERBOSE))
      timer.tick("Normalize");
    if (PRINT_LEVEL.is(Print.Level.TRACE))
      Print.print("DFT^-1", X);

    //carrying
    carrying(X);
    if (PRINT_LEVEL.is(Print.Level.VERBOSE))
      timer.tick("Carrying");

    if (PRINT_LEVEL.is(Print.Level.TRACE)) {
      Print.print("return", X);
    }

    x.combine(X, bitsPerElement.value >> Z.bitsPerDigit.exponent);
    for(int i = 0; i < D.value; i++) {
      X[i].reclaim();
      Y[i].reclaim();
    }
    return x;
  }

  /** Normalize reverse DFT */
  public void normalize(Zahlen.Element[] x) {
    for(int i = 0; i < x.length; i++) {
      x[i].ssMultiplyEqual(D_inverse, modulusExponent);
    }
  }

  public void carrying(final Zahlen.Element[] z) {
    final int exponent_in_digits = this.digitsPerElement();

    Zahlen.Element carry = Z.newElement();
    for(int i = 0; i < D.value; i++) {
      z[i].plusEqual(carry);
      carry.reclaim();
      carry = z[i].divideRemainderEqual(exponent_in_digits);
    }
    carry.reclaim();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("[").append(zeta[0]);
    int i = 1;
    for(; i < zeta.length && i < 10; i++)
      b.append(", ").append(zeta[i]);
    if (i < zeta.length)
      b.append(", ...");
    b.append("]");
    
    return getClass().getSimpleName()
        + "\n  D              = " + D
        + "\n  D^(-1)         = " + D_inverse
        + "\n  modulus        = 2^" + modulusExponent + " + 1"
        + "\n  bitsPerElement = " + bitsPerElement
        + "\n  zeta           = " + b;
  }
  //////////////////////////////////////////////////////////////////////////////
  final DiscreteFourierTransform discretefouriertransform = new DiscreteFourierTransform();

  /** Compute DFT directly from the definition. */
  class DiscreteFourierTransform implements FastFourierTransform {
    void dft(final Zahlen.Element[] x, final boolean inverse) {
//      Print.beginIndentation("DiscreteFourierTransform.dft: inverse = " + inverse);
//      for(int k = 0; k < x.length; k++) {
//        Print.println("x[" + k + "] = " + x[k]);
//      }
      final Zahlen.Element[] y = new Zahlen.Element[x.length];
      for(int k = 0; k < y.length; k++) {
        y[k] = Z.newElement();
        for(int j = 0; j < x.length; j++) {
          int index = (-j*k) % D.value;
          if (index < 0)
            index += D.value;
          if (index > 0 && inverse)
            index = D.value - index;

//          Print.println("zeta[" + index + "] = " + zeta[index]);
          final Zahlen.Element tmp = Z.newElement().set(x[j]);
//          Print.println("x[" + j+ "] = " + x[j] + ", tmp = " + tmp);
          tmp.ssMultiplyEqual(zeta[index], modulusExponent);
          y[k].plusEqual(tmp);
//          Print.println("y[" + k + "] = " + y[k] + ", tmp = " + tmp);
          y[k].ssModEqualBySubstraction(modulusExponent);
//          Print.println("y[" + k + "] = " + y[k]);
          tmp.reclaim();
        }
      }
      for(int k = 0; k < y.length; k++) {
        x[k].reclaim();
        x[k] = y[k];
      }
//      Print.endIndentation("reutrn x = " + x);
    }

    @Override
    public void fft(Element[] x, final JavaUtil.WorkGroup workers) {dft(x, false);}
    @Override
    public void fft_inverse(Element[] x, final JavaUtil.WorkGroup workers) {dft(x, true);}
  }
  //////////////////////////////////////////////////////////////////////////////
  final DanielsonLanczos danielsonlanczos = new DanielsonLanczos();

  /** Compute DFT by recursion. */
  class DanielsonLanczos implements FastFourierTransform {
    void fft(final Zahlen.Element[] x, final int offset, final int step,
        final int length_exponent, final boolean inverse) {
      if (length_exponent == 0)
        return;
      else if (length_exponent == 1) {
        final Zahlen.Element tmp = Z.newElement().set(x[offset]);
        final int i = offset + step;
        x[offset].plusEqual(x[i]);
        x[offset].ssModEqualBySubstraction(modulusExponent);
        
        tmp.plusEqual(x[i].negateEqual());
        tmp.ssModEqualByAddition(modulusExponent);
        x[i].reclaim();
        x[i] = tmp;
      } else {
        final int length = 1 << length_exponent;
        final int halflength = length >> 1;
        final int doublestep = step << 1;
        fft(x, offset, doublestep, length_exponent - 1, inverse);
        fft(x, offset + step, doublestep, length_exponent - 1, inverse);
  
        final Zahlen.Element[] y = new Zahlen.Element[length];
        final int ratio_exponent = D.exponent - length_exponent;
        for(int k = 0; k < y.length; k++) {
          int index = -k;
          if (index < 0)
            index += length;
          if (index > 0 && inverse)
            index = length - index;
          index <<= ratio_exponent;
        
          final int even_k = offset + doublestep*(k < halflength? k: k - halflength);
          final int odd_k = even_k + step;
          y[k] = Z.newElement().set(x[odd_k]);
          y[k].ssMultiplyEqual(zeta[index], modulusExponent);
          y[k].plusEqual(x[even_k]);
          y[k].ssModEqualBySubstraction(modulusExponent);
        }
  
        for(int k = 0; k < y.length; k++) {
          final int j = offset + step*k;
          x[j].reclaim();
          x[j] = y[k];
        }
      }
    }

    @Override
    public void fft(Element[] x, final JavaUtil.WorkGroup workers) {
      final int exponent = Integer.numberOfTrailingZeros(x.length);
      fft(x, 0, 1, exponent, false);
    }

    @Override
    public void fft_inverse(Element[] x, final JavaUtil.WorkGroup workers) {
      final int exponent = Integer.numberOfTrailingZeros(x.length);
      fft(x, 0, 1, exponent, true);
    }
  }
  //////////////////////////////////////////////////////////////////////////////
  public final Parallel parallel = new Parallel();

  /** Compute DFT by recursion. */
  public class Parallel implements FastFourierTransform {
    private void inner(final int k0, final PowerOfTwo_int K_step,
        final PowerOfTwo_int J, final int ratio_exponent,
        final Zahlen.Element[] x, final int offset, final PowerOfTwo_int step,
        final PowerOfTwo_int length, final boolean inverse,
        final Zahlen.Element[] x_tmp, final JavaUtil.WorkGroup workers) {
      //inner DFT
      int start = offset + (k0 << step.exponent);
      if (PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("inner DFT: k0 = " + k0);
      fft(x, start, K_step, J, inverse, x_tmp, null);

      //scale multiplication
      for(int j0 = 0; j0 < J.value; j0++) {
        int index = j0*k0;
        if (index > 0 && inverse)
          index = length.value - index;
        index <<= ratio_exponent;

        x[start].ssMultiplyEqual(zeta[index], modulusExponent);
        start += K_step.value;
      }
    }

    void fft(final Zahlen.Element[] x, final int offset, final PowerOfTwo_int step,
        final PowerOfTwo_int length, final boolean inverse,
        final Zahlen.Element[] x_tmp, final JavaUtil.WorkGroup workers) {
      if (length.value == 1)
        return;
      else if (length.value == 2) {
        final Zahlen.Element tmp = Z.newElement().set(x[offset]);
        final int i = offset + step.value;
        x[offset].plusEqual(x[i]);
        x[offset].ssModEqualBySubstraction(modulusExponent);
        
        tmp.plusEqual(x[i].negateEqual());
        tmp.ssModEqualByAddition(modulusExponent);
        x[i].reclaim();
        x[i] = tmp;
      } else {
        final int ratio_exponent = D.exponent - length.exponent;

        final PowerOfTwo_int J = PowerOfTwo_int.values()[length.exponent >> 1];
        final PowerOfTwo_int K = PowerOfTwo_int.values()[length.exponent - J.exponent];

        if (PRINT_LEVEL.is(Print.Level.TRACE)) {
          Print.beginIndentation("ParallelDft.dft");
          Print.println("offset=" + offset + ", step=" + step
            + ", length=" + length + "=2^" + length.exponent
            + ", J=" + J + ", K=" + K);
        }

        final PowerOfTwo_int K_step = PowerOfTwo_int.values()[step.exponent + K.exponent];
        if (workers == null) {
          for(int k0 = 0; k0 < K.value; k0++) {
            inner(k0, K_step, J, ratio_exponent, x, offset, step, length, inverse, x_tmp, workers);
          }
        } else {
          for(int i = 0; i < K.value; i++) {
            final int k0 = i;
            workers.submit("k0=" + k0, new Runnable() {
              @Override
              public void run() {
                inner(k0, K_step, J, ratio_exponent, x, offset, step, length, inverse, x_tmp, workers);
              }
            });
          }
          workers.waitUntilZero();
        }

        if (workers == null) {
          int start = offset;
          for(int j0 = 0; j0 < J.value; j0++) {
            //outer DFT
            fft(x, start, step, K, inverse, x_tmp, null);
            start += K_step.value;
          }
        } else {
          for(int j0 = 0; j0 < J.value; j0++) {
            //outer DFT
            final int start = (j0 << K_step.exponent) + offset;
            workers.submit("j0=" + j0, new Runnable() {
              @Override
              public void run() {
                fft(x, start, step, K, inverse, x_tmp, null);
              }
            });
          }
          workers.waitUntilZero();
        }
        
        //Transpose
        synchronized(x_tmp) {
          System.arraycopy(x, offset, x_tmp, offset, (step.value << length.exponent) - step.value + 1);
          for(int j1 = 0; j1 < K.value; j1++) {
            int x_tmp_i = j1 << step.exponent;
            int xi = offset + (x_tmp_i << J.exponent);
            x_tmp_i += offset;
  
            for(int j0 = 0; j0 < J.value; j0++) {
              x[xi] = x_tmp[x_tmp_i];
              xi += step.value;
              x_tmp_i += K_step.value;
            }
          }
          if (PRINT_LEVEL.is(Print.Level.TRACE))
            Print.endIndentation("return");
        }
      }
    }

    @Override
    public void fft(Element[] x, final JavaUtil.WorkGroup workers) {
      fft(x, 0, PowerOfTwo_int.TWO_00, PowerOfTwo_int.valueOf(x.length),
          false, new Element[x.length], workers);
    }

    @Override
    public void fft_inverse(Element[] x, final JavaUtil.WorkGroup workers) {
      fft(x, 0, PowerOfTwo_int.TWO_00, PowerOfTwo_int.valueOf(x.length),
          true, new Element[x.length], workers);
    }
  }
  //////////////////////////////////////////////////////////////////////////////
  public final CooleyTukey cooleytukey = new CooleyTukey();

  /**
   * Cooley-Tukey, decimation-in-time FFT
   */
  public class CooleyTukey implements FastFourierTransform {
    /**
     * Cooley-Tukey without digit-scrambling.
     */
    void fft_withoutScramble(final Zahlen.Element[] x, final boolean inverse) {
      final boolean forward = !inverse;
      int m_e = 0;
      for(int m = 1; m < D.value; m <<= 1) {
        final int two_m = m << 1;

        for(int j = 0; j < m; j++) {
          int index = -j<<(D.exponent - m_e - 1);
          if (index < 0)
            index += D.value;
          if (index > 0 && forward)
            index = D.value - index;
          final RootOfUnity z = zeta[index];

          for(int i = j; i < D.value; i += two_m) {
            final int iPm = i + m;
            final Zahlen.Element b = Z.newElement();
            b.set(x[iPm].ssMultiplyEqual(z, modulusExponent));

            x[iPm].negateEqual().plusEqual(x[i]);
            x[iPm].ssModEqualByAddition(modulusExponent);

            x[i].plusEqual(b);
            x[i].ssModEqualBySubstraction(modulusExponent);

            b.reclaim();
          }
        }
        m_e++;
      }
    }

    @Override
    public void fft(Element[] x, final JavaUtil.WorkGroup workers) {
      scramble(x);
      fft_withoutScramble(x, false);
    }

    @Override
    public void fft_inverse(Element[] x, final JavaUtil.WorkGroup workers) {
      scramble(x);
      fft_withoutScramble(x, true);
    }
  }
  //////////////////////////////////////////////////////////////////////////////
  final GentlemanSande gentlemansande = new GentlemanSande();

  /**
   * Gentleman-Sande, decimation-in-frequency FFT
   */
  class GentlemanSande implements FastFourierTransform {
    /**
     * Gentleman-Sande without digit-scrambling
     */
    void fft_withoutScramble(final Zahlen.Element[] x, final boolean inverse) {
      final boolean forward = !inverse;
      int m_eP1 = D.exponent;
      int two_m = D.value;
      for(int m = D.value >>> 1; m >= 1; m >>>= 1) {
        for(int j = 0; j < m; j++) {
          int index = -j << (D.exponent - m_eP1);
          if (index < 0)
            index += D.value;
          if (index > 0 && forward)
            index = D.value - index;
          final RootOfUnity z = zeta[index];

          for(int i = j; i < D.value; i += two_m) {
            final int iPm = i + m;
            final Zahlen.Element b = x[iPm];

            x[iPm] = Z.newElement().set(x[i]);
            x[i].plusEqual(b);
            x[i].ssModEqualBySubstraction(modulusExponent);
            
            x[iPm].plusEqual(b.negateEqual());
            x[iPm].ssModEqualByAddition(modulusExponent);
            x[iPm].ssMultiplyEqual(z, modulusExponent);
            
            b.reclaim();
          }
        }
        m_eP1--;
        two_m = m;
      }
    }
    
    @Override
    public void fft(Element[] x, final JavaUtil.WorkGroup workers) {
      fft_withoutScramble(x, false);
      scramble(x);
    }

    @Override
    public void fft_inverse(Element[] x, final JavaUtil.WorkGroup workers) {
      fft_withoutScramble(x, true);
      scramble(x);
    }
  }
  //////////////////////////////////////////////////////////////////////////////
  final GentlemanSande_CooleyTukey gentlemansande_cooleytukey = new GentlemanSande_CooleyTukey();

  class GentlemanSande_CooleyTukey implements FastFourierTransform {
    @Override
    public void fft(Element[] x, final JavaUtil.WorkGroup workers) {
      gentlemansande.fft_withoutScramble(x, false);
    }
    @Override
    public void fft_inverse(Element[] x, final JavaUtil.WorkGroup workers) {
      cooleytukey.fft_withoutScramble(x, true);
    }
  }
  //////////////////////////////////////////////////////////////////////////////
  /**
   * Fast Fourier Transform over Schonhage-Strassen modular.
   */
  public interface FastFourierTransform {
    void fft(final Zahlen.Element[] x, final JavaUtil.WorkGroup workers);
    void fft_inverse(final Zahlen.Element[] x, final JavaUtil.WorkGroup workers);
  }
  
  final FastFourierTransform[] algorithms = {
      cooleytukey,
      parallel,
      gentlemansande_cooleytukey,
      gentlemansande,
      danielsonlanczos,
//    discretefouriertransform,
  };
}
