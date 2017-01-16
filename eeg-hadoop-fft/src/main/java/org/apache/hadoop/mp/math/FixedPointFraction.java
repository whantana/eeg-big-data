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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.mp.util.Checksum;
import org.apache.hadoop.mp.util.Container;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.Print.Level;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

/**
 * Fractions with a fixed radix point. 
 */
public class FixedPointFraction
    extends ArbitraryPrecision<FixedPointFraction, FixedPointFraction.Element>
    implements Container<Zahlen> {
  private static final int VERSION = -1; 
  static final Print.Level PRINT_LEVEL = Print.Level.INFO;

  //static final boolean debug = false;
  //private static Stack<BigInteger> DEBUG_STACK = new Stack<BigInteger>();

  /** The underlying integer set. */
  final Zahlen Z;
  /**
   * The position of the decimal point in term of digits.
   * Must be a multiple of Z.digitsPerArray.
   */
  final int pointPos;

  private FixedPointFraction(final int pointPos, final Zahlen Z) {
    Print.println("NEW: " + FixedPointFraction.class.getSimpleName()
        + "(pointPos=" + pointPos
        + ", Z=" + Z.toBrief() + ")");
    this.pointPos = pointPos;
    this.Z = Z;

    Z.digitsPerArray.checkIsMultiple(pointPos);
  }

  @Override
  String getKey() {return  pointPos + "," + Z.getKey();}
  @Override
  protected Element newElement_private() {return new Element();}
  @Override
  public Element random(final int nDigit, final Random r) {
    final Element e = newElement();
    e.fraction = Z.random(nDigit, r);
    return e;
  }

  @Override
  public Zahlen get() {return Z;}

  @Override
  public String toBrief() {
    return getClass().getSimpleName()
        + "(pointPos=" + pointPos
        + ")";
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "\n  pointPos = " + pointPos
        + "\n  Z        = " + Z;
  }

  @Override
  public String serialize() {
    return "pointPos=" + pointPos
         + ", " + Z.serialize();
  }

  @Override
  public Void serialize(DataOutput out) throws IOException {
    //write version
    out.writeInt(VERSION);
    out.writeInt(pointPos);
    Z.serialize(out);
    return null;
  }

  public Element valueOf(final int n) {
    final Zahlen.Element z = Z.newElement().plusEqual(n);
    final Element e = newElement();
    e.fraction = z.shiftLeftEqual_digits(pointPos);
    return e;
  }

  public Element valueOf(final Zahlen.Element r) {
    final Element e = newElement();
    e.fraction = r;
    return e;
  }
  /////////////////////////////////////////////////////////////////////////////
  public final class Element
      extends ArbitraryPrecision<FixedPointFraction, Element>.Element {
    private Zahlen.Element fraction;
    
    Element() {this(Z.newElement());}

    Element(Zahlen.Element fraction) {this.fraction = fraction;}

    @Override
    Element setZero() {
      fraction.setZero();
      return this;
    }
    @Override
    Element set(Element that) {
      this.fraction.set(that.fraction);
      return this;
    }
    
    public Zahlen.Element getFraction() {return fraction;}
    
    boolean isOne() {
      return fraction.isOne(pointPos) && fraction.isAllDigitsZero(pointPos);
    }

    @Override
    public int compareTo(final Element that) {
      this.check(that);
      return this.fraction.compareTo(that.fraction);
    }
    
    private BigDecimal DIVISOR;

    /** For verifying results in testing or debugging. */
    synchronized BigDecimal toBigDecimal() {
      if (fraction.isZero())
        return BigDecimal.ZERO;
      if (DIVISOR == null)
        DIVISOR = new BigDecimal(BigInteger.valueOf(Z.digitLimit.value).pow(pointPos));
      return new BigDecimal(fraction.toBigInteger()).divide(DIVISOR);
    }

    @Override
    public Checksum serialize(DataOutput out) throws IOException {
      FixedPointFraction.this.serialize(out);
      return fraction.serialize(out);
    }

    @Override
    public String toBrief() {
      final int numOfDigits = fraction.numberOfDigits();
      return "{fraction=" + fraction.toBrief() + ", pointPos=" + pointPos
          + ", numOfDigits=" + numOfDigits + "}";
    }
    @Override
    public String toString() {return toBrief();}
    @Override
    public void printDetail(final String firstlineprefix) {
      if (firstlineprefix != null)
        Print.print(firstlineprefix + ": ");
      if (PRINT_LEVEL.is(Level.TRACE))
        fraction.printDetail("fraction");
      print(10);
    }
    @Override
    public void print(int digitsPerLine) {
      if (fraction.isZero()) {
        Print.println("0.0");
      } else {
        final int numOfDigits = fraction.numberOfDigits();
        Print.println((fraction.isPositive()? "+" : "-") + "[pointPos=" + pointPos
            + ", numOfDigits=" + numOfDigits
            + ", numOfBts=" + fraction.numberOfBits_long() + ":");
        if (numOfDigits > pointPos) {
          Print.print(" ");
          fraction.printDigits(pointPos, numOfDigits, false, true, digitsPerLine);      
          Print.println("  .");
        } else {
          Print.println(" 0.");
        }
        Print.print(" ");
        fraction.printDigits(0, pointPos, true, false, digitsPerLine);      
        Print.println("]");
      }
    }

    @Override
    public Element shiftRightEqual_bits(long shift_in_bits) {
      fraction.shiftRightEqual_bits(shift_in_bits);
      return this;
    }
    @Override
    public Element shiftLeftEqual_bits(long shift_in_bits) {
      fraction.shiftLeftEqual_bits(shift_in_bits);
      return this;
    }
    @Override
    public Element negateEqual() {
      fraction.negateEqual();
      return this;
    }
    @Override
    public Element plusEqual(Element that) {
      this.check(that);
      this.fraction.plusEqual(that.fraction);
      return this;
    }
    @Override
    public Element multiplyEqual(final Element that, final JavaUtil.WorkGroup workers) {
      if (this.fraction.numberOfDigits() + that.fraction.numberOfDigits() > Z.digitsSupported)
        throw new ArithmeticException("this.fraction.numberOfDigits() + that.fraction.numberOfDigits() > Z.digitsSupported,"
            + "\n  this.fraction.numberOfDigits() = " + this.fraction.numberOfDigits()
            + "\n  that.fraction.numberOfDigits() = " + that.fraction.numberOfDigits()
            + "\n  Z.digitsSupported              = " + Z.digitsSupported);
      fraction.multiplyEqual(that.fraction, workers);
      fraction.shiftRightEqual_digits(that.get().pointPos);
      return this;
    }
    
    /**
     * Use Newton method to compute reciprocal.
     * x_{n+1} = x_n(2 - a x_n)
     */
    public Element reciprocalEqual(final JavaUtil.WorkGroup workers) {
      if (fraction.isZero()) {
        throw new ArithmeticException("fraction.isZero()=" + fraction.isZero());
      } else if (isOne()) {
        return this;
      } else {
        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
          Print.beginIndentation("reciprocalEqual: this = " + this + " = " + this.toBigDecimal());

        //handle negative numbers
        final boolean negative = fraction.isNegative();
        if (negative) fraction.negateEqual();

        //normalize
        final int normalize_shift = fraction.normalize4division();
        final int shift_in_digits = pointPos - fraction.numberOfDigits();
        final int shift = normalize_shift + (shift_in_digits << (Z.bitsPerDigit.exponent + 1));
        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE)) {
          Print.println("normalize_shift = " + normalize_shift);
          Print.println("shift_in_digits = " + shift_in_digits);
          Print.println("shift           = " + shift);
        }

        fraction.approximateReciprocalEqual(workers);

        if (shift > 0) {
          fraction.shiftLeftEqual_bits(shift);
          //printDetail("ERROR");
          //throw new ArithmeticException("Lossing precision, shift = " + shift);
        }
        else
          fraction.shiftRightEqual_bits(-shift);

        if (negative) fraction.negateEqual();

        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("return this = " + this.toBigDecimal());
        return this;
      }
    }

    /**
     * Use Newton method to compute reciprocal.
     * x_{n+1} = x_n(2 - a x_n)
     */
    public Element sqrtReciprocalEqual(final JavaUtil.WorkGroup workers) {
      if (fraction.isNonPositive()) {
        this.printDetail("this");
        throw new ArithmeticException("fraction.isNonPositive()");
      } else if (isOne()) {
        return this;
      } else {
        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
          Print.beginIndentation("sqrtReciprocalEqual: this = " + this.toBigDecimal());
        
        final int normalize_shift = fraction.normalize4sqrt();
        final int n = fraction.numberOfDigits() - 1;
        final int shift_in_digits = 3*(pointPos - n);
        final int shift = (normalize_shift >> 1) + (shift_in_digits << (Z.bitsPerDigit.exponent - 1));

        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE)) {
          Print.println("normalize_shift  = " + normalize_shift + ", fraction = " + fraction);
          Print.println("n (degree)       = " + n);
          Print.println("shift_in_digits  = " + shift_in_digits);
          Print.println("shift            = " + shift);
        }

        fraction.approximateSqrtReciprocalEqual(workers);

        if (shift > 0) {
          fraction.shiftLeftEqual_bits(shift);
//          printDetail("ERROR");
//          throw new ArithmeticException("Losing precision, shift = " + shift);
        }
        else {
          fraction.shiftRightEqual_bits(-shift);
        }

        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("return this = " + this.toBigDecimal());
        return this;
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static final Factory FACTORY = new Factory();
  public static class Factory 
      implements DataSerializable.ValueOf<FixedPointFraction>, StringSerializable.ValueOf<FixedPointFraction> {
    private final Map<String, FixedPointFraction> cache = new HashMap<String, FixedPointFraction>();

    public FixedPointFraction valueOf(final int pointPos, final Zahlen Z) {
      final String key = pointPos + "," + Z.getKey();
      FixedPointFraction z = cache.get(key);
      if (z == null) {
        cache.put(key, z = new FixedPointFraction(pointPos, Z));
      }
      return z;
    }

    @Override
    public FixedPointFraction valueOf(DataInput in) throws IOException {
      final int version = in.readInt();
      if (version != VERSION) {
        throw new IOException(version + " = version != VERSION = " + VERSION);
      } else {
        final int pointPos = in.readInt();
        final Zahlen Z = Zahlen.FACTORY.valueOf(in);
        return valueOf(pointPos, Z);
      }
    }

    @Override
    public FixedPointFraction valueOf(String s) {
      int i = 0;
      int j = s.indexOf(", ", i);
      final int pointPos = Parse.parseIntVariable("pointPos", s.substring(i, j));

      i = j + 2;
      final Zahlen Z = Zahlen.FACTORY.valueOf(s.substring(i));

      return FACTORY.valueOf(pointPos, Z);
    }
  }
  public static final DataSerializable.ValueOf<Element> ELEMENT_FACTORY
      = new DataSerializable.ValueOf<Element>() {
    @Override
    public Element valueOf(DataInput in) throws IOException {
      final FixedPointFraction fpf = FACTORY.valueOf(in);
      final Element e = fpf.newElement();
      final Zahlen.Element fraction = Zahlen.ELEMENT_FACTORY.valueOf(in);
      e.fraction = fraction;
      return e;
    }
  };
  
  public static void main(String[] args) {
    String s = "1461501637330902918144401915499223635557982291207";
    Print.println("s = " + s);
    BigInteger x = new BigInteger(s);
    Print.println("x = " + x.toString(16));
  }
}