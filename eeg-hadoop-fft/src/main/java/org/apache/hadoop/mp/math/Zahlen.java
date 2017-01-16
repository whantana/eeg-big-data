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
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.mp.gmp.GmpMultiplier;
import org.apache.hadoop.mp.util.Checksum;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.Recycler;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StreamSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

/**
 * Zahlen, Z, the set of integers.
 */
public class Zahlen extends ArbitraryPrecision<Zahlen, Zahlen.Element>
    implements StreamSerializable.ValueOf<Zahlen.Element, InputStream> {
  private static final int VERSION = -1; 
  static final Print.Level PRINT_LEVEL = Print.Level.INFO;

  static final boolean debug = false;
  private static ThreadLocal<Stack<BigInteger>> DEBUG_STACK = new ThreadLocal<Stack<BigInteger>>() {
    @Override
    protected Stack<BigInteger> initialValue() {
      return new Stack<BigInteger>();
    }
  };

  //choose 1 << 13
  public static final int BIG_INTEGER_THRESHOLD = 1 << 9;
  public static final int KARATSUBA_THRESHOLD = 1 << 13;

  private final Recycler.IntArray arrayrecycler;
  
  public final PowerOfTwo_int bitsPerDigit; // must be either 8, 16 or 32
  final PowerOfTwo_long digitLimit;
  final int digitLimit_mask;
  public final PowerOfTwo_int digitsPerArray;
  final int numArrays;
  final int digitsSupported;
  final int hexPerDigit;
  final String hexFormat;
  
  private Zahlen(final int bitsPerDigit, final int digitsPerArray, final int numArrays) {
    Print.println("NEW: " + Zahlen.class.getSimpleName()
        + "(bitsPerDigit=" + bitsPerDigit
        + ", digitsPerArray=" + digitsPerArray
        + ", numArrays=" + numArrays + ")");
    if (bitsPerDigit % Byte.SIZE != 0)
      throw new IllegalArgumentException("bitsPerDigit (=" + bitsPerDigit
          + ") is not a multiple of Byte.SIZE (=" + Byte.SIZE + ")");
    if (Integer.SIZE < bitsPerDigit || Integer.SIZE % bitsPerDigit != 0)
      throw new IllegalArgumentException("bitsPerDigit (=" + bitsPerDigit
          + ") is not a factor of Integer.SIZE (=" + Integer.SIZE + ")");

    this.bitsPerDigit = PowerOfTwo_int.valueOf(bitsPerDigit);
    this.digitLimit = PowerOfTwo_long.valueOf(1L << this.bitsPerDigit.value);
    this.digitLimit_mask = (int)digitLimit.mask;
    this.digitsPerArray = PowerOfTwo_int.valueOf(digitsPerArray);
    this.numArrays = numArrays;
    this.digitsSupported = numArrays << this.digitsPerArray.exponent;

    this.hexPerDigit = bitsPerDigit >> 2;
    this.hexFormat = "%0" + hexPerDigit + "X";
    this.arrayrecycler = new Recycler.IntArray(RECYCLER_SIZE, this.digitsPerArray.value);
  }

  @Override
  String getKey() {return bitsPerDigit + "," + digitsPerArray + "," + numArrays;}

  @Override
  public String toBrief() {
    return getClass().getSimpleName()
        + "(bitsPerDigit=" + bitsPerDigit.toBrief()
        + ", digitLimit=" + digitLimit.toBrief()
        + ", digitsPerArray=" + digitsPerArray.toBrief()
        + ", numArrays=" + numArrays
        + ")";
  }

  @Override
  public String serialize() {
    return "bitsPerDigit=" + bitsPerDigit.value
         + ", digitsPerArray=" + digitsPerArray.value
         + ", numArrays=" + numArrays;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "\n  bitsPerDigit    = " + bitsPerDigit
        + "\n  digitLimit      = " + digitLimit
        + "\n  digitsPerArray  = " + digitsPerArray
        + "\n  numArrays       = " + numArrays
        + "\n  digitsSupported = " + digitsSupported;
  }

  @Override
  public Void serialize(DataOutput out) throws IOException {
    //write version
    out.writeInt(VERSION);

    bitsPerDigit.serialize(out);
    digitsPerArray.serialize(out);
    out.writeInt(numArrays);
    return null;
  }

  @Override
  protected Element newElement_private() {return new Element();}
  /////////////////////////////////////////////////////////////////////////////
  @Override
  public Element valueOf(final InputStream in) throws IOException {
    final Element e = newElement();
    e.positive = true;

    long hexcount = 0;
    for(int c; (c = in.read()) != '\n'; ) {
//      Print.println("c = " + (char)c);
      hexcount *= 10;
      hexcount += Parse.char2int(c);
    }
    e.nDigits = (((int)hexcount - 1)>> (bitsPerDigit.exponent - 2)) + 1;

//    Print.println("e.nDigits = " + e.nDigits);
    for(int i = e.nDigits - 1; hexcount > 0;) {
      final int q = i >> digitsPerArray.exponent;
      e.digits[q] = arrayrecycler.newObject();

      for(int r = i & digitsPerArray.mask; r >= 0; r--, i--) {
        int d = 0;
        for(int j = ((int)hexcount - 1) % hexPerDigit; j >= 0; j--, hexcount--) {
          d <<= 4;
          final int c = in.read();
//          Print.println("x = " + (char)c + ", hexcount=" + hexcount + ", i=" + i);
          d += Parse.char2int(c);
        }
        e.digits[q][r] = d;
      }
    }
    if (in.read() != '\n') {
      throw new IllegalArgumentException("Expected <eol>.");
    }
    return e;
  }

  /** Return a positive element with nDigit or less digits */
  public Element valueOf(boolean positive, final int[] digits) {
    for (int i = 0; i < digits.length; i++) {
      if (digits[i] < 0 || digits[i] >= digitLimit.value) {
        throw new RuntimeException("digits[i] < 0 || digits[i] >= digitLimit.value, i="
            + i + ", digits[i]=" + digits[i]);
      }
    }
    
    final Element e = newElement();
    e.positive = positive;
    e.nDigits = digits.length;
    
    int k = 0;
    for (int i = 0; i < e.nDigits;) {
      final int[] d = arrayrecycler.newObject();
      e.digits[i >> digitsPerArray.exponent] = d;

      for (int j = 0; i < e.nDigits && j < digitsPerArray.value; i++, j++) {
        d[j] = digits[k++];
      }
    }
    return e;
  }

  /** Return a positive element with nDigit or less digits */
  public Element random(final int nDigit, final Random r) {
    final Element e = newElement();
    e.positive = true;
    e.nDigits = nDigit;

    for (int i = 0; i < nDigit;) {
      final int[] d = arrayrecycler.newObject();
      e.digits[i >> digitsPerArray.exponent] = d;

      for (int j = 0; i < nDigit && j < digitsPerArray.value; i++, j++) {
        d[j] = r.nextInt() & digitLimit_mask;
      }
    }
    e.trimLeadingZeros();
    return e;
  }
  
  /////////////////////////////////////////////////////////////////////////////
  public interface Multiplier {
    Element multiply(
            final Element l, final Element r) throws Exception;
  }
  /////////////////////////////////////////////////////////////////////////////
  /** Elements in Z */
  public final class Element
      extends ArbitraryPrecision<Zahlen, Element>.Element
      implements StreamSerializable<PrintStream, PrintStream> {
    /** Is this an positive integer?
     *  Note that positive may be true or false for zero.
     */
    private boolean positive = false;
    /** Digits stored in a 2d-array */
    private final int[][] digits = new int[numArrays][];
    /** Total number of digits, not including leading zeros, not digits.length */
    private int nDigits = 0;

    /** Only {@link #newElement_private()} is allow to call constructor. */ 
    private Element() {}

    boolean isOne(int digitPos) {
      return digitPos < nDigits
          && digits[digitPos >> digitsPerArray.exponent][digitPos & digitsPerArray.mask] == 1;
    }
    boolean isAllDigitsZero(final int endDigitPos) {
      if (endDigitPos >= nDigits) {
        throw new IllegalArgumentException(endDigitPos
            + " = endDigitPos >= nDigits = " + nDigits);
      }
      
      final int lastq = (endDigitPos - 1) >> digitsPerArray.exponent;
      for(int q = 0; q < lastq; q++) {
        for(int r = 0; r < digitsPerArray.value; r++)
          if (digits[q][r] != 0)
            return false;
      }

      if (lastq >= 0) {
        final int lastr = (endDigitPos - 1) & digitsPerArray.mask;
        for(int r = 0; r <= lastr; r++)
          if (digits[lastq][r] != 0)
            return false;
      }

      return true;
    }

    @Override
    public Element setZero() {
      this.positive = true;
      this.nDigits = 0;
      clearArrays(0);
      return this;
    }
    @Override
    public Element set(final Element that) {
      this.positive = that.positive;
      this.nDigits = that.nDigits;
      int i = 0;
      for (; i < that.digits.length && that.digits[i] != null; i++) {
        if (this.digits[i] == null)
          this.digits[i] = arrayrecycler.newObject();
        System.arraycopy(that.digits[i], 0, this.digits[i], 0, that.digits[i].length);
      }
      clearArrays(i);
      return this;
    }

    @Override
    public Checksum serialize(final DataOutput out) throws IOException {
      //write Zahlen, compute checksum
      final Checksum c = Checksum.getChecksum();
      out.write(c.update(Zahlen.this));
      out.writeBoolean(c.update(positive));
      
      //write digits
      out.writeInt(c.update(nDigits));
      int q = 0;
      int r = 0;
      if (bitsPerDigit.value <= Integer.SIZE) {
        for(int i = 0; i < nDigits; i++) {
          out.writeInt(c.update((int)digits[q][r]));
          
          if (++r == digits[q].length) {
            r = 0;
            q++;
          }
        }
      } else {
        for(int i = 0; i < nDigits; i++) {
          out.writeLong(c.update(digits[q][r]));
          
          if (++r == digits[q].length) {
            r = 0;
            q++;
          }
        }
      }
      
      //write checksum
      c.writeDigest(out);
      return c;
    }

    @Override
    public int compareTo(Element that) {
      if (this == that || (this.nDigits == 0 && that.nDigits == 0)) {
        return 0;
      } else if (this.positive != that.positive) {
        return this.positive? 1: -1;
      } else {
        final int d = this.compareMagnitudeTo(that);
        if (d > 0)
          return positive? 1: -1;
        else if (d < 0)
          return positive? -1: 1;
        else
          return 0;
      }
    }

    public int compareMagnitudeTo(Element that) {
      if (this == that || (this.nDigits == 0 & that.nDigits == 0)) {
        return 0;
      } else if (this.nDigits > that.nDigits) {
        return 1;
      } else if (this.nDigits < that.nDigits) {
        return -1;
      } else {
        int r = (nDigits - 1) & digitsPerArray.mask;
        for(int q = (nDigits - 1) >> digitsPerArray.exponent; q >= 0; q--) {
          final int[] d = this.digits[q];
          final int[] that_d = that.digits[q];

          for(; r >= 0; r--)
            if (d[r] != that_d[r])
              return (d[r] & digitLimit.mask) > (that_d[r] & digitLimit.mask)? 1: -1;
          
          r = digitsPerArray.mask;
        }
        return 0;
      }
    }

    public Element[] split(final int bitsPerElement, final int parts) {
//      Print.beginIndentation("split(bitsPerElement=" + bitsPerElement + ", parts=" + parts + ") this = " + this);
      bitsPerDigit.checkIsMultiple(bitsPerElement);

      final Element[] elements = new Element[parts];
      for (int i = 0; i < elements.length; i++)
        elements[i] = newElement();
      if (this.nDigits == 0)
        return elements;

      final int digitsPerElement = bitsPerElement >> bitsPerDigit.exponent;
//      Print.println("digitsPerElement=" + digitsPerElement);
      int q = 0, r = 0;
      int[] a = this.digits[q];
      int i = 0;
      for (int d = 0; d < this.nDigits;) {
        int j = 0;
        for (; d < this.nDigits && j < digitsPerElement;) {
          final int[] array = arrayrecycler.newObject();
          for (int k = 0; d < this.nDigits && j < digitsPerElement && k < array.length; d++, j++, k++) {
            array[k] = a[r];
//            Print.println("array[" + k + "] = a[" + r + "] = " + array[k]);

            if (++r == digitsPerArray.value) {
              r = 0;
              q++;
              a = digits[q];   //may be null 
            }
          }
//          Print.println("  i=" + i + ", j=" + j + ", d=" + d);
          elements[i].digits[(j - 1) >> digitsPerArray.exponent] = array;
        }
        elements[i].positive = this.positive;
        elements[i].nDigits = j;
        elements[i].trimLeadingZeros();
//        Print.println("elements[" + i + "] = " + elements[i] + ", d=" + d);
        i++;
      }

//      Print.endIndentation("return");
      return elements;
    }

    public Element[] splitAndReclaim(final int digitsPerElement, final SchonhageStrassen schonhagestrassen) {
//    Print.beginIndentation("split(bitsPerElement=" + bitsPerElement + ", parts=" + parts + ") this = " + this);
      digitsPerArray.checkIsMultiple(digitsPerElement);

      final Element[] elements = new Element[schonhagestrassen.D.value];
      for (int i = 0; i < elements.length; i++)
        elements[i] = schonhagestrassen.Z.newElement();
      if (this.nDigits > 0) {
        final int arraysPerElement = digitsPerElement >> digitsPerArray.exponent;
        boolean done = false;
        int a = 0;
        for(int i = 0; !done && i < elements.length; i++) {
          elements[i].positive = this.positive;
          for(int j = 0; !done && j < arraysPerElement; j++) {
            elements[i].digits[j] = this.digits[a];
            this.digits[a] = null;
            done = elements[i].digits[j] == null;
            if (!done) {
              elements[i].nDigits += digitsPerArray.value;
            }
            a++;
          }
          elements[i].trimLeadingZeros();
        }
      }
    
//      Print.endIndentation("return");
      this.reclaim();
      return elements;
    }
    
    public Element combine(final Element[] elements, final int digitsPerElement) {
      if (!isZero()) {
        setZero();
      }
      positive = elements[0].positive;
      int[] d = arrayrecycler.newObject();
      digits[0] = d;
      int q = 0;
      int r = 0;
      for(int i = 0; i < elements.length; i++) {
        if (!elements[i].isZero() && elements[i].positive != positive)
          throw new RuntimeException("elements[" + i + "] = " + elements[i]);
        for(int j = 0; j < digitsPerElement; j++) {
          if (j < elements[i].nDigits)
            d[r] = elements[i].digits[j >> digitsPerArray.exponent][j & digitsPerArray.mask];
          
          if (++r == digitsPerArray.value) {
            r = 0;
            if (++q < digits.length)
              digits[q] = d = arrayrecycler.newObject();
          }
        }
        
        nDigits += digitsPerElement;
      }
      trimLeadingZeros();
      return this;
    }

    /** Clear all arrays above or equal to the start index. */
    private void clearArrays(final int startindex) {
      for(int i = startindex; i < digits.length && digits[i] != null; i++) {
        arrayrecycler.reclaim(digits[i]);
        digits[i] = null;
      }
    }

    private void set(final BigInteger x) {
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("set(x=" + x.toString(16)+ ")");
      final int cmp = x.compareTo(BigInteger.ZERO);
      positive = cmp >= 0;

      if (cmp == 0) {
        clearArrays(0);
        nDigits = 0;
      } else {
        final int bytePerDigit = bitsPerDigit.value >> 3;
        final byte[] bytes = (positive? x: x.negate()).toByteArray();


        if (PRINT_LEVEL.is(Print.Level.DEBUG)) {
          Print.println("bytes.length=" + bytes.length);
          Print.println(Parse.byteArray2string(bytes));
        }

        int[] d = digits[0];
        if (d == null)
          digits[0] = d = arrayrecycler.newObject();

        int i = bytes.length - 1;
        int starti = bytes[0] == 0? 1: 0;
        nDigits = (i - starti)/bytePerDigit + 1;
        if (PRINT_LEVEL.is(Print.Level.DEBUG)) {
          Print.println("i     =" + i);
          Print.println("nDigit=" + nDigits);
        }

        d[0] = bytes[i--] & 0xFF;
        int j = 1;
        for(; i >= starti && j < bytePerDigit; j++) {
          d[0] |= (bytes[i--] & 0xFFL) << (j << 3);
        }
        
        int q = 1;
        int r = 0;
        for(; i >= starti;) {
          if (++r == digitsPerArray.value) {
            r = 0;
            if (digits[q] == null)
              digits[q] = arrayrecycler.newObject();
            d = digits[q];
            ++q;
          }

          if (i >= starti)
            d[r] = bytes[i--] & 0xFF;
          for(j = 1; i >= starti && j < bytePerDigit; j++) {
            d[r] |= (bytes[i--] & 0xFFL) << (j << 3);
          }
        }
        if (++r < d.length)
          Arrays.fill(d, r, d.length, 0);

        if (q < digits.length)
          clearArrays(q);
      }

      //this.print(8, Print.out);
      if (debug) {
        final BigInteger c = toBigInteger();
        if (!c.equals(x))
          throw new RuntimeException("!c.equals(x)"
              + "\n c = " + c.toString(16)
              + "\n x = " + x.toString(16));
      }
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("set return " + this);
    }

    /** Is this > 0? */
    public boolean isPositive() {return nDigits > 0 && positive;}
    /** Is this < 0? */
    public boolean isNegative() {return nDigits > 0 && !positive;}
    /** Is this >= 0? */
    public boolean isNonNegative() {return nDigits == 0 || positive;}
    /** Is this <= 0? */
    public boolean isNonPositive() {return nDigits == 0 || !positive;}
    /** Is this == 0? */
    public boolean isZero() {return nDigits == 0;}
    /** Is this == 0? */
    public boolean isOne() {return nDigits == 1 && digits[0][0] == 1L;}

    public int numberOfDigits() {return nDigits;}
    
    /** @return ceiling(log_2 |n|),*/
    public long numberOfBits_long() {
      if (isZero())
        return 0;

      final int i = nDigits - 1;
//this.print(10, Print.out);
      final int last = digits[i >> digitsPerArray.exponent][i & digitsPerArray.mask];
      return ((long)nDigits << bitsPerDigit.exponent) - bitsPerDigit.value
          + Integer.SIZE - Integer.numberOfLeadingZeros(last);
    }

    public int numberOfBits_int() {
      return JavaUtil.toInt(numberOfBits_long());
    }
    
    public int toInt() {
      if (isZero())
        return 0;
      if (nDigits > 1) {
        throw new IllegalArgumentException(nDigits + " = nDigits > 1");
      }
      if (positive) {
        final long d = digits[0][0] & digitLimit.mask;
        if (d > Integer.MAX_VALUE)
          throw new IllegalArgumentException(d + " = d > Integer.MAX_VALUE");
        return (int)d;
      } else {
        final long d = -(digits[0][0] & digitLimit.mask);
        if (d < Integer.MIN_VALUE)
          throw new IllegalArgumentException(d + " = d < Integer.MIN_VALUE");
        return (int)d;
      }
    }

    public BigInteger toBigInteger() {
      if (isZero())
        return BigInteger.ZERO;

      final long size = ((numberOfBits_long() - 1) >> 3) + 1;
      if (size > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("size = " + size + " > Integer.MAX_VALUE = "
            + Integer.MAX_VALUE);
      }
      final byte[] bytes = new byte[(int)size];
      int k = bytes.length;
      for (int i = 0; i < digits.length && digits[i] != null; i++)
        for (int n : digits[i])
          for (int j = 0; k > 0 && j < bitsPerDigit.value; j += Byte.SIZE)
            bytes[--k] = (byte)(n >>> j);
      //Print.println("bytes.length = " + bytes.length + ", bytes = " + Parse.byteArray2string(bytes));
      return new BigInteger(positive ? 1: -1, bytes);
    }

    @Override
    public void print(final int digitsPerLine) {
      if (isZero())
        Print.println("0");

      Print.print((positive? "+" : "-") + "[nDigits=" + nDigits + ":\n ");
      printDigits(0, nDigits, false, true, digitsPerLine);      
      Print.println("]");
    }

    public void print(final int startdigit, final int enddigit, final int digitsPerLine) {
      if (isZero())
        Print.println("0");

      Print.print("digits[" + startdigit + ", " + enddigit + ":\n ");
      printDigits(startdigit, enddigit, false, true, digitsPerLine);      
      Print.println("]");
    }

    void printDigits(int startdigit, final int enddigit,
        final boolean leadingZeros, final boolean trailingZeros, final int digitsPerLine) {
      if (!leadingZeros && enddigit > nDigits) {
        throw new IllegalArgumentException(enddigit + "=enddigit>nDigit="
            + nDigits + " && trailingZeros=" + trailingZeros);
      } else if (!trailingZeros && startdigit > 0) {
        throw new IllegalArgumentException(startdigit
            + "=startdigit>0 && trailingZeros=" + trailingZeros);
      }

      int i = enddigit - 1;
      int k = 0;
      //print leading zeros
      for (; i >= nDigits; i--) {
        Print.print(" ");
        Print.print(String.format(hexFormat, 0));
        if (++k == digitsPerLine) {
          k = 0;
          Print.print("\n ");
        }
      }
      
      if (!trailingZeros) {
        //find the least non-zero digit
        int q = 0;
        int r = 0;
        for(boolean found = false; !found && startdigit <= i; ) {
          if (digits[q][r] != 0)
            found = true;
          else {
            startdigit++;
            if (++r == digitsPerArray.value) {
              r = 0;
              q++;
            }            
          }
        }        
      }

      int q = i >> digitsPerArray.exponent;
      int r = i & digitsPerArray.mask;
      for (; i >= startdigit && q >= 0; q--) {
        for (; i >= startdigit && r >= 0; r--) {
          Print.print(" ");
          Print.print(String.format(hexFormat, digits[q][r]));
          i--;
          if (++k == digitsPerLine) {
            k = 0;
            Print.print("\n ");
          }
        }
        r = digitsPerArray.value - 1;
      }
      if (k > 0)
        Print.println();
    }

    /** For {@link org.apache.hadoop.mp.gmp.GmpMultiplier} */
    @Override
    public PrintStream serialize(final PrintStream out) {
      return printDigits(out, 0, nDigits, false, true);      
    }

    PrintStream printDigits(final PrintStream out, int startdigit, final int enddigit,
        final boolean leadingZeros, final boolean trailingZeros) {
      if (!leadingZeros && enddigit > nDigits) {
        throw new IllegalArgumentException(enddigit + "=enddigit>nDigit="
            + nDigits + " && trailingZeros=" + trailingZeros);
      } else if (!trailingZeros && startdigit > 0) {
        throw new IllegalArgumentException(startdigit
            + "=startdigit>0 && trailingZeros=" + trailingZeros);
      }

      int i = enddigit - 1;
      //print leading zeros
      for (; i >= nDigits; i--) {
        final String s = String.format(hexFormat, 0);
        out.print(s);
//        Print.println("L = " + s);
      }
      
      if (!trailingZeros) {
        //find the least non-zero digit
        int q = 0;
        int r = 0;
        for(boolean found = false; !found && startdigit <= i; ) {
          if (digits[q][r] != 0)
            found = true;
          else {
            startdigit++;
            if (++r == digitsPerArray.value) {
              r = 0;
              q++;
            }            
          }
        }        
      }

      int q = i >> digitsPerArray.exponent;
      int r = i & digitsPerArray.mask;
      for (; i >= startdigit && q >= 0; q--) {
        for (; i >= startdigit && r >= 0; r--) {
          final String s = String.format(hexFormat, digits[q][r]);
          out.print(s);
//          Print.println("s = " + s);
          i--;
        }
        r = digitsPerArray.value - 1;
      }
      return out;
    }

    @Override
    public String toString() {
      final String s = toOneLineDetails();
      checkDigits();
      return s;
    }

    public String toOneLineDetails() {
      if (isZero())
        return "0";

      final StringBuilder b = new StringBuilder(positive ? "+" : "-");
      int r = (nDigits - 1) & digitsPerArray.mask;
      for (int q = (nDigits - 1) >> digitsPerArray.exponent; q >= 0; q--) {
        for (; r >= 0; r--)
          b.append(" ").append(String.format(hexFormat, digits[q][r]));
        if (q > 0)
          b.append(",");
        r = digitsPerArray.value - 1;
      }
      b.append("(").append(nDigits).append(
          nDigits <= 1 ? " digit, " : " digits, ");
      final long bits = numberOfBits_long();
      b.append(bits).append(bits <= 1L? " bit)" : " bits)");
      return b.toString();
    }

    @Override
    public String toBrief() {
      if (isZero())
        return "0";

      final StringBuilder b = new StringBuilder(positive ? "+" : "-");
      
      int i = nDigits - 1;
      for (int count = 0; count < 5 && i >= 0; count++, i--) {
        final int q = i >> digitsPerArray.exponent;
        final int r = i & digitsPerArray.mask;
        b.append(" ").append(String.format(hexFormat, digits[q][r]));
        if (r == 0 && q > 0) {
          b.append(",");
        }
      }
      if (i >= 5) {
        b.append(" ... ");
        i = 4;
      }
      for (int count = 0; count < 5 && i >= 0; count++, i--) {
        final int q = i >> digitsPerArray.exponent;
        final int r = i & digitsPerArray.mask;
        b.append(" ").append(String.format(hexFormat, digits[q][r]));
        if (r == 0 && q > 0) {
          b.append(",");
        }
      }
      b.append(" (").append(Parse.long2string(nDigits)).append(
          nDigits <= 1 ? " digit, " : " digits, ");
      final long bits = numberOfBits_long();
      b.append(Parse.long2string(bits)).append(bits <= 1L? " bit)" : " bits)");
      return b.toString();
    }

    @Override
    public void printDetail(final String firstlineprefix) {
      if (firstlineprefix != null)
        Print.print(firstlineprefix + ": ");
      Print.println("positive=" + positive
          + ", nDigit=" + nDigits
          + ", numberOfBits()=" + numberOfBits_long()
          + ", digits = [#=" + digits.length + ":");
      int j = digits.length - 1;
      for(; j >= 0 && digits[j] == null; j--);
      if (j < digits.length - 1)
        Print.println("null: " + (digits.length-1) + " - " + (j+1));

      for(; j >= 0; j--) {
        Print.print(j + ": ");
        final int[] d = digits[j];
        for (int i = d.length - 1; i >= 0; i--) {
          Print.print(String.format(hexFormat, d[i]));
          Print.print(" ");
          if (i % 50 == 0) {
            Print.println();
            Print.print("   ");
          }
        }
        Print.println();
      }
      Print.println("]");
    }

    private void checkNonNegative() {
      if (isNegative()) {
        printDetail("ERROR");
        throw new ArithmeticException("isNegative()");
      }
    }

    private void checkDigits() {
      if (nDigits == 0) {
        if (digits.length != 0 && digits[0] != null) {
          printDetail("ERROR");
          throw new RuntimeException("nDigit == 0 but digits[0] != null");
        }
      } else {
        //check leading digits
        int i = nDigits - 1;
        final int q = i >> digitsPerArray.exponent;
        int[] d = digits[q];
        int r = i & digitsPerArray.mask;
        if (d[r] == 0) {
          printDetail("ERROR");
          throw new RuntimeException("Leading digit = "
              + d[i & digitsPerArray.mask]);
        }
        
        int j = digits.length - 1;
        for(; j >= 0 && digits[j] == null; j--);
        if (j != q) {
          printDetail("ERROR");
          throw new RuntimeException(j + " = j != q = " + q);
        }
        for(r++; r < d.length; r++)
          if (d[r] != 0) {
            printDetail("ERROR");
            throw new RuntimeException("d[" + r + "] != 0");
          }
        
        //check digit range
        for(; i >= 0; i--) {
          r = i & digitsPerArray.mask;
          if ((bitsPerDigit.value < Integer.SIZE && d[r] < 0) || d[r] >= digitLimit.value) {
            printDetail("ERROR");
            throw new RuntimeException("d[r] < 0 || d[r] >= digitLimit.value, d["
                + r + "] = " + d[r]);
          }
          if(--r < 0) {
            r = digitsPerArray.mask;
            d = digits[i >> digitsPerArray.exponent];
          }
          
        }
      }
    }

    private void trimLeadingZeros() {
//      if (PRINT_LEVEL.is(Print.Level.TRACE))
//        Print.beginIndentation("trimLeadingZeros(), this=" + toOneLineDetails());

      clearArrays(((nDigits - 1) >> digitsPerArray.exponent) + 1);

      //re-compute nDigits
      for(; nDigits != 0; ) {
        final int q = (nDigits - 1) >> digitsPerArray.exponent;
        final int[] d = digits[q];
        if (d == null) {
          nDigits = q << digitsPerArray.exponent;
        } else {
          for (int i = (nDigits - 1) & digitsPerArray.mask; nDigits > 0 && i >= 0; i--) {
            if (d[i] != 0) {
//              if (PRINT_LEVEL.is(Print.Level.TRACE))
//                Print.endIndentation("trimLeadingZeros 1returns " + this);
              return;
            }
            nDigits--;
          }
          digits[q] = null;
        }
      }

//      if (PRINT_LEVEL.is(Print.Level.TRACE))
//        Print.endIndentation("trimLeadingZeros 2returns " + this);
    }

    public Element ssMultiplyEqual(RootOfUnity z, final int e) {
      if (isZero())
        return this;

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("ssMultiplyEqual(z=" + z + ", e=" + e + "), this=" + this);
      checkNonNegative();

      bitsPerDigit.checkIsMultiple(e);

      if (z.shift != 0) {
        shiftLeftEqual_bits(z.shift);
        ssModEqual(e);
      }
      if (z.negation) {
        negateEqual();
        ssModEqualByAddition(e);
      }

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("ssMultiplyEqual returns " + this);
      checkNonNegative();
      return  this;
    }

    /**
     * Plus equals Schonhage-Strassen modulus, i.e. this += 2^e + 1.
     */
    Element ssModEqualByAddition(final int e) {
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("ssPlusEqual(e=" + e + "), this=" + toOneLineDetails());
  
      if (isNegative()) {
        if (debug)
          DEBUG_STACK.get().push(this.toBigInteger());
  
        plusEqual_differentSign(1, e >> bitsPerDigit.exponent);
        //Printer.println(this.toDetails());
        if (positive)
          plusEqual_sameSign(1, 0);
        else
          plusEqual_differentSign(1, 0);
  
        if (debug) {
          final BigInteger a = DEBUG_STACK.get().pop();
          final BigInteger c = this.toBigInteger();
          final BigInteger ssModular = BigInteger.ONE.shiftLeft(e).add(BigInteger.ONE);
          final BigInteger expected = a.compareTo(BigInteger.ZERO) < 0?
              a.add(ssModular): a;
          if (!c.equals(expected))
            throw new RuntimeException(expected.toString(16) + " != c"
                + "\nssModular = " + ssModular.toString(16)
                + "\na = " + a.toString(16)
                + "\nc = " + c.toString(16) + " = " + this);
        }
      }
  
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("ssPlusEqual returns " + this);
      checkNonNegative();
      return this;
    }

    /**
     * Mod Schonhage-Strassen modulus given 0 <= this < 2M, where M = 2^e + 1)
     */
    Element ssModEqualBySubstraction(int e) {
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("ssModEqualBySubstraction(" + e + "), this=" + this);
      checkNonNegative();
      bitsPerDigit.checkIsMultiple(e);

      final int startDigit = e >> bitsPerDigit.exponent;
      if (startDigit >= nDigits) {
        if (PRINT_LEVEL.is(Print.Level.DEBUG))
          Print.println("trivial: " + startDigit + " = startDigit > nDigit = " + nDigits);
      } else {
        if (startDigit != nDigits - 1)
          throw new IllegalArgumentException(startDigit
              + " = startDigit != nDigit - 1, nDigit = " + nDigits);
        digits[startDigit >> digitsPerArray.exponent][startDigit & digitsPerArray.mask]--;
        trimLeadingZeros();
        if (!isZero()) {
          plusEqual(-1);
        } else {
          plusEqual(1).shiftLeftEqual_digits(startDigit);
        }
      }

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("ssModEqualBySubstraction returns " + this);
      checkNonNegative();
      return this;
    }

    /** Mod Schonhage-Strassen modulus, i.e. mod (2^e + 1) */
    Element ssModEqual(int e) {
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("ssModEqual(" + e + "), this=" + this);
      checkNonNegative();

      if (debug)
        DEBUG_STACK.get().push(this.toBigInteger());

      final int startDigit = e >> bitsPerDigit.exponent;
      if (startDigit > nDigits) {
        if (PRINT_LEVEL.is(Print.Level.DEBUG))
          Print.println("trivial: " + startDigit + " = startDigit > nDigit = " + nDigits);
      } else {
        final Element higher = extractHigherDigits(startDigit);
        higher.negateEqual();
        clearHigherDigits(startDigit);
        plusEqual(higher);
        ssModEqualByAddition(e);
      }

      if (debug) {
        final BigInteger a = DEBUG_STACK.get().pop();
        final BigInteger c = this.toBigInteger();
        final BigInteger ssModular = BigInteger.ONE.shiftLeft(e).add(BigInteger.ONE);
        final BigInteger expected = a.mod(ssModular);
        if (!c.equals(expected))
          throw new RuntimeException(expected.toString(16) + " != c"
              + "\nssModular = " + ssModular.toString(16)
              + "\na = " + a.toString(16)
              + "\nc = " + c.toString(16) + " = " + this);
      }

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("ssModEqual returns " + this);
      checkNonNegative();
      return this;
    }

    private void clearHigherDigits(final int startDigit) {
      if (startDigit < nDigits) {
        if (PRINT_LEVEL.is(Print.Level.DEBUG))
          Print.beginIndentation("clearHigherDigits(startDigit=" + startDigit + "), this=" + this);
        
        nDigits = startDigit;
        final int offset = nDigits & digitsPerArray.mask;
        final int i = nDigits >> digitsPerArray.exponent;
        Arrays.fill(digits[i], offset, digitsPerArray.value, 0);
        clearArrays(i + 1);
        trimLeadingZeros();

        if (PRINT_LEVEL.is(Print.Level.DEBUG))
          Print.endIndentation("clearHigherDigits returns " + this);
      }
    }

    /**
     * e        = exponent_in_digits * bitsPerDigit
     * quotient = this / 2^e,
     * this     = this % 2^e,
     * @return quotient 
     */
    public Element divideRemainderEqual(final int exponent_in_digits) {
      if (exponent_in_digits >= nDigits)
        return newElement();
      else {
        if (PRINT_LEVEL.is(Print.Level.DEBUG))
          Print.beginIndentation("divideRemainderEqual("
              + exponent_in_digits + "), this=" + this);

        final Element quotient = newElement();
        quotient.positive = this.positive;
        quotient.nDigits = this.nDigits - exponent_in_digits;

        for(int i = 0; i < quotient.nDigits; i++) {
          final int j = exponent_in_digits + i;
          final int[] d = this.digits[j >> digitsPerArray.exponent];
          final int r = j & digitsPerArray.mask;
          
          final int q = i >> digitsPerArray.exponent;
          if (quotient.digits[q] == null)
            quotient.digits[q] = arrayrecycler.newObject();
          quotient.digits[q][i & digitsPerArray.mask] = d[r];
          d[r] = 0;
        }
        this.trimLeadingZeros();

        if (PRINT_LEVEL.is(Print.Level.DEBUG))
          Print.endIndentation("divideRemainderEqual returns leading="
              + quotient + ", this=" + this);
        return quotient;
      }
    }

    public int difference(final int exponent_in_digits, final int m) {
      //Print.println("difference(exponent_in_digits=" + exponent_in_digits + ", m=" + m + "), this=" + this.toBrief());
      if (m < 2 || m > digitLimit.value) {
        throw new IllegalArgumentException(
            "m < 2 || m > digitLimit.value = " + digitLimit + ", m=");
      } else if (exponent_in_digits <= 0) {
        throw new IllegalArgumentException(
            exponent_in_digits + " = exponent_in_digits <= 0");
      } else if (nDigits > exponent_in_digits) {
        throw new IllegalArgumentException(
            nDigits + " = nDigits > exponent_in_digits = " + exponent_in_digits);
      } else if (nDigits < exponent_in_digits) {
        return m;
      } else {
        int i = nDigits - 1, r = -1;
        for(; i > 0 && r < 0; ) {
          //Print.println("difference: i=" + i + ", r=" + r);
          final int[] d = digits[i >> digitsPerArray.exponent];
          for(r = i & digitsPerArray.mask;
              i > 0 && r >= 0 && d[r] == digitLimit_mask;
              i--, r--);
        }
        if (i > 0)
          return m;
        else {
          final long d = digitLimit.value - (digits[0][0] & digitLimit.mask);
          return  d < m? (int)d: m;
        }
      }
    }

    private Element extractHigherDigits(final int startDigit) {
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("extractHigherDigits(startDigit=" + startDigit + "), this=" + this);

      final Element e = newElement();
      if (startDigit < this.nDigits) {
        //Printer.println("this = " + toDetails());

        e.positive = this.positive;
        e.nDigits = this.nDigits - startDigit;
  
        final int left = startDigit & digitsPerArray.mask;
        final int right = digitsPerArray.value - left;
  
        final int nArray = e.nDigits >> digitsPerArray.exponent;
        int srci = startDigit >> digitsPerArray.exponent;
        int q = 0;
        for (int i = 0; i < nArray; i++) {
          final int[] d = arrayrecycler.newObject();
          e.digits[q++] = d;
          System.arraycopy(digits[srci], left, d, 0, right);
          srci++;
          if (left > 0)
            System.arraycopy(digits[srci], 0, d, right, left);
        }
  
        //Printer.println("1) e = " + e.toDetails());
        int remaining = e.nDigits & digitsPerArray.mask;
        //Printer.println("remaining = " + remaining);
        if (remaining > 0) {
          final int[] d = arrayrecycler.newObject();
          e.digits[q++] = d;

          final int len = remaining < right ? remaining : right;
          System.arraycopy(digits[srci], left, d, 0, len);
          remaining -= len;
          if (remaining > 0) {
            srci++;
            System.arraycopy(digits[srci], 0, d, right, remaining);
          }
        }
        //Printer.println("2) e = " + e.toDetails());
      }
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("extractHigherDigits returns " + e);
      return e;
    }

    Element shiftRightEqual_digits(final int shift_in_digits) {
      if (shift_in_digits == 0)
        return this;
      else if (shift_in_digits < 0)
        throw new IllegalArgumentException("shift_in_digits = " + shift_in_digits + " < 0");

      if (PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.beginIndentation("shiftRightEqual_digits(shift_in_digits=" + shift_in_digits + ")");
        Print.println("this = " + this);
      }

      if (shift_in_digits >= nDigits)
        setZero();
      else {
        this.nDigits -= shift_in_digits;
        
        int shift_q = shift_in_digits >> digitsPerArray.exponent; 
        int shift_r = shift_in_digits & digitsPerArray.mask; 
        if (shift_r == 0) {
          //shift arrays
          final int nArray = ((this.nDigits - 1) >> digitsPerArray.exponent) + 1;
          int q = 0;
          for(; q < nArray; ) {
            this.digits[q++] = this.digits[shift_q];
            this.digits[shift_q++] = null;
          }
          for(; this.digits[q] != null; ) {
            this.digits[q++] = null;            
          }
        } else {
          //shift digits
          int q = 0;
          int r = 0;
          for(int i = 0; i < this.nDigits; i++) {
            this.digits[q][r] = this.digits[shift_q][shift_r];
            
            if (++r == digitsPerArray.value) {
              r = 0;
              q++;
            }
            if (++shift_r == digitsPerArray.value) {
              shift_r = 0;
              shift_q++;
            }
          }

          //set leading zeros
          if (r == 0) {
            clearArrays(q);
          } else {
            clearArrays(q + 1);
            Arrays.fill(digits[q], r, digitsPerArray.value, 0);
          }
        }
      }
      if (PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.endIndentation("shiftRightEqual_digits return this=" + this);
      }
      return this;
    }

    @Override
    public Element shiftRightEqual_bits(final long shift_in_bits) {
      final int shift_in_digits = JavaUtil.toInt(shift_in_bits >> bitsPerDigit.exponent);
      if (shift_in_bits < 0)
        throw new IllegalArgumentException("shift_in_bits = " + shift_in_bits + " < 0");
      else if (shift_in_bits == 0 || this.isZero())
        return this;
      else if ((shift_in_bits & bitsPerDigit.mask) == 0)
        return this.shiftRightEqual_digits(shift_in_digits);
      
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("shiftRightEqual_bits(" + shift_in_bits + "), this=" + this);

      final long oldNOB = numberOfBits_long();
      final long newNOB = oldNOB - shift_in_bits;
      if (newNOB <= 0) {
        return this.setZero();
      }

      //(0 < shift_in_bits < oldNOB) and (shift_in_bits % bitsPerDigit != 0)
      final int right_shift = (int)shift_in_bits & bitsPerDigit.mask;
      final int left_shift = bitsPerDigit.value - right_shift;
      int i = shift_in_digits;
      int i_q = i >> digitsPerArray.exponent;
      int i_r = i & digitsPerArray.mask;
      int q = 0;
      int r = 0;
      digits[q][r] = digits[i_q][i_r] >>> right_shift;
      
      for(i++; i < nDigits; i++) {
        if (++i_r == digitsPerArray.value) {i_r = 0;   i_q++;}
        digits[q][r] |= (digits[i_q][i_r] << left_shift) & digitLimit_mask;

        if (++r == digitsPerArray.value) {r = 0;   q++;}
        digits[q][r] = digits[i_q][i_r] >>> right_shift;
      }
      if (++r < digitsPerArray.value) {
        Arrays.fill(digits[q], r, digitsPerArray.value, 0);
      } 

      nDigits = JavaUtil.toInt(((newNOB - 1) >> bitsPerDigit.exponent) + 1);
      clearArrays(((nDigits - 1) >> digitsPerArray.exponent) + 1);

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("shiftRightEqual_bits returns " + this);
      return this;      
    }

    Element shiftLeftEqual_digits(final int shift_in_digits) {
      //TODO: Optimize this method; see shiftRightEqual_xxx(..)
      return shiftLeftEqual_bits(shift_in_digits << bitsPerDigit.exponent);
    }

    /** Shift this to left. */
    @Override
    public Element shiftLeftEqual_bits(final long shift_in_bits) {
      if (shift_in_bits == 0 || this.isZero())
        return this;

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("shiftLeftEqual_bits(" + shift_in_bits + "), this=" + this);

      if (debug)
        DEBUG_STACK.get().push(this.toBigInteger());
      
      final long oldNOB = numberOfBits_long();
      final long newNOB = oldNOB + shift_in_bits;
      final int oldNDigit = nDigits;
      nDigits = JavaUtil.toInt(((newNOB - 1) >> bitsPerDigit.exponent) + 1);
      final int nArray = ((nDigits - 1) >> digitsPerArray.exponent) + 1;
      for (int i = ((oldNDigit - 1) >> digitsPerArray.exponent) + 1; i < nArray; i++)
        digits[i] = arrayrecycler.newObject();

      final int digitdiff = JavaUtil.toInt(shift_in_bits >> bitsPerDigit.exponent);
      //Printer.println("oldNOB=" + oldNOB + ", newNOB=" + newNOB
      //    + ", oldNDigit=" + oldNDigit + ", nDigit=" + nDigit + ", digitdiff=" + digitdiff);
      if (digitdiff > 0) {
        {
          int i = oldNDigit + digitdiff;
          int j = oldNDigit;
          for (int k = 0; k < oldNDigit; k++) {
            --i;
            --j;
            // TODO: need optimization 
            digits[i >> digitsPerArray.exponent][i & digitsPerArray.mask]
                = digits[j >> digitsPerArray.exponent][j & digitsPerArray.mask];
          }
        }

        for (int k = 0; k < digitdiff; k++)
          digits[k >> digitsPerArray.exponent][k & digitsPerArray.mask] = 0;
      }

      final int bitOffset = (int)shift_in_bits & bitsPerDigit.mask;
      final int reverseBitOffset = bitsPerDigit.value - bitOffset;
      if (bitOffset > 0) {
        int i = nDigits - 1;
        int row = i >> digitsPerArray.exponent;
        int col = i & digitsPerArray.mask;
        for (; i > digitdiff;) {
          digits[row][col] <<= bitOffset;
          digits[row][col] &= digitLimit_mask;
          i--;
          final int prevRow = row;
          final int prevCol = col;
          row = i >> digitsPerArray.exponent;
          col = i & digitsPerArray.mask;
          digits[prevRow][prevCol] |= digits[row][col] >>> reverseBitOffset;
        }
        digits[row][col] <<= bitOffset;
        digits[row][col] &= digitLimit_mask;
      }

      if (debug) {
        final BigInteger a = DEBUG_STACK.get().pop();
        final BigInteger c = this.toBigInteger();
        final BigInteger expected = a.shiftLeft(JavaUtil.toInt(shift_in_bits));
        if (!c.equals(expected))
          throw new RuntimeException("a << " + shift_in_bits + " != " + expected.toString(16)
              + "\na = " + a.toString(16)
              + "\nc = " + c.toString(16) + " = " + this);
      }

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("shiftLeftEqual_bits returns " + this);
      return this;
    }

    public Element plusEqual(final int singleDigit) {
      if (singleDigit >= digitLimit.value || -singleDigit >= digitLimit.value)
        throw new IllegalArgumentException(
            "singleDigit >= digitLimit.value || singleDigit <= digitLimit.value"
                + "\n  singleDigit      = " + singleDigit
                + "\n  digitLimit.value = " + digitLimit.value);
      if (singleDigit == 0)
        return this;
      else {
        //final BigInteger a = this.toBigInteger();
  
        if (singleDigit > 0) {
          if (positive)
            plusEqual_sameSign(singleDigit, 0);
          else
            plusEqual_differentSign(singleDigit, 0);
        }
        else {
          if (positive)
            plusEqual_differentSign(-singleDigit, 0);
          else
            plusEqual_sameSign(-singleDigit, 0);
        }
        /*
        final BigInteger c = this.toBigInteger();
        if (!c.equals(a.add(BigInteger.valueOf(singleDigit))))
          throw new RuntimeException("a + b != c"
              + "\na = " + a
              + "\nb = " + singleDigit
              + "\nc = " + c + " = " + this);
              */
        return this;
      }
    }

    /**
     * @param n n >= 0.
     */
    Element plusEqual_sameSign(final int n, final int startDigit) {
      if (PRINT_LEVEL.is(Print.Level.TRACE))
        Print.beginIndentation("plusEqual_sameSign(n=" + n + ", startDigit=" + startDigit + "), this=" + toOneLineDetails());
      if (n == 0) {
        if (PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("plusEqual_sameSign 1trivial");
        return this;
      }
      else if (startDigit >= nDigits) {
        final int q = startDigit >> digitsPerArray.exponent;
        for (int i = ((nDigits - 1) >> digitsPerArray.exponent) + 1; i <= q; i++)
          digits[i] = arrayrecycler.newObject();
        digits[q][startDigit & digitsPerArray.mask] = n;
        nDigits = startDigit + 1;
        if (PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("plusEqual_sameSign 2returns " + this);
        return this;
      } else {
        int[] d = digits[startDigit >> digitsPerArray.exponent];
        long carry = n;
        for(int i = startDigit; i < nDigits; i++) {
          final int r = i & digitsPerArray.mask;
          if (r == 0)
            d = digits[i >> digitsPerArray.exponent];

          carry += (d[r] & digitLimit.mask);
          d[r] = (int)carry & digitLimit_mask;
          carry >>>= digitLimit.exponent;

          if (carry == 0) {
            if (PRINT_LEVEL.is(Print.Level.TRACE))
              Print.endIndentation("plusEqual_sameSign 3returns " + this);
            return this;
          }
        }

        //carry > 0
        final int r = nDigits & digitsPerArray.mask;
        if (r == 0) {
          final int q = nDigits >> digitsPerArray.exponent;
          if (digits[q] == null)
            digits[q] = d = arrayrecycler.newObject();
          else
            d = digits[q];
        }
        d[r] = (int)carry;
        nDigits++;
        if (PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("plusEqual_sameSign 4returns " + this);
        return this;
      }
    }

    private Element plusEqual_differentSign(int n, final int startDigit) {
      if (isZero()) {
        positive = !positive;
        return plusEqual_sameSign(n, startDigit);
      } else {
        if (PRINT_LEVEL.is(Print.Level.TRACE))
          Print.beginIndentation("plusEqual_differentSign(n=" + n + ", startDigit=" + startDigit + "), this=" + toOneLineDetails());

        final int q = startDigit >> digitsPerArray.exponent;
        for (int i = ((nDigits - 1) >> digitsPerArray.exponent) + 1; i <= q; i++)
          digits[i] = arrayrecycler.newObject();

        final boolean changeSign = startDigit == nDigits - 1?
            (digits[q][startDigit & digitsPerArray.mask] & digitLimit.mask) < (n & digitLimit.mask)
            : startDigit >= nDigits;
        if (changeSign) {
          positive = !positive;
          if (startDigit > 0) {
            int[] d = digits[0];
            int i = 0;
            for(boolean iszero = d[0] == 0; iszero && i < startDigit; ) {
              i++;
              final int r = i & digitsPerArray.mask;
              if (r == 0)
                d = digits[i >> digitsPerArray.exponent];
              iszero = d[r] == 0;
            }

            if (i < startDigit) {
              n--;
              final int r = i & digitsPerArray.mask;
              if (r == 0)
                d = digits[i >> digitsPerArray.exponent];
              d[r] = (int)(digitLimit.value - (d[r] & digitLimit.mask));
              
              i++;
              if (nDigits < i)
                nDigits = i;
            }
            for(; i < startDigit; ) {
              final int r = i & digitsPerArray.mask;
              if (r == 0)
                d = digits[i >> digitsPerArray.exponent];
              d[r] = (d[r] ^ digitLimit_mask);
              
              i++;
              if (nDigits < i)
                nDigits = i;
            }
          }
        }

        if (n == 0) {
          trimLeadingZeros();
          if (PRINT_LEVEL.is(Print.Level.TRACE))
            Print.endIndentation("plusEqual_differentSign 1returns " + this);
          return this;
        } else {
          //n > 0
          int[] d = digits[q];
          if (startDigit >= nDigits) {
            d[startDigit & digitsPerArray.mask] = n;
            nDigits = startDigit + 1;
            if (PRINT_LEVEL.is(Print.Level.TRACE))
              Print.endIndentation("plusEqual_differentSign 2returns " + this);
            return this;
          } else if (startDigit == nDigits - 1) {
            final int r = startDigit & digitsPerArray.mask;
            final long diff = (d[r] & digitLimit.mask) - n;
            if (diff == 0) {
              d[r] = 0;
              trimLeadingZeros();
            } else if (diff < 0) { 
              d[r] = (int)(-diff);
            } else {
              d[r] = (int)diff;
            }
            if (PRINT_LEVEL.is(Print.Level.TRACE))
              Print.endIndentation("plusEqual_differentSign 3returns " + this);
            return this;
          } else {
            for(int i = startDigit; i < nDigits; i++) {
              final int r = i & digitsPerArray.mask;
              if (r == 0)
                d = digits[i >> digitsPerArray.exponent];

              final long diff = (d[r] & digitLimit.mask) - n;
              if (diff >= 0) {
                d[r] = (int)diff;
                if (i == nDigits - 1 && diff == 0) {
                  trimLeadingZeros();
                }
                if (PRINT_LEVEL.is(Print.Level.TRACE))
                  Print.endIndentation("plusEqual_differentSign 4returns " + this);
                return this;
              } else {
                d[r] = (int)(diff + digitLimit.value);
                n = 1;
              }
            }
            throw new RuntimeException("Something wrong: n=" + n + ", this=" + this);
          }
        }
      }
    }

    @Override
    public Element negateEqual() {
      positive = !positive;
      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.println("negationEqual: " + (positive? "+ => -": "- => +") + ", this=" + this);
      return this;
    }

    @Override
    public Element plusEqual(final Element that) {
      if (that.isZero())
        return this;
      if (this == that)
        return shiftLeftEqual_bits(1);
      this.check(that);

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.beginIndentation("plusEqual(that=" + that.toOneLineDetails() + "), this=" + toOneLineDetails());
      
      if (debug)
        DEBUG_STACK.get().push(this.toBigInteger());
      
      if (this.positive == that.positive)
        plusEqual_sameSign(that);
      else
        plusEqual_differentSign(that);

      if (debug) {
        final BigInteger a = DEBUG_STACK.get().pop();
        final BigInteger b = that.toBigInteger();
        final BigInteger c = this.toBigInteger();
        if (!c.equals(a.add(b)))
          throw new RuntimeException("a + b != c"
              + "\na = " + a
              + "\nb = " + b + " = " + that
              + "\nc = " + c + " = " + this);
      }

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("plusEqual returns " + this);
      return this;
    }

    private Element plusEqual_sameSign(final Element that) {
      this.check(that);

      if (PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.beginIndentation("plusEqual_sameSign(that=" + that + ")");
        Print.println("                 this=" + this);
      }

      final int thatArraySize = ((that.nDigits - 1) >> digitsPerArray.exponent) + 1;
      for (int i = ((this.nDigits - 1) >> digitsPerArray.exponent) + 1; i < thatArraySize; i++)
        digits[i] = arrayrecycler.newObject();
      this.nDigits = Math.max(this.nDigits, that.nDigits);

      int that_pos = 0;
      long carry = 0;
      for (int j = 0; j < thatArraySize; j++) {
        final int[] d = this.digits[j];
        final int[] that_d = that.digits[j];
        for(int i = 0; i < digitsPerArray.value; i++) {
          carry += (d[i] & digitLimit.mask);
          if (that_pos < that.nDigits) {
            that_pos++;
            carry += (that_d[i] & digitLimit.mask);
          }
          d[i] = (int)carry & digitLimit_mask;
          carry >>>= digitLimit.exponent;

          if (carry == 0 && that_pos >= that.nDigits) {
            nDigits = Math.max(nDigits, (j << digitsPerArray.exponent) + i + 1);

            if (PRINT_LEVEL.is(Print.Level.TRACE))
              Print.endIndentation("plusEqual_sameSign returns " + this);
            return this;
          }
        }
      }

      if (carry <= 0)
        throw new RuntimeException(carry + " = carry <= 0");

      plusEqual_sameSign((int)carry, thatArraySize << digitsPerArray.exponent);

      if (PRINT_LEVEL.is(Print.Level.TRACE))
        Print.endIndentation("plusEqual_sameSign returns " + this);
      return this;
    }

    private Element plusEqual_differentSign(final Element that) {
      this.check(that);
      
      if (PRINT_LEVEL.is(Print.Level.TRACE))
        Print.beginIndentation("plusEqual_differentSign(that=" + that.toOneLineDetails()
            + "), this=" + toOneLineDetails());

      final int magitude = this.compareMagnitudeTo(that);
      if (magitude == 0) {
        this.positive = true;
        this.nDigits = 0;
        clearArrays(0);
      } else {
        final Element large;
        final Element small;
        if (magitude < 0) {
          this.positive = that.positive;
          large = that;
          small = this;          
        } else {
          large = this;
          small = that;          
        }
        
        int[] largeArray = null;
        int[] smallArray = null;
        int[] thisArray = null;
        long borrow = 0;
        int i = 0;
        for (; i < small.nDigits; i++) {
          final int r = i & digitsPerArray.mask;
          if (r == 0) {
            final int q = i >> digitsPerArray.exponent;
            largeArray = large.digits[q];
            smallArray = small.digits[q];
            thisArray = this.digits[q];
          }
          
          borrow += (largeArray[r] & digitLimit.mask);
          borrow -= (smallArray[r] & digitLimit.mask);
          if (borrow >= 0) {
            thisArray[r] = (int)borrow;
            borrow = 0;
          } else {
            thisArray[r] = (int)(borrow + digitLimit.value);
            borrow = -1;
          }
        }
        
        for(; borrow != 0 && i < large.nDigits; i++) {
          final int r = i & digitsPerArray.mask;
          if (r == 0) {
            final int q = i >> digitsPerArray.exponent;
            largeArray = large.digits[q];
            if (this.digits[q] != null)
              thisArray = this.digits[q];
            else
              this.digits[q] = thisArray = arrayrecycler.newObject();
          }

          borrow += (largeArray[r] & digitLimit.mask);
          if (borrow >= 0) {
            thisArray[r] = (int)borrow;
            borrow = 0;
          } else {
            thisArray[r] = (int)(borrow + digitLimit.value);
            borrow = -1;
          }
        }

        if (this != large) {
          for(; i < large.nDigits; i++) {
            final int r = i & digitsPerArray.mask;
            if (r == 0) {
              final int q = i >> digitsPerArray.exponent;
              largeArray = large.digits[q];
              this.digits[q] = thisArray = arrayrecycler.newObject();
            }
            thisArray[r] = largeArray[r];
          }
          
          this.nDigits = large.nDigits;
        }
        trimLeadingZeros();
      }

      if (PRINT_LEVEL.is(Print.Level.TRACE))
        Print.endIndentation("plusEqual_differentSign returns " + this);
      return this;
    }

    /** Multiplication using BigInteger, i.e. classical multiplication */
    void multiplyEqual_BigInteger(final Element that) {
      if (PRINT_LEVEL.is(Print.Level.DEBUG)) {
        Print.beginIndentation("multiplyEqual(that=" + that + ")");
        Print.println(         "            this=" + this);
      }

      final BigInteger thisBI = this.toBigInteger();
      final BigInteger thatBI = that.toBigInteger();
//      if (PRINT_LEVEL.is(Print.Level.DEBUG)) {
//        Print.println("thisBI = " + thisBI.toString(16));
//        Print.println("thatBI = " + thatBI.toString(16));
//      }
      this.set(thisBI.multiply(thatBI));

      if (PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("multiplyEqual returns " + this);
    }

//    public void multiplyEqual_Apfloat(final Element that) {
//      if (PRINT_LEVEL.is(Print.Level.DEBUG)) {
//        Print.beginIndentation("multiplyEqual_Apfloat(..)");
//        Print.println("this=" + this.toBrief());
//        Print.println("that=" + that.toBrief());
//      }
//      final Apint thisAPINT = new Apint(this.toBigInteger());
//      final Apint thatAPINT = new Apint(that.toBigInteger());
//      final Apint r = thisAPINT.multiply(thatAPINT);
//      set(r.toBigInteger());
//      if (PRINT_LEVEL.is(Print.Level.DEBUG)) {
//        Print.endIndentation("multiplyEqual_Apfloat returns " + this.toBrief());
//      }
//    }

    /**
     * Karatsuba's method.
     * 
     * Let a2*B^2 + a1*B + a0 = (x1*B + x0)(y1*B + y0).
     * 
     * Then, a2 = x1*y1,
     *       a0 = x0*y0,
     *       a1 = (x1 + x0)(y1 + y0) - a2 - a0.
     */
    public void multiplyEqual_Karatsuba(final Element that, final int max_nDigits,
        final JavaUtil.WorkGroup workers) {
//      Print.beginIndentation("multiplyEqual_Karatsuba: max_nDigits=" + max_nDigits);
//      Print.println("this = " + this);
//      Print.println("that = " + that);

      final int digitsPerElement = (max_nDigits + 1) >> 1;
      final int bitsPerElement = digitsPerElement << bitsPerDigit.exponent;
//      Print.println("digitsPerElement=" + digitsPerElement + ", bitsPerElement=" + bitsPerElement);
      final Element[] x = this.split(bitsPerElement, 2);
//      Print.print("x", x);
      final Element[] y = that.split(bitsPerElement, 2);
//      Print.print("y", y);

      //a2 = x1*y1
      this.set(x[1]).multiplyEqual(y[1], workers);
      //y1 + y0
      y[1].plusEqual(y[0]);
      //(x1 + x0)(y1 + y0)
      x[1].plusEqual(x[0]).multiplyEqual(y[1], workers);
      //a0 = x0*y0
      x[0].multiplyEqual(y[0], workers);
      //a1 = (x1 + x0)(y1 + y0) - a2 - a0
      x[1].plusEqual(this.negateEqual()).plusEqual(x[0].negateEqual());
      
      //a2*B^2
      this.negateEqual().shiftLeftEqual_digits(digitsPerElement << 1);
      //a2*B^2 + a1*B
      this.plusEqual(x[1].shiftLeftEqual_digits(digitsPerElement));
      //a2*B^2 + a1*B + a0
      this.plusEqual(x[0].negateEqual());

      x[0].reclaim();
      x[1].reclaim();
      y[0].reclaim();
      y[1].reclaim();
      
//      Print.endIndentation("return this = " + this);
    }
    
    public void multiplyEqual_SchonhageStrassen(final Element that, final int max_nDigits,
        final JavaUtil.WorkGroup workers) {
      if (SchonhageStrassen.PRINT_LEVEL.is(Print.Level.DEBUG)) {
        Print.beginIndentation("multiplyEqual_SchonhageStrassen: ");
        Print.println("this = " + this.toBrief());
        Print.println("that = " + that.toBrief());
      }

      final SchonhageStrassen ss = SchonhageStrassen.FACTORY.valueOf(
          max_nDigits, Zahlen.this);

      ss.multiplyEquals(this, that, ss.parallel, workers);

      if (SchonhageStrassen.PRINT_LEVEL.is(Print.Level.DEBUG))
        Print.endIndentation("return " + this.toBrief());
    }

    @Override
    public Element multiplyEqual(final Element that, final JavaUtil.WorkGroup workers) {
      if (this.isZero()) {
        return this;
      } else if (that.isZero()) {
        return this.setZero();
      }

      final boolean thispositive = !(this.positive ^ that.positive);
      final boolean thatpositive = that.positive;
      this.positive = that.positive = true;

      this.set(GmpMultiplier.get().multiply(this, that));
//      final int max = this.nDigits > that.nDigits? this.nDigits: that.nDigits;
//      if (max <= BIG_INTEGER_THRESHOLD) {
//        multiplyEqual_BigInteger(that);
//      } else if (max <= KARATSUBA_THRESHOLD) {
//        multiplyEqual_Karatsuba(that, max, workers);
//      } else {
////        final Element expected = newElement().set(this);
////        final Element this_copy = newElement().set(this);
////        final Element that_copy = newElement().set(that);
//
//        multiplyEqual_SchonhageStrassen(that, max, workers);
//
////        expected.multiplyEqual_BigInteger(that);
////        if (!this.equals(expected)) {
////          this_copy.printDetail("this_copy");
////          that_copy.printDetail("that_copy");
////          expected.printDetail("expected");
////          this.printDetail("computed");
////          JavaUtil.sleepms(100, null);
////          throw new ArithmeticException();
////        }
//      }
      
      this.positive = thispositive;
      that.positive = thatpositive;
      return this;
    }

    public Element multiply(final Element that) {
      if (this.isZero() || that.isZero()) {
        return newElement();
      }

      final boolean sign = !(this.positive ^ that.positive);
      final boolean thispositive = this.positive;
      final boolean thatpositive = that.positive;
      this.positive = that.positive = true;

      final Element e = GmpMultiplier.get().multiply(this, that);
      e.positive = sign;
      this.positive = thispositive;
      that.positive = thatpositive;
      return e;
    }

    /**
     * Use Newton method to compute reciprocal.
     * x_{n+1} = x_n(2 - a x_n)
     */
    Element approximateReciprocalEqual(final JavaUtil.WorkGroup workers) {
      if (isZero()) {
        throw new ArithmeticException("isZero()=" + isZero());
      }

      /*
      //find trailing zeros
      int q = 0;
      int r = 0;
      int i = 0;
      for(; i < nDigits && digits[q][r] == 0; i++) {
        if (++r == digitsPerArray.value) {
          r = 0;
          q++;
        }
      }
      if (i == nDigits) {
        //unexpected case, print details for debug.
        printDetail();
        throw new RuntimeException(i + " = shift_in_digits == nDigits");
      }
      
      //right shift, reciprocal, left shift
      final int shift_in_digits = i;
      shiftRightEqual_digits(shift_in_digits);
      */

      approximateReciprocalEqual_private(workers);

//      shiftLeftEqual_digits(shift_in_digits);
      return this;
    }
    
    int normalize4division() {
      final int q = (nDigits - 1) >> digitsPerArray.exponent;
      final int r = (nDigits - 1) & digitsPerArray.mask;
      final int leading = Integer.numberOfLeadingZeros(digits[q][r]) - (Integer.SIZE - bitsPerDigit.value);  
      shiftLeftEqual_bits(leading);
      return leading;      
    }

    /**
     * Use Newton's method to compute reciprocal.
     * x_{n+1} = 2x_n - a x_n^2
     * 
     * Algorithm 3.5 in page 112
     * Richard Brent and Paul Zimmermann.  Modern Computer Arithmetic.
     * Preprint Version 0.5
     */
    private void approximateReciprocalEqual_private(final JavaUtil.WorkGroup workers) {
      if ((digits[(nDigits - 1) >> digitsPerArray.exponent][(nDigits - 1) & digitsPerArray.mask] & digitLimit.mask) < (digitLimit.value >> 1))
        throw new ArithmeticException("Require normalization, this=" + this);

      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.beginIndentation("approximateReciprocalEqual_private(..)");
        Print.println("this = " + this + " = " + toBigInteger());
      }

      //TODO: need optimization
      if (nDigits <= 1) {
        final BigInteger a = BigInteger.ONE.shiftLeft(bitsPerDigit.value << 1).subtract(BigInteger.ONE);
        set(a.divide(BigInteger.valueOf(digits[0][0] & digitLimit.mask)));
        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("return " + this + " (nDigits = 1)");
        return;
      } else if (nDigits == 2) {
        final BigInteger a = BigInteger.ONE.shiftLeft(bitsPerDigit.value << 2).subtract(BigInteger.ONE);
        set(a.divide(toBigInteger()));
        if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
          Print.endIndentation("return " + this + " (nDigits = 2)");
        return;
      }

      final int n = this.nDigits;
      final int k = (n - 1) >> 1;
      final int h = n - k;
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("n = " + n + ", k = " + k + ", h = " + h);

      final Element x = newElement().set(this);
      x.shiftRightEqual_digits(k);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("a_h = " + x + " = " + x.toBigInteger());

      x.approximateReciprocalEqual_private(workers);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("x_h = " + x + " = " + x.toBigInteger());

      final Element a = newElement().set(this);
      this.multiplyEqual(x, workers);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("t   = a x_h = " + this+ " = " + this.toBigInteger());

      a.negateEqual();
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("-a  = " + a + " = " + a.toBigInteger());

      //T >= B^(n+h)
      int count = 0;
      for(; this.nDigits > n + h; ) {
        this.plusEqual(a);
        count++;
        if (count > 5) {
          throw new RuntimeException("count > 5, n=" + n + ", h=" + h);
        }
      }
      a.reclaim();
      x.plusEqual(-count);
      
      // T = (B^(n+h) - T)B^(-k) = B^(2h) - TB^(-k)
      this.shiftRightEqual_digits(k);
      this.substractBy_private(h << 1);
      
      this.multiplyEqual(x, workers);
      this.shiftRightEqual_digits((h << 1) - k);
      x.shiftLeftEqual_digits(k);
      this.plusEqual(x);
      x.reclaim();
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.endIndentation("return this = " + this + " = " + toBigInteger());
      }
    }
    
    /**
     * B^e - this, given this < B^e,
     * where B is the base, i.e. B = digitLimit.
     */
    private Element substractBy_private(final int exponent_in_digits) {
//      Print.println("substractBy_private: exponent=" + exponent);
      if (isZero()) {
        nDigits = exponent_in_digits + 1;
        final int last_q = exponent_in_digits >> digitsPerArray.exponent;
        for(int i = last_q; i >= 0; i--) {
          digits[i] = arrayrecycler.newObject();
        }
        digits[last_q][exponent_in_digits & digitsPerArray.mask] = 1;
      } else if (nDigits > exponent_in_digits) {
        this.plusEqual_differentSign(1, exponent_in_digits).negateEqual();
      } else {
        //0 < this < B^e, therefore, two's complement
        int q = 0;
        int r = 0;
        int i = 0;
        long carry = 1;
        for(; i < nDigits && carry > 0; i++) {
          carry += ((digits[q][r] ^ digitLimit_mask) & digitLimit.mask);
          digits[q][r] = (int)carry & digitLimit_mask;
          carry >>>= digitLimit.exponent;
          
          if (++r == digitsPerArray.value) {
            r = 0;
            q++;
          }
        }
        //no more carry
        for(; i < exponent_in_digits && digits[q] != null; i++) {
          digits[q][r] ^= digitLimit_mask;
          if (++r == digitsPerArray.value) {
            r = 0;
            q++;
          }
        }
        //digits[q] are null
        if (i < exponent_in_digits) {
          final int last_q = (exponent_in_digits - 1) >> digitsPerArray.exponent;
          for(; q < last_q; i++) {
            digits[q] = arrayrecycler.newObject();
            Arrays.fill(digits[q], digitLimit_mask);
          }
          r = ((exponent_in_digits - 1) & digitsPerArray.mask) + 1;
          Arrays.fill(digits[q], 0, r, digitLimit_mask);
        }
        this.nDigits = exponent_in_digits;
        this.trimLeadingZeros();
      }
      return this;
    }

    
    int normalize4sqrt() {
      final int q = (nDigits - 1) >> digitsPerArray.exponent;
      final int r = (nDigits - 1) & digitsPerArray.mask;
      final int bits = Integer.SIZE - Integer.numberOfLeadingZeros(digits[q][r]);
      if (bits <= 2) {
        return 0;
      } else {
        final int shift_in_bits = bitsPerDigit.value - bits + 2 - (bits & 1);  
        shiftLeftEqual_bits(shift_in_bits);
        return shift_in_bits;
      }
    }

    public void approximateSqrtReciprocalEqual(final JavaUtil.WorkGroup workers) {
      if (isZero()) {
        throw new ArithmeticException("isZero()=" + isZero());
      }
      final int q = (nDigits - 1) >> digitsPerArray.exponent;
      final int r = (nDigits - 1) & digitsPerArray.mask;
      if (digits[q][r] == 1L)
        if (nDigits == 1 || isAllDigitsZero(nDigits - 1)) {
          if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
            Print.println("approximateSqrtReciprocalEqual (easy case), this = " + this);
          return;
        }

      approximateSqrtReciprocalEqual_private(workers);
    }

    void approximateSqrtReciprocalEqual_bisection(final JavaUtil.WorkGroup workers) {
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.beginIndentation("approximateSqrtReciprocalEqual_bisection: this = " + this + " = " + this.toBigInteger());
      final int n = this.nDigits - 1;
      final Element a = newElement().set(this);
      final Element beta2 = newElement().plusEqual(1).shiftLeftEqual_digits(n << 1);
//      Print.println("beta2=" + beta2);

      final Element upper = newElement().setAllOne(n);
      final Element lower = newElement();
      final Element tmp = newElement();
      
      for(boolean done = false; !done; ) {
        this.set(lower).plusEqual(upper).shiftRightEqual_bits(1);
//        Print.println();
//        Print.println("upper=" + upper.toBigInteger());
//        Print.println("lower=" + lower.toBigInteger());
//        Print.println("this =" + this.toBigInteger());
        tmp.set(this)
           .multiplyEqual(tmp, workers)
           .shiftRightEqual_digits(n)
           .multiplyEqual(a, workers);
//        Print.println("tmp  =" + tmp);

        if (this.compareTo(lower) == 0) {
          tmp.plusEqual(beta2.negateEqual());
          lower.set(tmp);
//          Print.println("l=" + lower.toBigInteger());
          tmp.set(upper)
             .multiplyEqual(tmp, workers)
             .shiftRightEqual_digits(n)
             .multiplyEqual(a, workers)
             .plusEqual(beta2)
             .negateEqual();
//          Print.println("u=" + tmp.toBigInteger());
          tmp.plusEqual(lower);
          if (tmp.isNegative()) {
            this.set(upper);
          }
          done = true;
        } else {
          final int d = tmp.compareTo(beta2);
//          Print.println("d=" + d);
          if (d == 0) {
            done = true;
          } else if (d > 0) {
            upper.set(this);
          } else {
            lower.set(this);
          }
        }
      }

      if (this.nDigits > n) {
        this.setAllOne(n);
      }
      a.reclaim();
      beta2.reclaim();
      upper.reclaim();
      lower.reclaim();
      tmp.reclaim();
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.endIndentation("return this=" + this + " = " + this.toBigInteger());
    }

    /**
     * Given nDigits > 2 and 1 <= the leading coefficient < 4.
     * 
     * Algorithm 3.9 in page 112
     * Richard Brent and Paul Zimmermann.  Modern Computer Arithmetic.
     * Preprint Version 0.5.9
     */
    private void approximateSqrtReciprocalEqual_private(final JavaUtil.WorkGroup workers) {
      if (nDigits <= 3) {
        approximateSqrtReciprocalEqual_bisection(workers);
        return;
      }
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.beginIndentation("approximateReciprocalSqrtEqual_private: this = " + this);
      //JavaUtil.sleepsecond(1, null);

      final int n = this.nDigits - 1;
      final int k = (n - 1) >> 1;
      final int h = n - k;

      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.println("n = " + n + ", k = " + k + ", h = " + h);
        Print.println("a    = " + this + " = " + this.toBigInteger());
      }

      final Element x = newElement().set(this);
      x.shiftRightEqual_digits(k);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("a_h  = " + x + " = " + x.toBigInteger());

      x.approximateSqrtReciprocalEqual_private(workers);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("x_h  = " + x + " = " + x.toBigInteger());
      
      this.multiplyEqual(x, workers).multiplyEqual(x, workers);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("t    = " + this + " = " + this.toBigInteger());

      this.shiftRightEqual_digits(n);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("t_h  = " + this + " = " + this.toBigInteger());

      this.substractBy_private(h << 1);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("t_l  = " + this + " = " + this.toBigInteger());

      this.multiplyEqual(x, workers);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("u    = " + this + " = " + this.toBigInteger());

      final int shift_in_digits = (h << 1) - k;
      final int shift_in_bits = (shift_in_digits << bitsPerDigit.exponent) + 1;
      if (shift_in_bits >= this.numberOfBits_int()) {
        this.setZero();
      } else {
        final long d = digits[shift_in_digits >> digitsPerArray.exponent][shift_in_digits & digitsPerArray.mask];
        this.shiftRightEqual_bits(shift_in_bits);
        if ((d & 1) == 1)
          this.plusEqual(1);
      }
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("u*B^(l-2h)/2 = " + this + " = " + this.toBigInteger());

      this.plusEqual(x.shiftLeftEqual_digits(k));
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.println("this         = " + this + " = " + this.toBigInteger());

      if (this.nDigits > n)
        this.setAllOne(n);
      if (FixedPointFraction.PRINT_LEVEL.is(Print.Level.TRACE))
        Print.endIndentation("return: this = " + this);
    }
    
    /** Set this to B^n - 1. */
    private Element setAllOne(final int n) {
      if (n == 0) {
        setZero();
      } else {
//        Print.beginIndentation("setAllOne(" + n + ") this = " + this);
        this.clearHigherDigits(n);
        int q = (n - 1) >> digitsPerArray.exponent;
        if (digits[q] == null)
          digits[q] = arrayrecycler.newObject();
        Arrays.fill(digits[q], 0, ((n - 1) & digitsPerArray.mask) + 1, digitLimit_mask);
        for(q--; q >= 0; q--) {
          Arrays.fill(digits[q], digitLimit_mask);
        }
  
        this.nDigits = n;
        this.positive = true;
//        Print.endIndentation("return this = " + this);
      }
      return this;
    }

    /**
     * Take integer square root and store the remainder in this object 
     * @return a new object storing the integer square root
     */
    Element sqrtRemainderEqual(final JavaUtil.WorkGroup workers) {
      if (isZero()) {
        return newElement();
      } else if (!positive) {
        printDetail("ERROR");
        throw new ArithmeticException("positive = " + positive);
      }

      final int k = (nDigits - 1) >> 2;
      if (k == 0) {
        return sqrtRemainderEqual_baseCase(workers);
      } else {
        throw new UnsupportedOperationException();
      }
    }
    /**
     * Given 0 < nDigits <= 4,
     * take integer square root and store the remainder in this object 
     * @return a new object storing hte integer square root
     */
    private Element sqrtRemainderEqual_baseCase(final JavaUtil.WorkGroup workers) {
      if (this.nDigits == 1) {
        final int s = (int)MathUtil.sqrt_long(this.digits[0][0]);
        final Element sqrt = newElement();
        sqrt.nDigits = 1;
        sqrt.positive = true;
        sqrt.digits[0] = arrayrecycler.newObject();
        sqrt.digits[0][0] = s;
        this.digits[0][0] -= s*s;
        if (this.digits[0][0] == 0) {
          this.nDigits = 0;
          arrayrecycler.reclaim(this.digits[0]);
          this.digits[0] = null;          
        }
        return sqrt;
      } else {
        final int half_nBits = this.numberOfBits_int() >> 1;
        final Element original = newElement().set(this);
        final Element tmp = newElement();
  
        Element upper;
        Element lower;

        {
          final Element mid = newElement().set(this).shiftRightEqual_bits(half_nBits);
          this.plusEqual(tmp.set(mid).multiplyEqual(mid, workers).negateEqual());

          if (this.isNonNegative() && this.compareTo(tmp.set(mid).shiftRightEqual_bits(1)) <= 0) {
            original.reclaim();
            tmp.reclaim();
            return mid;
          } else if (this.isPositive()) {
            upper = newElement().set(original).shiftRightEqual_bits(half_nBits - 1);
            lower = mid;
          } else {
            upper = mid;
            lower = newElement().set(original).shiftRightEqual_bits(half_nBits + 1);
          }
        }
        
        
        for(tmp.set(lower).negateEqual().plusEqual(upper);
            !tmp.isZero() && !tmp.isOne();
            tmp.set(lower).negateEqual().plusEqual(upper)) {
          //Print.println("lower=" + lower + ", upper=" + upper);
          final Element mid = newElement().set(lower).plusEqual(upper).shiftRightEqual_bits(1);
          this.set(original).plusEqual(tmp.set(mid).multiplyEqual(mid, workers).negateEqual());

          if (this.isNonNegative() && this.compareTo(tmp.set(mid).shiftRightEqual_bits(1)) <= 0) {
            original.reclaim();
            tmp.reclaim();
            upper.reclaim();
            lower.reclaim();
            return mid;
          } else if (this.isPositive()) {
            lower.reclaim();
            lower = mid;
          } else {
            upper.reclaim();
            upper = mid;
          }
        }

        this.set(original).plusEqual(tmp.set(lower).multiplyEqual(lower, workers).negateEqual());
        upper.reclaim();
        original.reclaim();
        tmp.reclaim();
        return lower;
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static final Factory FACTORY = new Factory();
  public static class Factory 
      implements DataSerializable.ValueOf<Zahlen>, StringSerializable.ValueOf<Zahlen> {
    protected final Map<String, Zahlen> cache = new HashMap<String, Zahlen>();

    public Zahlen valueOf(final int bitsPerDigit, final int digitsPerArray, final int numArrays) {
      final String key = bitsPerDigit + "," + digitsPerArray + "," + numArrays;
      Zahlen z = cache.get(key);
      if (z == null)
        cache.put(key, z = new Zahlen(bitsPerDigit, digitsPerArray, numArrays));
      return z;
    }

    @Override
    public Zahlen valueOf(String s) {
      int i = 0;
      int j = s.indexOf(", ", i);
      final int bitsPerDigit = Parse.parseIntVariable("bitsPerDigit", s.substring(i, j));

      i = j + 2;
      j = s.indexOf(", ", i);
      final int digitsPerArray = Parse.parseIntVariable("digitsPerArray", s.substring(i, j));

      i = j + 2;
      final int numArrays = Parse.parseIntVariable("numArrays", s.substring(i));

      return valueOf(bitsPerDigit, digitsPerArray, numArrays);
    }

    @Override
    public Zahlen valueOf(DataInput in) throws IOException {
      final int version = in.readInt();
      if (version != VERSION) {
        throw new IOException(version + " = version != VERSION = " + VERSION);
      } else {
        final PowerOfTwo_int bitsPerDigit = PowerOfTwo_int.valueOf(in);
        final PowerOfTwo_int digitsPerArray = PowerOfTwo_int.valueOf(in);
        final int numArrays = in.readInt();
        return valueOf(bitsPerDigit.value, digitsPerArray.value, numArrays);
      }
    }
  }

  public static final DataSerializable.ValueOf<Element> ELEMENT_FACTORY
      = new DataSerializable.ValueOf<Element>() {
    @Override
    public Element valueOf(final DataInput in) throws IOException {
      //read Zahlen, positive
      final Checksum c = Checksum.getChecksum();
      final Zahlen z = FACTORY.valueOf(in);
      c.update(z);
      final Element e = z.newElement();
      e.positive = c.update(in.readBoolean());
    
      //read digits
      e.nDigits = c.update(in.readInt());
      if (e.nDigits > 0) {
        int q = -1;
        int r = z.digitsPerArray.value - 1;
    
        if (z.bitsPerDigit.value <= Integer.SIZE) {
          for(int i = 0; i < e.nDigits; i++) {
            if (++r == z.digitsPerArray.value) {
              r = 0;
              e.digits[++q] = z.arrayrecycler.newObject();
            }
            e.digits[q][r] = c.update(in.readInt()) & z.digitLimit_mask;;
          }
        } else {
          for(int i = 0; i < e.nDigits; i++) {
            if (++r == z.digitsPerArray.value) {
              r = 0;
              e.digits[++q] = z.arrayrecycler.newObject();
            }
            e.digits[q][r] = c.update(in.readInt());
          }
        }
      }
      
      //read and verify checksum
      c.readAndVerify(in);
      return e;
    }
  };
}