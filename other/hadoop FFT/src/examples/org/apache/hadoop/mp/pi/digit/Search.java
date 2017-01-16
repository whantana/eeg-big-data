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
package org.apache.hadoop.mp.pi.digit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.PowerOfTwo_long;
import org.apache.hadoop.mp.util.Print;

/** Z, the set of integers. */
public class Search {
  final PowerOfTwo_int digitsPerArray;
  final PowerOfTwo_int bitsPerDigit; // must be either 8 or 16
  final PowerOfTwo_long digitLimit;
  final int hexPerDigit;

  public Search(final int bitsPerDigit, final int digitsPerElement) {
    if (bitsPerDigit % Byte.SIZE != 0)
      throw new IllegalArgumentException("bitsPerDigit (=" + bitsPerDigit
          + ") is not a multiple of Byte.SIZE (=" + Byte.SIZE + ")");
    if (Integer.SIZE < bitsPerDigit || Integer.SIZE % bitsPerDigit != 0)
      throw new IllegalArgumentException("bitsPerDigit (=" + bitsPerDigit
          + ") is not a factor of Integer.SIZE (=" + Integer.SIZE + ")");

    this.digitsPerArray = PowerOfTwo_int.valueOf(digitsPerElement);
    this.bitsPerDigit = PowerOfTwo_int.valueOf(bitsPerDigit);
    this.digitLimit = PowerOfTwo_long.valueOf(1 << this.bitsPerDigit.value);
    hexPerDigit = bitsPerDigit >> 2;
  }

  public String toString() {
    return getClass().getSimpleName() + "\n    digitsPerElement = "
        + digitsPerArray + "\n    bitsPerDigit     = " + bitsPerDigit
        + "\n    digitLimit       = " + digitLimit;
  }
  // ///////////////////////////////////////////////////////////////////////////
  public class Digit implements Cloneable {
    private boolean positive = false;
    private List<long[]> digits = new ArrayList<long[]>(1);
    /** Total number of digits, not including leading zeros, not digits.size() */
    private int nDigit = 0;

    public Digit overwrite(final int startdigit, final String filename
        ) throws IOException {
      final BufferedReader in = new BufferedReader(new FileReader(filename));
      try {
        String line;
  
        trim(startdigit);
        final int startarray = startdigit >> digitsPerArray.exponent;
        int i = startdigit;
        int r = startdigit & digitsPerArray.mask;
        long[] a;
        if (startarray == digits.size()) {
          a = new long[digitsPerArray.value];
          digits.add(a);;
        } else { 
          a = digits.get(startarray);
        }
  
        for(int linenumber = 0; (line = in.readLine()) != null; linenumber++) {
          if (line.startsWith("  ")) {
            for(final StringTokenizer t = new StringTokenizer(line); t.hasMoreTokens(); ) {
              final String hex = t.nextToken();
              //expecting 8 hex per token
              if (hex.length() != 8) {
                throw new NumberFormatException(hex.length()
                    + " = hex.length() != 8, hex=" + hex + "\n"
                    + linenumber + ": " + line);
              } else {
                a[r] = (int)Long.parseLong(hex, 16);
                i++;
  
                if (++r == digitsPerArray.value) {
                  r = 0;
                  a = new long[digitsPerArray.value];
                  digits.add(a);
                }
              }
            }
          }
        }
        if (a != null)
          nDigit = i;
      } finally {
        in.close();
      }
      return reverseEqual(startdigit);
    }

    public Digit reverseEqual(int startdigit) {
      if (nDigit > startdigit) {
        final int half = (nDigit - startdigit) >> 1;

        int uq = (nDigit - 1) >> digitsPerArray.exponent;
        int ur = (nDigit - 1) & digitsPerArray.mask;
        long[] upper = digits.get(uq);

        int lq = startdigit >> digitsPerArray.exponent;
        int lr = startdigit & digitsPerArray.mask;
        long[] lower = digits.get(lq);

        for(int i = 0; i < half; i++) {
          final long tmp = lower[lr];
          lower[lr] = upper[ur];
          upper[ur] = tmp;
          
          if (--ur < 0) {
            ur = digitsPerArray.value - 1;
            upper = digits.get(--uq);
          }
          if (++lr == digitsPerArray.value) {
            lr = 0;
            lower = digits.get(++lq);
          }
        }
      }
      return this;
    }

    /** Is this == 0? */
    public boolean isZero() {
      return nDigit == 0;
    }

    /**
     * @return ceiling(log_2 |n|), where n is the number represented by this
     *         object.
     */
    public int numberOfBits() {
      if (isZero())
        return 0;

      final int i = nDigit - 1;
      final long last = digits.get(i >> digitsPerArray.exponent)[i
          & digitsPerArray.mask];
      return (nDigit << bitsPerDigit.exponent) - bitsPerDigit.value
          + Long.SIZE - Long.numberOfLeadingZeros(last);
    }

    public BigInteger toBigInteger() {
      if (isZero())
        return BigInteger.ZERO;

      final byte[] bytes = new byte[((numberOfBits() - 1) >> 3) + 1];
      int i = bytes.length;
      for (long[] digitarray : digits)
        for (long d : digitarray)
          for (int j = 0; i > 0 && j < bitsPerDigit.value; j += Byte.SIZE)
            bytes[--i] = (byte) ((d >> j) & 0xFF);
      return new BigInteger(positive ? 1 : -1, bytes);
    }

    int search(String query) {
      Print.println("\nquery = " + query);
      return search(Util.reverse(query.getBytes()));
    }

    int search(final byte[] query) {
      final int bitLength = query.length << 3;
      Print.println("query = "+ Util.toHexString(query, 2, "       ")
          + "\n      = "+ Util.toBitString(query, 8, " ")
          + "\nbitLength = " + bitLength);
      return search(Util.byte2long(query), bitLength);
    }

    int search(final long[] query, final int bitLength) {
      Print.println("\nquery = "+ Util.toBitString(query, 8, " ")
          + "\nbitLength = " + bitLength);

      final long[][] bits = new long[bitsPerDigit.value][];
      final long[] leftmask = new long[bitsPerDigit.value];
      final long[] rightmask = new long[bitsPerDigit.value];
      {
        final long[] a = query;
        bits[0] = a;
        leftmask[0] = -1;
        rightmask[0] = (-1) << (32 - (bitLength & 0x1F));
        if (bitLength < 32)
          leftmask[0] &= rightmask[0];
  
        final long digitmask = bitsPerDigit.value == 32? -1: digitLimit.mask;
        for(int i = 1; i < bits.length; i++) {
          leftmask[i] = -1 >>> i;
          rightmask[i] = (-1) << (32 - ((bitLength + i) & 0x1F));
          if (i + bitLength <= 32)
            rightmask[i] = leftmask[i] &= rightmask[i];

          final int leftshift = bitsPerDigit.value - i;
          int k = ((bitLength + i - 1) >> bitsPerDigit.exponent);
          final long[] b = bits[i] = new long[k + 1];
          b[k] = a[a.length - 1] >>> i;
          k--;
          for(int j = a.length - 2; j >= 0; j--, k--) {
            b[k] = (a[j + 1] << leftshift) & digitmask;
            b[k] |= a[j] >>> i;
          }
          if (k == 0) {
            b[0] = (a[0] << leftshift) & digitmask;
          }
        }
        /*
        for(int i = 0; i < bits.length; i++) {
          Printer.println(String.format("d[%2d] = ", i)
              + Util.toBitString(bits[i], 8, " ") + ", length=" + bits[i].length
              + String.format(", masks=(%08X, %08X)", leftmask[i], rightmask[i]));
        }
        */
      }

      for(int d = nDigit - 1; d >= bits[0].length - 1; d--) {
        final long first = digits.get(d >> digitsPerArray.exponent)[d & digitsPerArray.mask];

        for(int i = 0; i < bits.length; i++) {
          int j = bits[i].length - 1;
          if (bits[i][j] == (first & leftmask[i])) {
            if (bits[i].length == 1)
              return ((nDigit - 1 - d) << bitsPerDigit.exponent) - i;
            else if (d > 0) {
              int k = (d - 1) & digitsPerArray.mask;
              long[] b = digits.get((d - 1) >> digitsPerArray.exponent);
              for(j--; j > 0 && bits[i][j] == b[k]; j--)
                if (--k < 0) {
                  k = digitsPerArray.value - 1;
                  b = digits.get((d - (bits[i].length - 1 - j)) >> digitsPerArray.exponent);
                }
              if (j == 0)
                if (bits[i][j] == (b[k] & rightmask[i]))
                  return ((nDigit - 1 - d) << bitsPerDigit.exponent) + i;
            }
          }
        }
      }
      return -1;
    }

    public void print(final int digitsPerLine, final PrintStream out) {
      if (isZero())
        out.println("0");

      out.print((positive ? "+[" : "-[nDigit=") + nDigit + ":\n ");
      printDigits(0, nDigit, digitsPerLine, out);      
      out.println("]");
    }

    public void print(final int startdigit, final int enddigit,
        final int digitsPerLine, final PrintStream out) {
      if (isZero())
        out.println("0");

      out.print("digits[" + startdigit + ", " + enddigit + ":\n ");
      printDigits(startdigit, enddigit, digitsPerLine, out);      
      out.println("]");
    }

    private void printDigits(final int startdigit, final int enddigit,
       final int digitsPerLine, final PrintStream out) {
      if (enddigit > nDigit) {
        throw new IllegalArgumentException(
            enddigit + " = enddigit > nDigit = " + nDigit);
      }
      int i = enddigit - 1;
      int q = i >> digitsPerArray.exponent;
      int r = i & digitsPerArray.mask;
      int k = 0;
      for (; i >= startdigit && q >= 0; q--) {
        final long[] d = digits.get(q);
        for (; i >= startdigit && r >= 0; r--) {
          out.format(" %0" + hexPerDigit + "X", d[r]);
          i--;
          if (++k == digitsPerLine) {
            k = 0;
            out.print("\n ");
          }
        }
        r = digitsPerArray.value - 1;
      }
      if (k > 0)
        out.println();
    }

    @Override
    public String toString() {
      if (isZero())
        return "0";

      final StringBuilder b = new StringBuilder(positive ? "+" : "-");
      int j = digits.size() - 1;
      int i = (nDigit - 1) & digitsPerArray.mask;
      for (; j >= 0; j--) {
        final long[] d = digits.get(j);
        for (; i >= 0; i--)
          b.append(String.format("%0" + hexPerDigit + "X ", d[i]));
        i = digitsPerArray.value - 1;
      }
      b.append("(").append(nDigit).append(
          nDigit <= 1 ? " digit, " : " digits, ");
      final int bits = numberOfBits();
      b.append(bits).append(bits <= 1 ? " bit)" : " bits)");
      return b.toString();
    }

    private void trim(final int startdigit) {
      if (startdigit < nDigit) {
        final int startarray = startdigit >> digitsPerArray.exponent;
        for(int i = digits.size() - 1; i > startarray; i--)
          digits.remove(i);

        final long[] a = digits.get(startarray);
        Arrays.fill(a, startdigit & digitsPerArray.mask, a.length, 0L);
        nDigit = startdigit;
      }
    }
  }

  void print(final Digit e, final int fromBit, final int length,
      final int digitsPerLine, final PrintStream out) {
    if ((fromBit & bitsPerDigit.mask) != 0) {
      throw new IllegalArgumentException(
          "(fromBit & bitsPerDigit.mask) != 0, fromBit=" + fromBit
          + ", bitsPerDigit=" + bitsPerDigit);
    }
    out.println("fromBit = " + fromBit + ", length = " + length);
    final int d = e.nDigit - (fromBit >> bitsPerDigit.exponent);
    e.print(d - (length >> bitsPerDigit.exponent), d, digitsPerLine, out);
  }

  public static void main(String[] args) throws IOException {
    final Search Z = new Search(32, 1024);
    Print.println(Z);
    final Digit e = Z.new Digit();
    
    e.overwrite(0, "800,000k-200,001k_hex.txt");
    Print.println("e.nDigit = " + e.nDigit);

    final int b = (200*1000*1000) >> 5;
    final int d = (800*1000*1000) >> 5;
    e.overwrite(b, "0-800,001k_hex.txt");
    Print.println("e.nDigit = " + e.nDigit);
    e.print(0, 100, 10, System.out);
    e.print(e.nDigit - 50, e.nDigit, 10, System.out);
    e.print(e.nDigit - d - 50, e.nDigit - d, 10, System.out);
    e.print(e.nDigit - d, e.nDigit - d + 50, 10, System.out);
    Z.print(e, 100000, 1024, 10, System.out);

    //Printer.println("return " + e.search("012"));
    
    //243F6A88 85A308D3 13198A2E
    Print.println("return " + e.search(new long[]{0x243F6A8885A308D3L}, 64));
    Print.println("return " + e.search(new long[]{0x85A308D3}, 32));
    Print.println("return " + e.search(new long[]{0xA308D313}, 32));

    //1100 1000 0101 0000 1111 0111 1 
    Print.println("return " + e.search(new long[]{0xC850F780}, 25));

    Print.println("return " + e.search("yahoo"));
    Print.println("return " + e.search("Yahoo"));
    Print.println("return " + e.search("Yahoo!"));
    Print.println("return " + e.search("Y!"));
    Print.println("return " + e.search("YHOO"));

    Print.println("return " + e.search("HDFS"));
    Print.println("return " + e.search("hdfs"));

    Print.println("return " + e.search("MAPREDUCE"));
    Print.println("return " + e.search("MapReduce"));
    Print.println("return " + e.search("mapreduce"));
    Print.println("return " + e.search("mapred"));

    Print.println("return " + e.search("hadoop"));
    Print.println("return " + e.search("Hadoop"));
    Print.println("return " + e.search("HADOOP"));

    Print.println("return " + e.search("DistCp"));
    Print.println("return " + e.search("distcp"));
  }
}
