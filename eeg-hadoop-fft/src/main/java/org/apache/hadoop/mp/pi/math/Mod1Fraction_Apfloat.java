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

//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.io.PrintStream;
//
//import org.apache.hadoop.examples.util.JavaUtil;
//import org.apache.hadoop.examples.util.ValueOf;
//import org.apfloat.Apfloat;
//import org.apfloat.ApfloatContext;
//import org.apfloat.Apint;
//import org.apfloat.ApintMath;
//
//public class Mod1Fraction_Apfloat extends Mod1Fraction {
//  private static int radix_exponent;
//  private static int radix;
//
//  private static Apfloat ZERO;
//  private static Apfloat ONE;
//  
//  private static Apint TWOi;
//  private static Apint TWOto32;
//
//  public static class Factory extends Mod1Fraction.Factory {
//    @Override
//    public long getPrecision() {return precision*radix_exponent;}
//
//    @Override
//    public void setPrecision(long p) {
//      final int r_e = 4;
//      if (p % r_e != 0)
//        throw new IllegalArgumentException(
//            "p % r_e != 0, p=" + p + ", r_e=" + r_e);
//
//      radix_exponent = r_e;
//      radix = 1 << radix_exponent;
//      precision = p/radix_exponent;
//
//      ZERO = newApfloat(0);
//      ONE = newApfloat(1);
//
//      TWOi = newApint(2);
//      TWOto32 = newApint(1L << 32);
//
//      JavaUtil.out.println("precision=" + JavaUtil.long2string(getPrecision()) + ", radix=" + radix
//          + ", ONE.precision=" + JavaUtil.long2string(ONE.precision()) + ", ONE.radix=" + ONE.radix());
//    }
//
//    @Override
//    public String getName() {
//      return Mod1Fraction_Apfloat.class.getSimpleName();
//    }
//
//    @Override
//    public Mod1Fraction zero() {
//      return new Mod1Fraction_Apfloat(newApfloat(0));
//    }
//
//    private static final ValueOf.IO<Mod1Fraction> VALUE_OF_IO = new ValueOf.IO<Mod1Fraction>() {
//      @Override
//      public Mod1Fraction valueOf(DataInput in) throws IOException {
//        Apfloat x = ZERO;
//        long m = 0;
//        for(int count = in.readInt(); count > 0; count = in.readInt()) {
//          for(int i = 0; i < count; i++) {
//            x = x.multiply(TWOto32);
//            x = x.add(newApint(in.readInt()));
//            m++;
//          }
//        }
//        x = x.divide(ApintMath.pow(TWOto32, m));
//        return new Mod1Fraction_Apfloat(x);
//      }
//    };
//    @Override
//    public ValueOf.IO<Mod1Fraction> getValueOfIO() {return VALUE_OF_IO;}
//
//    private static final ValueOf.Str<Mod1Fraction> VALUE_OF_STR = new ValueOf.Str<Mod1Fraction>() {
//      @Override
//      public Mod1Fraction valueOf(String s) {
//        int i = 1;
//        int j = s.indexOf(":");
//        final int count = Integer.parseInt(s.substring(i, j));
//        
//        Apfloat x = ZERO;
//        for(int k = 0; k < count; k++) {
//          i = j + 1;
//          j = s.indexOf(" ", i);
//          x = x.multiply(TWOto32);
//          x = x.add(newApfloat(s.substring(i, j)));
//        }
//        x = x.divide(ApintMath.pow(TWOto32, count));
//        return new Mod1Fraction_Apfloat(x);
//      }
//    };
//    @Override
//    public ValueOf.Str<Mod1Fraction> getValueOfStr() {return VALUE_OF_STR;}
//  }
//
//  static {
//    final ApfloatContext ctx = ApfloatContext.getGlobalContext();
//    ctx.setMaxMemoryBlockSize(1024L << 20);
//    ctx.setMemoryTreshold(1024 << 20);
//    ctx.setSharedMemoryTreshold(1024L << 20);
//    setPrecision(100);
//    //dump();
//  }
//
//  static void dumpApfloatContext(final PrintStream out) {
//    final ApfloatContext ctx = ApfloatContext.getContext();
//    out.println("*************** ApfloatContext ***************");
//    out.println("builderFactory       = " + ctx.getBuilderFactory().getClass().getName());
//    out.println("numberOfProcessors   = " + ctx.getNumberOfProcessors());
//    out.println("cacheL1Size          = " + ctx.getCacheL1Size());
//    out.println("cacheL2Size          = " + ctx.getCacheL2Size());
//    out.println("cacheBurst           = " + ctx.getCacheBurst());
//    out.println("blockSize            = " + ctx.getBlockSize());
//    out.println("maxMemoryBlockSize   = " + ctx.getMaxMemoryBlockSize());
//    out.println("memoryTreshold       = " + ctx.getMemoryTreshold());
//    out.println("sharedMemoryTreshold = " + ctx.getSharedMemoryTreshold());
//    out.println("**********************************************");
//  }
//
//  private static Apint newApint(long n) {return new Apint(n, radix);}
//  private static Apfloat newApfloat(long n) {return new Apfloat(n, getPrecision()/radix_exponent, radix);}
//
//  private static Apfloat newApfloat(String s) {
////    Util.out.println("s = " + s);
//    try {
//      return new Apfloat(s, getPrecision(), radix);
//    } catch(NumberFormatException nfe) {
//      JavaUtil.err.println("s = " + s);
//      throw nfe;
//    }
//  }
//  //---------------------------------------------------------------------------
//  private Apfloat value;
//
//  private Mod1Fraction_Apfloat(Apfloat value) {this.value = value;}
//
//  @Override
//  public Mod1Fraction_Apfloat clone() {
//    return new Mod1Fraction_Apfloat(value);
//  }
//
//  @Override
//  public Mod1Fraction_Apfloat addMod1Equal(Mod1Fraction that) {
//    if (that != null) {
//      final Apfloat y = ((Mod1Fraction_Apfloat)that).value;
//      final Apfloat x = value.add(y);
//      value = x.compareTo(ONE) >= 0? x.subtract(ONE): x;
//    }
//    return this;
//  }
//
//  @Override
//  public Mod1Fraction_Apfloat subtractMod1Equal(Mod1Fraction that) {
//    if (that != null) {
//      final Apfloat y = ((Mod1Fraction_Apfloat)that).value;
//      final Apfloat x = value.subtract(y);
//      value = x.compareTo(ZERO) < 0? x.add(ONE): x;
//    }
//    return this;
//  }
//
//  @Override
//  public Mod1Fraction_Apfloat addFractionMod1Equal(final long numerator, final long denominator) {
//    Apfloat x = value.add(newApfloat(numerator).divide(newApfloat(denominator)));
//    if (x.compareTo(ONE) >= 0)
//      x = x.subtract(ONE);
//    value = x;
//    return this;
//  }
//
//  @Override
//  public Mod1Fraction_Apfloat addShiftFractionMod1Equal(long n, long e) {
//    Apfloat x = value.add(ONE.divide(newApint(n)).divide(ApintMath.pow(TWOi, e)));
//    if (x.compareTo(ONE) >= 0)
//      x = x.subtract(ONE);
//    value = x;
//    return this;
//  }
//
//  private void check0to1() {
//    if (value.compareTo(ZERO) < 0 || value.compareTo(ONE) >= 0)
//      throw new RuntimeException("value  < 0 || value >= 1, value = " + value);
//  }
//
//  @Override
//  public String toHexString(int partPerLine) {
//    check0to1();
//
//    int count = 0;
//    final StringBuilder b = new StringBuilder(); 
//    for(Apfloat x = value; x.compareTo(ZERO) > 0; ) {
//      x = x.multiply(TWOto32);
//      final Apint n = x.truncate();
//      if (partPerLine > 0 && count % partPerLine == 0)
//        b.append("\n  ");
//      b.append(String.format("%08X ", n.longValue()));
//      x = x.subtract(n);
//      count++;
//    }
//    return b.insert(0, "[" + count + ":").append("]").toString();
//  }
//
//  private static final int ARRAY_SIZE = 1024; 
//
//  @Override
//  public void write(DataOutput out) throws IOException {
//    check0to1();
//
//    for(Apfloat x = value; x.compareTo(ZERO) > 0; ) {
//      out.write(ARRAY_SIZE);
//      for(int i = 0; i < ARRAY_SIZE; i++) {
//        x = x.multiply(TWOto32);
//        final Apint n = x.truncate();
//        x = x.subtract(n);
//
//        out.writeInt((int)n.longValue());
//      }
//    }
//    out.write(0);
//  }
//
//  public static class Mod1Fraction_IntArray_Util {
//    private static final int iaRadix = 16;
//    private static final Apint iaTWO = new Apint(2, iaRadix);
//    private static final Apint iaTWOto32 = new Apint(1L << 32, iaRadix);
// 
//    private static Apfloat iaNewApfloat(long n) {
//      return new Apfloat(n, Mod1Fraction_IntArray.FACTORY.getPrecision(), iaRadix);
//    }
//
//    public static void initIntArray(final int[] a,
//        final long numerator, final long denominator, final long shift) {
//      Apfloat x = iaNewApfloat(numerator).divide(iaNewApfloat(denominator));
//      final long q = shift/32L;
//      if (q < a.length) {
//        final long r = shift & 0x1FL;
//        if (r > 0)
//          x = x.divide(ApintMath.pow(iaTWO, r));
//  
//        for(int i = (int)q; i < a.length; i++) {
//          x = x.multiply(iaTWOto32);
//          final Apint y = x.truncate();
//          a[i] = (int)y.longValue();
//          x = x.subtract(y);
//        }
//      }
//    }
//  }
//}
