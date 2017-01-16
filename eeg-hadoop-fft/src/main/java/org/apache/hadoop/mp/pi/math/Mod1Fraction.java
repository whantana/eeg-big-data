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

import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

public abstract class Mod1Fraction implements Cloneable, DataSerializable<Void> {
  public static Factory FACTORY = new Mod1Fraction_IntArray.Factory();

  public static abstract class Factory
      implements StringSerializable.ValueOf<Mod1Fraction>, DataSerializable.ValueOf<Mod1Fraction> {
    protected long precision = 100;

    public void setPrecision(long precision) {
      this.precision = precision;
    }

    public long getPrecision() {
      return precision;
    }

    public abstract String getName();

    public abstract Mod1Fraction zero();
  }

  public static long getPrecision() {return FACTORY.getPrecision();}

  public static void setPrecision(long precision) {FACTORY.setPrecision(precision);}

  public static Mod1Fraction zero() {return FACTORY.zero();}
  
  public static Mod1Fraction valueOf(String s) {return FACTORY.valueOf(s);}

  public static Mod1Fraction valueOf(DataInput in) throws IOException {
    return FACTORY.valueOf(in);
  }
  //---------------------------------------------------------------------------
  public abstract Mod1Fraction clone();

  public abstract Mod1Fraction subtractMod1Equal(Mod1Fraction that);

  public abstract Mod1Fraction addMod1Equal(Mod1Fraction that);

  public final Mod1Fraction addMod1(Mod1Fraction that) {
    final Mod1Fraction sum = clone().addMod1Equal(that);
    /*
    JavaUtil.out.println("\n  this = " + this);
    JavaUtil.out.println("  that = " + that);
    JavaUtil.out.println("  sum  = " + sum);
    */
    //new Exception().printStackTrace(JavaUtil.out);
    return sum;
  }

  public abstract Mod1Fraction addFractionMod1Equal(long numerator, long denominator);

  /** 
   * s += 1/(n 2^e)
   * s += 1.0 / (n << e);
   * if (s >= 1) s--;
   */
  public abstract Mod1Fraction addShiftFractionMod1Equal(long n, long e);

  /** Print out value. */
  public void print(PrintStream out) {printHex(10, out);}

  public abstract void printHex(int partsPerLine, PrintStream out);

  public abstract String toHexString(int partsPerLine);

  @Override
  public final String toString() {return toHexString(0);}
}
