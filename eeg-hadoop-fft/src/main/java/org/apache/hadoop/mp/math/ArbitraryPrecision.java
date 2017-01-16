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

import java.util.Random;

import org.apache.hadoop.mp.util.Checksum;
import org.apache.hadoop.mp.util.Container;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.Recycler;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

/** Numbers implemented with arbitrary precision arithmetic. */
abstract class ArbitraryPrecision<T, E extends ArbitraryPrecision<T,E>.Element>
    implements DataSerializable<Void>, StringSerializable, Print.Brief {
  /** Get the key of this object. */
  abstract String getKey();

  /** @return a new element. */
  public final E newElement() {return elementrecycler.newObject();}
  /** Call the private element constructor. */
  abstract E newElement_private();

  public abstract E random(final int nDigit, final Random r);
  /////////////////////////////////////////////////////////////////////////////
  /** Elements */
  public abstract class Element
      implements Comparable<E>, Container<T>, DataSerializable<Checksum>,
                 Print.Brief, Print.Detail {
    @Override
    public final T get() {
      @SuppressWarnings("unchecked")
      final T t = (T)ArbitraryPrecision.this;
      return t;
    }

    /** Set this to zero. */
    abstract E setZero();
    /** Set this to that. */
    abstract E set(E that);

    /** Check whether their containers are the same. */
    public void check(E that) {
      if (this.get() != that.get())
        throw new IllegalArgumentException("this.get() != that.get()"
            + "\nthis=" + this.toBrief() + ", this.get()=" + this.get()
            + "\nthat=" + that.toBrief() + ", that.get()=" + that.get());
    }

    @Override
    public final boolean equals(Object that) {
      if (this == that)
        return true;
      else if (this.getClass() == that.getClass()) {
        @SuppressWarnings("unchecked")
        final E t = (E)that;
        return this.compareTo(t) == 0;
      } else
        return false;
    }

    public final void reclaim() {
      @SuppressWarnings("unchecked")
      final E e = (E)this;
      elementrecycler.reclaim(e);
    }

    public abstract void print(final int digitsPerLine);

    /** Shift this to left in bits. */
    public abstract E shiftLeftEqual_bits(final long shift_in_bits);
    /** Shift this to right in bits. */
    public abstract E shiftRightEqual_bits(final long shift_in_bits);

    /** this = -this */
    public abstract E negateEqual();
    /** this += that */
    public abstract E plusEqual(final E that);
    /** this *= that */
    public abstract E multiplyEqual(final E that, final JavaUtil.WorkGroup workers);
  }
  /////////////////////////////////////////////////////////////////////////////
  protected static final int RECYCLER_SIZE = 1 << 16;
  private final ElementRecycler elementrecycler = new ElementRecycler();

  private final class ElementRecycler extends Recycler<E> {
    private ElementRecycler() {super(RECYCLER_SIZE);}

    @Override
    public final E newObject() {
      if (size > 0) {
        size--;
        @SuppressWarnings("unchecked")
        final E a = (E)array[size];
        array[size] = null;
        a.setZero();
        return a;
      } else
        return newElement_private();
    }
  }
}