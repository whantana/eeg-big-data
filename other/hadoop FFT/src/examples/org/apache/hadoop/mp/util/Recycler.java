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

import java.util.Arrays;

/** Recycle objects. */
public abstract class Recycler<T> {
  protected final Object[] array;
  protected int size = 0;

  protected Recycler(final int size) {this.array = new Object[size];}

  /** Create a new object. */
  public abstract T newObject();

  /** Reclaim an object. */
  public void reclaim(final T a) {
    if (size < array.length)
      array[size] = a;
  }

  /** Recycling int[]. */
  public static final class IntArray extends Recycler<int[]> {
    private final int arraylength;

    public IntArray(int factorysize, int arraylength) {
      super(factorysize);
      this.arraylength = arraylength;
    }

    @Override
    public final int[] newObject() {
      if (size > 0) {
        size--;
        final int[] a = (int[])array[size];
        array[size] = null;
        Arrays.fill(a, 0);
        return a;
      } else
        return new int[arraylength];
    }

    @Override
    public final void reclaim(final int[] a) {
      if (a.length != arraylength)
        throw new IllegalArgumentException("a.length = " + a.length
            + " != arraylength = " + arraylength);
      super.reclaim(a);
    }
  }

  /** Recycling long[]. */
  public static final class LongArray extends Recycler<long[]> {
    private final int arraylength;

    public LongArray(int factorysize, int arraylength) {
      super(factorysize);
      this.arraylength = arraylength;
    }

    @Override
    public final long[] newObject() {
      if (size > 0) {
        size--;
        final long[] a = (long[])array[size];
        array[size] = null;
        Arrays.fill(a, 0L);
        return a;
      } else
        return new long[arraylength];
    }

    @Override
    public final void reclaim(final long[] a) {
      if (a.length != arraylength)
        throw new IllegalArgumentException("a.length = " + a.length
            + " != arraylength = " + arraylength);
      super.reclaim(a);
    }
  }
}
