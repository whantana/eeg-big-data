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

import java.util.ArrayList;
import java.util.List;

/** Interval [start, end) */
public class Interval implements Partitionable<Interval>, Combinable<Interval> {
  public final long start; //inclusive
  public final long end;   //exclusive

  /** Construct [start, end) */
  public Interval(long start, long end) {
    if (start >= end) {
      throw new IllegalArgumentException("start = " + start + " <= end =" + end);
    }
    this.start = start;
    this.end = end;
  }

  /** Is x in this interval? */
  public boolean contains(long x) {return x >= start && x < end;}

  @Override
  public List<Interval> partition(int nParts) {
    final long d = end - start;
    if (d < nParts)
      nParts = (int)d;
    
    final List<Interval> a = new ArrayList<Interval>();
    final long size = d/nParts;
    long remainder = d % nParts;
    for(long x = start; x < end; ) {
      long y = x + size;
      if (remainder > 0) {
        y++;
        remainder--;
      }
      a.add(new Interval(x, y));
      x = y;
    }
    return a;
  }

  @Override
  public Interval combine(Interval that) {
    if (this.start != that.end)
      throw new IllegalArgumentException("this.start != that.end, this="
          + this + ", that=" + that);
    return new Interval(that.start, this.end);
  }

  @Override
  public int compareTo(Interval that) {
    final long d = this.start - that.start;
    return d > 0? 1: d < 0? -1: 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof Interval) {
      final Interval that = ((Interval)obj);
      return this.start == that.start && this.end == that.end;
    }
    return false;
  }

  @Override
  public String toString() {
    return "[" +  start + ", " + end + ")";
  }

  public static Interval valueOf(final String s, int beginIndex, int endIndex) {
    beginIndex++;
    final int i = s.indexOf(", ", beginIndex);
    return new Interval(
        Parse.string2long(s.substring(beginIndex, i)),
        Parse.string2long(s.substring(i+2, endIndex-1)));
  }

  public static Interval valueOf(final String s) {
    return valueOf(s, 0, s.length());
  }
}
