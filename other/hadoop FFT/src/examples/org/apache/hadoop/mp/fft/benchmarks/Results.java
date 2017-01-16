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
package org.apache.hadoop.mp.fft.benchmarks;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;


public class Results {
  private final SortedSet<Entry> set = new TreeSet<Entry>();
  private final String name; 
  
  Results(final String name) {
    this.name = name;
  }
  
  public void add(final String name, final long ms) {
    final Entry e = new Entry(name, ms);
    Print.println(e);
    set.add(e);
  }
  
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append(name).append(": ");

    final Iterator<Entry> i = set.iterator();
    Entry prev = i.next();
    b.append(prev);
    for(; i.hasNext(); ) {
      final Entry e = i.next();
      b.append(prev.ms == e.ms? " = ": " < ").append(e);
      prev = e;
    }
    return b.toString();
  }

  static class Entry implements Comparable<Entry> {
    final String name;
    final long ms;
    
    Entry(final String name, final long ms) {
      this.name = name;
      this.ms = ms;    
    }

    @Override
    public int compareTo(final Entry that) {
      final long d = this.ms - that.ms;
      if (d == 0) {
        return name.compareTo(that.name);
      } else {
        return d > 0? 1: -1;
      }
    }

    @Override
    public String toString() {
      return name + "(" + Parse.millis2String(ms) + ")";
    }
  }
}
