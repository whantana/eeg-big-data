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

import java.util.List;

import org.apache.hadoop.mp.util.Interval;
import org.apache.hadoop.mp.util.JavaUtil;


public class TestJava extends junit.framework.TestCase {
  public void testInterval() throws Exception {
    final Interval i = new Interval(1000000L, 2000000L);
    final String[] strings = {
        "[1000000, 2000000)",
        "[1000k, 2000k)",
        "[1,000,000, 2,000,000)",
        "[  1,000k   ,   2,000k  )"
    };
    for(String s : strings) {
      final Interval j = Interval.valueOf(s);
      final String mess = "s = " + s + "\ni = " + i + "\nj=" + j;
      System.out.println(mess);
      assertEquals(mess, i, j);
    }
  }

  public void testPartition() throws Exception {
    final Interval i = new Interval(100, 201);
    for(int n = 1; n < 9; n++) {
      final List<Interval> parts = i.partition(n);
      //System.out.println("parts = " + parts);
      assertEquals(n, parts.size());
      
      final List<Interval> combined = JavaUtil.combine(parts);
      assertEquals(1, combined.size());
      assertEquals(i, combined.get(0));
    }
  }
}