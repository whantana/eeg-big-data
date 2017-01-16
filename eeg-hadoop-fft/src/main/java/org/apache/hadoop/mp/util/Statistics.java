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

import java.util.Map;
import java.util.TreeMap;

public class Statistics {
  private static class Count {
    private final String name;
    private long count = 0;
    
    Count(String name) {
      this.name = name;
    }
    
    @Override
    public String toString() {
      return name + ": " + count;
    }
  }
  
  private final String name;
  private Map<String, Count> counts = new TreeMap<String, Count>();
  
  public Statistics(String name) {
    this.name = name;
  }
  
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append(getClass().getSimpleName()).append(" ").append(name)
     .append(", counts.size()=").append(counts.size());
    for(Count c : counts.values()) {
      b.append("\n  ").append(c);
    }
    return b.toString();
  }

  public void countExecutionPoint() {
    countExecutionPoint(Thread.currentThread().getStackTrace()[2].toString());
  }
  
  public void countExecutionPoint(final String key) {
    Count c = counts.get(key);
    if (c == null)
      counts.put(key, c = new Count(key));
    c.count++;
  }

  public static void main(String[] args) {
    final Statistics s = new Statistics("test");
    s.countExecutionPoint();
    s.countExecutionPoint();
    System.out.println(s);
  }
}
