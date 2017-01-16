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

import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.PowerOfTwo_long;

public class TestPowerOfTwo extends junit.framework.TestCase {
  public void test() {
    assertEquals(Integer.SIZE - 1, PowerOfTwo_int.values().length);
    for(int i = 0; i < Integer.SIZE - 1; i++) {
      assertEquals(1 << i, PowerOfTwo_int.values()[i].value);
    }

    assertEquals(Long.SIZE - 1, PowerOfTwo_long.values().length);
    for(int i = 0; i < Long.SIZE - 1; i++) {
      assertEquals(1L << i, PowerOfTwo_long.values()[i].value);
    }
  }
}
