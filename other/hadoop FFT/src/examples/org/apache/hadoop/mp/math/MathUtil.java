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

public class MathUtil {
  /** @return integer square root */
  public static long sqrt_long(final long x) {
    if (x == 0)
      return 0;
    else if (x < 0)
      throw new ArithmeticException("sqrt(" + x + ")");
    
    final int half_nBits = (Long.SIZE - Long.numberOfLeadingZeros(x)) >> 1;
    long upper;
    long lower;
    {
      final long mid = x >> half_nBits;
      final long r = x - mid*mid;
      if (r >= 0 && r <= (mid << 1)) {
        return mid;
      } else if (r > 0) {
        upper = x >> (half_nBits - 1);
        lower = mid;
      } else {
        upper = mid;
        lower = x >> (half_nBits + 1);
      }
    }

    for(; upper - lower > 1; ) {
      final long mid = (upper + lower) >> 1;
      final long r = x - mid*mid;
      if (r >= 0 && r <= (mid << 1)) {
        return mid;
      } else if (r > 0) {
        lower = mid;
      } else {
        upper = mid;
      }
    }
    return lower;
  }
}
