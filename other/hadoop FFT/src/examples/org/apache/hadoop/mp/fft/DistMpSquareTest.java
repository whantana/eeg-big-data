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
package org.apache.hadoop.mp.fft;

import java.io.FileNotFoundException;

import org.apache.hadoop.mp.Function;
import org.apache.hadoop.util.ToolRunner;

public class DistMpSquareTest extends DistMpMultTest {

  DistMpSquareTest() throws FileNotFoundException {}

  @Override
  public int run(String[] args) throws Exception {
    final Function.Variable x = Function.Variable.valueOf("x");
    return run(args, "y", x);
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistMpSquareTest(), args));
  }
}