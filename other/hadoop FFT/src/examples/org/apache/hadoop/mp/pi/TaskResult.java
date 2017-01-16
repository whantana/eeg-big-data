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
package org.apache.hadoop.mp.pi;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.mp.pi.math.Mod1Fraction;
import org.apache.hadoop.mp.pi.math.Summation;
import org.apache.hadoop.mp.util.Container;
import org.apache.hadoop.mp.util.MachineComputable;
import org.apache.hadoop.mp.util.Parse;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;

/** A class for map task results or reduce task results. */
public class TaskResult implements Container<Summation>, MachineComputable.Result<TaskResult> {
  private Summation sigma;
  private Mod1Fraction value;
  private long duration;

  public TaskResult() {}

  public TaskResult(Summation sigma, Mod1Fraction value, long duration) {
    this.sigma = sigma;
    this.value = value;
    this.duration = duration;      
  }

  /** {@inheritDoc} */
  @Override
  public Summation get() {return sigma;}

  public Mod1Fraction getValue() {return value;}

  /** @return The time duration used */
  long getDuration() {return duration;}

  /** {@inheritDoc} */
  @Override
  public int compareTo(TaskResult that) {
    return this.sigma.compareTo(that.sigma);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    else if (obj != null && obj instanceof TaskResult) {
      final TaskResult that = (TaskResult)obj;
      return this.compareTo(that) == 0;
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public TaskResult combine(TaskResult that) {
    try {
      final Summation s = this.sigma.combine(that.sigma);
      if (s == null)
        return null;

      this.sigma = s;
      this.value.addMod1Equal(that.value);
      this.duration += that.duration;
      return this;
    }
    catch(RuntimeException e) {
      Print.println("Got " + e + "!!!"
          + ",\n  this=" + this
          + ",\n  that=" + that);
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    sigma = SummationWritable.IO_VALUE_OF.valueOf(in).get();
    value = Mod1Fraction.valueOf(in);
    duration = in.readLong();
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    SummationWritable.write(sigma, out);
    value.serialize(out);
    out.writeLong(duration);
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "duration=" + duration + "(" + Parse.millis2String(duration)
        + "), sigma=" + sigma + ", value=" + value;
  }
  /////////////////////////////////////////////////////////////////////////////
  public static final Factory FACTORY = new Factory();

  public static final class Factory
      implements DataSerializable.ValueOf<TaskResult>, StringSerializable.ValueOf<TaskResult> {
    private Factory() {}

    @Override
    public TaskResult valueOf(DataInput in) throws IOException {
      final TaskResult  r = new TaskResult();
      r.readFields(in);
      return r;
    }

    @Override
    public TaskResult valueOf(final String s) {
      int i = -1, j = -1;
      try {
        i = 0;
        j = s.indexOf('(');
        final long duration = Parse.parseLongVariable("duration", s.substring(i, j));
  
        i = s.indexOf("sigma=", j+1);
        j = s.indexOf(", value=", i+1);
        final Summation sigma = Summation.valueOf(Parse.parseStringVariable("sigma", s.substring(i, j)));
    
        i = j + 2;
        final Mod1Fraction value = Mod1Fraction.FACTORY.valueOf(Parse.parseStringVariable("value", s.substring(i)));
        return new TaskResult(sigma, value, duration);
      } catch(RuntimeException e) {
        throw new RuntimeException("i=" + i + ", j=" + j + ", s=" + s, e);
      }
    }
  }

  static void parse(final BufferedReader in, final DistSum distsum
      ) throws IOException {
    for(String line; (line = in.readLine()) != null; ) {
      if (line.startsWith(DistSum.class.getSimpleName())) {
        final int i = line.indexOf("duration=");
        final TaskResult r = FACTORY.valueOf(line.substring(i));
        distsum.add(r);
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    final String filename = args[0];
    Print.print("args", args);

    Mod1Fraction.setPrecision(1024);

    final DistSum distsum = new DistSum();
    final BufferedReader in = new BufferedReader(new FileReader(filename));
    try {
      parse(in, distsum);
    } finally {
      in.close();
    }

    Print.println("\nCPU time = " + distsum.getDurationString());
    Print.println("END " + new Date(System.currentTimeMillis()));
    distsum.value.print(Print.out);
  }
}