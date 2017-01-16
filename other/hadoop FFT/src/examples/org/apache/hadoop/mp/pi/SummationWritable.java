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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mp.pi.math.ArithmeticProgression;
import org.apache.hadoop.mp.pi.math.Summation;
import org.apache.hadoop.mp.util.Container;
import org.apache.hadoop.mp.util.MachineComputable;
import org.apache.hadoop.mp.util.JavaUtil.Timer;
import org.apache.hadoop.mp.util.serialization.ConfSerializable;
import org.apache.hadoop.mp.util.serialization.DataSerializable;

/** A Writable class for Summation */
public final class SummationWritable
    implements Container<Summation>, MachineComputable<SummationWritable, TaskResult> {

  private Summation sigma;

  public SummationWritable() {}
  SummationWritable(Summation sigma) {this.sigma = sigma;}

  /** {@inheritDoc} */
  @Override
  public String toString() {return "" + sigma;}

  /** {@inheritDoc} */
  @Override
  public Summation get() {return sigma;}

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    else if (obj != null && obj instanceof SummationWritable) {
      final SummationWritable that = (SummationWritable)obj;
      return this.sigma.equals(that.sigma);
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskResult compute(int nThreads, Timer timer) {
    return sigma.compute(nThreads, timer);
  }

  @Override
  public List<SummationWritable> partition(int nParts) {
    final List<SummationWritable> writables = new ArrayList<SummationWritable>();
    for(Summation s : sigma.partition(nParts))
      writables.add(new SummationWritable(s));
    return writables;
  }

  //----------------------------------------------------------------------------
  /** A writable class for ArithmeticProgression */
  private static class ArithmeticProgressionWritable {
    /** Read ArithmeticProgression from DataInput */
    private static ArithmeticProgression read(DataInput in) throws IOException {
      return new ArithmeticProgression(in.readChar(),
          in.readLong(), in.readLong(), in.readLong());
    }

    /** Write ArithmeticProgression to DataOutput */
    private static void write(ArithmeticProgression ap, DataOutput out
        ) throws IOException {
      out.writeChar(ap.symbol);
      out.writeLong(ap.value);
      out.writeLong(ap.delta);
      out.writeLong(ap.limit);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    final boolean ispositive = in.readBoolean();
    final ArithmeticProgression N = ArithmeticProgressionWritable.read(in);
    final ArithmeticProgression E = ArithmeticProgressionWritable.read(in);
    sigma = new Summation(ispositive, N, E); 
  }

  public static final DataSerializable.ValueOf<SummationWritable> IO_VALUE_OF
      = new DataSerializable.ValueOf<SummationWritable>() {
    @Override
    public SummationWritable valueOf(DataInput in) throws IOException {
      final SummationWritable s = new SummationWritable();
      s.readFields(in);
      return s;
    }
  };

  /** Write sigma to DataOutput */
  public static void write(Summation sigma, DataOutput out) throws IOException {
    out.writeBoolean(sigma.ispositive);
    ArithmeticProgressionWritable.write(sigma.N, out);
    ArithmeticProgressionWritable.write(sigma.E, out);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    write(sigma, out);
  }
  //----------------------------------------------------------------------------
  private static final String CONF_PROPERTY_NAME = SummationWritable.class.getName() + ".conf";
  public static final ConfSerializable.ValueOf<SummationWritable> CONF_VALUE_OF
      = new ConfSerializable.ValueOf<SummationWritable>() {
    @Override
    public SummationWritable valueOf(Configuration conf) {
      return new SummationWritable(Summation.valueOf(conf.get(CONF_PROPERTY_NAME))); 
    }
  };

  @Override
  public void serialize(Configuration conf) {
    conf.set(CONF_PROPERTY_NAME, toString());
  }

  /** Read from conf */
  public static SummationWritable read(Configuration conf) {
    return CONF_VALUE_OF.valueOf(conf);
  }
}