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
package org.apache.hadoop.mp;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.ConfSerializable;
import org.apache.hadoop.mp.util.serialization.DataSerializable;

/**
 * Describe an Zahlen object.
 */
public class ZahlenDescriptor implements ConfSerializable, DataSerializable<Void> {
  private static final String PREFIX = ZahlenDescriptor.class.getSimpleName();
  private static final String PROPERTY_OUTPUT = PREFIX + ".output";
  private static final String PROPERTY_NUM_PARTS = PREFIX + ".numParts";
  private static final String PROPERTY_ELEMENTS_PER_PART = PREFIX + ".elementsPerPart";

  public final PowerOfTwo_int numParts;
  public final int elementsPerPart;
  public final Function.Variable output;
  
  public ZahlenDescriptor(final Function.Variable output,
      final PowerOfTwo_int numParts, final int elementsPerPart) {
    this.numParts = numParts;
    this.elementsPerPart = elementsPerPart;
    this.output = output;
    
    if (numParts.value != elementsPerPart) {
      throw new IllegalArgumentException("numParts.value != elementsPerPart = "
          + elementsPerPart + ", numParts = " + numParts);
    }
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(numParts=" + numParts
    + ", elementsPerPart=" + elementsPerPart + ")";
  }

  @Override
  public void serialize(final Configuration conf) {
    conf.set(PROPERTY_OUTPUT, output.serialize());
    conf.set(PROPERTY_NUM_PARTS, numParts.serialize());
    conf.setInt(PROPERTY_ELEMENTS_PER_PART, elementsPerPart);
  }

  public static ZahlenDescriptor valueOf(final Configuration conf) {
    final Function.Variable output = Function.Variable.valueOf(conf.get(PROPERTY_OUTPUT));
    final PowerOfTwo_int numParts = PowerOfTwo_int.VALUE_OF_STR.valueOf(conf.get(PROPERTY_NUM_PARTS));
    final int elementsPerPart = conf.getInt(PROPERTY_ELEMENTS_PER_PART, 0);
    return new ZahlenDescriptor(output, numParts, elementsPerPart);
  }

  @Override
  public Void serialize(DataOutput out) throws IOException {
    output.serialize(out);
    numParts.serialize(out);
    out.writeInt(elementsPerPart);
    return null;
  }
  
  public static ZahlenDescriptor valueOf(DataInput in) throws IOException {
    final Function.Variable output = Function.Variable.valueOf(in);
    final PowerOfTwo_int numParts = PowerOfTwo_int.valueOf(in);
    final int elementsPerPart = in.readInt();
    return new ZahlenDescriptor(output, numParts, elementsPerPart);
  }

  public static String getFileName(final String name, final int part) {
    return name + String.format(".%05d", part);
  }

  static Path getFilePath(final String name, final int part, final String parent) {
    return new Path(parent+Path.SEPARATOR+name, getFileName(name, part));
  }

  public Path getOutputPath(String parent) {return new Path(parent, output.getName());}

  public Zahlen.Element[] readOutput(final int offset,
      final String parent, final Configuration conf) throws IOException {
    return read(output, offset, numParts, elementsPerPart, parent, conf);
  }

  static Map<Function.Variable, Zahlen.Element> read(
      final Map<Function.Variable, Zahlen.Element> m, final Function f,
      final SchonhageStrassen schonhagestrassen, final Zahlen largeZ,
      final String dir, final Configuration conf) throws IOException {
    if (f instanceof Function.Variable) {
      final Function.Variable var = (Function.Variable)f;
      final Zahlen.Element e = read(var, schonhagestrassen, largeZ, dir, conf);
      m.put(var, e);
    } else if (f instanceof Function.BinaryOperator) {
      final Function.BinaryOperator opr = (Function.BinaryOperator)f;
      read(m, opr.left, schonhagestrassen, largeZ, dir, conf);
      read(m, opr.right, schonhagestrassen, largeZ, dir, conf);
    } else {
      throw new UnsupportedOperationException("f.getClass() = " + f.getClass());
    }
    return m;
  }

  public static Zahlen.Element read(final Function.Variable var,
      final SchonhageStrassen schonhagestrassen, final Zahlen largeZ,
      final String dir, final Configuration conf) throws IOException {
    final PowerOfTwo_int K = PowerOfTwo_int.values()[schonhagestrassen.D.exponent >> 1];
    final PowerOfTwo_int J = PowerOfTwo_int.values()[schonhagestrassen.D.exponent - K.exponent];

    final Zahlen.Element[] array = new Zahlen.Element[schonhagestrassen.D.value];
    for(int j0 = 0; j0 < J.value; j0++) {
      final Zahlen.Element[] t = read(var, j0, J, K.value, dir, conf);
      for(int i = 0; i < t.length; i++)
        array[(i << J.exponent) + j0] = t[i];
    }
    schonhagestrassen.carrying(array);
    
    final Zahlen.Element combined = largeZ.newElement().combine(
        array, schonhagestrassen.digitsPerElement());
    for(int i = 0; i < array.length; i++)
      array[i].reclaim();
    return combined;
  }

  private static Zahlen.Element[] read(final Function.Variable var,
      final int offset, final PowerOfTwo_int step, final int numElements,
      final String parent, final Configuration conf) throws IOException {
    final Path f = getFilePath(var.getName(), offset, parent);
    if (ZahlenSerialization.PRINT_LEVEL.is(Print.Level.TRACE)) {
      Print.println("read(name=" + var
          + ", offset=" + offset
          + ", step=" + step
          + ", numElements=" + numElements
          + ", parent=" + parent
          + ")");
      Print.println("     f = " + f);
    }

    final FileSystem fs = f.getFileSystem(conf);
    final SequenceFile.Reader in  = new SequenceFile.Reader(fs, f, conf);
    final Zahlen.Element[] elements = new Zahlen.Element[numElements];
    final IntWritable key = new IntWritable();
    final ZahlenSerialization.E e = new ZahlenSerialization.E();
    try {
      for(int i = 0; i < elements.length; i++) {
        in.next(key, e);
        if (key.get() != (i << step.exponent) + offset)
          throw new IOException(key.get() + " = key.get() != i*step + offset, i="
              + i + ", + offset=" + offset + ", step=" + step);
        elements[i] = e.get();
      }
    } finally {
      in.close();
    }
    return elements;
  }

  static class Reader implements Closeable {
    final Function.Variable var;
    final int offset;
    final PowerOfTwo_int step;
    final int numElements;

    private final SequenceFile.Reader in;
    private final IntWritable key = new IntWritable();
    private final ZahlenSerialization.E e = new ZahlenSerialization.E();
    private int i = 0;
    
    Reader(final Function.Variable var, final int offset,
        final PowerOfTwo_int step, final int numElements,
        final String dir, final Configuration conf) throws IOException {
      this.var = var;
      this.offset = offset;
      this.step = step;
      this.numElements = numElements;

      final Path f = getFilePath(var.getName(), offset, dir);
      if (ZahlenSerialization.PRINT_LEVEL.is(Print.Level.TRACE)) {
        Print.println(getClass().getSimpleName() + "(var=" + var
            + ", offset=" + offset
            + ", step=" + step
            + ", dir=" + dir
            + ")");
        Print.println("     f = " + f);
      }

      final FileSystem fs = f.getFileSystem(conf);
      in  = new SequenceFile.Reader(fs, f, conf);
    }

    Zahlen.Element readNext() throws IOException {
      if (i == numElements)
        throw new IOException(i + " = i == numElements = " + numElements);

      in.next(key, e);
      if (key.get() != (i << step.exponent) + offset) {
        throw new IOException(key.get() + " = key.get() != i*step + offset, i="
            + i + ", + offset=" + offset + ", step=" + step);
      }
      i++;
      return e.get();
    }

    @Override
    public void close() throws IOException {
      in.close();      
    }
  }

  public static void write(final Function.Variable var, final Zahlen.Element[] elements,
      final int offset, final PowerOfTwo_int step, final int numElements,
      final String parent, final Configuration conf) throws IOException {
    final Path f = getFilePath(var.getName(), offset, parent);
    final FileSystem fs = f.getFileSystem(conf);
    final SequenceFile.Writer out  = SequenceFile.createWriter(fs, conf, f,
        IntWritable.class, ZahlenSerialization.E.class,
        SequenceFile.CompressionType.NONE);

    final IntWritable key = new IntWritable();
    final ZahlenSerialization.E value = new ZahlenSerialization.E();

    try {
      for(int k1 = 0; k1 < numElements; k1++) {
        final int i = (k1 << step.exponent) + offset;
        key.set(i);
        out.append(key, value.set(elements[i]));
      }
    } finally {
      out.close();
    }      
  }

  public static void write(final Function.Variable var, final Zahlen.Element x,
      final SchonhageStrassen schonhagestrassen,
      final String dir, final Configuration conf) throws IOException {
    final PowerOfTwo_int K = PowerOfTwo_int.values()[schonhagestrassen.D.exponent >> 1];
    final PowerOfTwo_int J = PowerOfTwo_int.values()[schonhagestrassen.D.exponent - K.exponent];
    final int digitsPerElement = schonhagestrassen.digitsPerElement();
    final Zahlen.Element[] elements = x.splitAndReclaim(digitsPerElement, schonhagestrassen);
    for(int k0 = 0; k0 < K.value; k0++) {
      write(var, elements, k0, K, J.value, dir, conf);
    }
    for(int i = 0; i < elements.length; i++) {
      elements[i].reclaim();
    }
  }
}