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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.DataSerializable;

public class ZahlenSerialization {
  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;

  public static class E extends DataSerializable.W<Zahlen.Element> {
    public E() {}

    @Override
    public void readFields(final DataInput in) throws IOException {
      final Zahlen.Element e = get();
      if (e != null)
        e.reclaim();
      set(valueOf(in));
    }
    
    static Zahlen.Element valueOf(final DataInput in) throws IOException {
      return Zahlen.ELEMENT_FACTORY.valueOf(in);
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class Part implements DataSerializable<Void> {
    private final Zahlen.Element[] a;
    public final int index;
    
    public Part(final int index, final Zahlen.Element... a) {
      this.a = a;
      this.index = index;
    }
    
    public Zahlen.Element get(int i) {return a[i];}

    @Override
    public Void serialize(DataOutput out) throws IOException {
      out.writeInt(a.length);
      for(int i = 0; i < a.length; i++) {
        a[i].serialize(out);
      }
      out.writeInt(index);
      return null;
    }

    public static class W extends DataSerializable.W<Part> {
      public W() {}
      @Override
      public void readFields(DataInput in) throws IOException {
        final Zahlen.Element[] a = new Zahlen.Element[in.readInt()];
        for(int i = 0; i < a.length; i++) {
          a[i] = Zahlen.ELEMENT_FACTORY.valueOf(in);
        }
        final int k0 = in.readInt();
        set(new Part(k0, a));
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class ZahlenSplit extends InputSplit implements Writable {
    private FunctionDescriptor var;
    private int k0;
    private String[] locations;
    
    public ZahlenSplit() {}
    ZahlenSplit(final FunctionDescriptor var, final int k0, final String[] locations) {
      this.var = var;
      this.k0 = k0;
      this.locations = locations;
    }
    
    @Override
    public long getLength() {return 1;}
    @Override
    public String[] getLocations() {return locations;}
    @Override
    public final void readFields(DataInput in) throws IOException {
      var = FunctionDescriptor.valueOf(in);
      k0 = in.readInt();
    }
    @Override
    public final void write(DataOutput out) throws IOException {
      var.serialize(out);
      out.writeInt(k0);
    }
  }

  public static class ZahlenInputFormat
      extends InputFormat<IntWritable, FunctionDescriptor> {
    @Override
    public final RecordReader<IntWritable, FunctionDescriptor> createRecordReader(
        InputSplit generic, TaskAttemptContext context) {
      final ZahlenSplit split = (ZahlenSplit)generic;
      
      //return a record reader
      return new RecordReader<IntWritable, FunctionDescriptor>() {
        private boolean done = false;
        
        /** {@inheritDoc} */
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {}
        /** {@inheritDoc} */
        @Override
        public boolean nextKeyValue() {return !done ? done = true : false;}
        /** {@inheritDoc} */
        @Override
        public IntWritable getCurrentKey() {return new IntWritable(split.k0);}
        /** {@inheritDoc} */
        @Override
        public FunctionDescriptor getCurrentValue() {return split.var;}
        /** {@inheritDoc} */
        @Override
        public float getProgress() {return done? 1f: 0f;}
        /** {@inheritDoc} */
        @Override
        public void close() {}
      };
    }

    static final String[] EMPTY = {};

    @Override
    public List<InputSplit> getSplits(JobContext context
        ) throws IOException, InterruptedException {
      final Configuration conf = context.getConfiguration();
      final FunctionDescriptor var = FunctionDescriptor.valueOf(conf);
      final List<Function.Variable> variables = var.input.getVariables();
      
      if (variables.isEmpty()) {
        throw new IOException("variables.isEmpty(), " + var);
      }

      final DistMpBase parameters = DistMpBase.valueOf(conf);
      final String name = variables.get(0).getName();
      final Path vardir = new Path(parameters.dir, name);
      final FileSystem fs = vardir.getFileSystem(conf);
      final FileStatus[] statuses = fs.listStatus(vardir);
      Arrays.sort(statuses, CMP);

      final List<InputSplit> splits = new ArrayList<InputSplit>(var.numParts.value);
      for(int k0 = 0; k0 < var.numParts.value; k0++) {
        final String filename = FunctionDescriptor.getFileName(name, k0);
        final int i = Arrays.binarySearch(statuses, filename, CMP);
        if (i < 0) {
          throw new IOException("File not found: filename=" + filename
              + ", vardir=" + vardir);
        }
        final BlockLocation[] locations = fs.getFileBlockLocations(statuses[i], 0L, 1L);
        final String[] hosts = locations.length == 0? EMPTY: locations[0].getHosts();
        splits.add(new ZahlenSplit(var, k0, hosts));
      }
      return splits;
    }
    
    private static final Comparator<Object> CMP = new Comparator<Object>() {
      @Override
      public int compare(final Object l, final Object r) {
        return object2String(l).compareTo(object2String(r));
      }
    };
    private static String object2String(final Object obj) {
      return obj instanceof String? (String)obj: ((FileStatus)obj).getPath().getName(); 
    }
  }

  public static class ZahlenOutputFormat
      extends SequenceFileOutputFormat<IntWritable, E> {
    private static String NAME = ZahlenOutputFormat.class.getSimpleName().toLowerCase();
    private static String SPLIT_DIGITS_PER_ELEMENT_CONF_KEY = NAME + ".split.digitsPerElement";
    private static String SPLIT_N_SUMMANDS_CONF_KEY = NAME + ".split.nSummands";

    public static void setSplit(final int digitsPerElement, final int nSummands,
        final Configuration conf) {
      if (nSummands < 1) {
        throw new IllegalArgumentException(nSummands + " = nSummands < 1");
      }
      conf.setInt(SPLIT_N_SUMMANDS_CONF_KEY, nSummands);
      conf.setInt(SPLIT_DIGITS_PER_ELEMENT_CONF_KEY, digitsPerElement);
    }
    
    public static String toSplitName(final Function.Variable var, final int i) {
      return var.getName() + "_" + i;
    }

    public static Function.Variable[] toSplitVariables(final Function.Variable var, final int m) {
      final Function.Variable[] parts = new Function.Variable[m];
      for(int i = 0; i < parts.length; i++) {
        parts[i] = Function.Variable.valueOf(toSplitName(var, i));
      }
      return parts;
    }

    public ZahlenOutputFormat() {}

    @Override
    public RecordWriter<IntWritable, E> getRecordWriter(
        final TaskAttemptContext context) throws IOException, InterruptedException {
      final Configuration conf = context.getConfiguration();
      final int nSummands = conf.getInt(SPLIT_N_SUMMANDS_CONF_KEY, 1);
      final ZahlenDescriptor var = ZahlenDescriptor.valueOf(conf);
      final int part = context.getTaskAttemptID().getTaskID().getId();
      final Path workpath = ((FileOutputCommitter)getOutputCommitter(context)).getWorkPath();
      final FileSystem fs = workpath.getFileSystem(conf);

      if (nSummands == 1) {
        final String name = ZahlenDescriptor.getFileName(var.output.getName(), part);
        final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, 
            new Path(workpath, name), IntWritable.class, E.class,
            CompressionType.NONE, null, context);

        return new RecordWriter<IntWritable, E>() {
          @Override
          public void write(IntWritable key, E value) throws IOException {
            out.append(key, value);
          }
          @Override
          public void close(TaskAttemptContext context) throws IOException {out.close();}
        };
      } else {
        final SequenceFile.Writer[] out = new SequenceFile.Writer[nSummands];

        for(int s = 0; s < out.length; s++) {
          final String name = toSplitName(var.output, s);
          final Path parent = new Path(workpath, name);
          fs.mkdirs(parent);
          final String f = ZahlenDescriptor.getFileName(name, (part + s) & var.numParts.mask);

          out[s] = SequenceFile.createWriter(fs, conf, new Path(parent, f),
              IntWritable.class, E.class,
              CompressionType.NONE, null, context);
        }
      
        final int D = var.elementsPerPart << var.numParts.exponent;
        final int digitsPerElement = conf.getInt(SPLIT_DIGITS_PER_ELEMENT_CONF_KEY, 0);

        return new RecordWriter<IntWritable, E>() {
          private boolean first = true;

          @Override
          public void write(IntWritable key, E value) throws IOException {
            Zahlen.Element q = value.get();
            final int startkey = key.get();

            //insert zeros
            if (first) {
              first = false;
              final int d = part + out.length - var.numParts.value;
              if (d > 0) {
                value.set(q.get().newElement());
                for(int k = 0; k < d; k++) {
                  key.set(k);
                  out[out.length - d + k].append(key, value);
                }
              }
            }

            //write output
            final int limit = Math.min(out.length, D - startkey);
            for(int i = 0; i < limit; i++) {
              final Zahlen.Element r = q;
              q = r.divideRemainderEqual(digitsPerElement);
              key.set(startkey + i);
              out[i].append(key, value.set(r));
              r.reclaim();
            }
          }

          @Override
          public void close(TaskAttemptContext context) throws IOException { 
            for(int i = 0; i < out.length; i++)
              out[i].close();
          }
        };
      }
    }
  }
}