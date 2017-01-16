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
package org.apache.hadoop.mp.sum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.mp.DistMpBase;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenDescriptor;
import org.apache.hadoop.mp.ZahlenSerialization;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.serialization.DataSerializable;

public class CarryDifference implements DataSerializable<Void> {
  final byte c;
  final byte d;

  CarryDifference(final byte c, final byte d) {
    this.c = c;
    this.d = d;
  }

  @Override
  public Void serialize(DataOutput out) throws IOException {
    out.write(c);
    out.write(d);
    return null;
  }

  public static class CD extends DataSerializable.W<CarryDifference> {
    public CD() {}
    @Override
    public void readFields(DataInput in) throws IOException {
      final byte c = in.readByte();
      final byte d = in.readByte();
      set(new CarryDifference(c, d));
    }
  }

  public static class Remainder extends CarryDifference {
    final Zahlen.Element r;

    Remainder(final Zahlen.Element r, final int c, final int d) {
      super(JavaUtil.toByte(c), JavaUtil.toByte(d));
      this.r = r;
    }

    CarryDifference toCarryDifference() {return new CarryDifference(c, d);}

    @Override
    public Void serialize(DataOutput out) throws IOException {
      r.serialize(out);
      return super.serialize(out);
    }

    public static class W extends DataSerializable.W<Remainder> {
      public W() {}
      @Override
      public void readFields(DataInput in) throws IOException {
        final Zahlen.Element r = Zahlen.ELEMENT_FACTORY.valueOf(in);
        final byte c = in.readByte();
        final byte d = in.readByte();
        set(new Remainder(r, c, d));
      }
    }
  }

  static String getCarryDifferenceFileName(final String varname) {
    return varname + "_cd";
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class CdOutputFormat
      extends SequenceFileOutputFormat<IntWritable, Remainder.W> {
    public CdOutputFormat() {}
    
    @Override
    public RecordWriter<IntWritable, Remainder.W> getRecordWriter(
        final TaskAttemptContext context) throws IOException, InterruptedException {
      final Configuration conf = context.getConfiguration();
      final ZahlenDescriptor var = ZahlenDescriptor.valueOf(conf);
      final int part = context.getTaskAttemptID().getTaskID().getId();
      final Path workpath = ((FileOutputCommitter)getOutputCommitter(context)).getWorkPath();
      final FileSystem fs = workpath.getFileSystem(conf);
    
      final String name = ZahlenDescriptor.getFileName(var.output.getName(), part);
      final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, 
          new Path(workpath, name), IntWritable.class, ZahlenSerialization.E.class,
          CompressionType.NONE, null, context);
      final String name_cd = getCarryDifferenceFileName(name);
      final SequenceFile.Writer out_cd = SequenceFile.createWriter(fs, conf, 
          new Path(workpath, name_cd), IntWritable.class, CD.class,
          CompressionType.NONE, null, context);
    
      return new RecordWriter<IntWritable, Remainder.W>() {
        final ZahlenSerialization.E e = new ZahlenSerialization.E();
        final CD cd = new CD();
    
        @Override
        public void write(IntWritable key, Remainder.W value) throws IOException {
          final Remainder cdr = value.get();
          e.set(cdr.r);
          out.append(key, e);
          cd.set(cdr.toCarryDifference());
          out_cd.append(key, cd);
        }
        @Override
        public void close(TaskAttemptContext context) throws IOException {
          out.close();
          out_cd.close();
        }
      };
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class CdSplit extends InputSplit implements Writable {
    private FunctionDescriptor var;
    private int k0;
    private String[] locations;
    
    public CdSplit() {}
    CdSplit(final FunctionDescriptor var, final int k0, final String[] locations) {
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

  public static class CdInputFormat
      extends InputFormat<IntWritable, FunctionDescriptor> {
    @Override
    public final RecordReader<IntWritable, FunctionDescriptor> createRecordReader(
        InputSplit generic, TaskAttemptContext context) {
      final CdSplit split = (CdSplit)generic;
      
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
      Arrays.sort(statuses);
    
      final List<InputSplit> splits = new ArrayList<InputSplit>(var.numParts.value);
      for(int k0 = 0; k0 < var.numParts.value; k0++) {
        final FileStatus s = statuses[k0];
        if (!FunctionDescriptor.getFileName(name, k0).equals(s.getPath().getName())) {
          throw new IOException("Name mismatched");
        }
        final BlockLocation[] locations = fs.getFileBlockLocations(s, 0L, 1L);
        final String[] hosts = locations.length == 0? EMPTY: locations[0].getHosts();
        splits.add(new CdSplit(var, k0, hosts));
      }
      return splits;
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /** Read and evaluate a function. */
  static class Reader {
    private final CarryDifference[] carrydifferences;
    private final ZahlenDescriptor y;
    private final String dir;
    private final Configuration conf;
      
    Reader(final ZahlenDescriptor y, final String dir, final Configuration conf) {
      this.carrydifferences = new CarryDifference[y.elementsPerPart << y.numParts.exponent];
      this.y = y;
      this.dir = dir;
      this.conf = conf;
    }
    
    CarryDifference[] readAll(final JavaUtil.Timer timer, final JavaUtil.WorkGroup workers) {
      timer.tick("y = " + y);
      for(int i = 0; i < y.numParts.value; i++) {
        workers.submit("i=" + i, new Runner(i));
      }
      workers.waitUntilZero();

      return carrydifferences;
    }
    
    private class Runner implements Runnable {
      final int offset;

      Runner(final int offset) {this.offset = offset;}

      @Override
      public void run() {
        try {
          final String name = ZahlenDescriptor.getFileName(y.output.getName(), offset);
          final Path f = new Path(dir+Path.SEPARATOR+y.output.getName(), CarryDifference.getCarryDifferenceFileName(name));
          final FileSystem fs = f.getFileSystem(conf);
          final IntWritable key = new IntWritable();
          final CD cd = new CD();
  
          final SequenceFile.Reader in  = new SequenceFile.Reader(fs, f, conf);
          try {
            for(int i = 0; i < y.elementsPerPart; i++) {
              in.next(key, cd);
              final int index = (i << y.numParts.exponent) + offset;
              if (key.get() != index) {
                throw new IOException(key.get() + " = key.get() != i*numParts + offset = " + index
                    + ", i=" + i + ", + offset=" + offset + ", y=" + y);
              }
              carrydifferences[index] = cd.get();
            }
          } finally {
            in.close();
          }
        } catch(IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }
}