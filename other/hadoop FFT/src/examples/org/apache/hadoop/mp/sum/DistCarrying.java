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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mp.DistMpBase;
import org.apache.hadoop.mp.Function;
import org.apache.hadoop.mp.FunctionDescriptor;
import org.apache.hadoop.mp.ZahlenDescriptor;
import org.apache.hadoop.mp.ZahlenSerialization;
import org.apache.hadoop.mp.ZahlenSerialization.ZahlenOutputFormat;
import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.DataSerializable;

public class DistCarrying extends DistComponentwiseOp {
  public static final String VERSION = "20110128b";

  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;
  static final boolean IS_VERBOSE = PRINT_LEVEL.is(Print.Level.VERBOSE);

  public static final String NAME = DistCarrying.class.getSimpleName().toLowerCase();
  public static final String DESCRIPTION = "Distributed Carrying";
  
  public DistCarrying(SchonhageStrassen schonhagestrassen, PowerOfTwo_int J,
      PowerOfTwo_int K, String dir) {
    super(schonhagestrassen, J, K, dir);
  }

  public static class CarryingMapper
      extends Mapper<NullWritable, ZahlenDescriptor, IntWritable, CarryingDescriptor.W> {
    protected void map(final NullWritable nullwritable, final ZahlenDescriptor y,
        final Context context) throws IOException, InterruptedException {
      //initialize
      final DistMpBase.TaskHelper helper = new DistMpBase.TaskHelper(context,
          NAME + ".VERSION = " + VERSION);
    
      final Configuration conf = context.getConfiguration();
      final DistMpBase parameters = DistMpBase.valueOf(conf);
      parameters.printDetail(getClass().getSimpleName());
      helper.show("init: y=" + y);
    
      //read carries, differences
      final int nWorkers = conf.getInt(N_WORKERS_CONF_KEY, 16);
      final JavaUtil.WorkGroup workers = new JavaUtil.WorkGroup(
          getClass().getSimpleName(), nWorkers, null);
      final CarryDifference.Reader cdreader = new CarryDifference.Reader(
          y, parameters.dir, conf);
      final CarryDifference[] cds = cdreader.readAll(helper.timer, workers);
      helper.show("CarryDifference: cds.length = " + cds.length);

      //compute actual carries
      final byte[] carries = new byte[cds.length];
      carries[0] = cds[0].c;
      for(int t = 1; t < cds.length; t++) {
        carries[t] = cds[t].c;
        if (carries[t-1] >= cds[t].d)
          carries[t]++;
      }
      helper.show("Actuall carries: c.length = " + carries.length);

      //write output
      final byte[] c = new byte[y.elementsPerPart];
      final IntWritable index = new IntWritable();
      final CarryingDescriptor.W baw = new CarryingDescriptor.W();
      baw.set(new CarryingDescriptor(y, c));
      for(int i = 0; i < y.numParts.value; i++) {
        int k = i;
        for(int j = 0; j < c.length; j++) {
          c[j] = carries[k];
          k += y.numParts.value;
        }
        index.set((i + 1) & y.numParts.mask);
        context.write(index, baw);
      }
      helper.show(getClass().getSimpleName());
    }
  }

  public static class CarryingReducer
      extends Reducer<IntWritable, CarryingDescriptor.W, IntWritable, ZahlenSerialization.E> {
    @Override
    protected final void reduce(final IntWritable index,
        final Iterable<CarryingDescriptor.W> writables,
        final Context context) throws IOException, InterruptedException {
      //initialize
      final DistMpBase.TaskHelper helper = new DistMpBase.TaskHelper(context,
          NAME + ".VERSION = " + VERSION);
    
      final Configuration conf = context.getConfiguration();
      final DistMpBase parameters = DistMpBase.valueOf(conf);
      parameters.printDetail(getClass().getSimpleName());
    
      final int k = index.get();
      helper.show("init: k=" + k);
    
      //read inputs
      final Iterator<CarryingDescriptor.W> baw = writables.iterator();
      final CarryingDescriptor descriptor = baw.next().get();
      if (baw.hasNext()) {
        throw new IOException("Not a singleton list.");
      }
      final ZahlenDescriptor r = descriptor.remainder;
      final Zahlen.Element[] y = r.readOutput(k, parameters.dir, conf);
      helper.show("read input: " + r.output + ", length=" + y.length);
    
      //carrying
      if (k == 0) {
        for(int j = 1; j < y.length; j++)
          parameters.schonhagestrassen.plusEqual(y[j], descriptor.carries[j - 1]);
      } else {
        for(int j = 0; j < y.length; j++)
          parameters.schonhagestrassen.plusEqual(y[j], descriptor.carries[j]);
      }
      
      helper.show("carrying");

      //write output
      DistMpBase.TaskHelper.write(y, k, r.numParts, r.elementsPerPart, context);
      helper.show(getClass().getSimpleName());
    }
  }
  
  public static class CarryingDescriptor implements DataSerializable<Void> {
    final ZahlenDescriptor remainder;
    final byte[] carries;

    CarryingDescriptor(final ZahlenDescriptor remainder, final byte[] carries) {
      this.remainder = remainder;
      this.carries = carries;
    }
    
    @Override
    public Void serialize(DataOutput out) throws IOException {
      remainder.serialize(out);
      out.writeInt(carries.length);
      out.write(carries);
      return null;
    }
    
    public static class W extends DataSerializable.W<CarryingDescriptor> {
      @Override
      public void readFields(DataInput in) throws IOException {
        final ZahlenDescriptor remainder = ZahlenDescriptor.valueOf(in);
        final byte[] carries = new byte[in.readInt()];
        in.readFully(carries);
        set(new CarryingDescriptor(remainder, carries));
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class CarryingSplit extends InputSplit implements Writable {
    private static final String[] EMPTY = new String[0];

    private ZahlenDescriptor var;
    
    public CarryingSplit() {}
    CarryingSplit(final ZahlenDescriptor var) {
      this.var = var;
    }
    
    @Override
    public long getLength() {return 1;}
    @Override
    public String[] getLocations() {return EMPTY;}
    @Override
    public final void readFields(DataInput in) throws IOException {
      var = ZahlenDescriptor.valueOf(in);
    }
    @Override
    public final void write(DataOutput out) throws IOException {
      var.serialize(out);
    }
  }

  public static class CarryingInputFormat
      extends InputFormat<NullWritable, ZahlenDescriptor> {
    @Override
    public final RecordReader<NullWritable, ZahlenDescriptor> createRecordReader(
        InputSplit generic, TaskAttemptContext context) {
      final CarryingSplit split = (CarryingSplit)generic;
      
      //return a record reader
      return new RecordReader<NullWritable, ZahlenDescriptor>() {
        private boolean done = false;
        
        /** {@inheritDoc} */
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {}
        /** {@inheritDoc} */
        @Override
        public boolean nextKeyValue() {return !done ? done = true : false;}
        /** {@inheritDoc} */
        @Override
        public NullWritable getCurrentKey() {return NullWritable.get();}
        /** {@inheritDoc} */
        @Override
        public ZahlenDescriptor getCurrentValue() {return split.var;}
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
      if (variables.size() != 1) {
        throw new IOException("variables.size() != 1, " + var);
      }
    
      final List<InputSplit> splits = new ArrayList<InputSplit>(var.numParts.value);
      final ZahlenDescriptor d = new ZahlenDescriptor(variables.get(0), var.numParts, var.elementsPerPart);
      splits.add(new CarryingSplit(d));
      return splits;
    }
  }
  
  protected Job newJob(final Configuration conf) throws IOException {
    final Job job = super.newJob(conf);
    job.setJarByClass(DistCarrying.class);

    // setup mapper
    job.setMapperClass(CarryingMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(CarryingDescriptor.W.class);

    // setup reducer
    job.setReducerClass(CarryingReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(ZahlenSerialization.E.class);
    job.setNumReduceTasks(J.value);

    // setup input
    job.setInputFormatClass(CarryingInputFormat.class);
    job.setOutputFormatClass(ZahlenOutputFormat.class);
    return job; 
  }

  @Override
  protected String jobName(final FunctionDescriptor f) {
    return dir + ": " + f.output + " = carry(" + f.functionString() + ")";
  }
}