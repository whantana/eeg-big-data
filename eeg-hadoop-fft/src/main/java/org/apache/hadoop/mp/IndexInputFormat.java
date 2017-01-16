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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class IndexInputFormat extends InputFormat<IntWritable, NullWritable> {
  private static final String PREFIX = IndexInputFormat.class.getName();
  private static final String N_PARTS =  PREFIX + ".nParts";

  public static class IndexSplit extends InputSplit implements Writable {
    private final static String[] EMPTY = {};
    
    protected int index;
    
    public IndexSplit() {}
    IndexSplit(int index) {this.index = index;}

    @Override
    public long getLength() {return 1;}
    @Override
    public String[] getLocations() {return EMPTY;}

    @Override
    public final void readFields(DataInput in) throws IOException {
      index = in.readInt();
    }
    @Override
    public final void write(DataOutput out) throws IOException {
      out.writeInt(index);
    }
  }

  /** Specify how to read the records */
  @Override
  public final RecordReader<IntWritable, NullWritable> createRecordReader(
      InputSplit generic, TaskAttemptContext context) {
    final IndexSplit split = (IndexSplit)generic;

    //return a record reader
    return new RecordReader<IntWritable, NullWritable>() {
      private boolean done = false;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {}
      @Override
      public boolean nextKeyValue() {return !done ? done = true : false;}
      @Override
      public IntWritable getCurrentKey() {return new IntWritable(split.index);}
      @Override
      public NullWritable getCurrentValue() {return NullWritable.get();}
      @Override
      public float getProgress() {return done? 1f: 0f;}
      @Override
      public void close() {}
    };
  }

  @Override
  public final List<InputSplit> getSplits(final JobContext context) {
    final int nParts = context.getConfiguration().getInt(N_PARTS, 0);
    final List<InputSplit> splits = new ArrayList<InputSplit>(nParts);
    for(int i = 0; i < nParts; i++)
      splits.add(new IndexSplit(i));
    return splits;
  }
  
  public static void setNumParts(final int nParts, final Configuration conf) {
    conf.setInt(N_PARTS, nParts);
  }
}