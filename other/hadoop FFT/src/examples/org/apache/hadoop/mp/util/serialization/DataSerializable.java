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
package org.apache.hadoop.mp.util.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mp.util.Container;

/** Serialize to {@link DataOutput} */
public interface DataSerializable<R> {
  /** Serialize to {@link DataOutput} */
  public R serialize(final DataOutput out) throws IOException;

  /** Value of {@link DataInput} */
  public static interface ValueOf<R> {
    /** Value of {@link DataInput} */
    public R valueOf(DataInput in) throws IOException; 
  }
  
  public static abstract class W<T extends DataSerializable<?>>
      implements Container<T>, Writable {
    private T item;

    public final W<T> set(T item) {
      this.item = item;
      return this;
    }
    @Override
    public final T get() {return item;}
    @Override
    public final void write(DataOutput out) throws IOException {item.serialize(out);}
  }
}
