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
package org.apache.hadoop.mp.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.hadoop.mp.util.serialization.DataSerializable;



public class Checksum {
  private static final ThreadLocal<Checksum> CHECKSUM = new ThreadLocal<Checksum>() {
    protected Checksum initialValue() {return new Checksum();}
  };

  public static Checksum getChecksum() {return CHECKSUM.get();}

  private final MessageDigest md5;
  private final ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

  private Checksum() {
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void update() {
    buffer.limit(buffer.position()).position(0);
    md5.update(buffer);
    buffer.limit(buffer.capacity()).position(0);
  }

  public long update(final long i) {
    if (buffer.remaining() < 8)
      update();
    buffer.putLong(i);
    return i;
  }
  
  public int update(final int i) {
    if (buffer.remaining() < 4)
      update();
    buffer.putInt(i);
    return i;
  }

  public boolean update(final boolean b) {
    if (buffer.remaining() < 1)
      update();
    buffer.put(b? (byte)1: (byte)0);
    return b;
  }

  public void update(final int[] integers, final int start, final int end) {
    for(int i = start; i < end; i++)
      update(integers[i]);
  }

  public byte[] update(final byte[] bytes) {
    update();
    md5.update(bytes);
    return bytes;
  }

  public byte[] update(final DataSerializable<?> ios) throws IOException {
    //serialize the object
    final ByteArrayOutputStream ba = new ByteArrayOutputStream();
    final DataOutputStream baout = new DataOutputStream(ba);
    ios.serialize(baout);
    baout.flush();
    
    //update checksum
    return update(ba.toByteArray());
  }

  private byte[] digest() {
    if (buffer.position() > 0)
      update();
    return md5.digest();
  }

  public void writeDigest(final DataOutput out) throws IOException {
    final byte[] d = digest();
    out.writeInt(d.length);
    out.write(d);
  }

  /** Read a checksum from in and then verify with this. */
  public void readAndVerify(final DataInput in) throws IOException {
    final byte[] checksum = new byte[in.readInt()];
    in.readFully(checksum);

    if (!Arrays.equals(digest(), checksum))
      throw new IOException("Checksums not matched");
  }
}