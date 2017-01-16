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
package org.apache.hadoop.mp.gmp;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.hadoop.mp.math.PowerOfTwo_int;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.Print;

public class GmpMultiplier implements Zahlen.Multiplier, Closeable {
  public static final PowerOfTwo_int BASE = PowerOfTwo_int.values()[4];

  private static final String CMD = File.separatorChar == '/'? "./gmp_mult": "gmp_mult.exe";

  private static final ThreadLocal<GmpMultiplier> t = new ThreadLocal<GmpMultiplier>() {
    @Override
    protected GmpMultiplier initialValue() {
      try {
        return new GmpMultiplier();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
  
  public static GmpMultiplier get() {return t.get();}

  private final String name;
  private int count = 0;

  private final Process process;
  private final PrintStream stdin;
  private final InputStream stdout;

  private final BufferedReader stderr;
  private final Thread messager;
  
  private GmpMultiplier() throws IOException {
    this.name = Thread.currentThread().getName();

    final String[] cmd = {CMD, "" + BASE.value};
    Print.println(this + ": cmd=" + Arrays.asList(cmd));

    this.process = new ProcessBuilder(cmd).start();
    this.stdin = new PrintStream(process.getOutputStream(), true); 
    this.stdout = process.getInputStream();

    this.stderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    this.messager = new Thread() {
      @Override
      public void run() {
        Print.println(GmpMultiplier.this + ": messager started.");
        try {
          for(String line; !isInterrupted() && (line = stderr.readLine()) != null; ) {
            Print.println(GmpMultiplier.this + ": " + line);
          }
        } catch(IOException ioe) {
          Print.println(this);
          Print.printStackTrace(ioe);
        } finally {
          Print.println(GmpMultiplier.this + ": messager terminated.");
        }
      }
    };
    messager.start();
  }

  @Override
  public void close() throws IOException {
    messager.interrupt();
    process.destroy();
    stdin.close();
    stdout.close();
    stderr.close();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + name + ", count=" + count + ")";
  }
  
  @Override
  public synchronized Zahlen.Element multiply(
      final Zahlen.Element l, final Zahlen.Element r) {
    count++;
    l.serialize(stdin).println();
    r.serialize(stdin).println();
    try {
      return l.get().valueOf(stdout);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
