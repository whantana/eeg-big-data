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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;


/** Utility methods */
public class Print {
  public enum Level {
    TRACE, DEBUG, VERBOSE, INFO;
    
    public boolean is(Level that) {
      return this.ordinal() <= that.ordinal();
    }
  }

  /** Standard output stream */
  public static final PrintStream out = System.out;
  public static AtomicBoolean isOutEnabled = new AtomicBoolean(true);

  /** Log file output stream */
  public static PrintStream log = null;
  
  private static String prefix = "";
  private static Stack<String> prefixStack = new Stack<String>();
  
  static void appendPrefix(String s) {
    prefixStack.push(prefix);
    prefix += s;
  }

  private static void appendPrefix() {
    appendPrefix("  ");
  }

  private static void restorePrefix() {
    prefix = prefixStack.pop();
  }

  public static File initLogFile(String filename) throws FileNotFoundException {
    if (log != null)
      throw new RuntimeException("fout != null");
    final File f = JavaUtil.createFile(null, filename, ".log");
    log = new PrintStream(new FileOutputStream(f), true);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (log != null)
          log.close();
      }
    });
    return f;
  }

  public static void closeFileOutput(){
    log.close();
    log = null;
  }

  public static void print() {
    if (log != null)
      log.print(prefix);
    if (isOutEnabled.get() && out != log)
      out.print(prefix);
  }

  public static void print(Object obj) {
    if (log != null) {
      log.print(obj);
      log.flush();
    }
    if (isOutEnabled.get() && out != log) {
      out.print(obj);
      out.flush();
    }
  }

  public static void println() {
    if (log != null)
      log.println(prefix);
    if (isOutEnabled.get() && out != log)
      out.println(prefix);
  }

  public static void println(final Object obj) {
    final String t = obj.toString();
    for(int i = 0; i < t.length(); ) {
      final int j = t.indexOf('\n', i);
      final String s;
      if (j >= 0) {
        s = t.substring(i, j);
        i = j + 1;
      } else {
        s = t.substring(i);
        i = t.length();
      }

      if (log != null) {
        log.print(prefix);
        log.println(s);
      }
      if (isOutEnabled.get() && out != log) {
        out.print(prefix);
        out.println(s);
      }
    }
  }

  public static <T> void print(final Object name, final T[] x) {
    if (x == null) {
      println(name + " = null");
    } else if (x.length == 0) {
      println(name + " = <empty>");
    } else {
      beginIndentation(name + " = [#=" + x.length);
      for(int i = 0; i < x.length; i++) {
        println(i + ": " + x[i]);
      }
      endIndentation("]");
    }
  }

  public static void beginIndentation(Object obj) {
    if (prefix.isEmpty())
      println();
    println(obj);
    appendPrefix();
  }

  public static void endIndentation(Object obj) {
    endIndentation();
    println(obj);
  }
  public static void endIndentation() {
    restorePrefix();
  }

  public static void printStackTrace(final Throwable t) {
    if (log != null)
      t.printStackTrace(log);
    if (isOutEnabled.get() && out != log)
      t.printStackTrace(out);
  }

  public static String printSystemInfo() {
    final String[] names = {
        "java.runtime.name",
        "java.runtime.version",
        "java.vm.version",
        "java.vm.vendor",
        "java.vm.name",
        "java.vm.info",
        "java.vm.specification.version",
        "os.arch",
        "os.name",
        "os.version",
        "sun.cpu.isalist",
        "sun.arch.data.model",
    };
    int width = names[0].length();
    for(int i = 1; i < names.length; i++) {
      int n = names[i].length();
      if (n > width)
        width = n;
    }
    final String f = "\n%" + width + "s = ";

    final Properties p = System.getProperties();
    for(String n : names) {
      Print.print(String.format(f, n) + p.getProperty(n));
    }
    Print.println();

    final Runtime r = Runtime.getRuntime();
    Print.print(String.format(f, "availableProcessors") + r.availableProcessors());
    
    printMemoryInfo(f);
    return f;
  }

  public static String printMemoryInfo() {
    final String f = "\n%20s = ";
    return printMemoryInfo(f);    
  }

  private static String printMemoryInfo(final String format) {
    final String f = format + "%sB";
    final Runtime r = Runtime.getRuntime();
    Print.print(String.format(f, "maxMemory", Parse.TraditionalBinaryPrefix.long2string(r.maxMemory(), 2)));
    Print.print(String.format(f, "totalMemory", Parse.TraditionalBinaryPrefix.long2string(r.totalMemory(), 2)));
    Print.print(String.format(f, "freeMemory", Parse.TraditionalBinaryPrefix.long2string(r.freeMemory(), 2)));
    Print.println();
    Print.println();
    return f;
  }

  public static interface Brief {
    /** Return a brief description of the object. */
    public String toBrief();
  }

  public static interface Detail {
    /** Print a detail description of the object. */
    public void printDetail(final String firstlineprefix);
  }
}