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

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;


public class Parse {
  /** Covert milliseconds to a String. */
  public static String millis2String(long n) {
    if (n < 0)
      return "-" + millis2String(-n);
    else if (n < 1000)
      return n + "ms";

    final StringBuilder b = new StringBuilder();
    final int millis = (int)(n % 1000L);
    if (millis != 0)
      b.append(String.format(".%03d", millis)); 
    if ((n /= 1000) < 60)
      return b.insert(0, n).append("s").toString();

    b.insert(0, String.format(":%02d", (int)(n % 60L)));
    if ((n /= 60) < 60)
      return b.insert(0, n).toString();

    b.insert(0, String.format(":%02d", (int)(n % 60L)));
    if ((n /= 60) < 24)
      return b.insert(0, n).toString();

    b.insert(0, n % 24L);
    final int days = (int)((n /= 24) % 365L);
    b.insert(0, days == 1? " day ": " days ").insert(0, days);
    if ((n /= 365L) > 0)
      b.insert(0, n == 1? " year ": " years ").insert(0, n);

    return b.toString();
  }

  /** Covert milliseconds to a String. */
  public static String millis2String(double t, final int decimal) {
    if (t < 0)
      return "-" + millis2String(-t, decimal);
    else { 
      final String unit;
      if (t < 1000)
        unit = "ms";
      else if ((t /= 1000) < 60)
        unit = (t <= 1? "second": "seconds");
      else if ((t /= 60) < 60)
        unit = (t <= 1? "minute": "minutes");
      else if ((t /= 60) < 24)
        unit = (t <= 1? "hour": "hours");
      else if ((t /= 24) < 365)
        unit = (t <= 1? "day": "days");
      else {
        t /= 365;
        unit = (t <= 1? "year": "years");
      }
      return String.format("%." + decimal + "f ", t) + unit;
    }
  }

  public static int char2int(int d) {
    if (Character.isDigit(d)) {
      return d - '0';
    } else if (Character.isLowerCase(d)) {
      return d - 'a' + 10;
    } else if (Character.isUpperCase(d)) {
      return d - 'A' + 10;
    } else {
      throw new IllegalArgumentException("Unexpected character: " + (char)d
          + " (code=" + d + ")");
    }
  }
  
  public static int symbol2int(char c) {
    c = Character.toLowerCase(c);
    if (c == 'k')
      return 1000;
    if (c == 'm')
      return 1000000;
    if (c == 'b')
      return 1000000000;
    throw new IllegalArgumentException("Unknown symbol c = " + c);
  }


  /** Parse a variable. */  
  public static Boolean parseBooleanVariable(final String name, final String s) {
    return string2Boolean(parseStringVariable(name, s));
  }

  /** Covert a String to a Boolean. */
  public static Boolean string2Boolean(String s) {
    if ("null".equals(s))
      return null;
    else if ("true".equalsIgnoreCase(s))
      return Boolean.TRUE;
    else if ("false".equalsIgnoreCase(s))
      return Boolean.FALSE;

    throw new IllegalArgumentException("Cannot parse s = " + s);
  }

  /** Covert a String to a long.  
   * This support comma separated number format.
   */
  public static Long string2long(String s) {
    if (s.equals("null"))
      return null;

    s = s.trim().replace(",", "");
    final int lastpos = s.length() - 1;
    final char lastchar = s.charAt(lastpos);
    if (Character.isDigit(lastchar))
      return Long.parseLong(s);
    else {
      final long v = symbol2int(lastchar) * Long.parseLong(s.substring(0, lastpos));
      if (v < 0)
        throw new IllegalArgumentException("v < 0 || v > Integer.MAX_VALUE, v="
            + v + ", s=" + s);
      return v;
    }
  }

  /** Covert a String to a long.  
   * This support comma separated number format.
   */
  public static Integer string2integer(String s) {
    if (s.equals("null"))
      return null;

    s = s.trim().replace(",", "");
    final int lastpos = s.length() - 1;
    final char lastchar = s.charAt(lastpos);
    if (Character.isDigit(lastchar))
      return Integer.parseInt(s);
    else {
      final int v = symbol2int(lastchar) * Integer.parseInt(s.substring(0, lastpos));
      if (v < 0)
        throw new IllegalArgumentException("v < 0 || v > Integer.MAX_VALUE, v="
            + v + ", s=" + s);
      return v;
    }
  }

  /**
   * The traditional binary prefixes, kilo, mega, ..., exa,
   * which can be represented by a 64-bit integer.
   * TraditionalBinaryPrefix symbol are case insensitive. 
   */
  public static enum TraditionalBinaryPrefix {
    KILO(1024),
    MEGA(KILO.value << 10),
    GIGA(MEGA.value << 10),
    TERA(GIGA.value << 10),
    PETA(TERA.value << 10),
    EXA(PETA.value << 10);

    public final long value;
    public final char symbol;

    TraditionalBinaryPrefix(long value) {
      this.value = value;
      final char c = name().charAt(0);
      this.symbol = c == 'K'? 'k': c;
    }

    /**
     * @return The TraditionalBinaryPrefix object corresponding to the symbol.
     */
    public static TraditionalBinaryPrefix valueOf(char symbol) {
      symbol = Character.toUpperCase(symbol);
      for(TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
        if (symbol == prefix.symbol) {
          return prefix;
        }
      }
      throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
    }

    /**
     * Convert a string to long.
     * The input string is first be trimmed
     * and then it is parsed with traditional binary prefix.
     *
     * For example,
     * "-1230k" will be converted to -1230 * 1024 = -1259520;
     * "891g" will be converted to 891 * 1024^3 = 956703965184;
     *
     * @param s input string
     * @return a long value represented by the input string.
     */
    public static long string2long(String s) {
      s = s.trim();
      final int lastpos = s.length() - 1;
      final char lastchar = s.charAt(lastpos);
      if (Character.isDigit(lastchar))
        return Long.parseLong(s);
      else {
        long prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
        long num = Long.parseLong(s.substring(0, lastpos));
        if (num > (Long.MAX_VALUE/prefix) || num < (Long.MIN_VALUE/prefix)) {
          throw new IllegalArgumentException(s + " does not fit in a Long");
        }
        return num * prefix;
      }
    }

    public static String long2string(final long n, final int decimal) {
      if (n < 0)
        return "-" + long2string(-n, decimal);
      else if (n < 1024L)
        return String.valueOf(n);
      else {
        final TraditionalBinaryPrefix[] v = TraditionalBinaryPrefix.values();
        int i = 1;
        for(; i < v.length && n >= v[i].value; i++);
        i--;
        return String.format("%." + decimal + "f %c", n/(double)v[i].value, v[i].symbol);
      }
    }
  }

  public static String bigInteger2Brief(final BigInteger x) {
    String s = x.toString(16);
    if (s.length() > 100)
      s = s.substring(0, 100) + " ...";
    return s;
  }

  /** Covert a long to a String in comma separated number format. */  
  public static String long2string(long n) {
    if (n < 0)
      return "-" + long2string(-n);
    
    final StringBuilder b = new StringBuilder();
    for(; n >= 1000; n = n/1000)
      b.insert(0, String.format(",%03d", n % 1000));
    return n + b.toString();    
  }

  /** Covert an array of long to a String. */  
  public static String longArray2string(long[] a) {
    if (a.length == 0)
      return "[]";

    final StringBuilder b = new StringBuilder("[");
    b.append(a[0]);
    for(int i = 1; i < a.length; i++)
      b.append(", ").append(a[i]);
    return b.append("]").toString();
  }

  /** Covert an array of long to a String. */  
  public static String byteArray2string(byte[] a) {
    if (a.length == 0)
      return "[]";

    final StringBuilder b = new StringBuilder("[");
    b.append(String.format("%02X", a[0]));
    for(int i = 1; i < a.length; i++)
      b.append(String.format(", %02X", a[i]));
    return b.append("]").toString();
  }

  /** Covert a String to an array of long. */  
  public static long[] string2longArray(String s) {
    s = s.substring(1, s.length() - 1).replace(", ", ":");
    final StringTokenizer t = new StringTokenizer(s);
    final long[] a = new long[t.countTokens()];
    for(int i = 0; t.hasMoreTokens(); i++)
      a[i] = Long.parseLong(t.nextToken());
    return a;
  }

  /** Covert a String to an array of long. */  
  public static List<Long> string2longList(String s) {
    final List<Long> a = new ArrayList<Long>();
    s = s.substring(1, s.length() - 1).replace(", ", " ");
    for(final StringTokenizer t = new StringTokenizer(s); t.hasMoreTokens(); )
      a.add(Long.parseLong(t.nextToken()));
    return a;
  }

  /** Parse a variable. */  
  public static int parseIntVariable(final String name, final String s) {
    return Integer.parseInt(parseStringVariable(name, s));
  }

  /** Parse a variable. */  
  public static Long parseLongVariable(final String name, final String s) {
    return string2long(parseStringVariable(name, s));
  }

  /** Parse a variable. */  
  public static String parseStringVariable(final String name, final String s) {
    if (!s.startsWith(name + '='))
      throw new IllegalArgumentException("!s.startsWith(name + '='), name="
          + name + ", s=" + s);
    return s.substring(name.length() + 1);
  }

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
  public static String time2String(final long t) { 
    return DATE_FORMAT.format(new Date(t));
  }

  public static String currentTime2String() { 
    return time2String(System.currentTimeMillis());
  }

  /** Covert an array to a String. */  
  public static <T> String array2string(T[] a) {
    if (a == null)
      return "null";
    else if (a.length == 0)
      return "[]";
    else {
      final StringBuilder b = new StringBuilder("[#=").append(a.length)
          .append(": ").append(a[0]);
      for(int i = 1; i < a.length; i++)
        b.append(", ").append(a[i]);
      return b.append("]").toString();
    }
  }

  /** Covert an array to a String. */  
  public static <T> String list2string(List<T> a) {
    if (a == null)
      return "null";
    else if (a.size() == 0)
      return "[]";
    else {
      final StringBuilder b = new StringBuilder("[");
      for(int i = 0; i < a.size(); i++)
        b.append("\n  ").append(a.get(i));
      return b.append("\n]").toString();
    }
  }

  public static String description2brief(final String[] ... descriptions) {
    final StringBuilder list = new StringBuilder();
    for(String[] d: descriptions)
      for(String s: d)
        list.append(' ').append(s.substring(0, s.indexOf('>') + 1));
    return "" + list;      
  }

  public static String description(final String[] ... descriptions) {
    final StringBuilder description = new StringBuilder();
    for(String[] d: descriptions)
      for(String s: d)
        description.append("\n  ").append(s);
    return "" + description;      
  }
}