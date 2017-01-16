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
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mp.math.SchonhageStrassen;
import org.apache.hadoop.mp.math.Zahlen;
import org.apache.hadoop.mp.util.JavaUtil;
import org.apache.hadoop.mp.util.Print;
import org.apache.hadoop.mp.util.serialization.DataSerializable;
import org.apache.hadoop.mp.util.serialization.StringSerializable;


public abstract class Function implements StringSerializable, DataSerializable<Void> {
  static final String SEPARATOR = " ";
  static final Parser PARSER = new Parser();
  static final Print.Level PRINT_LEVEL = Print.Level.VERBOSE;

  /** Return the name of this function */
  public abstract String getName();

  public List<Variable> getVariables() {
    final List<Variable> variables = new ArrayList<Variable>();
    getVariablesRecursively(variables);
    return variables;
  }

  private void getVariablesRecursively(final List<Variable> variables) {
    if (this instanceof Variable) {
      variables.add((Variable)this);
    } else if (this instanceof UnaryOperator) {
      final UnaryOperator opr = (UnaryOperator)this;
      opr.fun.getVariablesRecursively(variables);
    } else if (this instanceof BinaryOperator) {
      final BinaryOperator opr = (BinaryOperator)this;
      opr.left.getVariablesRecursively(variables);
      opr.right.getVariablesRecursively(variables);
    }
  }

  /** Evaluate this function over Schonhage-Strassen ring. */
  public abstract Zahlen.Element ssEvaluate(
      final Map<Function.Variable, Zahlen.Element> m,
      final SchonhageStrassen schonhagestrassen);
  
  /** Evaluate this function */
  public abstract Zahlen.Element evaluate(
      final Map<Function.Variable, Zahlen.Element> m,
      final JavaUtil.WorkGroup workers);

  @Override
  public final Void serialize(DataOutput out) throws IOException {
    Text.writeString(out, serialize());
    return null;
  }

  /** Infix notation */
  public abstract String infix(boolean parenthesis);
  @Override
  public final String toString() {return infix(false);}
  ///////////////////////////////////////////////////////////////////////////
  public static final class Variable extends Function implements Comparable<Variable> {
    private final String name;
    
    private Variable(String name) {this.name = name;}
    
    @Override
    public String getName() {return name;}
    @Override
    public int compareTo(Variable that) {return this.name.compareTo(that.name);}
    @Override
    public int hashCode() {return name.hashCode();}

    @Override
    public final Zahlen.Element ssEvaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final SchonhageStrassen schonhagestrassen) {
      return m.get(this);
    }
    @Override
    public final Zahlen.Element evaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final JavaUtil.WorkGroup workers) {
      return m.get(this);
    }

    @Override
    public String infix(boolean parenthesis) {return name;}
    @Override
    public String serialize() {return name;}

    public static Variable valueOf(DataInput in) throws IOException {
      return valueOf(Text.readString(in));
    }
    
    static final String VALID_SPECIAL_CHARACTERS = "_'";

    public static Variable valueOf(String name) {
      boolean valid = Character.isLetter(name.charAt(0));
      int i = 1;
      for(; valid && i < name.length(); ) {
        final char c = name.charAt(i++);
        valid = VALID_SPECIAL_CHARACTERS.indexOf(c) >= 0
            || Character.isLetter(c) || Character.isDigit(c);
      }
      if (!valid) {
        throw new IllegalArgumentException("\"" + name
            + "\" is not valid variable name, i=" + i);
      }
      return new Variable(name);
    }
  }
  ///////////////////////////////////////////////////////////////////////////
  public static interface OperatorSymbol extends DataSerializable<Void> {
    String getValue();

    String getAcronym();
  }

  public static enum UnaryOperatorSymbol implements OperatorSymbol {
    SQUARE("^2", "square"),
    SQRT("sqrt", "sqrt")
    ;
    
    public final String value;
    public final String acronym;
    UnaryOperatorSymbol(String symbol, String acronym) {
      this.value = symbol;
      this.acronym = acronym;
    }
    
    public static UnaryOperatorSymbol string2operator(final String s) {
      for(UnaryOperatorSymbol symbol : values()) {
        if (s.equals(symbol.value))
          return symbol;
      }
      return null;
    }

    @Override
    public String getValue() {return value;}
    @Override
    public String getAcronym() {return acronym;}
    @Override
    public Void serialize(DataOutput out) throws IOException {
      Text.writeString(out, value);
      return null;
    }
  }
  
  public static enum BinaryOperatorSymbol implements OperatorSymbol {
    ADDITION("+", "plus"),
    MUPLICATION("**", "mult");
    
    public final String value;
    public final String acronym;
    BinaryOperatorSymbol(String symbol, String acronym) {
      this.value = symbol;
      this.acronym = acronym;
    }
    
    public static BinaryOperatorSymbol string2operator(final String s) {
      for(BinaryOperatorSymbol symbol : values()) {
        if (s.equals(symbol.value))
          return symbol;
      }
      return null;
    }

    @Override
    public String getValue() {return value;}
    @Override
    public String getAcronym() {return acronym;}
    @Override
    public Void serialize(DataOutput out) throws IOException {
      Text.writeString(out, value);
      return null;
    }
  }
  ///////////////////////////////////////////////////////////////////////////
  static abstract class Operator extends Function {
    public final OperatorSymbol symbol;
    Operator(final OperatorSymbol symbol) {this.symbol = symbol;}
  }

  static abstract class UnaryOperator extends Operator {
    final Function fun;

    UnaryOperator(final UnaryOperatorSymbol symbol, Function fun) {
      super(symbol);
      this.fun = fun;      
    }
    
    @Override
    public String getName() {
      return symbol.getAcronym() + "_" + fun.getName();
    }
    @Override
    public final String serialize() {
      return symbol.getValue() + SEPARATOR + fun.serialize();
    }
  }

  public static class Square extends UnaryOperator {
    final AtomicInteger count = new AtomicInteger(0);

    public Square(Function fun) {
      super(UnaryOperatorSymbol.SQUARE, fun);
    }

    @Override
    public String infix(boolean parenthesis) {
      final String s = fun.infix(true) + symbol.getValue();
      return parenthesis? "(" + s + ")": s;
    }

    @Override
    public final Zahlen.Element ssEvaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final SchonhageStrassen schonhagestrassen) {
      final Zahlen.Element E = fun.ssEvaluate(m, schonhagestrassen);

      final int c = count.getAndIncrement();
      if (c % 256 == 0) {
        System.out.println(getClass().getSimpleName() + ": c=" + c);
        System.out.println("    E = " + E.toBrief());
        if (c == 0) {
          System.out.println("    over " + E.get());
        }
      }
      schonhagestrassen.multiplyEqual(E, E);
      return E;
    }

    @Override
    public final Zahlen.Element evaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final JavaUtil.WorkGroup workers) {
      final Zahlen.Element E = fun.evaluate(m, workers);
      E.multiplyEqual(E, workers);
      if (PRINT_LEVEL.is(Print.Level.VERBOSE)) {
        Print.print(getClass().getSimpleName() + ": " + E.toBrief());
      }
      return E;
    }
  }
  
  public static class Sqrt extends UnaryOperator {
    final AtomicInteger count = new AtomicInteger(0);

    public Sqrt(Function fun) {
      super(UnaryOperatorSymbol.SQRT, fun);
    }

    @Override
    public String infix(boolean parenthesis) {
      final String s = symbol.getValue() + "(" + fun.infix(false) + ")";
      return parenthesis? "(" + s + ")": s;
    }

    @Override
    public final Zahlen.Element ssEvaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final SchonhageStrassen schonhagestrassen) {
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " does not support ssEvaluate(..)");
    }

    @Override
    public final Zahlen.Element evaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final JavaUtil.WorkGroup workers) {
      final Zahlen.Element E = fun.evaluate(m, workers);
      E.approximateSqrtReciprocalEqual(workers);
      if (PRINT_LEVEL.is(Print.Level.VERBOSE)) {
        Print.print(getClass().getSimpleName() + ": " + E.toBrief());
      }
      return E;
    }
  }

  static abstract class BinaryOperator extends Operator {
    final Function left;
    final Function right;

    BinaryOperator(BinaryOperatorSymbol symbol, Function left, Function right) {
      super(symbol);
      this.left = left;
      this.right = right;
    }

    @Override
    public String getName() {
      return left.getName() + "_" + symbol.getAcronym() + "_" + right.getName();
    }
    @Override
    public String infix(boolean parenthesis) {
      final String s = left.infix(true) + SEPARATOR + symbol.getValue()
          + SEPARATOR + right.infix(true);
      return parenthesis? "(" + s + ")": s;
    }
    @Override
    public final String serialize() {
      return symbol.getValue() + SEPARATOR + left.serialize() + SEPARATOR + right.serialize();
    }
  }

  public static class Multiplication extends BinaryOperator {
    final AtomicInteger count = new AtomicInteger(0);

    public Multiplication(Function left, Function right) {
      super(BinaryOperatorSymbol.MUPLICATION, left, right);
    }

    @Override
    public final Zahlen.Element ssEvaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final SchonhageStrassen schonhagestrassen) {
      final Zahlen.Element L = left.ssEvaluate(m, schonhagestrassen);
      final Zahlen.Element R = right.ssEvaluate(m, schonhagestrassen);

      final int c = count.getAndIncrement();
      if (c % 256 == 0) {
        System.out.println(getClass().getSimpleName() + ": c=" + c);
        System.out.println("    L = " + L.toBrief());
        System.out.println("    R = " + R.toBrief());
        if (c == 0) {
          System.out.println("    over " + L.get());
        }
      }
      schonhagestrassen.multiplyEqual(L, R);

      R.reclaim();
      return L;
    }

    @Override
    public final Zahlen.Element evaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final JavaUtil.WorkGroup workers) {
      final Zahlen.Element L = left.evaluate(m, workers);
      final Zahlen.Element R = right.evaluate(m, workers);
      L.multiplyEqual(R, workers);
      if (PRINT_LEVEL.is(Print.Level.VERBOSE)) {
        Print.print(getClass().getSimpleName() + ": " + L.toBrief());
      }
      return L;
    }
  }

  public static class Addition extends BinaryOperator {
    public Addition(Function left, Function right) {
      super(BinaryOperatorSymbol.ADDITION, left, right);
    }

    @Override
    public final Zahlen.Element ssEvaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final SchonhageStrassen schonhagestrassen) {
      final Zahlen.Element L = left.ssEvaluate(m, schonhagestrassen);
      final Zahlen.Element R = right.ssEvaluate(m, schonhagestrassen);

      schonhagestrassen.plusEqual(L, R);

      R.reclaim();
      return L;
    }

    @Override
    public final Zahlen.Element evaluate(
        final Map<Function.Variable, Zahlen.Element> m,
        final JavaUtil.WorkGroup workers) {
      final Zahlen.Element L = left.evaluate(m, workers);
      final Zahlen.Element R = right.evaluate(m, workers);
      L.plusEqual(R);
      if (PRINT_LEVEL.is(Print.Level.VERBOSE)) {
        Print.print(getClass().getSimpleName() + ": " + L.toBrief());
      }
      return L;
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  public static class Parser implements StringSerializable.ValueOf<Function>,
      DataSerializable.ValueOf<Function> {
    @Override
    public Function valueOf(final String s) {
      final StringTokenizer t = new StringTokenizer(s, SEPARATOR);
      final Function f = parse(t);
      if (t.hasMoreTokens()) {
        throw new IllegalArgumentException("t.hasMoreTokens()=" + t.hasMoreTokens()
            + ", s=" + s + ", f=" + f);
      }        
      return f;
    }

    private Function parse(final StringTokenizer t) {
      final String s = t.nextToken();
      final UnaryOperatorSymbol uos = UnaryOperatorSymbol.string2operator(s);
      if (uos != null) {
        final Function fun = parse(t);
        if (uos == UnaryOperatorSymbol.SQUARE) {
          return new Square(fun);
        } else if (uos == UnaryOperatorSymbol.SQRT) {
          return new Sqrt(fun);
        }
      }

      final BinaryOperatorSymbol bos = BinaryOperatorSymbol.string2operator(s);
      if (bos != null) {
        final Function left = parse(t);
        final Function right = parse(t);
        if (bos == BinaryOperatorSymbol.ADDITION) {
          return new Addition(left, right);
        } else if (bos == BinaryOperatorSymbol.MUPLICATION) {
          return new Multiplication(left, right);
        }
      }
      
      if (uos == null && bos == null) {
        return Variable.valueOf(s);
      }

      throw new IllegalArgumentException("parse failed, bos=" + bos
          + ", s=" + s + ", t=" + t);
    }

    @Override
    public Function valueOf(DataInput in) throws IOException {
      return valueOf(Text.readString(in));
    }
  }
  
  public static void main(String[] args) {
    String s = "+ + c'_0 c'_1 c'_2";
    Print.println("s = " + s);
    Function f = new Parser().valueOf(s);
    Print.println("f = " + f);
  }
}
