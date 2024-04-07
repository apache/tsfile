package org.apache.tsfile.read.expression;

public interface ExpressionTree {
  boolean satisfy(Object value);
}
