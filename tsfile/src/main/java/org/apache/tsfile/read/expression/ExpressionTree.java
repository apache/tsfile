package org.apache.tsfile.read.expression;

import org.apache.tsfile.read.filter.basic.Filter;

public interface ExpressionTree {
  boolean satisfy(Object value);
  Filter toFilter();
}
