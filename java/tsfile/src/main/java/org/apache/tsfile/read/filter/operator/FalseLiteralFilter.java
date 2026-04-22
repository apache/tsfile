/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read.filter.operator;

import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.utils.Binary;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FalseLiteralFilter extends Filter {
  @Override
  public boolean satisfy(long time, Object value) {
    return false;
  }

  @Override
  public boolean satisfyBoolean(long time, boolean value) {
    return false;
  }

  @Override
  public boolean satisfyInteger(long time, int value) {
    return false;
  }

  @Override
  public boolean satisfyLong(long time, long value) {
    return false;
  }

  @Override
  public boolean satisfyFloat(long time, float value) {
    return false;
  }

  @Override
  public boolean satisfyDouble(long time, double value) {
    return false;
  }

  @Override
  public boolean satisfyBinary(long time, Binary value) {
    return false;
  }

  @Override
  public boolean satisfyString(long time, String value) {
    return false;
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    return false;
  }

  @Override
  public boolean satisfyBooleanRow(long time, boolean[] values) {
    return false;
  }

  @Override
  public boolean satisfyIntegerRow(long time, int[] values) {
    return false;
  }

  @Override
  public boolean satisfyLongRow(long time, long[] values) {
    return false;
  }

  @Override
  public boolean satisfyFloatRow(long time, float[] values) {
    return false;
  }

  @Override
  public boolean satisfyDoubleRow(long time, double[] values) {
    return false;
  }

  @Override
  public boolean satisfyBinaryRow(long time, Binary[] values) {
    return false;
  }

  @Override
  public boolean[] satisfyTsBlock(TsBlock tsBlock) {
    return new boolean[tsBlock.getPositionCount()];
  }

  @Override
  public boolean[] satisfyTsBlock(boolean[] selection, TsBlock tsBlock) {
    return new boolean[selection.length];
  }

  @Override
  public boolean canSkip(IMetadata metadata) {
    return true;
  }

  @Override
  public boolean allSatisfy(IMetadata metadata) {
    return false;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return false;
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return false;
  }

  @Override
  public List<TimeRange> getTimeRanges() {

    return Collections.emptyList();
  }

  @Override
  public Filter reverse() {
    return null;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.FALSE_LITERAL;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof FalseLiteralFilter;
  }

  @Override
  public int hashCode() {
    return Objects.hash(FalseLiteralFilter.class);
  }
}
