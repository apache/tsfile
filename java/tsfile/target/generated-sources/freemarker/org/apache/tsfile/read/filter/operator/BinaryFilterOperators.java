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

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.CANNOT_PUSH_DOWN_MSG;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.BinaryFilter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

  /*
* This class is generated using freemarker and the FilterOperatorsTemplate.ftl template.
*/
public final class BinaryFilterOperators {

  private BinaryFilterOperators() {
    // forbidden construction
  }

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = "constant cannot be null";

  private static final String OPERATOR_TO_STRING_FORMAT = "measurements[%s] %s %s";

  // base class for ValueEq, ValueNotEq, ValueLt, ValueGt, ValueLtEq, ValueGtEq
  abstract static class ValueColumnCompareFilter extends BinaryFilter {

    protected final Binary constant;

    protected ValueColumnCompareFilter(int measurementIndex, Binary constant) {
      super(measurementIndex);
      this.constant = Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG);
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnCompareFilter(ByteBuffer buffer) {
      super(buffer);
      this.constant = Objects.requireNonNull(ReadWriteIOUtils.readBinary(buffer), CONSTANT_CANNOT_BE_NULL_MSG);
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(constant, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnCompareFilter that = (ValueColumnCompareFilter) o;
      return Objects.equals(constant,that.constant);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), constant);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), constant);
    }
  }

  public static final class ValueEq extends ValueColumnCompareFilter {

    public ValueEq(int measurementIndex, Binary constant) {
      super(measurementIndex, constant);
    }

    public ValueEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      }
      else{
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return constant.equals(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueNotEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_EQ;
    }
  }

  public static final class ValueNotEq extends ValueColumnCompareFilter {

    public ValueNotEq(int measurementIndex, Binary constant) {
      super(measurementIndex, constant);
    }

    public ValueNotEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return !constant.equals(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NEQ;
    }
  }

  public static final class ValueLt extends ValueColumnCompareFilter {

    public ValueLt(int measurementIndex, Binary constant) {
      super(measurementIndex, constant);
    }

    public ValueLt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return constant.compareTo(value) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueGtEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LT;
    }
  }

  public static final class ValueLtEq extends ValueColumnCompareFilter {

    public ValueLtEq(int measurementIndex, Binary constant) {
      super(measurementIndex, constant);
    }

    public ValueLtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return constant.compareTo(value) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueGt(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LTEQ;
    }
  }

  public static final class ValueGt extends ValueColumnCompareFilter {

    public ValueGt(int measurementIndex, Binary constant) {
      super(measurementIndex, constant);
    }

    public ValueGt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return constant.compareTo(value) < 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueLtEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GT;
    }
  }

  public static final class ValueGtEq extends ValueColumnCompareFilter {

    public ValueGtEq(int measurementIndex, Binary constant) {
      super(measurementIndex, constant);
    }

    public ValueGtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return constant.compareTo(value) <= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueLt(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GTEQ;
    }
  }

  // base class for ValueBetweenAnd, ValueNotBetweenAnd
  abstract static class ValueColumnRangeFilter extends BinaryFilter {

    protected final Binary min;
    protected final Binary max;

    protected ValueColumnRangeFilter(int measurementIndex, Binary min, Binary max) {
      super(measurementIndex);
      this.min = Objects.requireNonNull(min,"min cannot be null");
      this.max = Objects.requireNonNull(max,"max cannot be null");
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnRangeFilter(ByteBuffer buffer) {
      super(buffer);
      this.min = Objects.requireNonNull(ReadWriteIOUtils.readBinary(buffer),"min cannot be null");
      this.max = Objects.requireNonNull(ReadWriteIOUtils.readBinary(buffer),"max cannot be null");
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(min, outputStream);
      ReadWriteIOUtils.write(max, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnRangeFilter that = (ValueColumnRangeFilter) o;
      return min.equals(that.min) && max.equals(that.max);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), min, max);
    }

    @Override
    public String toString() {
      return String.format(
          "measurements[%s] %s %s AND %s",
          measurementIndex, getOperatorType().getSymbol(), min, max);
    }
  }

  public static final class ValueBetweenAnd extends ValueColumnRangeFilter {

    public ValueBetweenAnd(int measurementIndex, Binary min, Binary max) {
      super(measurementIndex, min, max);
    }

    public ValueBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return min.compareTo(value) <= 0
          && max.compareTo(value) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueNotBetweenAnd(measurementIndex, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_BETWEEN_AND;
    }
  }

  public static final class ValueNotBetweenAnd extends ValueColumnRangeFilter {

    public ValueNotBetweenAnd(int measurementIndex, Binary min, Binary max) {
      super(measurementIndex, min, max);
    }

    public ValueNotBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return valueSatisfy((Binary) value);
      } else {
        return valueSatisfy(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return min.compareTo(value) > 0 || max.compareTo(value) < 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueBetweenAnd(measurementIndex, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_BETWEEN_AND;
    }
  }

  // base class for ValueIn, ValueNotIn
  abstract static class ValueColumnSetFilter extends BinaryFilter {

    protected final Set<Binary> candidates;

    protected final Binary candidatesMin;
    protected final Binary candidatesMax;

    protected ValueColumnSetFilter(int measurementIndex, Set<Binary> candidates) {
      super(measurementIndex);
      this.candidates = candidates;

      Set<Binary> filteredSet = candidates.stream().filter(Objects::nonNull).collect(Collectors.toSet());
      this.candidatesMin = !filteredSet.isEmpty() ? Collections.min(filteredSet) : null;
      this.candidatesMax = !filteredSet.isEmpty() ? Collections.max(filteredSet) : null;
    }

    protected ValueColumnSetFilter(ByteBuffer buffer) {
      super(buffer);
      boolean hasNull = ReadWriteIOUtils.readBoolean(buffer);
      this.candidates = ReadWriteIOUtils.readBinarySet(buffer);
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : null;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : null;
      if(hasNull){
        this.candidates.add(null);
      }
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(candidates.contains(null), outputStream);
      ReadWriteIOUtils.writeBinarySet(candidates, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnSetFilter that = (ValueColumnSetFilter) o;
      return candidates.equals(that.candidates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), candidates);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), candidates);
    }
  }

  public static final class ValueIn extends ValueColumnSetFilter {

    public ValueIn(int measurementIndex, Set<Binary> candidates) {
      super(measurementIndex, candidates);
    }

    public ValueIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return candidates.contains((Binary) value);
      } else {
        return candidates.contains(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return candidates.contains(value);
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      return false;
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public Filter reverse() {
      return new ValueNotIn(measurementIndex, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IN;
    }

    private boolean isAllNulls(Statistics<? extends Serializable> statistics) {
      return statistics.getCount() == 0;
    }

    private static boolean statisticsNotAvailable(Statistics<?> statistics) {
      return statistics.getType() == TSDataType.TEXT
          || statistics.getType() == TSDataType.BOOLEAN
          || statistics.getType() == TSDataType.BLOB
          || statistics.isEmpty();
    }
  }

  public static final class ValueNotIn extends ValueColumnSetFilter {

    public ValueNotIn(int measurementIndex, Set<Binary> candidates) {
      super(measurementIndex, candidates);
    }

    public ValueNotIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      if(value instanceof Binary){
        return !candidates.contains((Binary) value);
      } else {
        return !candidates.contains(new Binary(String.valueOf(value), StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return !candidates.contains(value);
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueIn(measurementIndex, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_IN;
    }
  }

  // base class for ValueRegex, ValueNotRegex
  abstract static class ValueColumnPatternMatchFilter extends BinaryFilter {

    protected final Pattern pattern;

    protected ValueColumnPatternMatchFilter(int measurementIndex, Pattern pattern) {
      super(measurementIndex);
      this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
    }

    protected ValueColumnPatternMatchFilter(ByteBuffer buffer) {
      super(buffer);
      this.pattern =
      Pattern.compile(
        Objects.requireNonNull(
        ReadWriteIOUtils.readString(buffer), "pattern cannot be null"));
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(pattern.pattern(), outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnPatternMatchFilter that = (ValueColumnPatternMatchFilter) o;
      return pattern.pattern().equals(that.pattern.pattern());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), pattern.pattern());
    }

    @Override
    public String toString() {
      return String.format(
      OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), pattern);
    }
  }

  public static final class ValueRegexp extends ValueColumnPatternMatchFilter {

    public ValueRegexp(int measurementIndex, Pattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueRegexp(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueNotRegexp(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_REGEXP;
    }
  }

  public static final class ValueNotRegexp extends ValueColumnPatternMatchFilter {

    public ValueNotRegexp(int measurementIndex, Pattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueNotRegexp(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueRegexp(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_REGEXP;
    }
  }

  // base class for ValueLike, ValueNotLike
  abstract static class ValueColumnPatternLikeMatchFilter extends BinaryFilter {

    protected final LikePattern pattern;

    protected ValueColumnPatternLikeMatchFilter(int measurementIndex, LikePattern pattern) {
      super(measurementIndex);
      this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
    }

    protected ValueColumnPatternLikeMatchFilter(ByteBuffer buffer) {
      super(buffer);
      this.pattern =
          LikePattern.compile(
              ReadWriteIOUtils.readString(buffer),
              ReadWriteIOUtils.readBool(buffer)
                  ? Optional.of(ReadWriteIOUtils.readString(buffer).charAt(0))
                  : Optional.empty());
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(pattern.getPattern(), outputStream);
      if(pattern.getEscape().isPresent()){
        ReadWriteIOUtils.write(true, outputStream);
        ReadWriteIOUtils.write(pattern.getEscape().get().toString(), outputStream);
      }
      else{
        ReadWriteIOUtils.write(false, outputStream);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnPatternLikeMatchFilter that = (ValueColumnPatternLikeMatchFilter) o;
      return pattern.equals(that.pattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), pattern);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), pattern);
    }
  }

  public static final class ValueLike extends ValueColumnPatternLikeMatchFilter {

    public ValueLike(int measurementIndex, LikePattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueLike(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return pattern.getMatcher().match(value.toString().getBytes());
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return pattern.getMatcher().match(value.toString().getBytes());
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueNotLike(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LIKE;
    }
  }

  public static final class ValueNotLike extends ValueColumnPatternLikeMatchFilter {

    public ValueNotLike(int measurementIndex, LikePattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueNotLike(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return !pattern.getMatcher().match(value.toString().getBytes());
    }

    @Override
    public boolean valueSatisfy(Binary value) {
      return !pattern.getMatcher().match(value.toString().getBytes());
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueLike(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_LIKE;
    }
  }

  private static class AccessCount {
    private int count;
    private final int accessThreshold =
        TSFileDescriptor.getInstance().getConfig().getPatternMatchingThreshold();

    public void check() throws IllegalStateException {
      if (this.count++ > accessThreshold) {
        throw new IllegalStateException("Pattern access threshold exceeded");
      }
    }
  }

  private static class MatcherInput implements CharSequence {

    private final CharSequence value;

    private final AccessCount access;

    public MatcherInput(CharSequence value, AccessCount access) {
      this.value = value;
      this.access = access;
    }

    @Override
    public char charAt(int index) {
      this.access.check();
      return this.value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return new MatcherInput(this.value.subSequence(start, end), this.access);
    }

    @Override
    public int length() {
      return this.value.length();
    }

    @Override
    public String toString() {
      return this.value.toString();
    }
  }
}
