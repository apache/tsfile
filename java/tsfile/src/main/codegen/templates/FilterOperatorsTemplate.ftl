<@pp.dropOutputFile />
<#list filters as filter>
  <#assign className = "${filter.javaBoxName}FilterOperators">
  <@pp.changeOutputFile name="/org/apache/tsfile/read/filter/operator/${className}.java" />
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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.DisableStatisticsValueFilter;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.ValueFilter;
import org.apache.tsfile.read.filter.basic.OperatorType;
<#if filter.dataType == "Binary">
import org.apache.tsfile.utils.Binary;
</#if>
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
public final class ${className} {

  private ${className}() {
    // forbidden construction
  }

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = "constant cannot be null";
  public static final String CANNOT_PUSH_DOWN_MSG = " operator can not be pushed down.";

  private static final String OPERATOR_TO_STRING_FORMAT = "measurements[%s] %s %s";

  // base class for ValueEq, ValueNotEq, ValueLt, ValueGt, ValueLtEq, ValueGtEq
  abstract static class ValueColumnCompareFilter extends ValueFilter {

    protected final ${filter.dataType} constant;

    protected ValueColumnCompareFilter(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex);
      <#if filter.dataType == "Binary">
      this.constant = Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG);
      <#else>
      this.constant = constant;
      </#if>
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnCompareFilter(ByteBuffer buffer) {
      super(buffer);
      this.constant = ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer);
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
      return constant == that.constant;
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

    public ValueEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return constant.equals(value);
      <#else>
      return constant == value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      // drop if value < min || value > max
      return constant < (${filter.dataType}) statistics.getMinValue()
          || constant > (${filter.dataType}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return constant == (${filter.dataType}) statistics.getMinValue()
          && constant == (${filter.dataType}) statistics.getMaxValue();
      </#if>
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

    public ValueNotEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueNotEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return !constant.equals(value);
      <#else>
      return constant != value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      // drop if this is a column where min = max = value
      return constant == (${filter.dataType}) statistics.getMinValue()
          && constant == (${filter.dataType}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return constant < (${filter.dataType}) statistics.getMinValue()
          || constant > (${filter.dataType}) statistics.getMaxValue();
      </#if>
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

    public ValueLt(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueLt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) > 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) > 0;
      <#else>
      return constant > value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      // drop if value <= min
      return constant <= (${filter.dataType}) statistics.getMinValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return constant > (${filter.dataType}) statistics.getMaxValue();
      </#if>
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

    public ValueLtEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueLtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) >= 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) >= 0;
      <#else>
      return constant >= value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      // drop if value < min
      return constant < (${filter.dataType}) statistics.getMinValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return constant >= (${filter.dataType}) statistics.getMaxValue();
      </#if>
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

    public ValueGt(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueGt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) < 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) < 0;
      <#else>
      return constant < value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      // drop if value >= max
      return constant >= (${filter.dataType}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return constant < (${filter.dataType}) statistics.getMinValue();
      </#if>
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

    public ValueGtEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueGtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) <= 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) <= 0;
      <#else>
      return constant <= value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      // drop if value > max
      return constant > (${filter.dataType}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return constant <= (${filter.dataType}) statistics.getMinValue();
      </#if>
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
  abstract static class ValueColumnRangeFilter extends ValueFilter {

    protected final ${filter.dataType} min;
    protected final ${filter.dataType} max;

    protected ValueColumnRangeFilter(int measurementIndex, ${filter.dataType} min, ${filter.dataType} max) {
      super(measurementIndex);
      <#if filter.dataType == "Binary">
      this.min = Objects.requireNonNull(min,CONSTANT_CANNOT_BE_NULL_MSG);
      this.max = Objects.requireNonNull(max,CONSTANT_CANNOT_BE_NULL_MSG);
      <#else>
      this.min = min;
      this.max = max;
      </#if>

    }

    @SuppressWarnings("unchecked")
    protected ValueColumnRangeFilter(ByteBuffer buffer) {
      super(buffer);
      this.min = ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer);
      this.max = ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer);
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
      return min == that.min && max == that.max;
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

    public ValueBetweenAnd(int measurementIndex, ${filter.dataType} min, ${filter.dataType} max) {
      super(measurementIndex, min, max);
    }

    public ValueBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(min,value) <= 0
          && Boolean.compare(max,value) >= 0;
      <#elseif filter.dataType == "Binary">
      return min.compareTo(value) <= 0
          && max.compareTo(value) >= 0;
      <#else>
      return min <= value && max >= value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return (${filter.dataType}) statistics.getMaxValue() < min || (${filter.dataType}) statistics.getMinValue() > max;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return (${filter.dataType}) statistics.getMinValue() >= min && (${filter.dataType}) statistics.getMaxValue() <= max;
      </#if>
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

    public ValueNotBetweenAnd(int measurementIndex, ${filter.dataType} min, ${filter.dataType} max) {
      super(measurementIndex, min, max);
    }

    public ValueNotBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(min,value) > 0
          || Boolean.compare(max,value) < 0;
      <#elseif filter.dataType == "Binary">
      return min.compareTo(value) > 0 || max.compareTo(value) < 0;
      <#else>
      return min > value || max < value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return (${filter.dataType}) statistics.getMinValue() >= min && (${filter.dataType}) statistics.getMaxValue() <= max;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
      return false;
      <#else>
      return (${filter.dataType}) statistics.getMinValue() > max || (${filter.dataType}) statistics.getMaxValue() < min;
      </#if>
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

  // we have no statistics available, we cannot drop any blocks
  private static boolean statisticsNotAvailable(Statistics<?> statistics) {
    return statistics.getType() == TSDataType.TEXT
        || statistics.getType() == TSDataType.BOOLEAN
        || statistics.getType() == TSDataType.BLOB
        || statistics.isEmpty();
  }

  // TODO
  // base class for ValueIsNull and ValueIsNotNull
  abstract static class ValueCompareNullFilter extends ValueFilter {

    protected ValueCompareNullFilter(int measurementIndex) {
      super(measurementIndex);
    }

    protected ValueCompareNullFilter(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public String toString() {
      return String.format("measurements[%s] %s", measurementIndex, getOperatorType().getSymbol());
    }
  }

  // TODO
  // ValueIsNull can not be pushed down
  public static final class ValueIsNull extends ValueCompareNullFilter {

    public ValueIsNull(int measurementIndex) {
      super(measurementIndex);
    }

    public ValueIsNull(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public Filter reverse() {
      return new ValueIsNotNull(measurementIndex);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IS_NULL;
    }
  }

  // ValueIsNotNull are only used in ValueFilter
  public static final class ValueIsNotNull extends ValueCompareNullFilter {

    public ValueIsNotNull(int measurementIndex) {
      super(measurementIndex);
    }

    public ValueIsNotNull(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return value != null;
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return value != null;
      <#else>
      return true;
      </#if>
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      if (!statistics.isPresent()) {
        // the measurement isn't in this block so all values are null.
        // null is always equal to null
        return true;
      }

      // we are looking for records where v notEq(null)
      // so, if this is a column of all nulls, we can drop it
      return isAllNulls(statistics.get());
    }

    @Override
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      if (!statistics.isPresent()) {
        // block cannot match
        return false;
      }

      return !metadata.hasNullValue(measurementIndex);
    }

    @Override
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public Filter reverse() {
      return new ValueIsNull(measurementIndex);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IS_NOT_NULL;
    }

    private boolean isAllNulls(Statistics<? extends Serializable> statistics) {
      return statistics.getCount() == 0;
    }
  }

  // base class for ValueIn, ValueNotIn
  abstract static class ValueColumnSetFilter extends ValueFilter {

    protected final Set<${filter.javaBoxName}> candidates;
    protected final ${filter.dataType} candidatesMin;
    protected final ${filter.dataType} candidatesMax;

    protected ValueColumnSetFilter(int measurementIndex, Set<${filter.javaBoxName}> candidates) {
      super(measurementIndex);
      this.candidates = Objects.requireNonNull(candidates, "candidates cannot be null");
      <#if filter.dataType == "boolean">
      this.candidatesMin = false;
      this.candidatesMax = false;
      <#elseif filter.dataType == "Binary">
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : null;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : null;
      <#else>
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : ${filter.javaBoxName}.MIN_VALUE;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : ${filter.javaBoxName}.MAX_VALUE;
      </#if>
    }

    protected ValueColumnSetFilter(ByteBuffer buffer) {
      super(buffer);
      candidates = ReadWriteIOUtils.readObjectSet(buffer);
      <#if filter.dataType == "boolean">
      this.candidatesMin = false;
      this.candidatesMax = false;
      <#elseif filter.dataType == "Binary">
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : null;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : null;
      <#else>
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : ${filter.javaBoxName}.MAX_VALUE;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : ${filter.javaBoxName}.MAX_VALUE;
      </#if>
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.writeObjectSet(candidates, outputStream);
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
    public ValueIn(int measurementIndex, Set<${filter.javaBoxName}> candidates) {
      super(measurementIndex, candidates);
    }

    public ValueIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return candidates.contains((${filter.javaBoxName}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
      return candidates.contains(value);
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      <#if filter.dataType == "boolean">
      return false;
      <#else>
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      // All values are null, but candidates do not contain null
      if ((!statistics.isPresent() || isAllNulls(statistics.get())) && !candidates.isEmpty()) {
        return true;
      }

      // All values are not null, but candidate is one null value
      if (!metadata.hasNullValue(measurementIndex) && candidates.isEmpty()) {
        return true;
      }

      if (statistics.isPresent()) {
        Statistics<? extends Serializable> stat = statistics.get();
        if (!statisticsNotAvailable(stat)) {
          ${filter.dataType} valuesMin = (${filter.dataType}) stat.getMinValue();
          ${filter.dataType} valuesMax = (${filter.dataType}) stat.getMaxValue();
          // All values are same
          if (valuesMin == valuesMax) {
            return !candidates.contains(valuesMin);
          } else {
            if (!candidates.isEmpty()) {
              // All values are less than min, or greater than max
              <#if filter.dataType == "Binary">
              return candidatesMin.compareTo(valuesMax) > 0
                  || candidatesMax.compareTo(valuesMin) < 0;
              <#else>
              return candidatesMin > valuesMax || candidatesMax < valuesMin;
              </#if>
            }
          }
        }
      }

      return false;
      </#if>
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      <#if filter.dataType == "boolean">
      return false;
      <#else>
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      // All values are null, and candidate contains null
      // Note null value cannot be added to set
      if ((!statistics.isPresent() || isAllNulls(statistics.get())) && candidates.isEmpty()) {
        return true;
      }

      // All values are same
      if (statistics.isPresent()) {
        Statistics<? extends Serializable> stat = statistics.get();
        if (!statisticsNotAvailable(stat)) {
          ${filter.dataType} valuesMin = (${filter.dataType}) stat.getMinValue();
          ${filter.dataType} valuesMax = (${filter.dataType}) stat.getMaxValue();
          // All values are same
          if (valuesMin == valuesMax) {
            return candidates.contains(valuesMin);
          }
        }
      }

      return false;
      </#if>
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

    public ValueNotIn(int measurementIndex, Set<${filter.javaBoxName}> candidates) {
      super(measurementIndex, candidates);
    }

    public ValueNotIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return !candidates.contains((${filter.javaBoxName}) value);
    }

    public boolean valueSatisfy(${filter.dataType} value) {
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
  abstract static class ValueColumnPatternMatchFilter extends DisableStatisticsValueFilter {

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

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
      <#else>
      return false;
      </#if>
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

    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
      <#else>
      return false;
      </#if>
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
</#list>