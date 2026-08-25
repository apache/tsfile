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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.block.column.ColumnBuilderStatus;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.apache.tsfile.utils.Preconditions.checkArgument;

public class UnknownType extends AbstractType {
  public static final UnknownType UNKNOWN = new UnknownType();
  public static final String NAME = "unknown";

  private UnknownType() {}

  @Override
  public void writeBoolean(ColumnBuilder columnBuilder, boolean value) {
    // Ideally, this function should never be invoked for the unknown type.
    // However, some logic (e.g. AbstractMinMaxBy) relies on writing a default value before the null
    // check.
    checkArgument(!value);
    columnBuilder.appendNull();
  }

  @Override
  public boolean getBoolean(Column column, int position) {
    // Ideally, this function should never be invoked for the unknown type.
    // However, some logic relies on having a default value before the null check.
    checkArgument(column.isNull(position));
    return false;
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return null;
  }

  @Override
  public TsPrimitiveType deserialize(ByteBuffer buffer) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new BooleanColumnBuilder(null, expectedEntries);
  }

  @Override
  public ColumnBuilder createColumnBuilder(
      ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    throw new IllegalArgumentException(
        Messages.format("error.read.tsblock_builder_unknown_type", getTypeEnum()));
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.UNKNOWN;
  }

  @Override
  public String getDisplayName() {
    return NAME;
  }

  @Override
  public boolean isComparable() {
    return true;
  }

  @Override
  public boolean isOrderable() {
    return true;
  }

  @Override
  public List<Type> getTypeParameters() {
    return Collections.emptyList();
  }

  public static UnknownType getInstance() {
    return UNKNOWN;
  }
}
