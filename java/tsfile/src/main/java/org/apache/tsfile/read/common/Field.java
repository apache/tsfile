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

package org.apache.tsfile.read.common;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NullFieldException;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.time.LocalDate;

/**
 * Field is component of one {@code RowRecord} which stores a value in specific data type. The value
 * type of Field is primitive(int long, float, double, binary, boolean).
 */
public class Field {

  private final TSDataType dataType;
  private boolean boolV;
  private int intV;
  private long longV;
  private float floatV;
  private double doubleV;
  private Binary binaryV;

  public Field(TSDataType dataType) {
    this.dataType = dataType;
  }

  public static Field copy(Field field) {
    Field out = new Field(field.dataType);
    if (out.dataType != null) {
      Type.fromTsDataType(out.dataType).setTo(field, out);
    }

    return out;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean getBoolV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return boolV;
  }

  public void setBoolV(boolean boolV) {
    this.boolV = boolV;
  }

  public int getIntV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return intV;
  }

  public void setIntV(int intV) {
    this.intV = intV;
  }

  public long getLongV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return longV;
  }

  public void setLongV(long longV) {
    this.longV = longV;
  }

  public float getFloatV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return floatV;
  }

  public void setFloatV(float floatV) {
    this.floatV = floatV;
  }

  public double getDoubleV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return doubleV;
  }

  public void setDoubleV(double doubleV) {
    this.doubleV = doubleV;
  }

  public Binary getBinaryV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return binaryV;
  }

  public void setBinaryV(Binary binaryV) {
    this.binaryV = binaryV;
  }

  public LocalDate getDateV() {
    if (dataType == null) {
      throw new NullFieldException();
    }
    return DateUtils.parseIntToLocalDate(intV);
  }

  /**
   * get field value and convert to string.
   *
   * @return value string
   */
  public String getStringValue() {
    if (dataType == null) {
      return "null";
    }
    return Type.fromTsDataType(dataType).toString(this);
  }

  @Override
  public String toString() {
    return getStringValue();
  }

  public Object getObjectValue(TSDataType dataType) {
    if (this.dataType == null) {
      return null;
    }
    return Type.fromTsDataType(dataType).getValue(this);
  }

  public static Field getField(Object value, TSDataType dataType) {
    if (value == null) {
      return null;
    }
    return Type.fromTsDataType(dataType).getField(value);
  }

  public static void setTsPrimitiveValue(TsPrimitiveType value, Field field) {
    Type.fromTsDataType(value.getDataType()).setTo(value, field);
  }
}
