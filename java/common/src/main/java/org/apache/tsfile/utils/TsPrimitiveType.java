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
package org.apache.tsfile.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public abstract class TsPrimitiveType implements Serializable {

  public boolean getBoolean() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getBoolean()"));
  }

  public int getInt() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getInt()"));
  }

  public long getLong() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getLong()"));
  }

  public float getFloat() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getFloat()"));
  }

  public double getDouble() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getDouble()"));
  }

  public Binary getBinary() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getBinary()"));
  }

  public TsPrimitiveType[] getVector() {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "getVector()"));
  }

  public void setBoolean(boolean val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setBoolean()"));
  }

  public void setInt(int val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setInt()"));
  }

  public void setLong(long val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setLong()"));
  }

  public void setFloat(float val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setFloat()"));
  }

  public void setDouble(double val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setDouble()"));
  }

  public void setBinary(Binary val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setBinary()"));
  }

  public void setVector(TsPrimitiveType[] val) {
    throw new UnsupportedOperationException(
        Messages.format("error.common.subclass_op_not_supported", "setVector()"));
  }

  public abstract void setObject(Object val);

  public abstract void reset();

  /**
   * get the size of one instance of current class.
   *
   * @return size of one instance of current class
   */
  public abstract int getSize();

  public abstract Object getValue();

  public abstract String getStringValue();

  public abstract TSDataType getDataType();

  public abstract void copy(TsPrimitiveType value);

  private static TsPrimitiveType copyValue(TsPrimitiveType value) {
    if (value == null) {
      return null;
    }
    if (value instanceof TsBoolean) {
      return new TsBoolean((TsBoolean) value);
    }
    if (value instanceof TsInt) {
      return new TsInt((TsInt) value);
    }
    if (value instanceof TsLong) {
      return new TsLong((TsLong) value);
    }
    if (value instanceof TsFloat) {
      return new TsFloat((TsFloat) value);
    }
    if (value instanceof TsDouble) {
      return new TsDouble((TsDouble) value);
    }
    if (value instanceof TsBinary) {
      return new TsBinary((TsBinary) value);
    }
    if (value instanceof TsVector) {
      return new TsVector((TsVector) value);
    }
    throw new UnSupportedDataTypeException(String.valueOf(value.getDataType()));
  }

  @Override
  public String toString() {
    return getStringValue();
  }

  @Override
  public boolean equals(Object object) {
    return (object instanceof TsPrimitiveType)
        && (((TsPrimitiveType) object).getValue().equals(getValue()));
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  public static class TsBoolean extends TsPrimitiveType {

    private boolean value;

    public TsBoolean() {}

    public TsBoolean(boolean value) {
      this.value = value;
    }

    public TsBoolean(TsBoolean other) {
      this.value = other.value;
    }

    @Override
    public boolean getBoolean() {
      return value;
    }

    @Override
    public void setBoolean(boolean val) {
      this.value = val;
    }

    @Override
    public Binary getBinary() {
      return new Binary(String.valueOf(this.value), StandardCharsets.UTF_8);
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Boolean) {
        setBoolean((Boolean) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format("error.common.tsprimitive_wrong_value_type", "TsBoolean", "Boolean"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setBoolean(value.getBoolean());
    }

    @Override
    public void reset() {
      value = false;
    }

    @Override
    public int getSize() {
      return 4 + 1;
    }

    @Override
    public Object getValue() {
      return getBoolean();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.BOOLEAN;
    }

    @Override
    public int hashCode() {
      return Boolean.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsBoolean) {
        TsBoolean anotherTs = (TsBoolean) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsInt extends TsPrimitiveType {

    private int value;

    private TSDataType dataType = TSDataType.INT32;

    public TsInt() {}

    public TsInt(int value) {
      this.value = value;
    }

    public TsInt(TSDataType dataType) {
      this.dataType = dataType;
    }

    public TsInt(int value, TSDataType dataType) {
      this.value = value;
      this.dataType = dataType;
    }

    public TsInt(TsInt other) {
      this.value = other.value;
      this.dataType = other.dataType;
    }

    @Override
    public int getInt() {
      return value;
    }

    @Override
    public double getDouble() {
      return value;
    }

    @Override
    public long getLong() {
      return value;
    }

    @Override
    public float getFloat() {
      return (float) value;
    }

    @Override
    public Binary getBinary() {
      if (dataType == TSDataType.DATE) {
        return new Binary(TSDataType.getDateStringValue(value), StandardCharsets.UTF_8);
      }
      return new Binary(String.valueOf(this.value), StandardCharsets.UTF_8);
    }

    @Override
    public void setInt(int val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Integer) {
        setInt((Integer) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format("error.common.tsprimitive_wrong_value_type", "TsInt", "Integer"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setInt(value.getInt());
      dataType = value.getDataType();
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int getSize() {
      return 4 + 4;
    }

    @Override
    public Object getValue() {
      return getInt();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return dataType;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsInt) {
        TsInt anotherTs = (TsInt) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsLong extends TsPrimitiveType {

    private long value;

    public TsLong() {}

    public TsLong(long value) {
      this.value = value;
    }

    public TsLong(TsLong other) {
      this.value = other.value;
    }

    @Override
    public long getLong() {
      return value;
    }

    @Override
    public double getDouble() {
      return (double) value;
    }

    @Override
    public Binary getBinary() {
      return new Binary(String.valueOf(this.value), StandardCharsets.UTF_8);
    }

    @Override
    public void setLong(long val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Long) {
        setLong((Long) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format("error.common.tsprimitive_wrong_value_type", "TsLong", "Long"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setLong(value.getLong());
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int getSize() {
      return 4 + 8;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.INT64;
    }

    @Override
    public Object getValue() {
      return getLong();
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsLong) {
        TsLong anotherTs = (TsLong) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsFloat extends TsPrimitiveType {

    private float value;

    public TsFloat() {}

    public TsFloat(float value) {
      this.value = value;
    }

    public TsFloat(TsFloat other) {
      this.value = other.value;
    }

    @Override
    public float getFloat() {
      return value;
    }

    @Override
    public double getDouble() {
      return (double) value;
    }

    @Override
    public Binary getBinary() {
      return new Binary(String.valueOf(this.value), StandardCharsets.UTF_8);
    }

    @Override
    public void setFloat(float val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Float) {
        setFloat((Float) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format("error.common.tsprimitive_wrong_value_type", "TsFloat", "float"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setFloat(value.getFloat());
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int getSize() {
      return 4 + 4;
    }

    @Override
    public Object getValue() {
      return getFloat();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.FLOAT;
    }

    @Override
    public int hashCode() {
      return Float.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsFloat) {
        TsFloat anotherTs = (TsFloat) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsDouble extends TsPrimitiveType {

    private double value;

    public TsDouble() {}

    public TsDouble(double value) {
      this.value = value;
    }

    public TsDouble(TsDouble other) {
      this.value = other.value;
    }

    @Override
    public double getDouble() {
      return value;
    }

    @Override
    public Binary getBinary() {
      return new Binary(String.valueOf(this.value), StandardCharsets.UTF_8);
    }

    @Override
    public void setDouble(double val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Double) {
        setDouble((Double) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format("error.common.tsprimitive_wrong_value_type", "TsDouble", "Double"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setDouble(value.getDouble());
    }

    @Override
    public void reset() {
      value = 0.0;
    }

    @Override
    public int getSize() {
      return 4 + 8;
    }

    @Override
    public Object getValue() {
      return getDouble();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.DOUBLE;
    }

    @Override
    public int hashCode() {
      return Double.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsDouble) {
        TsDouble anotherTs = (TsDouble) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsBinary extends TsPrimitiveType {

    private Binary value;

    public TsBinary() {}

    public TsBinary(Binary value) {
      this.value = value;
    }

    public TsBinary(TsBinary other) {
      this.value = copyBinary(other.value);
    }

    private static Binary copyBinary(Binary value) {
      if (value == null) {
        return null;
      }
      byte[] bytes = value.getValues();
      return new Binary(bytes == null ? null : Arrays.copyOf(bytes, bytes.length));
    }

    @Override
    public Binary getBinary() {
      return value;
    }

    @Override
    public void setBinary(Binary val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Binary) {
        setBinary((Binary) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format("error.common.tsprimitive_wrong_value_type", "TsBinary", "Binary"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setBinary(copyBinary(value.getBinary()));
    }

    @Override
    public void reset() {
      value = null;
    }

    @Override
    public int getSize() {
      return 4 + 4 + value.getLength();
    }

    @Override
    public Object getValue() {
      return getBinary();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.TEXT;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsBinary) {
        TsBinary anotherTs = (TsBinary) anObject;
        return value.equals(anotherTs.value);
      }
      return false;
    }
  }

  public static class TsVector extends TsPrimitiveType {

    private TsPrimitiveType[] values;

    public TsVector() {}

    public TsVector(TsPrimitiveType[] values) {
      this.values = values;
    }

    public TsVector(TsVector other) {
      this.values = copyValues(other.values);
    }

    private static TsPrimitiveType[] copyValues(TsPrimitiveType[] values) {
      if (values == null) {
        return null;
      }
      TsPrimitiveType[] copiedValues = new TsPrimitiveType[values.length];
      for (int i = 0; i < values.length; i++) {
        copiedValues[i] = copyValue(values[i]);
      }
      return copiedValues;
    }

    @Override
    public TsPrimitiveType[] getVector() {
      return values;
    }

    @Override
    public void setVector(TsPrimitiveType[] vals) {
      this.values = vals;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof TsPrimitiveType[]) {
        setVector((TsPrimitiveType[]) val);
        return;
      }
      throw new UnSupportedDataTypeException(
          Messages.format(
              "error.common.tsprimitive_wrong_value_type", "TsVector", "TsPrimitiveType[]"));
    }

    @Override
    public void copy(TsPrimitiveType value) {
      if (value == null) {
        reset();
        return;
      }
      setVector(copyValues(value.getVector()));
    }

    @Override
    public void reset() {
      values = null;
    }

    @Override
    public int getSize() {
      int size = 0;
      for (TsPrimitiveType type : values) {
        if (type != null) {
          size += type.getSize();
        }
      }
      // object header + array object header
      return 4 + 4 + size;
    }

    @Override
    public Object getValue() {
      return getVector();
    }

    @Override
    public String getStringValue() {
      StringBuilder builder = new StringBuilder("[");
      builder.append(values[0] == null ? "null" : values[0].getStringValue());
      for (int i = 1; i < values.length; i++) {
        builder.append(", ").append(values[i] == null ? "null" : values[i].getStringValue());
      }
      builder.append("]");
      return builder.toString();
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.VECTOR;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsVector) {
        TsVector anotherTs = (TsVector) anObject;
        if (anotherTs.values.length != this.values.length) {
          return false;
        }
        for (int i = 0; i < this.values.length; i++) {
          if (values[i] == null && anotherTs.values[i] == null) {
            continue;
          }
          if (values[i] == null || anotherTs.values[i] == null) {
            return false;
          }
          if (!values[i].equals(anotherTs.values[i])) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }
}
