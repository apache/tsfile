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

package org.apache.tsfile.write.record.datapoint;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.StringContainer;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.time.LocalDate;

/**
 * This is a abstract class representing a data point. DataPoint consists of a measurement id and a
 * data type. subclass of DataPoint need override method {@code write(long time, IChunkWriter
 * writer)} .Every subclass has its data type and overrides a setting method for its data type.
 */
public abstract class DataPoint {

  /** value type of this DataPoint. */
  protected final TSDataType type;

  /** measurementId of this DataPoint. */
  protected final String measurementId;

  protected IMeasurementSchema measurementSchema;

  /**
   * constructor of DataPoint.
   *
   * @param type value type of this DataPoint
   * @param measurementId measurementId of this DataPoint
   */
  protected DataPoint(TSDataType type, String measurementId) {
    this.type = type;
    this.measurementId = measurementId;
  }

  /**
   * Construct one data point with data type and value.
   *
   * @param dataType data type
   * @param measurementId measurement id
   * @param value value in string format
   * @return data point class according to data type
   */
  public static DataPoint getDataPoint(TSDataType dataType, String measurementId, String value) {
    try {
      return Type.fromTsDataType(dataType).getDataPoint(measurementId, value);
    } catch (Exception e) {
      throw new UnSupportedDataTypeException(
          Messages.format(
              "error.write.datapoint_type_value_mismatch", measurementId, dataType, value));
    }
  }

  /**
   * write this DataPoint by a SeriesWriter.
   *
   * @param time timestamp
   * @param writer writer
   * @throws IOException exception in IO
   */
  public abstract void writeTo(long time, ChunkWriterImpl writer) throws IOException;

  public String getMeasurementId() {
    return measurementId;
  }

  public abstract Object getValue();

  public TSDataType getType() {
    return type;
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer(" ");
    sc.addTail("{measurement id:", measurementId, "type:", type, "value:", getValue(), "}");
    return sc.toString();
  }

  public void setInteger(int value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "Integer"));
  }

  public void setLong(long value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "Long"));
  }

  public void setBoolean(boolean value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "Boolean"));
  }

  public void setFloat(float value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "Float"));
  }

  public void setDouble(double value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "Double"));
  }

  public void setString(Binary value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "String"));
  }

  public void setDate(LocalDate value) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.datapoint_set_not_supported", "Date"));
  }

  public IMeasurementSchema getMeasurementSchema() {
    return measurementSchema;
  }

  public void setMeasurementSchema(IMeasurementSchema measurementSchema) {
    this.measurementSchema = measurementSchema;
  }
}
