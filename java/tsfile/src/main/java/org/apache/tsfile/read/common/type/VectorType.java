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
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.tsfile.encoding.decoder.LongChimpDecoder;
import org.apache.tsfile.encoding.decoder.LongGorillaDecoder;
import org.apache.tsfile.encoding.decoder.LongRleDecoder;
import org.apache.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class VectorType extends AbstractLongType {

  public static final VectorType VECTOR = new VectorType();

  private VectorType() {}

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, long value) {
    return new StringDataPoint(
        measurementId, new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET));
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  @Override
  public void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return Arrays.copyOf((long[]) array, newLength);
  }

  @Override
  public void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    ((long[]) toArray)[toIndex] = from.getLong(fromIndex);
  }

  @Override
  public void setTo(Column from, int fromIndex, Column to, int toIndex) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  @Override
  public void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.chunk_unknown_type", getTypeEnum()));
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initVectorValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putVector(timestamp, (TsPrimitiveType[]) value);
  }

  @Override
  public void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      TsPrimitiveType[] values = batchData.getVectorByIndex(index);
      outputStream.writeInt(values.length);
      for (TsPrimitiveType value : values) {
        if (value == null) {
          outputStream.write(0);
          continue;
        }
        outputStream.write(1);
        outputStream.write(value.getDataType().serialize());
        Type.fromTsDataType(value.getDataType()).serialize(value, outputStream);
      }
    }
  }

  @Override
  public void serialize(TsPrimitiveType value, DataOutputStream stream) {
    throw new IllegalArgumentException(
        Messages.format("error.read.batch_data_unknown_type", value.getDataType()));
  }

  @Override
  public void serializeValue(Object value, ByteBuffer buffer) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void serializeValue(Object value, DataOutputStream stream) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void serializeArray(Object array, int length, ByteBuffer buffer) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE -> new LongRleDecoder();
      case TS_2DIFF -> new DeltaBinaryDecoder.LongDeltaDecoder();
      case REGULAR -> new RegularDataDecoder.LongRegularDecoder();
      case GORILLA -> new LongGorillaDecoder();
      case CHIMP -> new LongChimpDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getVector();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsVector();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsVector((TsPrimitiveType[]) value);
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", getTypeEnum()));
  }

  @Override
  public void addPoint(TSRecord record, String columnName, Field field) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", getTypeEnum()));
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void deserialize(Object[] array, int index, ByteBuffer buffer) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public void deserialize(Object[] array, int index, InputStream stream) {
    throw new UnsupportedOperationException(getDisplayName());
  }

  @Override
  public Column createNullColumn(int positionCount) {
    throw new IllegalArgumentException(
        Messages.format("error.read.null_col_unknown_type", getTypeEnum()));
  }

  @Override
  public String toString(Field field) {
    throw new UnSupportedDataTypeException(field.getDataType().toString());
  }

  @Override
  public Object getValue(Field field) {
    throw new UnSupportedDataTypeException(getTypeEnum().toString());
  }

  @Override
  public Field getField(Object value) {
    throw new UnSupportedDataTypeException(getTypeEnum().toString());
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", from.getDataType()));
  }

  @Override
  public void setTo(BatchData from, Field to) {
    TsPrimitiveType value = from.getVector()[0];
    Type.fromTsDataType(value.getDataType()).setTo(value, to);
  }

  @Override
  public long estimateValueSize() {
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.VECTOR;
  }

  @Override
  public String getDisplayName() {
    return "VECTOR";
  }

  public static VectorType getInstance() {
    return VECTOR;
  }
}
