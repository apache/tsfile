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

package org.apache.tsfile.write.schema;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.StringContainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class VectorMeasurementSchema
    implements IMeasurementSchema, Comparable<VectorMeasurementSchema>, Serializable {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(VectorMeasurementSchema.class);
  private static final long BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TSEncodingBuilder.class);
  private static final byte NO_UNIFIED_COMPRESSOR = -1;

  private String deviceId;
  private Map<String, Integer> measurementsToIndexMap;
  private byte[] types;
  private byte[] encodings;
  private TSEncodingBuilder[] encodingConverters;

  /** For compatibility of old versions. */
  private byte unifiedCompressor;

  /** [0] is for the time column. */
  private byte[] compressors;

  public VectorMeasurementSchema() {}

  public VectorMeasurementSchema(
      String deviceId,
      String[] subMeasurements,
      TSDataType[] types,
      TSEncoding[] encodings,
      CompressionType compressionType) {
    this.deviceId = deviceId;
    this.measurementsToIndexMap = new HashMap<>();
    for (int i = 0; i < subMeasurements.length; i++) {
      measurementsToIndexMap.put(subMeasurements[i], i);
    }
    byte[] typesInByte = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      typesInByte[i] = types[i].serialize();
    }
    this.types = typesInByte;

    byte[] encodingsInByte = new byte[encodings.length];
    for (int i = 0; i < encodings.length; i++) {
      encodingsInByte[i] = encodings[i].serialize();
    }
    this.encodings = encodingsInByte;
    this.encodingConverters = new TSEncodingBuilder[subMeasurements.length];
    this.unifiedCompressor = compressionType.serialize();
  }

  public VectorMeasurementSchema(
      String deviceId,
      String[] subMeasurements,
      TSDataType[] types,
      TSEncoding[] encodings,
      byte[] compressors) {
    this.deviceId = deviceId;
    this.measurementsToIndexMap = new HashMap<>();
    for (int i = 0; i < subMeasurements.length; i++) {
      measurementsToIndexMap.put(subMeasurements[i], i);
    }
    byte[] typesInByte = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      typesInByte[i] = types[i].serialize();
    }
    this.types = typesInByte;

    byte[] encodingsInByte = new byte[encodings.length];
    for (int i = 0; i < encodings.length; i++) {
      encodingsInByte[i] = encodings[i].serialize();
    }
    this.encodings = encodingsInByte;
    this.encodingConverters = new TSEncodingBuilder[subMeasurements.length];
    this.unifiedCompressor = NO_UNIFIED_COMPRESSOR;
    this.compressors = compressors;
  }

  public VectorMeasurementSchema(String deviceId, String[] subMeasurements, TSDataType[] types) {
    this.deviceId = deviceId;
    this.measurementsToIndexMap = new HashMap<>();
    for (int i = 0; i < subMeasurements.length; i++) {
      measurementsToIndexMap.put(subMeasurements[i], i);
    }
    this.types = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      this.types[i] = types[i].serialize();
    }

    this.encodings = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      this.encodings[i] =
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder(types[i]))
              .serialize();
    }
    this.encodingConverters = new TSEncodingBuilder[subMeasurements.length];
    this.unifiedCompressor = NO_UNIFIED_COMPRESSOR;
    // the first column is time
    this.compressors = new byte[subMeasurements.length + 1];
    compressors[0] =
        TSFileDescriptor.getInstance().getConfig().getCompressor(TSDataType.INT64).serialize();
    for (int i = 0; i < types.length; i++) {
      compressors[i + 1] =
          TSFileDescriptor.getInstance().getConfig().getCompressor(types[i]).serialize();
    }
  }

  public VectorMeasurementSchema(
      String deviceId, String[] subMeasurements, TSDataType[] types, TSEncoding[] encodings) {
    this(
        deviceId,
        subMeasurements,
        types,
        encodings,
        TSFileDescriptor.getInstance().getConfig().getCompressor());
  }

  @Override
  public MeasurementSchemaType getSchemaType() {
    return MeasurementSchemaType.VECTOR_MEASUREMENT_SCHEMA;
  }

  @Override
  public String getMeasurementName() {
    return deviceId;
  }

  @Deprecated // Aligned series should not invoke this method
  @Override
  public CompressionType getCompressor() {
    throw new UnsupportedOperationException("Aligned series should not invoke this method");
  }

  public CompressionType getTimeCompressor() {
    if (compressors != null) {
      return CompressionType.deserialize(compressors[0]);
    }
    return CompressionType.deserialize(unifiedCompressor);
  }

  public CompressionType getValueCompressor(int index) {
    if (compressors != null) {
      return CompressionType.deserialize(compressors[index + 1]);
    }
    return CompressionType.deserialize(unifiedCompressor);
  }

  @Override
  public TSEncoding getEncodingType() {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public TSDataType getType() {
    return TSDataType.VECTOR;
  }

  @Override
  public byte getTypeInByte() {
    return ((byte) 6);
  }

  @Override
  public void setDataType(TSDataType dataType) {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public TSEncoding getTimeTSEncoding() {
    return TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
  }

  @Override
  public Encoder getTimeEncoder() {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    return TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
  }

  @Override
  public Encoder getValueEncoder() {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public Map<String, String> getProps() {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public List<String> getSubMeasurementsList() {
    String[] measurements = new String[measurementsToIndexMap.size()];
    for (Map.Entry<String, Integer> entry : measurementsToIndexMap.entrySet()) {
      measurements[entry.getValue()] = entry.getKey();
    }
    return Arrays.asList(measurements);
  }

  @Override
  public List<TSDataType> getSubMeasurementsTSDataTypeList() {
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (byte dataType : types) {
      dataTypeList.add(TSDataType.deserialize(dataType));
    }
    return dataTypeList;
  }

  @Override
  public List<TSEncoding> getSubMeasurementsTSEncodingList() {
    List<TSEncoding> encodingList = new ArrayList<>();
    for (byte encoding : encodings) {
      encodingList.add(TSEncoding.deserialize(encoding));
    }
    return encodingList;
  }

  @Override
  public List<Encoder> getSubMeasurementsEncoderList() {
    List<Encoder> encoderList = new ArrayList<>();
    for (int i = 0; i < encodings.length; i++) {
      TSEncoding encoding = TSEncoding.deserialize(encodings[i]);
      // it is ok even if encodingConverter is constructed two instances for concurrent scenario
      if (encodingConverters[i] == null) {
        // initialize TSEncoding. e.g. set max error for PLA and SDT
        encodingConverters[i] = TSEncodingBuilder.getEncodingBuilder(encoding);
        encodingConverters[i].initFromProps(null);
      }
      encoderList.add(encodingConverters[i].getEncoder(TSDataType.deserialize(types[i])));
    }
    return encoderList;
  }

  @Override
  public int getSubMeasurementIndex(String subMeasurement) {
    return measurementsToIndexMap.getOrDefault(subMeasurement, -1);
  }

  @Override
  public int getSubMeasurementsCount() {
    return measurementsToIndexMap.size();
  }

  @Override
  public boolean containsSubMeasurement(String subMeasurement) {
    return measurementsToIndexMap.containsKey(subMeasurement);
  }

  public void addMeasurement(String measurementId, TSDataType dataType, TSEncoding encoding) {
    measurementsToIndexMap.put(measurementId, measurementsToIndexMap.size());
    byte[] typesInByte = new byte[measurementsToIndexMap.size()];
    if (measurementsToIndexMap.size() - 1 >= 0) {
      System.arraycopy(types, 0, typesInByte, 0, measurementsToIndexMap.size() - 1);
    }
    typesInByte[typesInByte.length - 1] = dataType.serialize();
    this.types = typesInByte;
    byte[] encodingsInByte = new byte[measurementsToIndexMap.size()];
    if (measurementsToIndexMap.size() - 1 >= 0) {
      System.arraycopy(encodings, 0, encodingsInByte, 0, measurementsToIndexMap.size() - 1);
    }
    encodingsInByte[encodingsInByte.length - 1] = encoding.serialize();
    this.encodings = encodingsInByte;
    this.encodingConverters = new TSEncodingBuilder[measurementsToIndexMap.size()];
  }

  @Override
  public int serializedSize() {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.sizeToWrite(deviceId);
    byteLen += Integer.BYTES;
    for (Map.Entry<String, Integer> entry : measurementsToIndexMap.entrySet()) {
      byteLen += ReadWriteIOUtils.sizeToWrite(entry.getKey());
      byteLen += Integer.BYTES;
    }
    byteLen += (types.length + encodings.length + 1) * Byte.BYTES;
    return byteLen;
  }

  @Override
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(deviceId, buffer);
    byteLen += ReadWriteIOUtils.write(measurementsToIndexMap.size(), buffer);

    for (Map.Entry<String, Integer> entry : measurementsToIndexMap.entrySet()) {
      byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
      byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);
    }
    for (byte type : types) {
      byteLen += ReadWriteIOUtils.write(type, buffer);
    }
    for (byte encoding : encodings) {
      byteLen += ReadWriteIOUtils.write(encoding, buffer);
    }
    byteLen += ReadWriteIOUtils.write(unifiedCompressor, buffer);
    if (unifiedCompressor == NO_UNIFIED_COMPRESSOR) {
      buffer.put(compressors);
      byteLen += compressors.length;
    }

    return byteLen;
  }

  @Override
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(deviceId, outputStream);
    byteLen += ReadWriteIOUtils.write(measurementsToIndexMap.size(), outputStream);

    for (Map.Entry<String, Integer> entry : measurementsToIndexMap.entrySet()) {
      byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
      byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    for (byte type : types) {
      byteLen += ReadWriteIOUtils.write(type, outputStream);
    }
    for (byte encoding : encodings) {
      byteLen += ReadWriteIOUtils.write(encoding, outputStream);
    }
    byteLen += ReadWriteIOUtils.write(unifiedCompressor, outputStream);
    if (unifiedCompressor == NO_UNIFIED_COMPRESSOR) {
      outputStream.write(compressors);
      byteLen += compressors.length;
    }

    return byteLen;
  }

  @Override
  public int partialSerializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    return 1 + serializeTo(outputStream);
  }

  @Override
  public boolean isLogicalView() {
    return false;
  }

  @Override
  public int partialSerializeTo(ByteBuffer buffer) {
    ReadWriteIOUtils.write((byte) 1, buffer);
    return 1 + serializeTo(buffer);
  }

  public static VectorMeasurementSchema partialDeserializeFrom(ByteBuffer buffer) {
    return deserializeFrom(buffer);
  }

  public static VectorMeasurementSchema deserializeFrom(InputStream inputStream)
      throws IOException {
    VectorMeasurementSchema vectorMeasurementSchema = new VectorMeasurementSchema();
    vectorMeasurementSchema.deviceId = ReadWriteIOUtils.readString(inputStream);

    int measurementSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, Integer> measurementsToIndexMap = new HashMap<>();
    for (int i = 0; i < measurementSize; i++) {
      measurementsToIndexMap.put(
          ReadWriteIOUtils.readString(inputStream), ReadWriteIOUtils.readInt(inputStream));
    }
    vectorMeasurementSchema.measurementsToIndexMap = measurementsToIndexMap;

    byte[] types = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      types[i] = ReadWriteIOUtils.readByte(inputStream);
    }
    vectorMeasurementSchema.types = types;

    byte[] encodings = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      encodings[i] = ReadWriteIOUtils.readByte(inputStream);
    }
    vectorMeasurementSchema.encodings = encodings;

    vectorMeasurementSchema.unifiedCompressor = ReadWriteIOUtils.readByte(inputStream);
    if (vectorMeasurementSchema.unifiedCompressor == NO_UNIFIED_COMPRESSOR) {
      byte[] compressors = new byte[measurementSize + 1];
      int read = inputStream.read(compressors);
      if (read != measurementSize) {
        throw new IOException("Unexpected end of stream when reading compressors");
      }
      vectorMeasurementSchema.compressors = compressors;
    }
    return vectorMeasurementSchema;
  }

  public static VectorMeasurementSchema deserializeFrom(ByteBuffer buffer) {
    VectorMeasurementSchema vectorMeasurementSchema = new VectorMeasurementSchema();
    vectorMeasurementSchema.deviceId = ReadWriteIOUtils.readString(buffer);
    int measurementSize = ReadWriteIOUtils.readInt(buffer);
    Map<String, Integer> measurementsToIndexMap = new HashMap<>();
    for (int i = 0; i < measurementSize; i++) {
      measurementsToIndexMap.put(
          ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readInt(buffer));
    }
    vectorMeasurementSchema.measurementsToIndexMap = measurementsToIndexMap;

    byte[] types = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      types[i] = ReadWriteIOUtils.readByte(buffer);
    }
    vectorMeasurementSchema.types = types;

    byte[] encodings = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      encodings[i] = ReadWriteIOUtils.readByte(buffer);
    }
    vectorMeasurementSchema.encodings = encodings;

    vectorMeasurementSchema.unifiedCompressor = ReadWriteIOUtils.readByte(buffer);
    if (vectorMeasurementSchema.unifiedCompressor == NO_UNIFIED_COMPRESSOR) {
      byte[] compressors = new byte[measurementSize + 1];
      buffer.get(compressors);
      vectorMeasurementSchema.compressors = compressors;
    }
    return vectorMeasurementSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorMeasurementSchema that = (VectorMeasurementSchema) o;
    return Arrays.equals(types, that.types)
        && Arrays.equals(encodings, that.encodings)
        && Objects.equals(deviceId, that.deviceId)
        && Objects.equals(unifiedCompressor, that.unifiedCompressor)
        && Objects.equals(compressors, that.compressors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceId, types, encodings, unifiedCompressor, compressors);
  }

  /** compare by vector name */
  @Override
  public int compareTo(VectorMeasurementSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.deviceId.compareTo(o.deviceId);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail(deviceId, ",");
    // string is not in real order
    for (Map.Entry<String, Integer> entry : measurementsToIndexMap.entrySet()) {
      sc.addTail(
          "[",
          entry.getKey(),
          ",",
          TSDataType.deserialize(types[entry.getValue()]).toString(),
          ",",
          TSEncoding.deserialize(encodings[entry.getValue()]).toString());
      sc.addTail("],");
    }
    if (unifiedCompressor != NO_UNIFIED_COMPRESSOR) {
      sc.addTail(CompressionType.deserialize(unifiedCompressor).toString());
    } else {
      for (byte compressor : compressors) {
        sc.addTail(CompressionType.deserialize(compressor).toString()).addTail(",");
      }
    }

    return sc.toString();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(deviceId)
        + RamUsageEstimator.sizeOf(types)
        + RamUsageEstimator.sizeOf(encodings)
        + (long) encodingConverters.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        + Arrays.stream(encodingConverters)
            .map(o -> Objects.nonNull(o) ? BUILDER_SIZE : 0)
            .reduce(0L, Long::sum);
  }
}
