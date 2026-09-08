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
package org.apache.tsfile.write.writer;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MeasurementSchemaSerializeTest {

  @Test
  public void deserializeFromByteBufferTest() throws IOException {
    MeasurementSchema standard =
        new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    standard.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    MeasurementSchema measurementSchema = MeasurementSchema.deserializeFrom(byteBuffer);
    assertEquals(standard, measurementSchema);
  }

  @Test
  public void deserializeFromInputStreamTest() throws IOException {
    MeasurementSchema standard =
        new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    standard.serializeTo(byteBuffer);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(byteBuffer.array());
    MeasurementSchema measurementSchema = MeasurementSchema.deserializeFrom(inputStream);
    assertEquals(standard, measurementSchema);
  }

  @Test
  public void deserializeVectorWithUnifiedCompressorFromInputStreamTest() throws IOException {
    VectorMeasurementSchema standard = createVectorSchema(CompressionType.LZ4);
    ByteArrayInputStream inputStream = serializeToInputStream(standard);

    VectorMeasurementSchema measurementSchema =
        VectorMeasurementSchema.deserializeFrom(inputStream);

    assertVectorSchemaEquals(standard, measurementSchema);
    assertEquals(-1, inputStream.read());
  }

  @Test
  public void deserializeVectorWithPerColumnCompressorsFromInputStreamTest() throws IOException {
    VectorMeasurementSchema standard = createVectorSchemaWithPerColumnCompressors();
    ByteArrayInputStream inputStream = serializeToInputStream(standard);

    VectorMeasurementSchema measurementSchema =
        VectorMeasurementSchema.deserializeFrom(inputStream);

    assertVectorSchemaEquals(standard, measurementSchema);
    assertEquals(-1, inputStream.read());
  }

  @Test
  public void deserializeVectorWithPerColumnCompressorsFromShortReadInputStreamTest()
      throws IOException {
    VectorMeasurementSchema standard = createVectorSchemaWithPerColumnCompressors();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    standard.serializeTo(outputStream);
    byte[] serialized = outputStream.toByteArray();
    int compressorListStart = serialized.length - standard.getSubMeasurementsCount() - 1;
    int shortReadBoundary = compressorListStart + 1;
    InputStream inputStream =
        new SequenceInputStream(
            new ByteArrayInputStream(Arrays.copyOf(serialized, shortReadBoundary)),
            new ByteArrayInputStream(
                Arrays.copyOfRange(serialized, shortReadBoundary, serialized.length)));

    VectorMeasurementSchema measurementSchema =
        VectorMeasurementSchema.deserializeFrom(inputStream);

    assertVectorSchemaEquals(standard, measurementSchema);
    assertEquals(-1, inputStream.read());
  }

  @Test
  public void deserializeVectorRejectsTruncatedPerColumnCompressors() throws IOException {
    VectorMeasurementSchema standard = createVectorSchemaWithPerColumnCompressors();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    standard.serializeTo(outputStream);
    byte[] serialized = outputStream.toByteArray();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(Arrays.copyOf(serialized, serialized.length - 1));

    assertThrows(IOException.class, () -> VectorMeasurementSchema.deserializeFrom(inputStream));
  }

  private static VectorMeasurementSchema createVectorSchema(CompressionType compressionType) {
    return new VectorMeasurementSchema(
        "device",
        new String[] {"sensor_1", "sensor_2"},
        new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64},
        new TSEncoding[] {TSEncoding.RLE, TSEncoding.TS_2DIFF},
        compressionType);
  }

  private static VectorMeasurementSchema createVectorSchemaWithPerColumnCompressors() {
    return new VectorMeasurementSchema(
        "device",
        new String[] {"sensor_1", "sensor_2"},
        new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64},
        new TSEncoding[] {TSEncoding.RLE, TSEncoding.TS_2DIFF},
        new byte[] {
          CompressionType.LZ4.serialize(),
          CompressionType.SNAPPY.serialize(),
          CompressionType.GZIP.serialize()
        });
  }

  private static ByteArrayInputStream serializeToInputStream(VectorMeasurementSchema schema)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    schema.serializeTo(outputStream);
    return new ByteArrayInputStream(outputStream.toByteArray());
  }

  private static void assertVectorSchemaEquals(
      VectorMeasurementSchema expected, VectorMeasurementSchema actual) {
    assertEquals(expected.getMeasurementName(), actual.getMeasurementName());
    assertEquals(expected.getSubMeasurementsList(), actual.getSubMeasurementsList());
    assertEquals(
        expected.getSubMeasurementsTSDataTypeList(), actual.getSubMeasurementsTSDataTypeList());
    assertEquals(
        expected.getSubMeasurementsTSEncodingList(), actual.getSubMeasurementsTSEncodingList());
    assertEquals(expected.getTimeCompressor(), actual.getTimeCompressor());
    for (int i = 0; i < expected.getSubMeasurementsCount(); i++) {
      assertEquals(expected.getValueCompressor(i), actual.getValueCompressor(i));
    }
  }
}
