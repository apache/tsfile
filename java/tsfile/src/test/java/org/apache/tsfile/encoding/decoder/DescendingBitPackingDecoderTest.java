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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.DescendingBitPackingEncoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.StringJoiner;

import static org.junit.Assert.assertEquals;

public class DescendingBitPackingDecoderTest {
  protected static long[] getTestData() {
    return new long[] {
      0,
      -1,
      1,
      -2,
      2,
      0,
      0,
      -3,
      3,
      0,
      -4,
      0,
      0,
      0,
      4,
      -5,
      5,
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE - 1,
      Long.MIN_VALUE + 1,
      0,
      0,
      5,
      -5,
      10,
      -2,
      4,
      3,
      2,
      1,
      -1,
      2,
      -3,
      1,
      2,
      1,
      1,
      -1,
      -1,
      0,
      0,
      0
    };
  }

  protected static long[] getEndToEndTestData() {
    int size = 10000;
    long[] data = new long[size];
    for (int i = 0; i < size; i++) {
      data[i] = i % 2 == 0 ? i : -i;
    }
    return data;
  }

  @Test
  public void test() throws Exception {
    long[] original = getTestData();
    compressDecompressAndAssert(
        original, new DescendingBitPackingEncoder(), new DescendingBitPackingDecoder());
  }

  @Test
  public void endToEndTest() throws Exception {
    long[] original = getEndToEndTestData();
    endToEndCompressDecompressAndAssert(original, "DESCENDING_BIT_PACKING");
  }

  protected static void compressDecompressAndAssert(
      long[] original, Encoder encoder, Decoder decoder) throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    for (long v : original) {
      encoder.encode(v, bout);
    }
    encoder.flush(bout);
    // Decode and verify
    ByteBuffer buffer = ByteBuffer.wrap(bout.toByteArray());

    int i = 0;
    while (decoder.hasNext(buffer)) {
      long actual = decoder.readLong(buffer);
      long expected = original[i];
      assertEquals("Mismatch at index " + i, expected, actual);
      i++;
    }
    assertEquals(original.length, i);
  }

  protected static void endToEndCompressDecompressAndAssert(long[] original, String encoder)
      throws Exception {
    int rowNum = original.length;

    TSFileDescriptor.getInstance().getConfig().setTimeEncoder(encoder);
    TSFileDescriptor.getInstance().getConfig().setInt64Encoding(encoder);
    String path = "test.tsfile";
    File f = FSFactoryProducer.getFSFactory().getFile(path);

    String tableName = "table1";

    TableSchema tableSchema =
        new TableSchema(
            tableName,
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("value")
                    .dataType(TSDataType.INT64)
                    .category(ColumnCategory.FIELD)
                    .build()));

    long memoryThreshold = 512;

    ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(tableSchema)
            .memoryThreshold(memoryThreshold)
            .build();

    Tablet tablet = new Tablet(Arrays.asList("value"), Arrays.asList(TSDataType.INT64), rowNum);

    for (int row = 0; row < rowNum; row++) {
      long timestamp = row;
      tablet.addTimestamp(row, timestamp);
      tablet.addValue(row, "value", original[row]);
    }

    writer.write(tablet);
    writer.close();

    f = FSFactoryProducer.getFSFactory().getFile(path);

    ITsFileReader reader = new TsFileReaderBuilder().file(f).build();

    ResultSet resultSet = reader.query(tableName, Arrays.asList("value"), 0, rowNum - 1);

    ResultSetMetadata metadata = resultSet.getMetadata();
    System.out.println(metadata);

    StringJoiner sj = new StringJoiner(" ");
    for (int column = 1; column <= 1; column++) {
      sj.add(metadata.getColumnName(column) + "(" + metadata.getColumnType(column) + ") ");
    }
    System.out.println(sj.toString());

    int index = 0;
    while (resultSet.next()) {
      Long timeField = resultSet.getLong("Time");
      Long valueField = resultSet.isNull("value") ? null : resultSet.getLong("value");
      assertEquals(original[index], (long) valueField);
      index++;
    }
    assertEquals(original.length, index);
  }
}
