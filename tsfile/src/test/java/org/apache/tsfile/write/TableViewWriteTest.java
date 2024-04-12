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

package org.apache.tsfile.write;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.TableQueryOrdering;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableViewWriteTest {

  private final String testDir = "target" + File.separator + "tableViewTest";
  private final int idSchemaNum = 5;
  private final int measurementSchemaNum = 5;
  private TableSchema testTableSchema;

  @Before
  public void setUp() throws Exception {
    new File(testDir).mkdirs();
    testTableSchema = genTableSchema(0);
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  @Test
  public void testWriteOneTable() throws IOException, WriteProcessException, ReadProcessException {
    final File testFile = new File(testDir, "testFile");
    TsFileWriter writer = new TsFileWriter(testFile);
    writer.registerTableSchema(testTableSchema);

    writer.writeTable(genTablet(testTableSchema, 0, 100));
    writer.close();

    TsFileSequenceReader sequenceReader = new TsFileSequenceReader(testFile.getAbsolutePath());
    TableQueryExecutor tableQueryExecutor =
        new TableQueryExecutor(
            new MetadataQuerierByFileImpl(sequenceReader),
            new CachedChunkLoaderImpl(sequenceReader),
            TableQueryOrdering.DEVICE);

    final List<String> columns =
        testTableSchema.getColumnSchemas().stream()
            .map(MeasurementSchema::getMeasurementId)
            .collect(Collectors.toList());
    final TsBlockReader reader =
        tableQueryExecutor.query(testTableSchema.getTableName(), columns, null, null, null);
    assertTrue(reader.hasNext());
    int cnt = 0;
    while (reader.hasNext()) {
      final TsBlock result = reader.next();
      cnt += result.getPositionCount();
    }
    assertEquals(100, cnt);
  }

  private Tablet genTablet(TableSchema tableSchema, int offset, int num) {
    Tablet tablet = new Tablet(tableSchema.getTableName(), tableSchema.getColumnSchemas(),
        tableSchema.getColumnTypes());
    for (int i = 0; i < num; i++) {
      tablet.addTimestamp(i, offset + i);
      for (MeasurementSchema columnSchema : tableSchema.getColumnSchemas()) {
        tablet.addValue(columnSchema.getMeasurementId(), i, getValue(columnSchema.getType(), i));
      }
    }
    tablet.rowSize = num;
    return tablet;
  }

  public Object getValue(TSDataType dataType, int i) {
    switch (dataType) {
      case INT64:
        return (long) i;
      case TEXT:
        return new Binary(String.valueOf(i), StandardCharsets.UTF_8);
      default:
        return i;
    }
  }

  private TableSchema genTableSchema(int tableNum) {
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnType> columnTypes = new ArrayList<>();

    for (int i = 0; i < idSchemaNum; i++) {
      measurementSchemas.add(
          new MeasurementSchema(
              "id" + i, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      columnTypes.add(ColumnType.ID);
    }
    for (int i = 0; i < measurementSchemaNum; i++) {
      measurementSchemas.add(
          new MeasurementSchema(
              "s" + i, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      columnTypes.add(ColumnType.MEASUREMENT);
    }
    return new TableSchema("testTable" + tableNum, measurementSchemas, columnTypes);
  }
}
