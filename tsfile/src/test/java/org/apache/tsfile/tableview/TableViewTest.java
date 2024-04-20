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

package org.apache.tsfile.tableview;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.executor.QueryExecutor;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.TableQueryOrdering;
import org.apache.tsfile.read.query.executor.TsFileExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
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

public class TableViewTest {

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
            .map(IMeasurementSchema::getMeasurementId)
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

  @Test
  public void testWriteMultipleTables() throws Exception {
    final File testFile = new File(testDir, "testFile");
    TsFileWriter writer = new TsFileWriter(testFile);
    List<TableSchema> tableSchemas = new ArrayList<>();

    int tableNum = 10;
    for (int i = 0; i < tableNum; i++) {
      final TableSchema tableSchema = genTableSchema(i);
      tableSchemas.add(tableSchema);
      writer.registerTableSchema(tableSchema);
    }

    for (int i = 0; i < tableNum; i++) {
      writer.writeTable(genTablet(tableSchemas.get(i), 0, 100));
    }
    writer.close();

    TsFileSequenceReader sequenceReader = new TsFileSequenceReader(testFile.getAbsolutePath());
    TableQueryExecutor tableQueryExecutor =
        new TableQueryExecutor(
            new MetadataQuerierByFileImpl(sequenceReader),
            new CachedChunkLoaderImpl(sequenceReader),
            TableQueryOrdering.DEVICE);

    final List<String> columns =
        testTableSchema.getColumnSchemas().stream()
            .map(IMeasurementSchema::getMeasurementId)
            .collect(Collectors.toList());

    for (int i = 0; i < tableNum; i++) {
      int cnt;
      try (TsBlockReader reader =
          tableQueryExecutor.query(tableSchemas.get(i).getTableName(), columns, null, null, null)) {
        assertTrue(reader.hasNext());
        cnt = 0;
        while (reader.hasNext()) {
          final TsBlock result = reader.next();
          cnt += result.getPositionCount();
        }
      }
      assertEquals(100, cnt);
    }
  }

  @Test
  public void testHybridWrite() throws Exception {
    final File testFile = new File(testDir, "testFile");
    TsFileWriter writer = new TsFileWriter(testFile);
    // table-view registration
    writer.registerTableSchema(testTableSchema);
    // tree-view registration
    final IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.a.b.c.d1");
    List<IMeasurementSchema> treeSchemas = new ArrayList<>();
    for (int i = 0; i < measurementSchemaNum; i++) {
      final MeasurementSchema measurementSchema =
          new MeasurementSchema(
              "s" + i, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      treeSchemas.add(measurementSchema);
      writer.registerTimeseries(deviceID, measurementSchema);
    }

    // table-view write
    final Tablet tablet = genTablet(testTableSchema, 0, 100);
    writer.writeTable(tablet);
    // tree-view write
    for (int i = 0; i < 50; i++) {
      final TSRecord tsRecord = new TSRecord(i, deviceID);
      for (int j = 0; j < measurementSchemaNum; j++) {
        tsRecord.addTuple(new LongDataPoint("s" + j, i));
      }
      writer.write(tsRecord);
    }
    writer.close();

    // table-view read table-view
    int cnt;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(testFile.getAbsolutePath())) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryOrdering.DEVICE);

      List<String> columns =
          testTableSchema.getColumnSchemas().stream()
              .map(IMeasurementSchema::getMeasurementId)
              .collect(Collectors.toList());
      TsBlockReader reader =
          tableQueryExecutor.query(testTableSchema.getTableName(), columns, null, null, null);
      assertTrue(reader.hasNext());
      cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        cnt += result.getPositionCount();
      }
      assertEquals(100, cnt);
    }

    // tree-view read tree-view
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(testFile.getAbsolutePath())) {
      QueryExecutor queryExecutor =
          new TsFileExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader));

      List<Path> selectedSeries = new ArrayList<>();
      for (int i = 0; i < measurementSchemaNum; i++) {
        selectedSeries.add(new Path(deviceID, "s" + i, false));
      }
      final QueryExpression queryExpression = QueryExpression.create(selectedSeries, null);
      final QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
      cnt = 0;
      while (queryDataSet.hasNext()) {
        queryDataSet.next();
        cnt++;
      }
      assertEquals(50, cnt);
    }

    // table-view read tree-view
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(testFile.getAbsolutePath())) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryOrdering.DEVICE);

      List<String> columns =
          treeSchemas.stream()
              .map(IMeasurementSchema::getMeasurementId)
              .collect(Collectors.toList());
      TsBlockReader reader =
          tableQueryExecutor.query(deviceID.getTableName(), columns, null, null, null);
      assertTrue(reader.hasNext());
      cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        cnt += result.getPositionCount();
      }
      assertEquals(50, cnt);
    }

    // tree-view read table-view
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(testFile.getAbsolutePath())) {
      QueryExecutor queryExecutor =
          new TsFileExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader));

      List<Path> selectedSeries = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        final IDeviceID tabletDeviceID = tablet.getDeviceID(i);
        for (int j = 0; j < measurementSchemaNum; j++) {
          selectedSeries.add(new Path(tabletDeviceID, "s" + j, false));
        }
      }

      final QueryExpression queryExpression = QueryExpression.create(selectedSeries, null);
      final QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
      cnt = 0;
      List<RowRecord> rowRecords = new ArrayList<>();
      while (queryDataSet.hasNext()) {
        rowRecords.add(queryDataSet.next());
        cnt++;
      }
      assertEquals(100, cnt);
    }
  }

  private Tablet genTablet(TableSchema tableSchema, int offset, int num) {
    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            tableSchema.getColumnSchemas(),
            tableSchema.getColumnTypes());
    for (int i = 0; i < num; i++) {
      tablet.addTimestamp(i, offset + i);
      List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
      for (int j = 0; j < columnSchemas.size(); j++) {
        IMeasurementSchema columnSchema = columnSchemas.get(j);
        tablet.addValue(
            columnSchema.getMeasurementId(),
            i,
            getValue(columnSchema.getType(), i, tableSchema.getColumnTypes().get(j)));
      }
    }
    tablet.rowSize = num;
    return tablet;
  }

  public Object getValue(TSDataType dataType, int i, ColumnType columnType) {
    switch (dataType) {
      case INT64:
        return (long) i;
      case TEXT:
        if (columnType.equals(ColumnType.MEASUREMENT)) {
          return new Binary(String.valueOf(i), StandardCharsets.UTF_8);
        } else {
          return String.valueOf(i);
        }
      default:
        return i;
    }
  }

  private TableSchema genTableSchema(int tableNum) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
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
