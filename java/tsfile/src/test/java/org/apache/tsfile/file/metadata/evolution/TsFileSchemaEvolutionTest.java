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

package org.apache.tsfile.file.metadata.evolution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.DeviceTableModelWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TsFileSchemaEvolutionTest {

  private static final String TEST_FILE_PATH = "target/schema_evolution_test.tsfile";
  private File tsFile;

  @Before
  public void setUp() throws Exception {
    tsFile = new File(TEST_FILE_PATH);
    if (tsFile.exists()) {
      assertTrue(tsFile.delete());
    }

    List<TableSchema> tableSchemas = Arrays.asList(
        new TableSchema("t1", Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("tag2", TSDataType.STRING),
            new MeasurementSchema("f1", TSDataType.INT32),
            new MeasurementSchema("f2", TSDataType.DOUBLE)
        ), Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD
        )),
        new TableSchema("t2", Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("tag2", TSDataType.STRING),
            new MeasurementSchema("f1", TSDataType.INT32),
            new MeasurementSchema("f2", TSDataType.DOUBLE)
        ), Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD
        )),
        new TableSchema("t3", Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("tag2", TSDataType.STRING),
            new MeasurementSchema("f1", TSDataType.INT32),
            new MeasurementSchema("f2", TSDataType.DOUBLE)
        ), Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD
        ))
    );

    try (DeviceTableModelWriter writer = new DeviceTableModelWriter(tsFile)) {
      tableSchemas.forEach(writer::registerTableSchema);

      for (int i = 0; i < tableSchemas.size(); i++) {
        TableSchema tableSchema = tableSchemas.get(i);
        Tablet tablet = new Tablet(tableSchema);
        tablet.addTimestamp(0, 0);
        tablet.addValue(0, 0, "t" + (i + 1) + "-tag1");
        tablet.addValue(0, 1, "t" + (i + 1) + "-tag2");
        tablet.addValue(0, 2, i * 10  + 3);
        tablet.addValue(0, 3, i * 10  + 4.0);
        writer.write(tablet);
      }
    }
  }

  @After
  public void tearDown() {
    if (tsFile != null && tsFile.exists()) {
      assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testTableRename()
      throws IOException, ReadProcessException, NoTableException, NoMeasurementException {
    // rename t1 -> t4
    TableRename tableRename = new TableRename("t1", "t4");
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(TEST_FILE_PATH);
    rewriter.appendProperties(Collections.singletonMap(tableRename.propertyKey(), tableRename.propertyValue()));

    // Verify the table has been renamed
    try (ITsFileReader reader = new TsFileReaderBuilder().file(new File(TEST_FILE_PATH)).build()) {
      assertFalse(reader.getTableSchemas("t1").isPresent());
      Optional<TableSchema> t = reader.getTableSchemas("t4");
      assertTrue(t.isPresent());
      TableSchema tableSchema = t.get();
      assertEquals("t1", tableSchema.getTableName());

      assertThrows(NoTableException.class, () -> reader.query("t1", Arrays.asList("f1", "f2"), 0, 10));
      ResultSet resultSet = reader.query("t4", Arrays.asList("f1", "f2"), 0, 10);
      assertTrue(resultSet.next());
      assertEquals(3, resultSet.getInt("f1"));
      assertEquals(4.0, resultSet.getDouble("f2"), 0.0001);
      assertFalse(resultSet.next());
    }

    // rename t2 -> t1
    tableRename = new TableRename("t2", "t1");
    rewriter.appendProperties(Collections.singletonMap(tableRename.propertyKey(), tableRename.propertyValue()));

    // Verify the table has been renamed
    try (ITsFileReader reader = new TsFileReaderBuilder().file(new File(TEST_FILE_PATH)).build()) {
      assertFalse(reader.getTableSchemas("t2").isPresent());
      Optional<TableSchema> t = reader.getTableSchemas("t1");
      assertTrue(t.isPresent());
      TableSchema tableSchema = t.get();
      assertEquals("t2", tableSchema.getTableName());

      assertThrows(NoTableException.class, () -> reader.query("t2", Arrays.asList("f1", "f2"), 0, 10));
      ResultSet resultSet = reader.query("t1", Arrays.asList("f1", "f2"), 0, 10);
      assertTrue(resultSet.next());
      assertEquals(13, resultSet.getInt("f1"));
      assertEquals(14.0, resultSet.getDouble("f2"), 0.0001);
      assertFalse(resultSet.next());
    }

    // t3 is not affected
    try (ITsFileReader reader = new TsFileReaderBuilder().file(new File(TEST_FILE_PATH)).build()) {
      Optional<TableSchema> t1 = reader.getTableSchemas("t3");
      assertTrue(t1.isPresent());
      TableSchema tableSchema = t1.get();
      assertEquals("t3", tableSchema.getTableName());

      ResultSet resultSet = reader.query("t3", Arrays.asList("f1", "f2"), 0, 10);
      assertTrue(resultSet.next());
      assertEquals(23, resultSet.getInt("f1"));
      assertEquals(24.0, resultSet.getDouble("f2"), 0.0001);
      assertFalse(resultSet.next());
    }

    // test read timeseries metadata
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(TEST_FILE_PATH)) {
      TimeseriesMetadata timeseriesMetadata = sequenceReader.readTimeseriesMetadata(
          Factory.DEFAULT_FACTORY.create(new String[]{"t4", "t1-tag1", "t1-tag2"}), "f1", false);
      assertEquals(3, timeseriesMetadata.getStatistics().getMaxValue());
    }
  }
}
