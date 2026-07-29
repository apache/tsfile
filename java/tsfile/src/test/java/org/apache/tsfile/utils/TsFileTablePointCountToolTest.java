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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;

public class TsFileTablePointCountToolTest {

  /**
   * Creates a two-table file without point-count properties. The repair tool must count only
   * non-null FIELD values from chunk statistics, preserve unrelated properties and file data, and
   * leave the file byte-for-byte unchanged when run again.
   */
  @Test
  public void backfillMissingTablePointCountProperties() throws Exception {
    File file = File.createTempFile("table-point-count-tool", ".tsfile");
    try {
      TableSchema tableSchema1 =
          new TableSchema(
              "table1",
              Arrays.asList(
                  new ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                  new ColumnSchema("s1", TSDataType.INT32, ColumnCategory.FIELD),
                  new ColumnSchema("s2", TSDataType.INT32, ColumnCategory.FIELD)));
      TableSchema tableSchema2 =
          new TableSchema(
              "table2",
              Arrays.asList(
                  new ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                  new ColumnSchema("s1", TSDataType.INT32, ColumnCategory.FIELD)));
      try (TsFileIOWriter ioWriter = new TsFileIOWriter(file);
          TsFileWriter writer = new TsFileWriter(ioWriter)) {
        ioWriter.addTsFileProperty("custom.property", "preserved");
        writer.registerTableSchema(tableSchema1);
        writer.registerTableSchema(tableSchema2);
        writer.writeTable(createTablet(tableSchema1, 3, true));
        writer.writeTable(createTablet(tableSchema2, 2, false));
      }

      Assert.assertFalse(TsFileTablePointCountTool.containsTablePointCount(file));
      Assert.assertEquals(
          TsFileTablePointCountTool.UpdateStatus.UPDATED,
          TsFileTablePointCountTool.updateTablePointCountIfMissing(file));
      Assert.assertTrue(TsFileTablePointCountTool.containsTablePointCount(file));

      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
        Assert.assertTrue(reader.isComplete());
        Map<String, String> properties = reader.getTsFileProperties();
        Assert.assertEquals("preserved", properties.get("custom.property"));
        Assert.assertEquals(
            "5", properties.get(TsFileIOWriter.TABLE_POINT_COUNT_PROPERTY_PREFIX + "table1"));
        Assert.assertEquals(
            "2", properties.get(TsFileIOWriter.TABLE_POINT_COUNT_PROPERTY_PREFIX + "table2"));
        Assert.assertEquals(2, reader.getAllDevices().size());
      }

      byte[] repairedFile = Files.readAllBytes(file.toPath());
      Assert.assertEquals(
          TsFileTablePointCountTool.UpdateStatus.ALREADY_PRESENT,
          TsFileTablePointCountTool.updateTablePointCountIfMissing(file));
      Assert.assertArrayEquals(repairedFile, Files.readAllBytes(file.toPath()));
    } finally {
      Files.deleteIfExists(file.toPath());
    }
  }

  private Tablet createTablet(TableSchema tableSchema, int rowCount, boolean nullableSecondField) {
    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes());
    for (int row = 0; row < rowCount; row++) {
      tablet.addTimestamp(row, row);
      tablet.addValue("device", row, "d1");
      tablet.addValue("s1", row, row);
      if (nullableSecondField) {
        tablet.addValue("s2", row, row == rowCount - 1 ? null : row);
      }
    }
    return tablet;
  }
}
