/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TabletBuilderTest {

  private ImportSchema buildSchema(
      String tableName,
      String timeCol,
      List<ImportSchema.TagColumn> tags,
      ImportSchema.SourceColumn... srcCols) {
    ImportSchema schema = new ImportSchema();
    schema.setTableName(tableName);
    schema.setTimeColumnName(timeCol);
    schema.setTagColumns(tags != null ? tags : new ArrayList<>());
    schema.setSourceColumns(Arrays.asList(srcCols));
    return schema;
  }

  @Test
  public void testBasicBuild() {
    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            null,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "value"),
            Arrays.asList(new Object[] {"1000", "3.14"}, new Object[] {"2000", "2.71"}));

    Tablet tablet = builder.build(batch);
    assertEquals(2, tablet.getRowSize());
  }

  @Test
  public void testTimeSorting() {
    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            null,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "value"),
            Arrays.asList(
                new Object[] {"3000", "3.0"},
                new Object[] {"1000", "1.0"},
                new Object[] {"2000", "2.0"}));

    Tablet tablet = builder.build(batch);
    assertEquals(3, tablet.getRowSize());
    assertTrue(tablet.getTimestamps()[0] <= tablet.getTimestamps()[1]);
    assertTrue(tablet.getTimestamps()[1] <= tablet.getTimestamps()[2]);
    assertEquals(1000L, tablet.getTimestamps()[0]);
    assertEquals(2000L, tablet.getTimestamps()[1]);
    assertEquals(3000L, tablet.getTimestamps()[2]);
  }

  @Test
  public void testTagColumns() {
    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    tags.add(new ImportSchema.TagColumn("device"));

    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            tags,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("device", TSDataType.TEXT),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "device", "value"),
            Collections.singletonList(new Object[] {"1000", "dev1", "3.14"}));

    Tablet tablet = builder.build(batch);
    assertEquals(1, tablet.getRowSize());
  }

  @Test
  public void testTagDefaultValue() {
    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    tags.add(new ImportSchema.TagColumn("region", "beijing"));

    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            tags,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "value"),
            Collections.singletonList(new Object[] {"1000", "3.14"}));

    Tablet tablet = builder.build(batch);
    assertEquals(1, tablet.getRowSize());
  }

  @Test
  public void testNullValues() {
    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            null,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "value"),
            Arrays.asList(new Object[] {"1000", null}, new Object[] {"2000", "2.71"}));

    Tablet tablet = builder.build(batch);
    assertEquals(2, tablet.getRowSize());
  }

  @Test
  public void testNullFormatRecognition() {
    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            null,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));
    schema.setNullFormat("\\N");

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "value"),
            Arrays.asList(new Object[] {"1000", "\\N"}, new Object[] {"2000", "2.71"}));

    Tablet tablet = builder.build(batch);
    assertEquals(2, tablet.getRowSize());
  }

  @Test
  public void testSkipColumn() {
    ImportSchema schema =
        buildSchema(
            "test",
            "time",
            null,
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            ImportSchema.SourceColumn.skip(),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("time", "unused", "value"),
            Collections.singletonList(new Object[] {"1000", "x", "3.14"}));

    Tablet tablet = builder.build(batch);
    assertEquals(1, tablet.getRowSize());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(1, fields.size());
    assertEquals("value", fields.get(0).getName());
  }

  @Test
  public void testTableSchemaStructure() {
    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    tags.add(new ImportSchema.TagColumn("device"));

    ImportSchema schema =
        buildSchema(
            "myTable",
            "ts",
            tags,
            new ImportSchema.SourceColumn("ts", TSDataType.INT64),
            new ImportSchema.SourceColumn("device", TSDataType.TEXT),
            new ImportSchema.SourceColumn("temp", TSDataType.FLOAT),
            new ImportSchema.SourceColumn("humidity", TSDataType.DOUBLE));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    assertEquals("mytable", builder.getTableSchema().getTableName());
    // TAG: device, FIELD: temp, humidity → 3 column schemas
    assertEquals(3, builder.getTableSchema().getColumnSchemas().size());
  }
}
