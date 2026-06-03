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
import static org.junit.Assert.assertNotEquals;
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

  /**
   * Tag and source column names use mixed case. {@link org.apache.tsfile.file.metadata.TableSchema}
   * rewrites measurement names to lowercase internally, so tagDefaults / sourceColumnIndex must
   * also be keyed by lowercase for the lookups in {@link TabletBuilder#build} to hit. Regression
   * guard for that fix: if it breaks, all tag values silently fall through and every row collapses
   * to a single device id.
   */
  @Test
  public void testMixedCaseTagAndSourceColumnsResolve() {
    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    tags.add(new ImportSchema.TagColumn("Region"));
    tags.add(new ImportSchema.TagColumn("FactoryNumber"));

    ImportSchema schema =
        buildSchema(
            "test",
            "Time",
            tags,
            new ImportSchema.SourceColumn("Region", TSDataType.TEXT),
            new ImportSchema.SourceColumn("FactoryNumber", TSDataType.TEXT),
            new ImportSchema.SourceColumn("Time", TSDataType.INT64),
            new ImportSchema.SourceColumn("Value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("Region", "FactoryNumber", "Time", "Value"),
            Arrays.asList(
                new Object[] {"hebei", "1001", "1000", "3.14"},
                new Object[] {"jiangsu", "2002", "2000", "2.71"}));

    Tablet tablet = builder.build(batch);
    assertEquals(2, tablet.getRowSize());

    // Different tag values must produce different device ids.
    String dev0 = tablet.getDeviceID(0).toString();
    String dev1 = tablet.getDeviceID(1).toString();
    assertNotEquals("device ids must differ for different tag values", dev0, dev1);

    // Each device id must actually carry the tag value (not just the table name).
    assertTrue("device 0 should contain 'hebei': " + dev0, dev0.contains("hebei"));
    assertTrue("device 0 should contain '1001': " + dev0, dev0.contains("1001"));
    assertTrue("device 1 should contain 'jiangsu': " + dev1, dev1.contains("jiangsu"));
    assertTrue("device 1 should contain '2002': " + dev1, dev1.contains("2002"));
  }

  /**
   * time_column declared in one case, the same logical column declared in source_columns in a
   * different case. resolveTimeColumnIndex must match case-insensitively.
   */
  @Test
  public void testCaseInsensitiveTimeColumnMatch() {
    ImportSchema schema =
        buildSchema(
            "test",
            "time", // declared lowercase
            null,
            new ImportSchema.SourceColumn("Time", TSDataType.INT64), // listed mixed case
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("Time", "value"),
            Collections.singletonList(new Object[] {"1234", "9.99"}));

    Tablet tablet = builder.build(batch);
    assertEquals(1, tablet.getRowSize());
    assertEquals(1234L, tablet.getTimestamps()[0]);
  }

  /**
   * Virtual tag column with DEFAULT value uses a mixed-case name. The default must still land in
   * the device id even though TableSchema lowercases the measurement name.
   */
  @Test
  public void testTagDefaultWithMixedCaseName() {
    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    tags.add(new ImportSchema.TagColumn("Group", "Datang"));

    ImportSchema schema =
        buildSchema(
            "test",
            "Time",
            tags,
            new ImportSchema.SourceColumn("Time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT));

    TabletBuilder builder = new TabletBuilder(schema, new TimeConverter("ms"));

    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("Time", "value"),
            Collections.singletonList(new Object[] {"1000", "3.14"}));

    Tablet tablet = builder.build(batch);
    assertEquals(1, tablet.getRowSize());

    String dev = tablet.getDeviceID(0).toString();
    assertTrue("device should contain 'Datang' (tag default): " + dev, dev.contains("Datang"));
  }

  /**
   * Virtual tag (TAG column not present in the source file, filled in via DEFAULT keyword). All
   * names lowercase, so this isolates the "DEFAULT value actually lands in the device id" assertion
   * from any case-sensitivity concern. The pre-existing testTagDefaultValue only checked row count,
   * so a regression that silently dropped DEFAULT values would not have been caught.
   */
  @Test
  public void testVirtualTagDefaultLandsInDeviceId() {
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

    String dev = tablet.getDeviceID(0).toString();
    assertTrue("device should contain DEFAULT value 'beijing': " + dev, dev.contains("beijing"));
  }
}
