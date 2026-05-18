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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CsvSourceReaderTest {

  private final String testDir = "target" + File.separator + "csvReaderTest";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
  }

  @After
  public void tearDown() {
    File dir = new File(testDir);
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        f.delete();
      }
    }
    dir.delete();
  }

  private File writeCsv(String name, String content) throws IOException {
    File file = new File(testDir, name);
    try (BufferedWriter w = new BufferedWriter(new FileWriter(file))) {
      w.write(content);
    }
    return file;
  }

  private ImportSchema buildSchema(String timeCol, List<String[]> tagCols, String[]... srcColDefs) {
    ImportSchema schema = new ImportSchema();
    schema.setTableName("test");
    schema.setTimeColumnName(timeCol);

    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    if (tagCols != null) {
      for (String[] t : tagCols) {
        tags.add(new ImportSchema.TagColumn(t[0]));
      }
    }
    schema.setTagColumns(tags);

    List<ImportSchema.SourceColumn> sources = new ArrayList<>();
    for (String[] sc : srcColDefs) {
      if ("SKIP".equals(sc[1])) {
        sources.add(ImportSchema.SourceColumn.skip());
      } else {
        sources.add(new ImportSchema.SourceColumn(sc[0], TSDataType.valueOf(sc[1])));
      }
    }
    schema.setSourceColumns(sources);

    return schema;
  }

  @Test
  public void testReadWithHeader() throws Exception {
    File csv =
        writeCsv(
            "data.csv",
            "time,device,value\n" + "1000,dev1,3.14\n" + "2000,dev1,2.71\n" + "3000,dev2,1.0\n");

    ImportSchema schema =
        buildSchema(
            "time",
            null,
            new String[] {"time", "INT64"},
            new String[] {"device", "TEXT"},
            new String[] {"value", "FLOAT"});

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(3, batch.getRowCount());
      assertEquals(3, batch.getColumnCount());
      assertEquals("time", batch.getColumnName(0));
      assertEquals("device", batch.getColumnName(1));
      assertEquals("value", batch.getColumnName(2));
      assertEquals("1000", batch.getValue(0, 0));
      assertEquals("dev1", batch.getValue(0, 1));
      assertEquals("3.14", batch.getValue(0, 2));

      assertNull(reader.readBatch());
    }
  }

  @Test
  public void testReadWithoutHeader() throws Exception {
    File csv = writeCsv("noheader.csv", "1000,dev1,3.14\n" + "2000,dev1,2.71\n");

    ImportSchema schema =
        buildSchema(
            "time",
            null,
            new String[] {"time", "INT64"},
            new String[] {"device", "TEXT"},
            new String[] {"value", "FLOAT"});
    schema.setHasHeader(false);

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals("1000", batch.getValue(0, 0));
    }
  }

  @Test
  public void testNullFormatHandling() throws Exception {
    File csv = writeCsv("nulls.csv", "time,value\n" + "1000,3.14\n" + "2000,\\N\n" + "3000,\n");

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});
    schema.setNullFormat("\\N");

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(3, batch.getRowCount());
      assertEquals("3.14", batch.getValue(0, 1));
      assertNull(batch.getValue(1, 1));
      assertNull(batch.getValue(2, 1));
    }
  }

  @Test
  public void testChunking() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("time,value\n");
    for (int i = 0; i < 100; i++) {
      sb.append(i).append(",").append(i * 1.0).append("\n");
    }
    File csv = writeCsv("large.csv", sb.toString());

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});

    // Very small chunk size to force multiple batches
    try (CsvSourceReader reader = new CsvSourceReader(csv, schema, 100)) {
      int totalRows = 0;
      int batchCount = 0;
      SourceBatch batch;
      while ((batch = reader.readBatch()) != null) {
        totalRows += batch.getRowCount();
        batchCount++;
      }
      assertEquals(100, totalRows);
      assertTrue("Should have multiple batches", batchCount > 1);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnCountMismatch() throws Exception {
    File csv = writeCsv("mismatch.csv", "time,value,extra\n" + "1000,3.14,x\n");

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      reader.readBatch();
    }
  }

  @Test
  public void testEmptyFile() throws Exception {
    File csv = writeCsv("empty.csv", "time,value\n");

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNull(batch);
    }
  }

  @Test
  public void testSkipColumn() throws Exception {
    File csv = writeCsv("skip.csv", "time,unused,value\n" + "1000,x,3.14\n" + "2000,y,2.71\n");

    ImportSchema schema =
        buildSchema(
            "time",
            null,
            new String[] {"time", "INT64"},
            new String[] {"SKIP", "SKIP"},
            new String[] {"value", "FLOAT"});

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals(3, batch.getColumnCount());
      assertEquals("x", batch.getValue(0, 1));
    }
  }

  @Test
  public void testSemicolonSeparator() throws Exception {
    File csv = writeCsv("semi.csv", "time;value\n" + "1000;3.14\n" + "2000;2.71\n");

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});
    schema.setSeparator(";");

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals("1000", batch.getValue(0, 0));
      assertEquals("3.14", batch.getValue(0, 1));
    }
  }

  @Test
  public void testTabSeparator() throws Exception {
    File csv = writeCsv("tab.csv", "time\tvalue\n" + "1000\t3.14\n" + "2000\t2.71\n");

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});
    schema.setSeparator("\t");

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals("1000", batch.getValue(0, 0));
      assertEquals("3.14", batch.getValue(0, 1));
    }
  }

  // ===== Auto mode tests =====

  @Test
  public void testAutoModeInferSchema() throws Exception {
    File csv =
        writeCsv("auto.csv", "time,temp,status\n" + "1000,25.5,true\n" + "2000,30.1,false\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("auto", schema.getTableName());
      assertEquals("time", schema.getTimeColumnName());
      assertTrue(schema.getTagColumns().isEmpty());

      List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
      assertEquals(2, fields.size());
      assertEquals("temp", fields.get(0).getName());
      assertEquals(TSDataType.DOUBLE, fields.get(0).getDataType());
      assertEquals("status", fields.get(1).getName());
      assertEquals(TSDataType.BOOLEAN, fields.get(1).getDataType());
    }
  }

  @Test
  public void testAutoModeReadBatchIncludesSampleRows() throws Exception {
    File csv = writeCsv("autoread.csv", "time,value\n" + "1000,10\n" + "2000,20\n" + "3000,30\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      reader.inferSchema();
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(3, batch.getRowCount());
      assertNull(reader.readBatch());
    }
  }

  @Test
  public void testAutoModeTableNameFromFilename() throws Exception {
    File csv = writeCsv("sensor_data.csv", "time,val\n1000,1\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("sensor_data", schema.getTableName());
    }
  }

  @Test
  public void testAutoModeTableNameOverride() throws Exception {
    File csv = writeCsv("data.csv", "time,val\n1000,1\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      reader.setOverrideTableName("my_table");
      ImportSchema schema = reader.inferSchema();
      assertEquals("my_table", schema.getTableName());
    }
  }

  @Test
  public void testAutoModeTimePrecisionDefault() throws Exception {
    File csv = writeCsv("prec.csv", "time,val\n1000,1\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("ms", schema.getTimePrecision());
    }
  }

  @Test
  public void testAutoModeTimePrecisionOverride() throws Exception {
    File csv = writeCsv("precov.csv", "time,val\n1000,1\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      reader.setOverrideTimePrecision("us");
      ImportSchema schema = reader.inferSchema();
      assertEquals("us", schema.getTimePrecision());
    }
  }

  @Test
  public void testAutoModeNullFormatSet() throws Exception {
    File csv = writeCsv("autonull.csv", "time,val\n1000,\\N\n2000,3.14\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("\\N", schema.getNullFormat());

      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertNull(batch.getValue(0, 1));
      assertEquals("3.14", batch.getValue(1, 1));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAutoModeNoTimeColumnFails() throws Exception {
    File csv = writeCsv("notime.csv", "ts,val\n1000,1\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      reader.inferSchema();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAutoModeEmptyFileFails() throws Exception {
    File csv = writeCsv("emptyauto.csv", "");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ",")) {
      reader.inferSchema();
    }
  }

  @Test
  public void testAutoModeSemicolonSeparator() throws Exception {
    File csv = writeCsv("semi_auto.csv", "time;value\n1000;3.14\n2000;2.71\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, ";")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("time", schema.getTimeColumnName());

      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
    }
  }

  @Test
  public void testAutoModeTabSeparator() throws Exception {
    File csv = writeCsv("tab_auto.csv", "time\tvalue\n1000\t3.14\n2000\t2.71\n");

    try (CsvSourceReader reader = new CsvSourceReader(csv, "\t")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("time", schema.getTimeColumnName());

      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
    }
  }

  @Test
  public void testMultipleBatchesReturnAllData() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("time,value\n");
    for (int i = 0; i < 50; i++) {
      sb.append((1000 + i)).append(",").append(i * 0.1).append("\n");
    }
    File csv = writeCsv("multi.csv", sb.toString());

    ImportSchema schema =
        buildSchema("time", null, new String[] {"time", "INT64"}, new String[] {"value", "FLOAT"});

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema, 200)) {
      List<SourceBatch> batches = new ArrayList<>();
      SourceBatch batch;
      while ((batch = reader.readBatch()) != null) {
        batches.add(batch);
      }
      int totalRows = 0;
      for (SourceBatch b : batches) {
        totalRows += b.getRowCount();
      }
      assertEquals(50, totalRows);
    }
  }

  // --- Quoted field tokenizer (RFC 4180-ish) ---

  @Test
  public void testSplitLineNoQuotesUsesFastPath() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), ",", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("1000,2.5,hello");
    assertEquals(3, tokens.length);
    assertEquals("1000", tokens[0]);
    assertEquals("2.5", tokens[1]);
    assertEquals("hello", tokens[2]);
  }

  @Test
  public void testSplitLineQuotedFieldWithEmbeddedComma() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), ",", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("1000,\"hello,world\",2.5");
    assertEquals(3, tokens.length);
    assertEquals("1000", tokens[0]);
    assertEquals("hello,world", tokens[1]);
    assertEquals("2.5", tokens[2]);
  }

  @Test
  public void testSplitLineEscapedDoubleQuotes() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), ",", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("1000,\"she said \"\"hi\"\"\",done");
    assertEquals(3, tokens.length);
    assertEquals("1000", tokens[0]);
    assertEquals("she said \"hi\"", tokens[1]);
    assertEquals("done", tokens[2]);
  }

  @Test
  public void testSplitLineEmptyQuotedField() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), ",", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("1000,\"\",2.5");
    assertEquals(3, tokens.length);
    assertEquals("", tokens[1]);
  }

  @Test
  public void testSplitLineTrailingEmptyField() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), ",", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("1000,2.5,");
    assertEquals(3, tokens.length);
    assertEquals("", tokens[2]);
  }

  @Test
  public void testSplitLineMultipleQuotedFields() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), ",", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("\"a,b\",\"c,d\",\"e\"");
    assertEquals(3, tokens.length);
    assertEquals("a,b", tokens[0]);
    assertEquals("c,d", tokens[1]);
    assertEquals("e", tokens[2]);
  }

  @Test
  public void testSplitLineTabSeparator() throws Exception {
    CsvSourceReader reader =
        new CsvSourceReader(new File(testDir, "dummy.csv"), "\t", DEFAULT_CHUNK_SIZE_FOR_TEST);
    String[] tokens = reader.splitLine("1000\t\"hello\tworld\"\t2.5");
    assertEquals(3, tokens.length);
    assertEquals("hello\tworld", tokens[1]);
  }

  @Test
  public void testReadBatchWithQuotedFields() throws Exception {
    File csv =
        writeCsv(
            "quoted.csv",
            "time,note,value\n"
                + "1000,\"hello, world\",1.5\n"
                + "2000,\"she said \"\"hi\"\"\",2.5\n"
                + "3000,plain,3.5\n");

    ImportSchema schema =
        buildSchema(
            "time",
            null,
            new String[] {"time", "INT64"},
            new String[] {"note", "STRING"},
            new String[] {"value", "DOUBLE"});

    try (CsvSourceReader reader = new CsvSourceReader(csv, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(3, batch.getRowCount());
      assertEquals("hello, world", batch.getValue(0, 1));
      assertEquals("she said \"hi\"", batch.getValue(1, 1));
      assertEquals("plain", batch.getValue(2, 1));
      assertEquals("1.5", batch.getValue(0, 2));
      assertEquals("2.5", batch.getValue(1, 2));
    }
  }

  private static final long DEFAULT_CHUNK_SIZE_FOR_TEST = 1024 * 1024;
}
