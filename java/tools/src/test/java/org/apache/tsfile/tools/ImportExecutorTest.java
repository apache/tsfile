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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImportExecutorTest {

  private final String testDir = "target" + File.separator + "executorTest";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
  }

  @After
  public void tearDown() {
    deleteRecursive(new File(testDir));
  }

  private void deleteRecursive(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteRecursive(f);
        }
        f.delete();
      }
    }
    dir.delete();
  }

  private ImportSchema buildSchema() {
    ImportSchema schema = new ImportSchema();
    schema.setTableName("test");
    schema.setTimeColumnName("time");
    schema.setTimePrecision("ms");
    schema.setTagColumns(new ArrayList<ImportSchema.TagColumn>());
    schema.setSourceColumns(
        Arrays.asList(
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT)));
    return schema;
  }

  private SourceBatch makeBatch(Object[]... rows) {
    return SourceBatch.fromRows(Arrays.asList("time", "value"), Arrays.asList(rows));
  }

  @Test
  public void testSingleBatchOutputFileName() {
    ImportSchema schema = buildSchema();
    ImportExecutor executor = new ImportExecutor(schema);
    String outputDir = testDir + File.separator + "single";

    SourceBatch batch = makeBatch(new Object[] {"1000", "3.14"}, new Object[] {"2000", "2.71"});
    TestSourceReader reader = new TestSourceReader(batch);

    boolean ok = executor.execute(reader, outputDir, "sensor");
    assertTrue(ok);
    assertTrue(new File(outputDir, "sensor.tsfile").exists());
    assertFalse(new File(outputDir, "sensor_1.tsfile").exists());
  }

  @Test
  public void testMultiBatchOutputFileNames() {
    ImportSchema schema = buildSchema();
    ImportExecutor executor = new ImportExecutor(schema);
    String outputDir = testDir + File.separator + "multi";

    SourceBatch batch1 = makeBatch(new Object[] {"1000", "1.0"}, new Object[] {"2000", "2.0"});
    SourceBatch batch2 = makeBatch(new Object[] {"3000", "3.0"}, new Object[] {"4000", "4.0"});
    TestSourceReader reader = new TestSourceReader(batch1, batch2);

    boolean ok = executor.execute(reader, outputDir, "sensor");
    assertTrue(ok);
    assertTrue(new File(outputDir, "sensor_1.tsfile").exists());
    assertTrue(new File(outputDir, "sensor_2.tsfile").exists());
    assertFalse(new File(outputDir, "sensor.tsfile").exists());
  }

  @Test
  public void testNullBatchEndsNormally() {
    ImportSchema schema = buildSchema();
    ImportExecutor executor = new ImportExecutor(schema);
    String outputDir = testDir + File.separator + "nullbatch";

    TestSourceReader reader = new TestSourceReader();
    boolean ok = executor.execute(reader, outputDir, "empty");
    assertTrue(ok);
  }

  @Test
  public void testEmptyBatchSkipped() {
    ImportSchema schema = buildSchema();
    ImportExecutor executor = new ImportExecutor(schema);
    String outputDir = testDir + File.separator + "emptybatch";

    SourceBatch emptyBatch =
        SourceBatch.fromRows(Arrays.asList("time", "value"), new ArrayList<Object[]>());
    SourceBatch realBatch = makeBatch(new Object[] {"1000", "1.0"});
    TestSourceReader reader = new TestSourceReader(emptyBatch, realBatch);

    boolean ok = executor.execute(reader, outputDir, "data");
    assertTrue(ok);
    assertTrue(new File(outputDir, "data.tsfile").exists());
  }

  @Test
  public void testOutputDirAutoCreated() {
    ImportSchema schema = buildSchema();
    ImportExecutor executor = new ImportExecutor(schema);
    String outputDir = testDir + File.separator + "auto" + File.separator + "created";

    assertFalse(new File(outputDir).exists());

    SourceBatch batch = makeBatch(new Object[] {"1000", "1.0"});
    TestSourceReader reader = new TestSourceReader(batch);

    boolean ok = executor.execute(reader, outputDir, "out");
    assertTrue(ok);
    assertTrue(new File(outputDir).exists());
    assertTrue(new File(outputDir, "out.tsfile").exists());
  }

  @Test
  public void testExecutorWritesCorrectData() {
    ImportSchema schema = buildSchema();
    ImportExecutor executor = new ImportExecutor(schema);
    String outputDir = testDir + File.separator + "verify";

    SourceBatch batch = makeBatch(new Object[] {"1000", "3.14"}, new Object[] {"2000", "2.71"});
    TestSourceReader reader = new TestSourceReader(batch);

    boolean ok = executor.execute(reader, outputDir, "check");
    assertTrue(ok);

    File tsfile = new File(outputDir, "check.tsfile");
    assertTrue(tsfile.exists());
    assertTrue(tsfile.length() > 0);
  }

  private static class TestSourceReader implements SourceReader {
    private final List<SourceBatch> batches;
    private int index = 0;

    TestSourceReader(SourceBatch... batches) {
      this.batches = Arrays.asList(batches);
    }

    @Override
    public ImportSchema inferSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SourceBatch readBatch() {
      if (index >= batches.size()) {
        return null;
      }
      return batches.get(index++);
    }

    @Override
    public void close() {}
  }
}
