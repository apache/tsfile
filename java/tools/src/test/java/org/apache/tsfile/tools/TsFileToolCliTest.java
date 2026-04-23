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

import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileToolCliTest {
  private final String testDir = "target" + File.separator + "cliTest";
  private final String outputDir = testDir + File.separator + "output";
  private final String failedDir = testDir + File.separator + "failed";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
    new File(outputDir).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  private String createCsvFile(String name, String[] headers, String[][] rows) throws IOException {
    String path = testDir + File.separator + name;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path))) {
      w.write(String.join(",", headers) + "\n");
      for (String[] row : rows) {
        w.write(String.join(",", row) + "\n");
      }
    }
    return new File(path).getAbsolutePath();
  }

  private String createSchemaFile(String name, String content) throws IOException {
    String path = testDir + File.separator + name;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path))) {
      w.write(content);
    }
    return new File(path).getAbsolutePath();
  }

  private int queryRowCount(String tsfilePath, String tableName, List<String> columns)
      throws Exception {
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      TsBlockReader reader = tableQueryExecutor.query(tableName, columns, null, null, null);
      int cnt = 0;
      while (reader.hasNext()) {
        cnt += reader.next().getPositionCount();
      }
      return cnt;
    }
  }

  @Test
  public void testCsvAutoModeViaCli() throws Exception {
    String csvPath =
        createCsvFile(
            "auto.csv",
            new String[] {"time", "temp", "humidity"},
            new String[][] {
              {"1000", "25.5", "60.0"},
              {"1001", "26.0", "61.0"},
              {"1002", "26.5", "62.0"}
            });

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(new String[] {"-s" + csvPath, "-t" + target});

    String tsfile = outputDir + File.separator + "auto.tsfile";
    assertTrue(new File(tsfile).exists());
    assertEquals(3, queryRowCount(tsfile, "auto", Arrays.asList("temp", "humidity")));
  }

  @Test
  public void testCsvSchemaModeViaCli() throws Exception {
    String csvPath =
        createCsvFile(
            "schema.csv",
            new String[] {"time", "tag1", "val"},
            new String[][] {
              {"1000", "s1", "10.0"},
              {"1001", "s1", "11.0"}
            });

    String schemaPath =
        createSchemaFile(
            "schema.txt",
            "table_name=root.test\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n"
                + "null_format=\\N\n\n"
                + "id_columns\n"
                + "tag1\n"
                + "time_column=time\n"
                + "csv_columns\n"
                + "time INT64,\n"
                + "tag1 TEXT,\n"
                + "val DOUBLE\n");

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(new String[] {"-s" + csvPath, "-schema" + schemaPath, "-t" + target});

    String tsfile = outputDir + File.separator + "schema.tsfile";
    assertTrue(new File(tsfile).exists());
    assertEquals(2, queryRowCount(tsfile, "root.test", Arrays.asList("val")));
  }

  @Test
  public void testTableNameOverrideViaCli() throws Exception {
    String csvPath =
        createCsvFile(
            "data.csv",
            new String[] {"time", "val"},
            new String[][] {{"1000", "1.0"}, {"1001", "2.0"}});

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(new String[] {"-s" + csvPath, "-t" + target, "--table_name", "custom_table"});

    String tsfile = outputDir + File.separator + "data.tsfile";
    assertTrue(new File(tsfile).exists());
    assertEquals(2, queryRowCount(tsfile, "custom_table", Arrays.asList("val")));
  }

  @Test
  public void testSeparatorTabViaCli() throws Exception {
    String csvPath = testDir + File.separator + "tab.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(csvPath))) {
      w.write("time\tval\n");
      w.write("1000\t10.0\n");
      w.write("1001\t20.0\n");
    }

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(csvPath).getAbsolutePath(), "-t" + target, "--separator", "tab"
        });

    String tsfile = outputDir + File.separator + "tab.tsfile";
    assertTrue(new File(tsfile).exists());
    assertEquals(2, queryRowCount(tsfile, "tab", Arrays.asList("val")));
  }

  @Test
  public void testFormatParquetViaCli() throws Exception {
    String pqFile = testDir + File.separator + "data.parquet";
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(new File(pqFile).toPath()))
            .withType(pqSchema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      for (int i = 0; i < 5; i++) {
        writer.write(factory.newGroup().append("time", 1000L + i).append("value", i * 1.1));
      }
    }

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(pqFile).getAbsolutePath(), "-t" + target, "--format", "parquet"
        });

    String tsfile = outputDir + File.separator + "data.tsfile";
    assertTrue(new File(tsfile).exists());
    assertEquals(5, queryRowCount(tsfile, "data", Arrays.asList("value")));
  }

  @Test
  public void testFormatArrowViaCli() throws Exception {
    String arFile = testDir + File.separator + "data.arrow";
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    fields.add(
        new Field(
            "value",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null));
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(fields);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc);
        FileOutputStream fos = new FileOutputStream(arFile);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      BigIntVector tv = (BigIntVector) root.getVector("time");
      Float8Vector vv = (Float8Vector) root.getVector("value");
      tv.allocateNew(4);
      vv.allocateNew(4);
      for (int i = 0; i < 4; i++) {
        tv.set(i, 1000L + i);
        vv.set(i, i * 2.2);
      }
      root.setRowCount(4);
      writer.writeBatch();
      writer.end();
    }

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(arFile).getAbsolutePath(), "-t" + target, "--format", "arrow"
        });

    String tsfile = outputDir + File.separator + "data.tsfile";
    assertTrue(new File(tsfile).exists());
    assertEquals(4, queryRowCount(tsfile, "data", Arrays.asList("value")));
  }

  @Test
  public void testMissingSourceParam() {
    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(new String[] {"-t" + target});
    File[] files = new File(outputDir).listFiles((d, n) -> n.endsWith(".tsfile"));
    assertTrue("No tsfile should be generated", files == null || files.length == 0);
  }

  @Test
  public void testMissingTargetParam() throws Exception {
    String csvPath =
        createCsvFile(
            "missing_target.csv", new String[] {"time", "val"}, new String[][] {{"1000", "1.0"}});

    TsFileTool.main(new String[] {"-s" + csvPath});
    File[] files = new File(outputDir).listFiles((d, n) -> n.endsWith(".tsfile"));
    assertTrue("No tsfile should be generated", files == null || files.length == 0);
  }

  @Test
  public void testDirectoryModeViaCli() throws Exception {
    String subDir = testDir + File.separator + "multi";
    new File(subDir).mkdirs();

    for (String name : new String[] {"a.csv", "b.csv"}) {
      try (BufferedWriter w = new BufferedWriter(new FileWriter(subDir + File.separator + name))) {
        w.write("time,val\n");
        w.write("1000,10.0\n");
        w.write("1001,20.0\n");
      }
    }

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(new String[] {"-s" + new File(subDir).getAbsolutePath(), "-t" + target});

    assertTrue(new File(outputDir, "a.tsfile").exists());
    assertTrue(new File(outputDir, "b.tsfile").exists());
    assertEquals(
        2, queryRowCount(outputDir + File.separator + "a.tsfile", "a", Arrays.asList("val")));
    assertEquals(
        2, queryRowCount(outputDir + File.separator + "b.tsfile", "b", Arrays.asList("val")));
  }

  @Test
  public void testDirectoryModeWithTableNameOverride() throws Exception {
    String subDir = testDir + File.separator + "multi_tn";
    new File(subDir).mkdirs();

    for (String name : new String[] {"x.csv", "y.csv"}) {
      try (BufferedWriter w = new BufferedWriter(new FileWriter(subDir + File.separator + name))) {
        w.write("time,val\n");
        w.write("2000,5.0\n");
      }
    }

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(subDir).getAbsolutePath(), "-t" + target, "--table_name", "shared"
        });

    assertTrue(new File(outputDir, "x.tsfile").exists());
    assertTrue(new File(outputDir, "y.tsfile").exists());
    assertEquals(
        1, queryRowCount(outputDir + File.separator + "x.tsfile", "shared", Arrays.asList("val")));
    assertEquals(
        1, queryRowCount(outputDir + File.separator + "y.tsfile", "shared", Arrays.asList("val")));
  }

  @Test
  public void testParquetSchemaModeWithTagViaCli() throws Exception {
    String pqFile = testDir + File.separator + "tagged.parquet";
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("device")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("temp")
            .named("sensor");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(new File(pqFile).toPath()))
            .withType(pqSchema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      for (int i = 0; i < 5; i++) {
        writer.write(
            factory
                .newGroup()
                .append("time", 1000L + i)
                .append("device", "d1")
                .append("temp", 20.0 + i));
      }
    }

    String schemaPath =
        createSchemaFile(
            "pq_schema.txt",
            "table_name=root.pq\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n\n"
                + "tag_columns\n"
                + "device\n"
                + "time_column=time\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "device TEXT,\n"
                + "temp DOUBLE\n");

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(pqFile).getAbsolutePath(),
          "-schema" + schemaPath,
          "-t" + target,
          "--format",
          "parquet"
        });

    String tsfile = outputDir + File.separator + "tagged.tsfile";
    assertTrue("TsFile should exist", new File(tsfile).exists());
    assertEquals(5, queryRowCount(tsfile, "root.pq", Arrays.asList("temp")));
  }

  @Test
  public void testArrowSchemaModeWithTagViaCli() throws Exception {
    String arFile = testDir + File.separator + "tagged.arrow";
    List<Field> arrowFields = new ArrayList<>();
    arrowFields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    arrowFields.add(new Field("device", FieldType.notNullable(new ArrowType.Utf8()), null));
    arrowFields.add(
        new Field(
            "temp",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null));
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(arrowFields);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc);
        FileOutputStream fos = new FileOutputStream(arFile);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      BigIntVector tv = (BigIntVector) root.getVector("time");
      org.apache.arrow.vector.VarCharVector dv =
          (org.apache.arrow.vector.VarCharVector) root.getVector("device");
      Float8Vector tp = (Float8Vector) root.getVector("temp");
      tv.allocateNew(4);
      dv.allocateNew();
      tp.allocateNew(4);
      for (int i = 0; i < 4; i++) {
        tv.set(i, 2000L + i);
        dv.set(i, "sensor1".getBytes(StandardCharsets.UTF_8));
        tp.set(i, 30.0 + i);
      }
      root.setRowCount(4);
      writer.writeBatch();
      writer.end();
    }

    String schemaPath =
        createSchemaFile(
            "ar_schema.txt",
            "table_name=root.ar\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n\n"
                + "tag_columns\n"
                + "device\n"
                + "time_column=time\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "device TEXT,\n"
                + "temp DOUBLE\n");

    String target = new File(outputDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(arFile).getAbsolutePath(),
          "-schema" + schemaPath,
          "-t" + target,
          "--format",
          "arrow"
        });

    String tsfile = outputDir + File.separator + "tagged.tsfile";
    assertTrue("TsFile should exist", new File(tsfile).exists());
    assertEquals(4, queryRowCount(tsfile, "root.ar", Arrays.asList("temp")));
  }

  @Test
  public void testParseBlockSize() {
    assertEquals(1024L, TsFileTool.parseBlockSize("1K"));
    assertEquals(256L * 1024 * 1024, TsFileTool.parseBlockSize("256M"));
    assertEquals(1024L * 1024 * 1024, TsFileTool.parseBlockSize("1G"));
    assertEquals(12345L, TsFileTool.parseBlockSize("12345"));
  }

  @Test
  public void testCsvFailDirViaCli() throws Exception {
    String csvPath = testDir + File.separator + "bad.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(csvPath))) {
      w.write("time,tag1,val\n");
      w.write("1000,s1,10.0\n");
      w.write("badtime,s1,20.0\n");
      w.write("1002,s1,30.0\n");
    }

    String schemaPath =
        createSchemaFile(
            "fail_schema.txt",
            "table_name=root.fail\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n"
                + "null_format=\\N\n\n"
                + "id_columns\n"
                + "tag1\n"
                + "time_column=time\n"
                + "csv_columns\n"
                + "time INT64,\n"
                + "tag1 TEXT,\n"
                + "val DOUBLE\n");

    String target = new File(outputDir).getAbsolutePath();
    String fd = new File(failedDir).getAbsolutePath();
    TsFileTool.main(
        new String[] {
          "-s" + new File(csvPath).getAbsolutePath(),
          "-schema" + schemaPath,
          "-t" + target,
          "-fail_dir" + fd
        });

    assertTrue("Failed file should exist in fail_dir", new File(failedDir, "bad.csv").exists());
  }

  @Test
  public void testCsvAutoModeTypeVerification() throws Exception {
    String csvPath =
        createCsvFile(
            "types.csv",
            new String[] {"time", "bool_col", "int_col", "double_col", "str_col"},
            new String[][] {
              {"1000", "true", "42", "3.14", "hello"},
              {"1001", "false", "99", "2.72", "world"},
              {"1002", "true", "7", "1.41", "test"}
            });

    File csvFile = new File(csvPath);
    try (CsvSourceReader reader = new CsvSourceReader(csvFile, ",")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("types", schema.getTableName());
      assertEquals("time", schema.getTimeColumnName());

      List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
      assertEquals(4, fields.size());
      assertEquals(
          org.apache.tsfile.enums.TSDataType.BOOLEAN, findField(fields, "bool_col").getDataType());
      assertEquals(
          org.apache.tsfile.enums.TSDataType.INT64, findField(fields, "int_col").getDataType());
      assertEquals(
          org.apache.tsfile.enums.TSDataType.DOUBLE, findField(fields, "double_col").getDataType());
      assertEquals(
          org.apache.tsfile.enums.TSDataType.STRING, findField(fields, "str_col").getDataType());

      String outDir = testDir + File.separator + "type_output";
      ImportExecutor executor = new ImportExecutor(schema);
      assertTrue(executor.execute(reader, outDir, "types"));

      String tsfile = outDir + File.separator + "types.tsfile";
      assertTrue(new File(tsfile).exists());
      assertEquals(
          3, queryRowCount(tsfile, "types", Arrays.asList("bool_col", "int_col", "double_col")));
    }
  }

  @Test
  public void testCsvAutoModeTypePromotion() throws Exception {
    String csvPath =
        createCsvFile(
            "promo.csv",
            new String[] {"time", "mixed"},
            new String[][] {
              {"1000", "42"},
              {"1001", "3.14"},
              {"1002", "100"}
            });

    File csvFile = new File(csvPath);
    try (CsvSourceReader reader = new CsvSourceReader(csvFile, ",")) {
      ImportSchema schema = reader.inferSchema();
      ImportSchema.SourceColumn mixedField = findField(schema.fieldColumns(), "mixed");
      assertEquals(org.apache.tsfile.enums.TSDataType.DOUBLE, mixedField.getDataType());
    }
  }

  private ImportSchema.SourceColumn findField(List<ImportSchema.SourceColumn> fields, String name) {
    for (ImportSchema.SourceColumn f : fields) {
      if (f.getName().equals(name)) {
        return f;
      }
    }
    throw new AssertionError("Field not found: " + name);
  }
}
