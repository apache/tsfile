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
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TsfiletoolsTest {
  private final String testDir = "target" + File.separator + "csvTest";
  private final String csvFile = testDir + File.separator + "data.csv";

  private final String wrongCsvFile = testDir + File.separator + "dataWrong.csv";
  private final String schemaFile = testDir + File.separator + "schemaFile.txt";

  private final String failedDir = testDir + File.separator + "failed";

  float[] tmpResult2 = new float[20];
  float[] tmpResult3 = new float[20];
  float[] tmpResult5 = new float[20];

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
    genCsvFile(20);
    genWrongCsvFile(100);
    genSchemaFile();
  }

  public void genSchemaFile() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFile))) {
      writer.write("table_name=root.db1");
      writer.newLine();
      writer.write("time_precision=ms");
      writer.newLine();
      writer.write("has_header=true");
      writer.newLine();
      writer.write("separator=,");
      writer.newLine();
      writer.write("null_format=\\N");
      writer.newLine();
      writer.newLine();
      writer.write("id_columns");
      writer.newLine();
      writer.write("tmp1");
      writer.newLine();
      writer.write("time_column=time");
      writer.newLine();
      writer.write("csv_columns");
      writer.newLine();
      writer.write("time INT64,");
      writer.newLine();
      writer.write("tmp1 TEXT,");
      writer.newLine();
      writer.write("tmp2 FLOAT,");
      writer.newLine();
      writer.write("tmp3 FLOAT,");
      writer.newLine();
      writer.write("SKIP,");
      writer.newLine();
      writer.write("tmp5 FLOAT");
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate schema file", e);
    }
  }

  public void genWrongCsvFile(int rows) {

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(wrongCsvFile))) {
      writer.write("time,tmp1,tmp2,tmp3,tmp4,tmp5");
      writer.newLine();
      Random random = new Random();
      long timestamp = System.currentTimeMillis();

      for (int i = 0; i < rows; i++) {
        timestamp = timestamp + i;
        String tmp1 = "s1";
        float tmp2 = random.nextFloat();
        float tmp3 = random.nextFloat();
        float tmp4 = random.nextFloat();
        float tmp5 = random.nextFloat();
        if (i % 99 == 0) {
          writer.write(
              timestamp + "aa" + "," + tmp1 + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        } else {
          writer.write(timestamp + "," + tmp1 + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        }

        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate wrong CSV file", e);
    }
  }

  public void genCsvFile(int rows) {

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
      writer.write("time,tmp1,tmp2,tmp3,tmp4,tmp5");
      writer.newLine();
      Random random = new Random();
      long timestamp = System.currentTimeMillis();

      for (int i = 0; i < rows; i++) {
        timestamp = timestamp + i;
        String tmp1 = "s1";
        float tmp2 = random.nextFloat();
        float tmp3 = random.nextFloat();
        float tmp4 = random.nextFloat();
        float tmp5 = random.nextFloat();
        tmpResult2[i] = tmp2;
        tmpResult3[i] = tmp3;
        tmpResult5[i] = tmp5;
        writer.write(timestamp + "," + tmp1 + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate CSV file", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  private int queryTsFile(String tsfilePath, String tableName, List<String> columns)
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
  public void testCsvToTsfile() throws Exception {
    String scFilePath = new File(schemaFile).getAbsolutePath();
    String csvFilePath = new File(csvFile).getAbsolutePath();
    String targetPath = new File(testDir).getAbsolutePath();
    String dataTsfilePath = new File(targetPath + File.separator + "data.tsfile").getAbsolutePath();
    String[] args = new String[] {"-s" + csvFilePath, "-schema" + scFilePath, "-t" + targetPath};
    TsFileTool.main(args);
    List<String> columns = new ArrayList<>();
    columns.add("tmp2");
    columns.add("tmp3");
    columns.add("tmp5");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(dataTsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader = tableQueryExecutor.query("root.db1", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        float[] floats_tmp2 = result.getColumn(0).getFloats();
        float[] floats_tmp3 = result.getColumn(1).getFloats();
        float[] floats_tmp5 = result.getColumn(2).getFloats();
        for (int i = 0; i < 20; i++) {
          assertEquals(tmpResult2[i], floats_tmp2[i], 0);
          assertEquals(tmpResult3[i], floats_tmp3[i], 0);
          assertEquals(tmpResult5[i], floats_tmp5[i], 0);
        }
        cnt += result.getPositionCount();
      }
      assertEquals(20, cnt);
    }
  }

  @Test
  public void testCsvAutoModeEndToEnd() throws Exception {
    String autoDir = testDir + File.separator + "auto";
    new File(autoDir).mkdirs();

    String autoCsvFile = autoDir + File.separator + "autotest.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(autoCsvFile))) {
      w.write("time,temperature,humidity\n");
      long ts = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        w.write((ts + i) + "," + (20.0 + i) + "," + (50.0 + i) + "\n");
      }
    }

    String autoOutput = autoDir + File.separator + "output";
    File csvFile = new File(autoCsvFile);

    try (CsvSourceReader reader = new CsvSourceReader(csvFile, ",")) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("autotest", schema.getTableName());
      assertEquals("time", schema.getTimeColumnName());
      assertTrue(schema.getTagColumns().isEmpty());
      assertEquals(2, schema.fieldColumns().size());

      ImportExecutor executor = new ImportExecutor(schema);
      boolean ok = executor.execute(reader, autoOutput, "autotest");
      assertTrue(ok);
    }

    String tsfilePath = autoOutput + File.separator + "autotest.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());

    List<String> columns = new ArrayList<>();
    columns.add("temperature");
    columns.add("humidity");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader = tableQueryExecutor.query("autotest", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        cnt += result.getPositionCount();
      }
      assertEquals(10, cnt);
    }
  }

  @Test
  public void testCsvAutoModeWithTableNameOverride() throws Exception {
    String autoDir = testDir + File.separator + "auto_override";
    new File(autoDir).mkdirs();

    String autoCsvFile = autoDir + File.separator + "raw.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(autoCsvFile))) {
      w.write("time,val\n");
      w.write(System.currentTimeMillis() + ",1.23\n");
    }

    String autoOutput = autoDir + File.separator + "output";
    File csvFile = new File(autoCsvFile);

    try (CsvSourceReader reader = new CsvSourceReader(csvFile, ",")) {
      reader.setOverrideTableName("my_custom_table");
      ImportSchema schema = reader.inferSchema();
      assertEquals("my_custom_table", schema.getTableName());

      ImportExecutor executor = new ImportExecutor(schema);
      boolean ok = executor.execute(reader, autoOutput, "raw");
      assertTrue(ok);
    }

    assertTrue(new File(autoOutput, "raw.tsfile").exists());
  }

  @Test
  public void testParquetAutoModeEndToEnd() throws Exception {
    String pqDir = testDir + File.separator + "parquet_auto";
    new File(pqDir).mkdirs();

    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("temperature")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("humidity")
            .named("sensor");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    String pqFile = pqDir + File.separator + "sensor.parquet";
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(new File(pqFile).toPath()))
            .withType(pqSchema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      long ts = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        writer.write(
            factory
                .newGroup()
                .append("time", ts + i)
                .append("temperature", 20.0f + i)
                .append("humidity", 50.0 + i));
      }
    }

    String outputDir = pqDir + File.separator + "output";
    File inputFile = new File(pqFile);

    try (ParquetSourceReader reader = new ParquetSourceReader(inputFile)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("sensor", schema.getTableName());

      ImportExecutor executor = new ImportExecutor(schema);
      boolean ok = executor.execute(reader, outputDir, "sensor");
      assertTrue(ok);
    }

    String tsfilePath = outputDir + File.separator + "sensor.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());

    List<String> columns = new ArrayList<>();
    columns.add("temperature");
    columns.add("humidity");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader = tableQueryExecutor.query("sensor", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        cnt += result.getPositionCount();
      }
      assertEquals(10, cnt);
    }
  }

  @Test
  public void testArrowAutoModeEndToEnd() throws Exception {
    String arDir = testDir + File.separator + "arrow_auto";
    new File(arDir).mkdirs();

    List<Field> arrowFields = new ArrayList<>();
    arrowFields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    arrowFields.add(
        new Field(
            "temperature",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null));
    arrowFields.add(
        new Field(
            "humidity",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null));
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(arrowFields);

    String arrowFile = arDir + File.separator + "telemetry.arrow";
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc);
        FileOutputStream fos = new FileOutputStream(arrowFile);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      BigIntVector tv = (BigIntVector) root.getVector("time");
      Float4Vector tp = (Float4Vector) root.getVector("temperature");
      Float8Vector hm = (Float8Vector) root.getVector("humidity");
      long ts = System.currentTimeMillis();
      tv.allocateNew(10);
      tp.allocateNew(10);
      hm.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        tv.set(i, ts + i);
        tp.set(i, 20.0f + i);
        hm.set(i, 50.0 + i);
      }
      root.setRowCount(10);
      writer.writeBatch();
      writer.end();
    }

    String outputDir = arDir + File.separator + "output";
    File inputFile = new File(arrowFile);

    try (ArrowSourceReader reader = new ArrowSourceReader(inputFile)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("telemetry", schema.getTableName());

      ImportExecutor executor = new ImportExecutor(schema);
      boolean ok = executor.execute(reader, outputDir, "telemetry");
      assertTrue(ok);
    }

    String tsfilePath = outputDir + File.separator + "telemetry.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());

    List<String> columns = new ArrayList<>();
    columns.add("temperature");
    columns.add("humidity");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader = tableQueryExecutor.query("telemetry", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        cnt += result.getPositionCount();
      }
      assertEquals(10, cnt);
    }
  }

  @Test
  public void testCsvNewSchemaEndToEnd() throws Exception {
    String newDir = testDir + File.separator + "new_schema";
    new File(newDir).mkdirs();

    String newCsvFile = newDir + File.separator + "data.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(newCsvFile))) {
      w.write("time,tmp1,tmp2,tmp3,tmp4,tmp5\n");
      long timestamp = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        w.write(
            (timestamp + i)
                + ",s1,"
                + (i * 1.1f)
                + ","
                + (i * 2.2f)
                + ","
                + (i * 3.3f)
                + ","
                + (i * 4.4f)
                + "\n");
      }
    }

    String newSchemaFile = newDir + File.separator + "schema.txt";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(newSchemaFile))) {
      w.write("table_name=root.newdb\n");
      w.write("time_precision=ms\n");
      w.write("has_header=true\n");
      w.write("separator=,\n");
      w.write("null_format=\\N\n\n");
      w.write("tag_columns\n");
      w.write("tmp1\n");
      w.write("time_column=time\n");
      w.write("source_columns\n");
      w.write("time INT64,\n");
      w.write("tmp1 TEXT,\n");
      w.write("tmp2 FLOAT,\n");
      w.write("tmp3 FLOAT,\n");
      w.write("SKIP,\n");
      w.write("tmp5 FLOAT\n");
    }

    String scFilePath = new File(newSchemaFile).getAbsolutePath();
    String csvFilePath = new File(newCsvFile).getAbsolutePath();
    String targetPath = new File(newDir + File.separator + "output").getAbsolutePath();
    String[] args = new String[] {"-s" + csvFilePath, "-schema" + scFilePath, "-t" + targetPath};
    TsFileTool.main(args);

    String tsfilePath = targetPath + File.separator + "data.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());

    List<String> columns = new ArrayList<>();
    columns.add("tmp2");
    columns.add("tmp3");
    columns.add("tmp5");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader =
          tableQueryExecutor.query("root.newdb", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        cnt += reader.next().getPositionCount();
      }
      assertEquals(10, cnt);
    }
  }

  @Test
  public void testParquetSchemaModeEndToEnd() throws Exception {
    String pqDir = testDir + File.separator + "parquet_schema";
    new File(pqDir).mkdirs();

    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("region")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    String pqFile = pqDir + File.separator + "sensor.parquet";
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
                .append("region", "east")
                .append("value", i * 1.1));
      }
    }

    String schemaFile2 = pqDir + File.separator + "schema.txt";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(schemaFile2))) {
      w.write("table_name=root.pq\n");
      w.write("time_precision=ms\n");
      w.write("has_header=true\n");
      w.write("separator=,\n\n");
      w.write("tag_columns\n");
      w.write("region\n");
      w.write("time_column=time\n");
      w.write("source_columns\n");
      w.write("time INT64,\n");
      w.write("region TEXT,\n");
      w.write("value DOUBLE\n");
    }

    ImportSchema schema = ImportSchemaParser.parse(schemaFile2);
    String outputDir = pqDir + File.separator + "output";

    try (ParquetSourceReader reader = new ParquetSourceReader(new File(pqFile), schema)) {
      ImportExecutor executor = new ImportExecutor(schema);
      assertTrue(executor.execute(reader, outputDir, "sensor"));
    }

    String tsfilePath = outputDir + File.separator + "sensor.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());
    assertEquals(5, queryTsFile(tsfilePath, "root.pq", Arrays.asList("value")));
  }

  @Test
  public void testArrowSchemaModeEndToEnd() throws Exception {
    String arDir = testDir + File.separator + "arrow_schema";
    new File(arDir).mkdirs();

    List<Field> arrowFields = new ArrayList<>();
    arrowFields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    arrowFields.add(new Field("region", FieldType.notNullable(new ArrowType.Utf8()), null));
    arrowFields.add(
        new Field(
            "value",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null));
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(arrowFields);

    String arrowFile = arDir + File.separator + "device.arrow";
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc);
        FileOutputStream fos = new FileOutputStream(arrowFile);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      BigIntVector tv = (BigIntVector) root.getVector("time");
      org.apache.arrow.vector.VarCharVector rv =
          (org.apache.arrow.vector.VarCharVector) root.getVector("region");
      Float8Vector vv = (Float8Vector) root.getVector("value");
      tv.allocateNew(5);
      rv.allocateNew();
      vv.allocateNew(5);
      for (int i = 0; i < 5; i++) {
        tv.set(i, 1000L + i);
        rv.set(i, "west".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        vv.set(i, i * 2.2);
      }
      root.setRowCount(5);
      writer.writeBatch();
      writer.end();
    }

    String schemaFile2 = arDir + File.separator + "schema.txt";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(schemaFile2))) {
      w.write("table_name=root.ar\n");
      w.write("time_precision=ms\n");
      w.write("has_header=true\n");
      w.write("separator=,\n\n");
      w.write("tag_columns\n");
      w.write("region\n");
      w.write("time_column=time\n");
      w.write("source_columns\n");
      w.write("time INT64,\n");
      w.write("region TEXT,\n");
      w.write("value DOUBLE\n");
    }

    ImportSchema schema = ImportSchemaParser.parse(schemaFile2);
    String outputDir = arDir + File.separator + "output";

    try (ArrowSourceReader reader = new ArrowSourceReader(new File(arrowFile), schema)) {
      ImportExecutor executor = new ImportExecutor(schema);
      assertTrue(executor.execute(reader, outputDir, "device"));
    }

    String tsfilePath = outputDir + File.separator + "device.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());
    assertEquals(5, queryTsFile(tsfilePath, "root.ar", Arrays.asList("value")));
  }

  @Test
  public void testCsvLargeFileMultiBatch() throws Exception {
    String bigDir = testDir + File.separator + "big_csv";
    new File(bigDir).mkdirs();

    String bigCsvFile = bigDir + File.separator + "big.csv";
    int rowCount = 5000;
    try (BufferedWriter w = new BufferedWriter(new FileWriter(bigCsvFile))) {
      w.write("time,value1,value2\n");
      for (int i = 0; i < rowCount; i++) {
        w.write((1000L + i) + "," + (i * 1.1) + "," + (i * 2.2) + "\n");
      }
    }

    String outputDir = bigDir + File.separator + "output";
    File csvFile = new File(bigCsvFile);
    long smallChunkSize = 1024;

    try (CsvSourceReader reader = new CsvSourceReader(csvFile, ",", smallChunkSize)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("big", schema.getTableName());
      assertEquals("time", schema.getTimeColumnName());
      assertEquals(2, schema.fieldColumns().size());

      ImportExecutor executor = new ImportExecutor(schema);
      assertTrue(executor.execute(reader, outputDir, "big"));
    }

    File outputDirFile = new File(outputDir);
    File[] tsfiles = outputDirFile.listFiles((dir, name) -> name.endsWith(".tsfile"));
    assertNotNull(tsfiles);
    assertTrue("Should have multiple output files for chunked CSV", tsfiles.length > 1);

    int totalRows = 0;
    for (File tsfile : tsfiles) {
      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(tsfile.getAbsolutePath())) {
        TableQueryExecutor tableQueryExecutor =
            new TableQueryExecutor(
                new MetadataQuerierByFileImpl(sequenceReader),
                new CachedChunkLoaderImpl(sequenceReader),
                TableQueryExecutor.TableQueryOrdering.DEVICE);
        final TsBlockReader reader =
            tableQueryExecutor.query("big", Arrays.asList("value1"), null, null, null);
        while (reader.hasNext()) {
          totalRows += reader.next().getPositionCount();
        }
      }
    }
    assertEquals(rowCount, totalRows);

    for (File tsfile : tsfiles) {
      String name = tsfile.getName();
      assertTrue(
          "File should follow naming convention: " + name, name.matches("big_\\d+\\.tsfile"));
    }
  }

  @Test
  public void testCsvDirectoryMultipleFiles() throws Exception {
    String multiDir = testDir + File.separator + "multi_csv";
    new File(multiDir).mkdirs();

    String[] fileNames = {"alpha.csv", "beta.csv", "gamma.csv"};
    for (String fn : fileNames) {
      try (BufferedWriter w = new BufferedWriter(new FileWriter(multiDir + File.separator + fn))) {
        w.write("time,measurement\n");
        for (int i = 0; i < 5; i++) {
          w.write((1000L + i) + "," + (fn.hashCode() + i * 1.0) + "\n");
        }
      }
    }

    String outputDir = multiDir + File.separator + "output";
    new File(outputDir).mkdirs();

    for (String fn : fileNames) {
      File csvFile = new File(multiDir + File.separator + fn);
      String baseName = fn.substring(0, fn.lastIndexOf('.'));
      try (CsvSourceReader reader = new CsvSourceReader(csvFile, ",")) {
        ImportSchema schema = reader.inferSchema();
        assertEquals(baseName, schema.getTableName());

        ImportExecutor executor = new ImportExecutor(schema);
        assertTrue(executor.execute(reader, outputDir, baseName));
      }
    }

    for (String fn : fileNames) {
      String baseName = fn.substring(0, fn.lastIndexOf('.'));
      File tsfile = new File(outputDir, baseName + ".tsfile");
      assertTrue("Missing output: " + tsfile.getName(), tsfile.exists());

      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(tsfile.getAbsolutePath())) {
        TableQueryExecutor tableQueryExecutor =
            new TableQueryExecutor(
                new MetadataQuerierByFileImpl(sequenceReader),
                new CachedChunkLoaderImpl(sequenceReader),
                TableQueryExecutor.TableQueryOrdering.DEVICE);
        final TsBlockReader reader =
            tableQueryExecutor.query(baseName, Arrays.asList("measurement"), null, null, null);
        assertTrue(reader.hasNext());
        int cnt = 0;
        while (reader.hasNext()) {
          cnt += reader.next().getPositionCount();
        }
        assertEquals(5, cnt);
      }
    }
  }

  @Test
  public void testCsvToTsfileFailed() {
    String scFilePath = new File(schemaFile).getAbsolutePath();
    String csvFilePath = new File(wrongCsvFile).getAbsolutePath();
    String targetPath = new File(testDir).getAbsolutePath();
    String fd = new File(failedDir).getAbsolutePath();
    String[] args =
        new String[] {
          "-s" + csvFilePath, "-schema" + scFilePath, "-t" + targetPath, "-fail_dir" + fd
        };
    TsFileTool.main(args);
    assertTrue(new File(failedDir + File.separator + "dataWrong.csv").exists());
    try (BufferedReader br =
        new BufferedReader(new FileReader(failedDir + File.separator + "dataWrong.csv"))) {
      int num = 0;
      while (br.readLine() != null) {
        num++;
      }
      assertEquals(101, num);
    } catch (IOException e) {
      throw new RuntimeException("IOException occurred while reading file", e);
    }
  }
}
