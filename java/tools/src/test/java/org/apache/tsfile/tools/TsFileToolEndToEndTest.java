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
import org.apache.tsfile.utils.DateUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end tests that drive TsFileTool.main and verify the produced TsFile contents. These cover
 * scenarios that unit tests alone cannot prove out:
 *
 * <ol>
 *   <li>DATE / TIMESTAMP columns are actually writable through CSV schema mode.
 *   <li>Arrow DATE / TIMESTAMP vectors are writable through auto mode.
 *   <li>Parquet DATE / TIMESTAMP logical types in auto mode produce correct values.
 *   <li>Legacy Parquet INT96 timestamps decode correctly instead of crashing.
 *   <li>{@code --format} filtering rejects foreign-extension files in directory mode.
 * </ol>
 */
public class TsFileToolEndToEndTest {

  private final String testDir = "target" + File.separator + "endToEndTest";
  private final String inputDir = testDir + File.separator + "input";
  private final String outputDir = testDir + File.separator + "output";
  private final String failedDir = testDir + File.separator + "failed";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
    new File(inputDir).mkdirs();
    new File(outputDir).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  // ===================================================================================
  // Fix 1 — DATE / TIMESTAMP via CSV schema mode
  // ===================================================================================

  @Test
  public void testEndToEndCsvDateAndTimestamp() throws Exception {
    String csvPath = inputDir + File.separator + "events.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(csvPath))) {
      w.write("time,birthday,event_ts\n");
      // time in ms, birthday as DATE, event_ts as TIMESTAMP literal (no offset, JVM zone)
      w.write("1000,2024-01-15,2024-01-15T12:00:00+00:00\n");
      w.write("2000,1990-06-20,2024-01-16T08:30:00+00:00\n");
      w.write("3000,2030-12-31,2024-01-17T23:59:59+00:00\n");
    }

    String schemaPath = inputDir + File.separator + "schema.txt";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(schemaPath))) {
      w.write("table_name=root.events\n");
      w.write("time_precision=ms\n");
      w.write("has_header=true\n");
      w.write("separator=,\n\n");
      w.write("time_column=time\n");
      w.write("csv_columns\n");
      w.write("time INT64,\n");
      w.write("birthday DATE,\n");
      w.write("event_ts TIMESTAMP\n");
    }

    TsFileTool.main(
        new String[] {
          "-s" + new File(csvPath).getAbsolutePath(),
          "-t" + new File(outputDir).getAbsolutePath(),
          "-schema" + new File(schemaPath).getAbsolutePath()
        });

    String tsfile = outputDir + File.separator + "events.tsfile";
    assertTrue("TsFile must be produced", new File(tsfile).exists());

    try (TsFileSequenceReader seq = new TsFileSequenceReader(tsfile)) {
      TableQueryExecutor exec =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(seq),
              new CachedChunkLoaderImpl(seq),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      TsBlockReader reader =
          exec.query("root.events", Arrays.asList("birthday", "event_ts"), null, null, null);

      List<Integer> birthdays = new ArrayList<>();
      List<Long> tsValues = new ArrayList<>();
      while (reader.hasNext()) {
        TsBlock block = reader.next();
        for (int i = 0; i < block.getPositionCount(); i++) {
          birthdays.add(block.getColumn(0).getInt(i));
          tsValues.add(block.getColumn(1).getLong(i));
        }
      }

      assertEquals(3, birthdays.size());
      // TsFile encodes DATE as YYYYMMDD int, decode via DateUtils.
      assertEquals(LocalDate.of(2024, 1, 15), DateUtils.parseIntToLocalDate(birthdays.get(0)));
      assertEquals(LocalDate.of(1990, 6, 20), DateUtils.parseIntToLocalDate(birthdays.get(1)));
      assertEquals(LocalDate.of(2030, 12, 31), DateUtils.parseIntToLocalDate(birthdays.get(2)));
      assertEquals(1705320000000L, (long) tsValues.get(0));
      assertEquals(1705393800000L, (long) tsValues.get(1));
      assertEquals(1705535999000L, (long) tsValues.get(2));
    }
  }

  // ===================================================================================
  // Arrow DATE / TIMESTAMP logical vectors in auto mode
  // ===================================================================================

  @Test
  public void testEndToEndArrowAutoDateAndTimestamp() throws Exception {
    List<Field> fields = new ArrayList<>();
    fields.add(
        new Field(
            "time",
            FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
            null));
    fields.add(
        new Field("birthday", FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null));
    fields.add(
        new Field(
            "event_ts",
            FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
            null));
    fields.add(
        new Field(
            "score",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null));

    String arrowPath = inputDir + File.separator + "arrow_dt.arrow";
    writeArrow(
        arrowPath,
        new Schema(fields),
        (root, writer) -> {
          TimeStampMilliVector time = (TimeStampMilliVector) root.getVector("time");
          DateDayVector birthday = (DateDayVector) root.getVector("birthday");
          TimeStampMilliVector eventTs = (TimeStampMilliVector) root.getVector("event_ts");
          Float8Vector score = (Float8Vector) root.getVector("score");

          time.allocateNew(3);
          birthday.allocateNew(3);
          eventTs.allocateNew(3);
          score.allocateNew(3);

          time.set(0, 1705276800000L);
          time.set(1, 1705363200000L);
          time.set(2, 1705449600000L);
          birthday.set(0, 19737);
          birthday.set(1, 19738);
          birthday.set(2, 19739);
          eventTs.set(0, 1705320000000L);
          eventTs.set(1, 1705393800000L);
          eventTs.set(2, 1705535999000L);
          score.set(0, 1.5);
          score.set(1, 2.5);
          score.set(2, 3.5);

          root.setRowCount(3);
          writer.writeBatch();
        });

    TsFileTool.main(
        new String[] {
          "-s" + new File(arrowPath).getAbsolutePath(),
          "-t" + new File(outputDir).getAbsolutePath(),
          "--format",
          "arrow"
        });

    String tsfile = outputDir + File.separator + "arrow_dt.tsfile";
    assertTrue("TsFile must be produced", new File(tsfile).exists());

    try (TsFileSequenceReader seq = new TsFileSequenceReader(tsfile)) {
      TableQueryExecutor exec =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(seq),
              new CachedChunkLoaderImpl(seq),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      TsBlockReader reader =
          exec.query("arrow_dt", Arrays.asList("birthday", "event_ts", "score"), null, null, null);

      List<Long> times = new ArrayList<>();
      List<Integer> birthdays = new ArrayList<>();
      List<Long> eventTsValues = new ArrayList<>();
      List<Double> scores = new ArrayList<>();
      while (reader.hasNext()) {
        TsBlock block = reader.next();
        for (int i = 0; i < block.getPositionCount(); i++) {
          times.add(block.getTimeByIndex(i));
          birthdays.add(block.getColumn(0).getInt(i));
          eventTsValues.add(block.getColumn(1).getLong(i));
          scores.add(block.getColumn(2).getDouble(i));
        }
      }

      assertEquals(3, times.size());
      assertEquals(1705276800000L, (long) times.get(0));
      assertEquals(1705363200000L, (long) times.get(1));
      assertEquals(1705449600000L, (long) times.get(2));
      assertEquals(LocalDate.of(2024, 1, 15), DateUtils.parseIntToLocalDate(birthdays.get(0)));
      assertEquals(LocalDate.of(2024, 1, 16), DateUtils.parseIntToLocalDate(birthdays.get(1)));
      assertEquals(LocalDate.of(2024, 1, 17), DateUtils.parseIntToLocalDate(birthdays.get(2)));
      assertEquals(1705320000000L, (long) eventTsValues.get(0));
      assertEquals(1705393800000L, (long) eventTsValues.get(1));
      assertEquals(1705535999000L, (long) eventTsValues.get(2));
      assertEquals(1.5, scores.get(0), 1e-9);
      assertEquals(2.5, scores.get(1), 1e-9);
      assertEquals(3.5, scores.get(2), 1e-9);
    }
  }

  // ===================================================================================
  // Fix 2 — Parquet DATE / TIMESTAMP logical types in auto mode
  // ===================================================================================

  @Test
  public void testEndToEndParquetAutoDateAndTimestamp() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("birthday")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("auto_dt");

    String pqPath = inputDir + File.separator + "auto_dt.parquet";
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);
    List<Group> rows = new ArrayList<>();
    // birthday as epoch day: 2024-01-15 = 19737
    rows.add(
        factory.newGroup().append("time", 1000L).append("birthday", 19737).append("score", 1.5));
    rows.add(
        factory.newGroup().append("time", 2000L).append("birthday", 19738).append("score", 2.5));
    rows.add(
        factory.newGroup().append("time", 3000L).append("birthday", 19739).append("score", 3.5));
    writeParquet(pqPath, pqSchema, rows);

    TsFileTool.main(
        new String[] {
          "-s" + new File(pqPath).getAbsolutePath(),
          "-t" + new File(outputDir).getAbsolutePath(),
          "--format",
          "parquet"
        });

    String tsfile = outputDir + File.separator + "auto_dt.tsfile";
    assertTrue("TsFile must be produced", new File(tsfile).exists());

    try (TsFileSequenceReader seq = new TsFileSequenceReader(tsfile)) {
      TableQueryExecutor exec =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(seq),
              new CachedChunkLoaderImpl(seq),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      TsBlockReader reader =
          exec.query("auto_dt", Arrays.asList("birthday", "score"), null, null, null);

      List<Integer> birthdays = new ArrayList<>();
      List<Double> scores = new ArrayList<>();
      while (reader.hasNext()) {
        TsBlock block = reader.next();
        for (int i = 0; i < block.getPositionCount(); i++) {
          birthdays.add(block.getColumn(0).getInt(i));
          scores.add(block.getColumn(1).getDouble(i));
        }
      }

      assertEquals(3, birthdays.size());
      assertEquals(LocalDate.of(2024, 1, 15), DateUtils.parseIntToLocalDate(birthdays.get(0)));
      assertEquals(LocalDate.of(2024, 1, 16), DateUtils.parseIntToLocalDate(birthdays.get(1)));
      assertEquals(LocalDate.of(2024, 1, 17), DateUtils.parseIntToLocalDate(birthdays.get(2)));
      assertEquals(1.5, scores.get(0), 1e-9);
      assertEquals(2.5, scores.get(1), 1e-9);
      assertEquals(3.5, scores.get(2), 1e-9);
    }
  }

  // ===================================================================================
  // Fix 3 — Parquet INT96 timestamp (Spark / Hive legacy format)
  // ===================================================================================

  @Test
  public void testEndToEndParquetInt96() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT96)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("legacy");

    String pqPath = inputDir + File.separator + "legacy.parquet";
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);
    List<Group> rows = new ArrayList<>();
    // 2024-01-15 00:00:00 UTC, 01:00:00 UTC, 02:00:00 UTC
    rows.add(factory.newGroup().append("time", new NanoTime(2460325, 0L)).append("value", 10.0));
    rows.add(
        factory
            .newGroup()
            .append("time", new NanoTime(2460325, 3_600_000_000_000L))
            .append("value", 20.0));
    rows.add(
        factory
            .newGroup()
            .append("time", new NanoTime(2460325, 7_200_000_000_000L))
            .append("value", 30.0));
    writeParquet(pqPath, pqSchema, rows);

    TsFileTool.main(
        new String[] {
          "-s" + new File(pqPath).getAbsolutePath(),
          "-t" + new File(outputDir).getAbsolutePath(),
          "--format",
          "parquet"
        });

    String tsfile = outputDir + File.separator + "legacy.tsfile";
    assertTrue("TsFile must be produced", new File(tsfile).exists());

    try (TsFileSequenceReader seq = new TsFileSequenceReader(tsfile)) {
      TableQueryExecutor exec =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(seq),
              new CachedChunkLoaderImpl(seq),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      TsBlockReader reader =
          exec.query("legacy", Collections.singletonList("value"), null, null, null);

      List<Long> times = new ArrayList<>();
      List<Double> values = new ArrayList<>();
      while (reader.hasNext()) {
        TsBlock block = reader.next();
        for (int i = 0; i < block.getPositionCount(); i++) {
          times.add(block.getTimeByIndex(i));
          values.add(block.getColumn(0).getDouble(i));
        }
      }

      // Auto mode detects INT96 → precision ns
      assertEquals(3, times.size());
      assertEquals(1705276800L * 1_000_000_000L, (long) times.get(0));
      assertEquals(1705276800L * 1_000_000_000L + 3_600_000_000_000L, (long) times.get(1));
      assertEquals(1705276800L * 1_000_000_000L + 7_200_000_000_000L, (long) times.get(2));
      assertEquals(10.0, values.get(0), 1e-9);
      assertEquals(20.0, values.get(1), 1e-9);
      assertEquals(30.0, values.get(2), 1e-9);
    }
  }

  // ===================================================================================
  // Fix 4 — --format filter in directory mode ignores foreign extensions
  // ===================================================================================

  @Test
  public void testEndToEndFormatFilterIgnoresForeignFiles() throws Exception {
    // The CSV gets processed; README.md / build.log / notes.parquet must be ignored
    // (not processed, not copied to failed dir).
    try (BufferedWriter w =
        new BufferedWriter(new FileWriter(inputDir + File.separator + "data.csv"))) {
      w.write("time,val\n");
      w.write("1000,10.0\n");
      w.write("2000,20.0\n");
    }
    try (BufferedWriter w =
        new BufferedWriter(new FileWriter(inputDir + File.separator + "README.md"))) {
      w.write("# This is a readme, not data\n");
    }
    try (BufferedWriter w =
        new BufferedWriter(new FileWriter(inputDir + File.separator + "build.log"))) {
      w.write("some log line\n");
    }
    // Even a real Parquet file should be skipped when --format=csv.
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("v")
            .named("notes");
    SimpleGroupFactory pf = new SimpleGroupFactory(pqSchema);
    writeParquet(
        inputDir + File.separator + "notes.parquet",
        pqSchema,
        Collections.singletonList(pf.newGroup().append("time", 1000L).append("v", 1.0)));

    TsFileTool.main(
        new String[] {
          "-s" + new File(inputDir).getAbsolutePath(),
          "-t" + new File(outputDir).getAbsolutePath(),
          "-fail_dir" + new File(failedDir).getAbsolutePath(),
          "--format",
          "csv"
        });

    assertTrue(new File(outputDir, "data.tsfile").exists());

    // No tsfile should have been produced for the foreign files
    assertFalse(new File(outputDir, "README.tsfile").exists());
    assertFalse(new File(outputDir, "build.tsfile").exists());
    assertFalse(new File(outputDir, "notes.tsfile").exists());

    // And they must not have been copied into the failed dir either, since they were
    // never processed in the first place.
    File fd = new File(failedDir);
    if (fd.exists()) {
      File[] failed = fd.listFiles();
      String[] names = failed == null ? new String[0] : namesOf(failed);
      for (String n : names) {
        assertFalse("Foreign file leaked into failed dir: " + n, n.equals("README.md"));
        assertFalse("Foreign file leaked into failed dir: " + n, n.equals("build.log"));
        assertFalse("Foreign file leaked into failed dir: " + n, n.equals("notes.parquet"));
      }
    }
  }

  // ===================================================================================
  // Helpers
  // ===================================================================================

  private interface ArrowWriteCallback {
    void write(VectorSchemaRoot root, ArrowFileWriter writer) throws IOException;
  }

  private static void writeArrow(String path, Schema schema, ArrowWriteCallback callback)
      throws IOException {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        FileOutputStream fos = new FileOutputStream(file);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      callback.write(root, writer);
      writer.end();
    }
  }

  private static void writeParquet(String path, MessageType schema, List<Group> rows)
      throws IOException {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file.toPath()))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      for (Group r : rows) {
        writer.write(r);
      }
    }
  }

  private static String[] namesOf(File[] files) {
    String[] names = new String[files.length];
    for (int i = 0; i < files.length; i++) {
      names[i] = files[i].getName();
    }
    return names;
  }
}
