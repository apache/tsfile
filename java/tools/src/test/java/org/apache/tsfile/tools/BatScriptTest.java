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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Drives the packaged Windows {@code .bat} scripts to verify two recent fixes:
 *
 * <ol>
 *   <li>The {@code start /B /WAIT "" cmd /C "(...)"} wrapper was removed so paths containing
 *       parentheses (e.g. {@code csv(1).csv}) survive cmd's argument parsing.
 *   <li>The {@code logback-cvs2tsfile.xml} now silences Arrow allocator and Parquet/Hadoop
 *       CodecPool INFO noise.
 * </ol>
 *
 * The test always copies the current source bat scripts and logback config over the packaged
 * assembly before running, so the assertions reflect the latest sources even if the assembly was
 * built earlier. The test is Windows-only and skipped when the assembly under {@code target/} has
 * not been produced yet (run {@code mvn package -P with-java -pl java/tools -am -DskipTests}
 * first).
 */
public class BatScriptTest {

  private static File toolsDir; // target/tools-VERSION/tools-VERSION/tools
  private static File confDir; //  target/tools-VERSION/tools-VERSION/conf

  private final String testDir = "target" + File.separator + "batScriptIT";
  private final String inputDir = testDir + File.separator + "input";
  private final String outputDir = testDir + File.separator + "output";

  @BeforeClass
  public static void locateAssemblyAndSyncSources() throws IOException {
    assumeTrue(
        "Skipping bat script test on non-Windows OS",
        System.getProperty("os.name").toLowerCase().contains("win"));

    File target = new File("target");
    File assemblyRoot = firstChildMatching(target, "tools-", "-SNAPSHOT");
    assumeTrue(
        "Skipping: assembly not built. Run `mvn package -P with-java -pl java/tools -am -DskipTests`",
        assemblyRoot != null);
    File inner = firstChildMatching(assemblyRoot, "tools-", "-SNAPSHOT");
    assumeTrue("Skipping: inner assembly dir missing under " + assemblyRoot, inner != null);

    toolsDir = new File(inner, "tools");
    confDir = new File(inner, "conf");
    assumeTrue("Skipping: tools dir not found at " + toolsDir, toolsDir.isDirectory());
    assumeTrue("Skipping: conf dir not found at " + confDir, confDir.isDirectory());

    // Ensure we test the freshest scripts/config regardless of when the assembly was built.
    File srcTools = new File("src/assembly/resources/tools");
    File srcConf = new File("src/assembly/resources/conf");
    copy(new File(srcTools, "csv2tsfile.bat"), new File(toolsDir, "csv2tsfile.bat"));
    copy(new File(srcTools, "arrow2tsfile.bat"), new File(toolsDir, "arrow2tsfile.bat"));
    copy(new File(srcTools, "parquet2tsfile.bat"), new File(toolsDir, "parquet2tsfile.bat"));
    copy(new File(srcConf, "logback-cvs2tsfile.xml"), new File(confDir, "logback-cvs2tsfile.xml"));
  }

  @Before
  public void setUp() {
    new File(inputDir).mkdirs();
    new File(outputDir).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  // ===================================================================================
  // 1. Normal CSV filename — sanity baseline for csv2tsfile.bat
  // ===================================================================================

  @Test
  public void csvBatHandlesNormalFilename() throws Exception {
    String csvName = "events.csv";
    File csv = new File(inputDir, csvName);
    writeMinimalCsv(csv);
    File schema = writeMinimalSchema(new File(inputDir, "schema.txt"), "root.events");

    BatResult r = runBat("csv2tsfile.bat", csv, schema);

    assertEquals("csv2tsfile.bat exit code (stdout=" + r.stdout + ")", 0, r.exitCode);
    assertTrue(
        "events.tsfile should be produced, stdout=" + r.stdout,
        new File(outputDir, "events.tsfile").exists());
  }

  // ===================================================================================
  // 2. Filename with parentheses — the regression the bat fix addresses
  // ===================================================================================

  @Test
  public void csvBatHandlesFilenameWithParentheses() throws Exception {
    String csvName = "events(1).csv";
    File csv = new File(inputDir, csvName);
    writeMinimalCsv(csv);
    File schema = writeMinimalSchema(new File(inputDir, "schema.txt"), "root.events");

    BatResult r = runBat("csv2tsfile.bat", csv, schema);

    assertEquals(
        "csv2tsfile.bat should accept paths with parentheses; stdout=" + r.stdout, 0, r.exitCode);
    assertFalse(
        "stdout must not report a truncated path; stdout=" + r.stdout,
        r.stdout.contains("目录或文件不存在") || r.stdout.toLowerCase().contains("does not exist"));
    assertTrue(
        "events(1).tsfile should be produced, stdout=" + r.stdout,
        new File(outputDir, "events(1).tsfile").exists());
  }

  // ===================================================================================
  // 3. Arrow + Parquet bats should no longer print third-party INFO noise
  // ===================================================================================

  @Test
  public void arrowAndParquetBatSuppressLibraryInfoLogs() throws Exception {
    File arrow = new File(inputDir, "sample.arrow");
    writeArrow(arrow);
    File parquet = new File(inputDir, "sample.parquet");
    writeParquet(parquet);

    BatResult arrowRun = runBat("arrow2tsfile.bat", arrow, null);
    BatResult parquetRun = runBat("parquet2tsfile.bat", parquet, null);

    assertEquals(
        "arrow2tsfile.bat exit code (stdout=" + arrowRun.stdout + ")", 0, arrowRun.exitCode);
    assertEquals(
        "parquet2tsfile.bat exit code (stdout=" + parquetRun.stdout + ")", 0, parquetRun.exitCode);

    String[] arrowNoise = {"BaseAllocator", "DefaultAllocationManagerOption", "CheckAllocator"};
    for (String marker : arrowNoise) {
      assertFalse(
          "Arrow INFO noise '" + marker + "' should be suppressed; stdout=" + arrowRun.stdout,
          arrowRun.stdout.contains(marker));
    }
    String[] parquetNoise = {"CodecPool", "Got brand-new decompressor"};
    for (String marker : parquetNoise) {
      assertFalse(
          "Parquet INFO noise '" + marker + "' should be suppressed; stdout=" + parquetRun.stdout,
          parquetRun.stdout.contains(marker));
    }
  }

  // ===================================================================================
  // Helpers
  // ===================================================================================

  private static final class BatResult {
    final int exitCode;
    final String stdout;

    BatResult(int exitCode, String stdout) {
      this.exitCode = exitCode;
      this.stdout = stdout;
    }
  }

  private BatResult runBat(String batName, File source, File schemaOrNull) throws Exception {
    File bat = new File(toolsDir, batName);
    assertTrue("bat missing: " + bat, bat.isFile());

    ProcessBuilder pb = new ProcessBuilder();
    pb.redirectErrorStream(true);
    if (schemaOrNull != null) {
      pb.command(
          "cmd.exe",
          "/c",
          bat.getAbsolutePath(),
          "--source",
          source.getAbsolutePath(),
          "-schema",
          schemaOrNull.getAbsolutePath(),
          "-t",
          new File(outputDir).getAbsolutePath());
    } else {
      pb.command(
          "cmd.exe",
          "/c",
          bat.getAbsolutePath(),
          "--source",
          source.getAbsolutePath(),
          "-t",
          new File(outputDir).getAbsolutePath());
    }
    Process p = pb.start();
    String stdout = drain(p.getInputStream());
    int rc = p.waitFor();
    return new BatResult(rc, stdout);
  }

  private static String drain(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] tmp = new byte[8192];
    int n;
    while ((n = in.read(tmp)) > 0) {
      buf.write(tmp, 0, n);
    }
    return buf.toString("UTF-8");
  }

  private static void writeMinimalCsv(File csv) throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(csv))) {
      w.write("time,v\n");
      w.write("1000,1.0\n");
      w.write("2000,2.0\n");
      w.write("3000,3.0\n");
    }
  }

  private static File writeMinimalSchema(File schema, String tableName) throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(schema))) {
      w.write("table_name=" + tableName + "\n");
      w.write("time_precision=ms\n");
      w.write("has_header=true\n");
      w.write("separator=,\n\n");
      w.write("time_column=time\n");
      w.write("csv_columns\n");
      w.write("time INT64,\n");
      w.write("v DOUBLE\n");
    }
    return schema;
  }

  private static void writeArrow(File file) throws IOException {
    if (file.exists()) {
      file.delete();
    }
    List<Field> fields =
        Arrays.asList(
            new Field(
                "time",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                null),
            new Field(
                "v",
                FieldType.notNullable(
                    new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                null));
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
        FileOutputStream fos = new FileOutputStream(file);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      TimeStampMilliVector t = (TimeStampMilliVector) root.getVector("time");
      Float8Vector v = (Float8Vector) root.getVector("v");
      t.allocateNew(2);
      v.allocateNew(2);
      t.set(0, 1000L);
      t.set(1, 2000L);
      v.set(0, 1.0);
      v.set(1, 2.0);
      root.setRowCount(2);
      writer.writeBatch();
      writer.end();
    }
  }

  private static void writeParquet(File file) throws IOException {
    if (file.exists()) {
      file.delete();
    }
    MessageType schema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("v")
            .named("sample");
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    List<Group> rows =
        Arrays.asList(
            f.newGroup().append("time", 1000L).append("v", 1.0),
            f.newGroup().append("time", 2000L).append("v", 2.0));
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file.toPath()))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
      for (Group r : rows) {
        writer.write(r);
      }
    }
  }

  private static File firstChildMatching(File dir, String prefix, String suffix) {
    if (!dir.isDirectory()) {
      return null;
    }
    File[] children = dir.listFiles();
    if (children == null) {
      return null;
    }
    for (File c : children) {
      if (c.isDirectory() && c.getName().startsWith(prefix) && c.getName().endsWith(suffix)) {
        return c;
      }
    }
    return null;
  }

  private static void copy(File src, File dst) throws IOException {
    if (!src.isFile()) {
      throw new IOException("source missing: " + src.getAbsolutePath());
    }
    Files.copy(src.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }
}
