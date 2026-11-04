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

package org.apache.tsfile.compatibility;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.DateDataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Writes and validates the Java side of the cross-language compatibility matrix in {@code
 * compat/compatibility-matrix.csv}. The C++ counterpart is {@code
 * cpp/test/reader/table_view/encoding_compression_compatibility_test.cc}; both read the same spec,
 * write every case they declare writable, and validate the fixtures written by the other language.
 *
 * <p>See {@code compat/README.md} for the spec format and the value profile contract.
 */
public class EncodingCompressionCompatibilityTest {

  private static final String SPEC_PROPERTY = "tsfile.compat.spec";
  private static final String GENERATE_DIR_PROPERTY = "tsfile.compat.generate.dir";
  private static final String VALIDATE_DIR_PROPERTY = "tsfile.compat.validate.dir";
  private static final String DEFAULT_SPEC = "compat/compatibility-matrix.csv";

  private static final String MANIFEST_FILE = "manifest.csv";
  private static final String REPORT_FILE = "report.csv";
  private static final String SUMMARY_FILE = "summary.md";
  private static final String LANGUAGE = "java";

  private static final String SPEC_HEADER =
      "id,model,dataType,encoding,compressions,rowCount,pageRows,chunks,devices,valueProfile,"
          + "nullPatterns,javaWrite,cppWrite,javaRead,cppRead,gapReason";
  private static final String MANIFEST_HEADER =
      "producer,id,model,dataType,encoding,compression,rowCount,pageRows,chunks,devices,"
          + "valueProfile,nullPattern";

  private static final String[] ALL_COMPRESSIONS = {
    "UNCOMPRESSED", "SNAPPY", "GZIP", "LZ4", "ZSTD", "LZMA2"
  };

  // Concrete storage types covered by the matrix. OBJECT is a pseudo type in
  // TYPE_SUPPORTED_ENCODINGS and VECTOR has no writer on either side.
  private static final List<TSDataType> MATRIX_DATA_TYPES =
      Collections.unmodifiableList(
          Arrays.asList(
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.TIMESTAMP,
              TSDataType.DATE,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.TEXT,
              TSDataType.STRING,
              TSDataType.BLOB));

  private static final String TABLE_NAME = "compat_table";
  private static final String TAG_COLUMN = "device";
  private static final String VALUE_COLUMN = "value";
  private static final String ANCHOR_COLUMN = "anchor";
  private static final String DEVICE_PREFIX = "compat_table_device_";

  private enum Model {
    TABLE,
    TREE
  }

  private enum NullPattern {
    NONE,
    MIXED,
    ALL
  }

  private enum ValueProfile {
    DEFAULT,
    TS2DIFF,
    TS2DIFF_MPN0,
    SPRINTZ,
    RUNS,
    STRINGS,
    DICT_HIGH,
    BLOB
  }

  private static final class FixtureCase {
    final String specId;
    final String id;
    final Model model;
    final TSDataType dataType;
    final TSEncoding encoding;
    final CompressionType compression;
    final int rowCount;
    final int pageRows;
    final int chunks;
    final int devices;
    final ValueProfile profile;
    final NullPattern nullPattern;
    final boolean javaWrite;
    final boolean cppWrite;
    final boolean javaRead;
    final boolean cppRead;
    final String gapReason;

    FixtureCase(
        String specId,
        String id,
        Model model,
        TSDataType dataType,
        TSEncoding encoding,
        CompressionType compression,
        int rowCount,
        int pageRows,
        int chunks,
        int devices,
        ValueProfile profile,
        NullPattern nullPattern,
        boolean javaWrite,
        boolean cppWrite,
        boolean javaRead,
        boolean cppRead,
        String gapReason) {
      this.specId = specId;
      this.id = id;
      this.model = model;
      this.dataType = dataType;
      this.encoding = encoding;
      this.compression = compression;
      this.rowCount = rowCount;
      this.pageRows = pageRows;
      this.chunks = chunks;
      this.devices = devices;
      this.profile = profile;
      this.nullPattern = nullPattern;
      this.javaWrite = javaWrite;
      this.cppWrite = cppWrite;
      this.javaRead = javaRead;
      this.cppRead = cppRead;
      this.gapReason = gapReason;
    }

    String fileName() {
      return id + ".tsfile";
    }

    boolean writes(String language) {
      return "java".equals(language) ? javaWrite : cppWrite;
    }

    boolean reads(String language) {
      return "java".equals(language) ? javaRead : cppRead;
    }

    String describe() {
      return id
          + " ("
          + model
          + "/"
          + dataType
          + "/"
          + encoding
          + "/"
          + compression
          + ")";
    }
  }

  // ---------------------------------------------------------------------------
  // Entry points
  // ---------------------------------------------------------------------------

  /**
   * Guards the spec itself: every (type, encoding, compression) combination the Java writer
   * supports must be covered, every declared Java case must be constructible, and every declared
   * gap must carry a reason. Runs without any environment configuration, so spec drift fails the
   * normal Java test suite.
   */
  @Test
  public void specMatchesJavaCapabilities() throws IOException {
    List<FixtureCase> cases = readSpec(resolveSpecPath());
    Set<String> covered = new LinkedHashSet<>();
    for (FixtureCase fixtureCase : cases) {
      if (fixtureCase.gapReason.isEmpty()) {
        assertTrue(
            "gapReason is required when a case declares a gap: " + fixtureCase.describe(),
            fixtureCase.javaWrite && fixtureCase.cppWrite
                && fixtureCase.javaRead
                && fixtureCase.cppRead);
      }
      if (fixtureCase.javaWrite) {
        assertTrue(
            "a Java-written case must also be Java-readable: " + fixtureCase.describe(),
            fixtureCase.javaRead);
        Encoder encoder =
            TSEncodingBuilder.getEncodingBuilder(fixtureCase.encoding)
                .getEncoder(fixtureCase.dataType);
        assertNotNull(
            "Java cannot build an encoder for " + fixtureCase.describe(), encoder);
        covered.add(capabilityKey(fixtureCase));
      }
    }

    List<String> missing = new ArrayList<>();
    for (Model model : Model.values()) {
      for (TSDataType dataType : MATRIX_DATA_TYPES) {
        Set<TSEncoding> supported = TSEncoding.TYPE_SUPPORTED_ENCODINGS.get(dataType);
        assertNotNull("no supported encodings recorded for " + dataType, supported);
        for (TSEncoding encoding : supported) {
          for (String compression : ALL_COMPRESSIONS) {
            String key = model + "/" + dataType + "/" + encoding + "/" + compression;
            if (!covered.contains(key)) {
              missing.add(key);
            }
          }
        }
      }
    }
    if (!missing.isEmpty()) {
      fail(
          "compatibility-matrix.csv does not declare Java-writable cases for: "
              + String.join(", ", missing));
    }
  }

  @Test
  public void generateFixtures() throws Exception {
    String outputDir = System.getProperty(GENERATE_DIR_PROPERTY);
    Assume.assumeTrue(
        GENERATE_DIR_PROPERTY + " is not set",
        outputDir != null && !outputDir.isEmpty());
    Path directory = Paths.get(outputDir);
    Files.createDirectories(directory);

    List<FixtureCase> cases = readSpec(resolveSpecPath());
    List<FixtureCase> written = new ArrayList<>();
    List<String[]> report = new ArrayList<>();
    for (FixtureCase fixtureCase : cases) {
      if (!fixtureCase.javaWrite) {
        report.add(
            new String[] {fixtureCase.id, "not-mine", fixtureCase.gapReason});
        continue;
      }
      writeFixture(directory, fixtureCase);
      // A writer must be able to read back what it wrote; declared-unreadable
      // cases are rejected by specMatchesJavaCapabilities.
      validateFixture(directory, fixtureCase);
      written.add(fixtureCase);
      report.add(new String[] {fixtureCase.id, "generated-and-validated", ""});
    }
    writeManifest(directory, written);
    writeReport(directory, REPORT_FILE, report);
    writeSummary(directory, SUMMARY_FILE, "Java fixture generation", cases, report);
  }

  @Test
  public void validateFixtures() throws Exception {
    String inputDir = System.getProperty(VALIDATE_DIR_PROPERTY);
    Assume.assumeTrue(
        VALIDATE_DIR_PROPERTY + " is not set", inputDir != null && !inputDir.isEmpty());
    Path directory = Paths.get(inputDir);

    List<ManifestEntry> manifest = readManifest(directory);
    String producer = manifest.isEmpty() ? "" : manifest.get(0).producer;
    assertFalse("empty manifest in " + directory, manifest.isEmpty());
    assertTrue("unexpected producer " + producer, "java".equals(producer) || "cpp".equals(producer));

    Map<String, FixtureCase> spec = new LinkedHashMap<>();
    for (FixtureCase fixtureCase : readSpec(resolveSpecPath())) {
      spec.put(fixtureCase.id, fixtureCase);
    }

    Set<String> expectedIds = new LinkedHashSet<>();
    for (FixtureCase fixtureCase : spec.values()) {
      if (fixtureCase.writes(producer)) {
        expectedIds.add(fixtureCase.id);
      }
    }
    Set<String> manifestIds = new LinkedHashSet<>();
    for (ManifestEntry entry : manifest) {
      manifestIds.add(entry.id);
    }
    List<String> missing = new ArrayList<>(expectedIds);
    missing.removeAll(manifestIds);
    List<String> extra = new ArrayList<>(manifestIds);
    extra.removeAll(expectedIds);
    if (!missing.isEmpty() || !extra.isEmpty()) {
      fail(
          "manifest for "
              + producer
              + " does not match the spec; missing="
              + missing
              + " extra="
              + extra);
    }

    List<String[]> report = new ArrayList<>();
    for (ManifestEntry entry : manifest) {
      FixtureCase fixtureCase = spec.get(entry.id);
      assertNotNull("manifest case absent from the spec: " + entry.id, fixtureCase);
      entry.assertMatches(fixtureCase);
      if (!fixtureCase.reads(LANGUAGE)) {
        report.add(new String[] {entry.id, "skipped-declared-gap", fixtureCase.gapReason});
        continue;
      }
      Path file = directory.resolve(entry.id + ".tsfile");
      assertTrue("missing fixture declared as readable: " + file, Files.isRegularFile(file));
      validateFixture(directory, fixtureCase);
      report.add(new String[] {entry.id, "validated", ""});
    }
    writeReport(directory, REPORT_FILE + ".validate", report);
    writeSummary(
        directory,
        SUMMARY_FILE + ".validate",
        "Java validation of " + producer + " fixtures",
        new ArrayList<>(spec.values()),
        report);
  }

  // ---------------------------------------------------------------------------
  // Spec reading and expansion
  // ---------------------------------------------------------------------------

  private static Path resolveSpecPath() {
    String configured = System.getProperty(SPEC_PROPERTY);
    if (configured != null && !configured.isEmpty()) {
      Path path = Paths.get(configured);
      if (!Files.isRegularFile(path)) {
        throw new IllegalStateException(SPEC_PROPERTY + " does not name a file: " + path);
      }
      return path;
    }
    Path directory = Paths.get("").toAbsolutePath();
    for (int depth = 0; depth < 8 && directory != null; depth++, directory = directory.getParent()) {
      Path candidate = directory.resolve(DEFAULT_SPEC);
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
    }
    throw new IllegalStateException(
        "Cannot locate " + DEFAULT_SPEC + " above " + Paths.get("").toAbsolutePath());
  }

  private static List<FixtureCase> readSpec(Path specPath) throws IOException {
    List<String> lines = Files.readAllLines(specPath, StandardCharsets.UTF_8);
    Map<String, Integer> columns = null;
    List<FixtureCase> cases = new ArrayList<>();
    Set<String> ids = new LinkedHashSet<>();
    for (String raw : lines) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] parts = line.split(",", -1);
      if (columns == null) {
        columns = indexColumns(parts, SPEC_HEADER, specPath);
        continue;
      }
      if (parts.length != columns.size()) {
        throw new IllegalArgumentException("Bad spec line (" + parts.length + " fields): " + line);
      }
      String id = part(parts, columns, "id");
      String model = part(parts, columns, "model");
      String dataType = part(parts, columns, "dataType");
      String encoding = part(parts, columns, "encoding");
      String compressions = part(parts, columns, "compressions");
      int rowCount = Integer.parseInt(part(parts, columns, "rowCount"));
      int pageRows = Integer.parseInt(part(parts, columns, "pageRows"));
      int chunks = Integer.parseInt(part(parts, columns, "chunks"));
      int devices = Integer.parseInt(part(parts, columns, "devices"));
      String profile = part(parts, columns, "valueProfile");
      String patterns = part(parts, columns, "nullPatterns");
      boolean javaWrite = parseYesNo(part(parts, columns, "javaWrite"));
      boolean cppWrite = parseYesNo(part(parts, columns, "cppWrite"));
      boolean javaRead = parseYesNo(part(parts, columns, "javaRead"));
      boolean cppRead = parseYesNo(part(parts, columns, "cppRead"));
      String gapReason = part(parts, columns, "gapReason");

      List<String> compressionList =
          "ALL".equals(compressions)
              ? Arrays.asList(ALL_COMPRESSIONS)
              : Arrays.asList(compressions.split("\\+"));
      List<String> patternList = Arrays.asList(patterns.split("\\|"));
      for (String compression : compressionList) {
        for (String pattern : patternList) {
          String expandedId =
              id
                  + "."
                  + compression.toLowerCase(Locale.ROOT)
                  + "."
                  + pattern.toLowerCase(Locale.ROOT);
          if (!ids.add(expandedId)) {
            throw new IllegalArgumentException("Duplicate expanded case id: " + expandedId);
          }
          cases.add(
              new FixtureCase(
                  id,
                  expandedId,
                  Model.valueOf(model.toUpperCase(Locale.ROOT)),
                  TSDataType.valueOf(dataType),
                  TSEncoding.valueOf(encoding),
                  CompressionType.valueOf(compression),
                  rowCount,
                  pageRows,
                  chunks,
                  devices,
                  ValueProfile.valueOf(profile.toUpperCase(Locale.ROOT)),
                  NullPattern.valueOf(pattern.toUpperCase(Locale.ROOT)),
                  javaWrite,
                  cppWrite,
                  javaRead,
                  cppRead,
                  gapReason));
        }
      }
    }
    if (columns == null) {
      throw new IllegalArgumentException("Empty compatibility spec: " + specPath);
    }
    return cases;
  }

  private static Map<String, Integer> indexColumns(
      String[] header, String expected, Path specPath) {
    Map<String, Integer> columns = new LinkedHashMap<>();
    for (int i = 0; i < header.length; i++) {
      columns.put(header[i], i);
    }
    if (!String.join(",", header).equals(expected)) {
      throw new IllegalArgumentException(
          "Unexpected header in " + specPath + ": " + String.join(",", header));
    }
    return columns;
  }

  private static String part(String[] parts, Map<String, Integer> columns, String name) {
    Integer index = columns.get(name);
    if (index == null) {
      throw new IllegalArgumentException("Missing column " + name);
    }
    return parts[index];
  }

  private static boolean parseYesNo(String value) {
    if ("yes".equals(value)) {
      return true;
    }
    if ("no".equals(value)) {
      return false;
    }
    throw new IllegalArgumentException("Expected yes/no, got: " + value);
  }

  private static String capabilityKey(FixtureCase fixtureCase) {
    return fixtureCase.model
        + "/"
        + fixtureCase.dataType
        + "/"
        + fixtureCase.encoding
        + "/"
        + fixtureCase.compression;
  }

  // ---------------------------------------------------------------------------
  // Manifest and reports
  // ---------------------------------------------------------------------------

  private static final class ManifestEntry {
    final String producer;
    final String id;
    final Model model;
    final TSDataType dataType;
    final TSEncoding encoding;
    final CompressionType compression;
    final int rowCount;
    final int pageRows;
    final int chunks;
    final int devices;
    final ValueProfile profile;
    final NullPattern nullPattern;

    ManifestEntry(
        String producer,
        String id,
        Model model,
        TSDataType dataType,
        TSEncoding encoding,
        CompressionType compression,
        int rowCount,
        int pageRows,
        int chunks,
        int devices,
        ValueProfile profile,
        NullPattern nullPattern) {
      this.producer = producer;
      this.id = id;
      this.model = model;
      this.dataType = dataType;
      this.encoding = encoding;
      this.compression = compression;
      this.rowCount = rowCount;
      this.pageRows = pageRows;
      this.chunks = chunks;
      this.devices = devices;
      this.profile = profile;
      this.nullPattern = nullPattern;
    }

    void assertMatches(FixtureCase fixtureCase) {
      assertEquals("spec id " + id, fixtureCase.id, id);
      assertEquals("model " + id, fixtureCase.model, model);
      assertEquals("dataType " + id, fixtureCase.dataType, dataType);
      assertEquals("encoding " + id, fixtureCase.encoding, encoding);
      assertEquals("compression " + id, fixtureCase.compression, compression);
      assertEquals("rowCount " + id, fixtureCase.rowCount, rowCount);
      assertEquals("pageRows " + id, fixtureCase.pageRows, pageRows);
      assertEquals("chunks " + id, fixtureCase.chunks, chunks);
      assertEquals("devices " + id, fixtureCase.devices, devices);
      assertEquals("valueProfile " + id, fixtureCase.profile, profile);
      assertEquals("nullPattern " + id, fixtureCase.nullPattern, nullPattern);
    }
  }

  private static void writeManifest(Path directory, List<FixtureCase> cases) throws IOException {
    try (BufferedWriter writer =
        Files.newBufferedWriter(directory.resolve(MANIFEST_FILE), StandardCharsets.UTF_8)) {
      writer.write(MANIFEST_HEADER);
      writer.newLine();
      for (FixtureCase fixtureCase : cases) {
        writer.write(
            String.join(
                ",",
                LANGUAGE,
                fixtureCase.id,
                fixtureCase.model.name(),
                fixtureCase.dataType.name(),
                fixtureCase.encoding.name(),
                fixtureCase.compression.name(),
                String.valueOf(fixtureCase.rowCount),
                String.valueOf(fixtureCase.pageRows),
                String.valueOf(fixtureCase.chunks),
                String.valueOf(fixtureCase.devices),
                fixtureCase.profile.name(),
                fixtureCase.nullPattern.name()));
        writer.newLine();
      }
    }
  }

  private static List<ManifestEntry> readManifest(Path directory) throws IOException {
    Path manifest = directory.resolve(MANIFEST_FILE);
    if (!Files.isRegularFile(manifest)) {
      throw new IllegalStateException("Missing manifest: " + manifest);
    }
    List<String> lines = Files.readAllLines(manifest, StandardCharsets.UTF_8);
    Map<String, Integer> columns = null;
    List<ManifestEntry> entries = new ArrayList<>();
    for (String raw : lines) {
      String line = raw.trim();
      if (line.isEmpty()) {
        continue;
      }
      String[] parts = line.split(",", -1);
      if (columns == null) {
        columns = indexColumns(parts, MANIFEST_HEADER, manifest);
        continue;
      }
      if (parts.length != columns.size()) {
        throw new IllegalArgumentException("Bad manifest line: " + line);
      }
      entries.add(
          new ManifestEntry(
              part(parts, columns, "producer"),
              part(parts, columns, "id"),
              Model.valueOf(part(parts, columns, "model")),
              TSDataType.valueOf(part(parts, columns, "dataType")),
              TSEncoding.valueOf(part(parts, columns, "encoding")),
              CompressionType.valueOf(part(parts, columns, "compression")),
              Integer.parseInt(part(parts, columns, "rowCount")),
              Integer.parseInt(part(parts, columns, "pageRows")),
              Integer.parseInt(part(parts, columns, "chunks")),
              Integer.parseInt(part(parts, columns, "devices")),
              ValueProfile.valueOf(part(parts, columns, "valueProfile")),
              NullPattern.valueOf(part(parts, columns, "nullPattern"))));
    }
    return entries;
  }

  private static void writeReport(Path directory, String name, List<String[]> report)
      throws IOException {
    try (BufferedWriter writer =
        Files.newBufferedWriter(directory.resolve(name), StandardCharsets.UTF_8)) {
      writer.write("id,status,detail");
      writer.newLine();
      for (String[] row : report) {
        writer.write(String.join(",", row));
        writer.newLine();
      }
    }
  }

  private static void writeSummary(
      Path directory, String name, String title, List<FixtureCase> cases, List<String[]> report)
      throws IOException {
    int generated = 0;
    int validated = 0;
    int skipped = 0;
    Map<String, Integer> byStatus = new LinkedHashMap<>();
    List<String> gaps = new ArrayList<>();
    for (String[] row : report) {
      byStatus.merge(row[1], 1, Integer::sum);
      if (row[1].startsWith("generated") || "written".equals(row[1])) {
        generated++;
      }
      if (row[1].startsWith("validated") || row[1].endsWith("and-validated")) {
        validated++;
      }
      if (row[1].startsWith("skipped")) {
        skipped++;
        gaps.add(row[0] + ": " + row[2]);
      }
    }
    Map<String, Integer> byModel = new LinkedHashMap<>();
    Map<String, Integer> byType = new LinkedHashMap<>();
    for (FixtureCase fixtureCase : cases) {
      byModel.merge(fixtureCase.model.name(), 1, Integer::sum);
      byType.merge(fixtureCase.dataType.name(), 1, Integer::sum);
    }
    StringBuilder out = new StringBuilder();
    out.append("### ").append(title).append("\n\n");
    out.append("- Spec cases: ").append(cases.size()).append(" (");
    out.append(formatCounts(byModel)).append(")\n");
    out.append("- Status: ").append(formatCounts(byStatus)).append("\n");
    out.append("- Generated: ").append(generated).append(", validated: ").append(validated);
    out.append(", skipped: ").append(skipped).append("\n");
    out.append("- Types: ").append(formatCounts(byType)).append("\n");
    if (!gaps.isEmpty()) {
      out.append("\nDeclared/observed gaps:\n\n");
      for (String gap : gaps) {
        out.append("- ").append(gap).append("\n");
      }
    }
    Files.write(
        directory.resolve(name),
        out.toString().getBytes(StandardCharsets.UTF_8));
    System.out.println(out);
  }

  private static String formatCounts(Map<String, Integer> counts) {
    StringBuilder out = new StringBuilder();
    for (Map.Entry<String, Integer> entry : counts.entrySet()) {
      if (out.length() > 0) {
        out.append(", ");
      }
      out.append(entry.getKey()).append('=').append(entry.getValue());
    }
    return out.toString();
  }

  // ---------------------------------------------------------------------------
  // Deterministic value profiles (shared contract with the C++ test)
  // ---------------------------------------------------------------------------

  private static final int[] INT_VALUES = {
    0, 1, -1, 7, -8, 1024, -1024, 123456, -654321,
    Integer.MAX_VALUE - 1024, Integer.MIN_VALUE + 1024, 42, 42, 0, -8, 2048
  };

  private static final long[] LONG_VALUES = {
    0L, 1L, -1L, 7L, -8L, 1_234_567_890L, -987_654_321L, 4_294_967_296L,
    -4_294_967_296L, 1_234_567_890_123L, -1_234_567_890_123L,
    Long.MAX_VALUE - 4096, Long.MIN_VALUE + 4096, 42L, 42L, 0L
  };

  private static final int[] FLOAT_BITS = {
    0x00000000, 0x80000000, 0x3f800000, 0xbf800000, 0x41280000, 0xc0700000,
    0x44801000, 0xc5002000, 0x40490fdb, 0xc02df854, 0x3f800000, 0x3f800000,
    0x00000000, 0x80000000, 0x42f6e979, 0xc2f6e979
  };

  private static final long[] DOUBLE_BITS = {
    0x0000000000000000L, 0x8000000000000000L, 0x3ff0000000000000L,
    0xbff0000000000000L, 0x4029000000000000L, 0xc00c000000000000L,
    0x4090008000000000L, 0xc0a0010000000000L, 0x400921fb54442d18L,
    0xc005bf0a8b145769L, 0x3ff0000000000000L, 0x3ff0000000000000L,
    0x0000000000000000L, 0x8000000000000000L, 0x405edd2f1a9fbe77L,
    0xc05edd2f1a9fbe77L
  };

  private static final int[] TS_2DIFF_FLOAT_BITS = {
    0x00000000, 0x3f800000, 0xbf800000, 0x461c4000, 0xc61c4000, 0x4eb2d05e,
    0x7f800000, 0xff800000, 0x7fc00000, 0x3f800000, 0x00000000, 0x4a742400,
    0xca742400, 0x4e6e6b28, 0x3f800000, 0x00000000
  };

  private static final long[] TS_2DIFF_DOUBLE_BITS = {
    0x0000000000000000L, 0x3ff0000000000000L, 0xbff0000000000000L,
    0x40c3880000000000L, 0xc0c3880000000000L, 0x43abc16d674ec800L,
    0x7ff0000000000000L, 0xfff0000000000000L, 0x7ff8000000000000L,
    0x3ff0000000000000L, 0x0000000000000000L, 0x41f0000000000000L,
    0xc1cd6f3458800000L, 0x430c6bf526340000L, 0x3ff0000000000000L,
    0x0000000000000000L
  };

  // SPRINTZ predicts from first-order differences of the stored integers; a
  // block that spans INT_MIN and INT_MAX overflows the predictor, so these
  // values stay inside a safe range (see compat/README.md).
  private static final int[] SPRINTZ_INT_VALUES = {
    0, 1, -1, 7, -8, 1024, -1024, 123456, -654321,
    100_000_000, -100_000_000, 42, 42, 0, -8, 2048
  };

  private static final long[] SPRINTZ_LONG_VALUES = {
    0L, 1L, -1L, 7L, -8L, 1_234_567_890L, -987_654_321L, 4_294_967_296L,
    -4_294_967_296L, 1_000_000_000_000L, -1_000_000_000_000L,
    42L, 42L, 0L, -8L, 2048L
  };

  private static final int[] SPRINTZ_FLOAT_BITS = {
    0x3f800000, 0x3fa00000, 0x3fc00000, 0x3fe00000, 0x40000000, 0x3fc00000,
    0x3fa00000, 0x3f800000, 0x3fc00000, 0x3fe00000, 0x40000000, 0x3fe00000,
    0x3fc00000, 0x3f800000, 0x3fa00000, 0x3fc00000
  };

  private static final long[] SPRINTZ_DOUBLE_BITS = {
    0x3ff0000000000000L, 0x3ff4000000000000L, 0x3ff8000000000000L,
    0x3ffc000000000000L, 0x4000000000000000L, 0x3ff8000000000000L,
    0x3ff4000000000000L, 0x3ff0000000000000L, 0x3ff8000000000000L,
    0x3ffc000000000000L, 0x4000000000000000L, 0x3ffc000000000000L,
    0x3ff8000000000000L, 0x3ff0000000000000L, 0x3ff4000000000000L,
    0x3ff8000000000000L
  };

  private static final LocalDate[] DATE_VALUES = {
    LocalDate.of(1970, 1, 1),
    LocalDate.of(1999, 12, 31),
    LocalDate.of(2000, 2, 29),
    LocalDate.of(2024, 2, 29),
    LocalDate.of(2038, 1, 19),
    LocalDate.of(2050, 6, 15),
    LocalDate.of(1969, 7, 20),
    LocalDate.of(1980, 1, 6)
  };

  private static final String[] STRING_VALUES = {
    "",
    "hello",
    "你好，世界",
    "nul\u0000inside",
    "ÄÖÜ ß 日本語",
    "value",
    "value",
    repeat('a', 1024),
    "😀 emoji 🌍"
  };

  private static final byte[][] BLOB_VALUES = {
    {},
    {0},
    {0, 0, 0, 0},
    {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
    "blob-as-text".getBytes(StandardCharsets.UTF_8),
    {(byte) 0xff, (byte) 0xfe, (byte) 0x80, (byte) 0x00, (byte) 0x7f},
    {},
    "blob-as-text".getBytes(StandardCharsets.UTF_8)
  };

  // The TS_2DIFF builder and the RLE float/double builder both default
  // maxPointNumber to TSFileConfig.floatPrecision, currently 2.
  private static final int DEFAULT_MAX_POINT_NUMBER = 2;
  private static final double DEFAULT_MAX_POINT_VALUE = 100.0;

  private static String repeat(char c, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, c);
    return new String(chars);
  }

  private static boolean isNull(FixtureCase fixtureCase, int row) {
    if (fixtureCase.nullPattern == NullPattern.ALL) {
      return true;
    }
    if (fixtureCase.nullPattern == NullPattern.NONE) {
      return false;
    }
    int pageRows = nullPatternPageRows(fixtureCase);
    int page = row / pageRows;
    // Empty first page, a consecutive empty pair in the middle, empty tail
    // pages, and alternating nulls inside one interior page. Both
    // implementations share this contract.
    return page == 0 || page == 2 || page == 3 || page >= 6 || (page == 4 && row % 2 == 0);
  }

  private static Object valueFor(FixtureCase fixtureCase, int row) {
    switch (fixtureCase.dataType) {
      case BOOLEAN:
        return booleanValue(row);
      case INT32:
        return intValue(fixtureCase, row);
      case DATE:
        return dateValue(fixtureCase, row);
      case INT64:
      case TIMESTAMP:
        return longValue(fixtureCase, row);
      case FLOAT:
        return floatValue(fixtureCase, row);
      case DOUBLE:
        return doubleValue(fixtureCase, row);
      case TEXT:
      case STRING:
        return stringValue(fixtureCase, row);
      case BLOB:
        return blobValue(fixtureCase, row);
      default:
        throw new IllegalArgumentException("Unsupported data type " + fixtureCase.dataType);
    }
  }

  private static Object expectedValue(FixtureCase fixtureCase, int row) {
    if (isFloatEncoderCase(fixtureCase)) {
      if (fixtureCase.dataType == TSDataType.FLOAT) {
        return expectedFloatValue(fixtureCase, row);
      }
      return expectedDoubleValue(fixtureCase, row);
    }
    return valueFor(fixtureCase, row);
  }

  // TS_2DIFF and RLE for FLOAT/DOUBLE scale by 10^maxPointNumber before
  // storing an integer and divide on the way out.
  private static boolean isFloatEncoderCase(FixtureCase fixtureCase) {
    return (fixtureCase.encoding == TSEncoding.TS_2DIFF
            || fixtureCase.encoding == TSEncoding.RLE)
        && (fixtureCase.dataType == TSDataType.FLOAT
            || fixtureCase.dataType == TSDataType.DOUBLE);
  }

  private static double maxPointValue(FixtureCase fixtureCase) {
    return fixtureCase.profile == ValueProfile.TS2DIFF_MPN0 ? 1.0 : DEFAULT_MAX_POINT_VALUE;
  }

  private static boolean booleanValue(int row) {
    return row % 192 < 64 ? row % 2 == 0 : row % 192 < 128;
  }

  private static int intValue(FixtureCase fixtureCase, int row) {
    if (fixtureCase.profile == ValueProfile.RUNS) {
      return (row / 256) % 4 - 1;
    }
    if (fixtureCase.profile == ValueProfile.SPRINTZ) {
      return SPRINTZ_INT_VALUES[row % SPRINTZ_INT_VALUES.length];
    }
    return INT_VALUES[row % INT_VALUES.length];
  }

  private static long longValue(FixtureCase fixtureCase, int row) {
    if (fixtureCase.profile == ValueProfile.RUNS) {
      return (long) ((row / 256) % 4 - 1) * 1_000_000_000L;
    }
    if (fixtureCase.profile == ValueProfile.SPRINTZ) {
      return SPRINTZ_LONG_VALUES[row % SPRINTZ_LONG_VALUES.length];
    }
    return LONG_VALUES[row % LONG_VALUES.length];
  }

  private static LocalDate dateValue(FixtureCase fixtureCase, int row) {
    if (fixtureCase.profile == ValueProfile.RUNS) {
      return DATE_VALUES[(row / 256) % DATE_VALUES.length];
    }
    return DATE_VALUES[row % DATE_VALUES.length];
  }

  private static float floatValue(FixtureCase fixtureCase, int row) {
    switch (fixtureCase.profile) {
      case TS2DIFF:
      case TS2DIFF_MPN0:
        return Float.intBitsToFloat(
            TS_2DIFF_FLOAT_BITS[row % TS_2DIFF_FLOAT_BITS.length]);
      case RUNS:
        return (float) ((row / 256) % 4 - 1);
      case SPRINTZ:
        return Float.intBitsToFloat(
            SPRINTZ_FLOAT_BITS[row % SPRINTZ_FLOAT_BITS.length]);
      default:
        return Float.intBitsToFloat(FLOAT_BITS[row % FLOAT_BITS.length]);
    }
  }

  private static double doubleValue(FixtureCase fixtureCase, int row) {
    switch (fixtureCase.profile) {
      case TS2DIFF:
      case TS2DIFF_MPN0:
        return Double.longBitsToDouble(
            TS_2DIFF_DOUBLE_BITS[row % TS_2DIFF_DOUBLE_BITS.length]);
      case RUNS:
        return (double) ((row / 256) % 4 - 1);
      case SPRINTZ:
        return Double.longBitsToDouble(
            SPRINTZ_DOUBLE_BITS[row % SPRINTZ_DOUBLE_BITS.length]);
      default:
        return Double.longBitsToDouble(DOUBLE_BITS[row % DOUBLE_BITS.length]);
    }
  }

  private static String stringValue(FixtureCase fixtureCase, int row) {
    if (fixtureCase.profile == ValueProfile.DICT_HIGH) {
      return "key_" + row;
    }
    return STRING_VALUES[row % STRING_VALUES.length];
  }

  private static byte[] blobValue(FixtureCase fixtureCase, int row) {
    return BLOB_VALUES[row % BLOB_VALUES.length];
  }

  // Mirror FloatEncoder/DoubleEncoder at the effective maxPointNumber:
  // scaled -> round(v * mpv) / mpv, scale-overflow -> round(v) / 1,
  // raw bits -> back from the IEEE pattern (NaN is canonicalized on write).
  private static float expectedFloatValue(FixtureCase fixtureCase, int row) {
    if (!isFloatEncoderCase(fixtureCase)) {
      return floatValue(fixtureCase, row);
    }
    float value = floatValue(fixtureCase, row);
    if (Float.isNaN(value)) {
      return Float.intBitsToFloat(0x7fc00000);
    }
    double mpv = maxPointValue(fixtureCase);
    double scaled = (double) value * mpv;
    if (scaled > Integer.MAX_VALUE || scaled < Integer.MIN_VALUE) {
      if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        return Float.intBitsToFloat(Float.floatToIntBits(value));
      }
      return (float) ((double) Math.round(value) / 1.0);
    }
    return (float) ((double) Math.round(scaled) / mpv);
  }

  private static double expectedDoubleValue(FixtureCase fixtureCase, int row) {
    if (!isFloatEncoderCase(fixtureCase)) {
      return doubleValue(fixtureCase, row);
    }
    double value = doubleValue(fixtureCase, row);
    if (Double.isNaN(value)) {
      return Double.longBitsToDouble(0x7ff8000000000000L);
    }
    double mpv = maxPointValue(fixtureCase);
    double scaled = value * mpv;
    if (scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
      if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
        return Double.longBitsToDouble(Double.doubleToLongBits(value));
      }
      return (double) Math.round(value) / 1.0;
    }
    return (double) Math.round(scaled) / mpv;
  }

  // ---------------------------------------------------------------------------
  // Fixture writing
  // ---------------------------------------------------------------------------

  private static void writeFixture(Path directory, FixtureCase fixtureCase) throws Exception {
    File file = directory.resolve(fixtureCase.fileName()).toFile();
    Files.deleteIfExists(file.toPath());
    if (fixtureCase.model == Model.TABLE) {
      writeTableFixture(file, fixtureCase);
    } else {
      writeTreeFixture(file, fixtureCase);
    }
    assertTrue("fixture was not created: " + file, file.isFile() && file.length() > 0);
    assertOnWireCodec(file, fixtureCase);
  }

  // Prove that the declared encoding and compression are what the file
  // actually carries. Reading the values back cannot tell a PLAIN column from
  // an RLE column that happens to decode to the same values, nor whether a
  // codec request was silently ignored in favor of a default.
  private static void assertOnWireCodec(File file, FixtureCase fixtureCase) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      List<IDeviceID> devices = reader.getAllDevices();
      assertEquals(
          fixtureCase.id + " device count on wire", fixtureCase.devices, devices.size());
      for (IDeviceID device : devices) {
        List<ChunkMetadata> chunks =
            reader.getChunkMetadataList(device, VALUE_COLUMN, false);
        assertEquals(
            fixtureCase.id + " chunk count on wire", fixtureCase.chunks, chunks.size());
        for (ChunkMetadata chunk : chunks) {
          ChunkHeader header = reader.readChunkHeader(chunk.getOffsetOfChunkHeader());
          assertEquals(
              fixtureCase.id + " encoding on wire",
              fixtureCase.encoding,
              header.getEncodingType());
          assertEquals(
              fixtureCase.id + " compression on wire",
              fixtureCase.compression,
              header.getCompressionType());
        }
      }
    }
  }

  private static void writeTableFixture(File file, FixtureCase fixtureCase) throws Exception {
    TableSchema tableSchema = tableSchema(fixtureCase);
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int oldPageRows = config.getMaxNumberOfPointsInPage();
    int oldPageBytes = config.getPageSizeInByte();
    try {
      applyPageConfiguration(config, fixtureCase);
      try (TsFileWriter writer = new TsFileWriter(file)) {
        writer.setGenerateTableSchema(true);
        writer.registerTableSchema(tableSchema);
        for (int device = 0; device < fixtureCase.devices; device++) {
          for (int[] segment : segments(fixtureCase)) {
            writer.writeTable(tablet(tableSchema, fixtureCase, device, segment[0], segment[1]));
            writer.flush();
          }
        }
      }
    } finally {
      config.setMaxNumberOfPointsInPage(oldPageRows);
      config.setPageSizeInByte(oldPageBytes);
    }
  }

  private static void writeTreeFixture(File file, FixtureCase fixtureCase) throws Exception {
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int oldPageRows = config.getMaxNumberOfPointsInPage();
    int oldPageBytes = config.getPageSizeInByte();
    try {
      applyPageConfiguration(config, fixtureCase);
      try (TsFileWriter writer = new TsFileWriter(file)) {
        for (int device = 0; device < fixtureCase.devices; device++) {
          org.apache.tsfile.read.common.Path devicePath =
              new org.apache.tsfile.read.common.Path(deviceId(device));
          writer.registerTimeseries(devicePath, valueSchema(fixtureCase));
          if (fixtureCase.nullPattern != NullPattern.NONE) {
            writer.registerTimeseries(
                devicePath,
                new MeasurementSchema(
                    ANCHOR_COLUMN,
                    TSDataType.INT64,
                    TSEncoding.PLAIN,
                    fixtureCase.compression));
          }
        }
        for (int device = 0; device < fixtureCase.devices; device++) {
          String deviceId = deviceId(device);
          for (int[] segment : segments(fixtureCase)) {
            for (int row = segment[0]; row < segment[1]; row++) {
              TSRecord record = new TSRecord(deviceId, row);
              int points = 0;
              if (!isNull(fixtureCase, row)) {
                record.addTuple(dataPoint(fixtureCase, row));
                points++;
              }
              if (fixtureCase.nullPattern != NullPattern.NONE) {
                record.addTuple(new LongDataPoint(ANCHOR_COLUMN, row));
                points++;
              }
              if (points > 0) {
                writer.writeRecord(record);
              }
            }
            writer.flush();
          }
        }
      }
    } finally {
      config.setMaxNumberOfPointsInPage(oldPageRows);
      config.setPageSizeInByte(oldPageBytes);
    }
  }

  private static void applyPageConfiguration(TSFileConfig config, FixtureCase fixtureCase) {
    if (fixtureCase.pageRows > 0) {
      config.setMaxNumberOfPointsInPage(fixtureCase.pageRows);
      // Keep the byte-size trigger from splitting pages described by pageRows.
      config.setPageSizeInByte(16 * 1024 * 1024);
    }
  }

  private static int[][] segments(FixtureCase fixtureCase) {
    int chunks = Math.max(1, fixtureCase.chunks);
    int[][] segments = new int[chunks][];
    int base = fixtureCase.rowCount / chunks;
    int extra = fixtureCase.rowCount % chunks;
    int start = 0;
    for (int i = 0; i < chunks; i++) {
      int length = base + (i < extra ? 1 : 0);
      segments[i] = new int[] {start, start + length};
      start += length;
    }
    return segments;
  }

  private static String deviceId(int device) {
    return DEVICE_PREFIX + device;
  }

  private static MeasurementSchema valueSchema(FixtureCase fixtureCase) {
    Map<String, String> props =
        fixtureCase.profile == ValueProfile.TS2DIFF_MPN0
            ? Collections.singletonMap(Encoder.MAX_POINT_NUMBER, "0")
            : null;
    return new MeasurementSchema(
        VALUE_COLUMN,
        fixtureCase.dataType,
        fixtureCase.encoding,
        fixtureCase.compression,
        props);
  }

  private static TableSchema tableSchema(FixtureCase fixtureCase) {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    schemas.add(
        new MeasurementSchema(
            TAG_COLUMN,
            TSDataType.STRING,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED));
    categories.add(ColumnCategory.TAG);
    schemas.add(valueSchema(fixtureCase));
    categories.add(ColumnCategory.FIELD);
    if (fixtureCase.nullPattern != NullPattern.NONE) {
      schemas.add(
          new MeasurementSchema(
              ANCHOR_COLUMN,
              TSDataType.INT64,
              TSEncoding.PLAIN,
              fixtureCase.compression));
      categories.add(ColumnCategory.FIELD);
    }
    return new TableSchema(TABLE_NAME, schemas, categories);
  }

  private static Tablet tablet(
      TableSchema tableSchema, FixtureCase fixtureCase, int device, int start, int end) {
    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes(),
            Math.max(1, end - start));
    int index = 0;
    for (int row = start; row < end; row++, index++) {
      tablet.addTimestamp(index, row);
      tablet.addValue(TAG_COLUMN, index, deviceId(device));
      if (isNull(fixtureCase, row)) {
        tablet.addValue(VALUE_COLUMN, index, null);
      } else {
        addTabletValue(tablet, fixtureCase, index, row);
      }
      if (fixtureCase.nullPattern != NullPattern.NONE) {
        tablet.addValue(ANCHOR_COLUMN, index, (long) row);
      }
    }
    return tablet;
  }

  private static void addTabletValue(
      Tablet tablet, FixtureCase fixtureCase, int index, int row) {
    Object value = valueFor(fixtureCase, row);
    switch (fixtureCase.dataType) {
      case BOOLEAN:
        tablet.addValue(index, VALUE_COLUMN, (Boolean) value);
        break;
      case INT32:
        tablet.addValue(index, VALUE_COLUMN, (Integer) value);
        break;
      case DATE:
        tablet.addValue(index, VALUE_COLUMN, (LocalDate) value);
        break;
      case INT64:
      case TIMESTAMP:
        tablet.addValue(index, VALUE_COLUMN, (Long) value);
        break;
      case FLOAT:
        tablet.addValue(index, VALUE_COLUMN, (Float) value);
        break;
      case DOUBLE:
        tablet.addValue(index, VALUE_COLUMN, (Double) value);
        break;
      case TEXT:
      case STRING:
        tablet.addValue(index, VALUE_COLUMN, (String) value);
        break;
      case BLOB:
        tablet.addValue(index, VALUE_COLUMN, (byte[]) value);
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type " + fixtureCase.dataType);
    }
  }

  private static DataPoint dataPoint(FixtureCase fixtureCase, int row) {
    Object value = valueFor(fixtureCase, row);
    switch (fixtureCase.dataType) {
      case BOOLEAN:
        return new BooleanDataPoint(VALUE_COLUMN, (Boolean) value);
      case INT32:
        return new IntDataPoint(VALUE_COLUMN, (Integer) value);
      case DATE:
        return new DateDataPoint(VALUE_COLUMN, (LocalDate) value);
      case INT64:
      case TIMESTAMP:
        return new LongDataPoint(VALUE_COLUMN, (Long) value);
      case FLOAT:
        return new FloatDataPoint(VALUE_COLUMN, (Float) value);
      case DOUBLE:
        return new DoubleDataPoint(VALUE_COLUMN, (Double) value);
      case TEXT:
      case STRING:
        return new StringDataPoint(
            VALUE_COLUMN, new Binary((String) value, StandardCharsets.UTF_8));
      case BLOB:
        return new BlobDataPoint(VALUE_COLUMN, new Binary((byte[]) value));
      default:
        throw new IllegalArgumentException("Unsupported data type " + fixtureCase.dataType);
    }
  }

  /** BLOB value for the tree write path, which has no built-in BLOB data point. */
  private static final class BlobDataPoint extends DataPoint {
    private Binary value;

    BlobDataPoint(String measurementId, Binary value) {
      super(TSDataType.BLOB, measurementId);
      this.value = value;
    }

    @Override
    public void writeTo(long time, ChunkWriterImpl writer) {
      writer.write(time, value);
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public void setString(Binary value) {
      this.value = value;
    }
  }

  // ---------------------------------------------------------------------------
  // Fixture validation
  // ---------------------------------------------------------------------------

  private static int nullPatternPageRows(FixtureCase fixtureCase) {
    return fixtureCase.pageRows > 0 ? fixtureCase.pageRows : 64;
  }

  private static void validateFixture(Path directory, FixtureCase fixtureCase) throws Exception {
    File file = directory.resolve(fixtureCase.fileName()).toFile();
    assertTrue("missing fixture " + file, file.isFile());
    if (fixtureCase.model == Model.TABLE) {
      validateTableFixture(file, fixtureCase);
    } else {
      validateTreeFixture(file, fixtureCase);
    }
  }

  private static List<long[]> queryRanges(FixtureCase fixtureCase) {
    List<long[]> ranges = new ArrayList<>();
    ranges.add(new long[] {0, fixtureCase.rowCount - 1});
    int pageRows = fixtureCase.pageRows;
    if (fixtureCase.nullPattern != NullPattern.NONE
        && pageRows > 0
        && fixtureCase.rowCount > 2L * pageRows) {
      ranges.add(new long[] {pageRows - 1L, Math.min(fixtureCase.rowCount - 1, 2L * pageRows + 1)});
      ranges.add(new long[] {pageRows, Math.min(fixtureCase.rowCount - 1L, 2L * pageRows - 1L)});
    }
    return ranges;
  }

  private static void validateTableFixture(File file, FixtureCase fixtureCase) throws Exception {
    boolean nulls = fixtureCase.nullPattern != NullPattern.NONE;
    for (long[] range : queryRanges(fixtureCase)) {
      validateTableRange(
          file, fixtureCase, (int) range[0], (int) range[1], false);
      if (nulls) {
        validateTableRange(file, fixtureCase, (int) range[0], (int) range[1], true);
      }
    }
  }

  private static void validateTableRange(
      File file, FixtureCase fixtureCase, int start, int end, boolean withAnchor)
      throws Exception {
    List<String> columns = new ArrayList<>(Arrays.asList(TAG_COLUMN, VALUE_COLUMN));
    if (withAnchor) {
      columns.add(ANCHOR_COLUMN);
    }
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet resultSet = reader.query(TABLE_NAME, columns, start, end)) {
      ResultSetMetadata metadata = resultSet.getMetadata();
      assertEquals("Time", metadata.getColumnName(1));
      assertEquals(TSDataType.INT64, metadata.getColumnType(1));
      assertEquals(TAG_COLUMN, metadata.getColumnName(2));
      assertEquals(TSDataType.STRING, metadata.getColumnType(2));
      assertEquals(VALUE_COLUMN, metadata.getColumnName(3));
      assertEquals(fixtureCase.dataType, metadata.getColumnType(3));
      if (withAnchor) {
        assertEquals(ANCHOR_COLUMN, metadata.getColumnName(4));
        assertEquals(TSDataType.INT64, metadata.getColumnType(4));
      }

      Map<String, List<Long>> rowsByDevice = new LinkedHashMap<>();
      Map<String, Map<Long, Object[]>> valuesByDevice = new LinkedHashMap<>();
      while (resultSet.next()) {
        long time = resultSet.getLong(1);
        String device = resultSet.getString(2);
        rowsByDevice.computeIfAbsent(device, key -> new ArrayList<>()).add(time);
        boolean isNull = resultSet.isNull(3);
        Object value = isNull ? null : readResultValue(fixtureCase, resultSet, 3);
        Long anchor = null;
        if (withAnchor) {
          assertFalse("anchor null at " + fixtureCase.id + " t=" + time, resultSet.isNull(4));
          anchor = resultSet.getLong(4);
        }
        valuesByDevice
            .computeIfAbsent(device, key -> new LinkedHashMap<>())
            .put(time, new Object[] {isNull, value, anchor});
      }

      assertEquals(
          fixtureCase.id + " devices", fixtureCase.devices, rowsByDevice.size());
      for (int device = 0; device < fixtureCase.devices; device++) {
        String deviceKey = deviceId(device);
        List<Long> times = rowsByDevice.get(deviceKey);
        assertNotNull("missing device " + deviceKey + " in " + fixtureCase.id, times);
        assertEquals(fixtureCase.id + " rows for " + deviceKey, end - start + 1, times.size());
        Map<Long, Object[]> values = valuesByDevice.get(deviceKey);
        long expectedTime = start;
        for (Long time : times) {
          assertEquals(fixtureCase.id + " timestamp", expectedTime, time.longValue());
          Object[] entry = values.get(time);
          assertNotNull(entry);
          boolean expectedNull = isNull(fixtureCase, (int) expectedTime);
          assertEquals(
              fixtureCase.id + " null at t=" + expectedTime, expectedNull, entry[0]);
          if (!expectedNull) {
            assertValue(
                fixtureCase, expectedValue(fixtureCase, (int) expectedTime), entry[1], expectedTime);
          }
          if (withAnchor) {
            assertEquals(
                fixtureCase.id + " anchor at t=" + expectedTime,
                expectedTime,
                ((Long) entry[2]).longValue());
          }
          expectedTime++;
        }
      }
    }
  }

  private static void validateTreeFixture(File file, FixtureCase fixtureCase) throws Exception {
    boolean nulls = fixtureCase.nullPattern != NullPattern.NONE;
    for (long[] range : queryRanges(fixtureCase)) {
      for (int device = 0; device < fixtureCase.devices; device++) {
        validateTreeRange(
            file, fixtureCase, device, (int) range[0], (int) range[1], false);
        if (nulls) {
          validateTreeRange(
              file, fixtureCase, device, (int) range[0], (int) range[1], true);
        }
      }
    }
  }

  private static void validateTreeRange(
      File file,
      FixtureCase fixtureCase,
      int device,
      int start,
      int end,
      boolean withAnchor)
      throws Exception {
    List<org.apache.tsfile.read.common.Path> paths = new ArrayList<>();
    paths.add(new org.apache.tsfile.read.common.Path(deviceId(device), VALUE_COLUMN, true));
    if (withAnchor) {
      paths.add(new org.apache.tsfile.read.common.Path(deviceId(device), ANCHOR_COLUMN, true));
    }
    IExpression filter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilterApi.gtEq(start)),
            new GlobalTimeExpression(TimeFilterApi.ltEq(end)));
    QueryExpression expression = QueryExpression.create(paths, filter);
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(file.getPath())) {
      try (TsFileReader reader = new TsFileReader(sequenceReader)) {
        QueryDataSet dataSet = reader.query(expression);
        long expectedTime = start;
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          if (!withAnchor) {
            // Without the anchor, timestamps whose value is missing are not
            // returned at all; skip to the next present value.
            while (expectedTime <= end && isNull(fixtureCase, (int) expectedTime)) {
              expectedTime++;
            }
          }
          assertEquals(
              fixtureCase.id + " tree timestamp for device " + device,
              expectedTime,
              record.getTimestamp());
          Field valueField = record.getFields().get(0);
          boolean isNull = valueField == null || valueField.getDataType() == null;
          assertEquals(
              fixtureCase.id + " tree null at t=" + expectedTime,
              isNull(fixtureCase, (int) expectedTime),
              isNull);
          if (!isNull) {
            assertFieldValue(
                fixtureCase,
                expectedValue(fixtureCase, (int) expectedTime),
                valueField,
                expectedTime);
          }
          if (withAnchor) {
            Field anchorField = record.getFields().get(1);
            assertNotNull(anchorField);
            assertEquals(
                fixtureCase.id + " tree anchor at t=" + expectedTime,
                expectedTime,
                anchorField.getLongV());
          }
          expectedTime++;
        }
        if (!withAnchor) {
          while (expectedTime <= end && isNull(fixtureCase, (int) expectedTime)) {
            expectedTime++;
          }
        }
        assertEquals(
            fixtureCase.id + " tree row count for device " + device, end + 1, expectedTime);
      }
    }
  }

  private static Object readResultValue(FixtureCase fixtureCase, ResultSet resultSet, int index) {
    switch (fixtureCase.dataType) {
      case BOOLEAN:
        return resultSet.getBoolean(index);
      case INT32:
        return resultSet.getInt(index);
      case DATE:
        return resultSet.getDate(index);
      case INT64:
      case TIMESTAMP:
        return resultSet.getLong(index);
      case FLOAT:
        return resultSet.getFloat(index);
      case DOUBLE:
        return resultSet.getDouble(index);
      case TEXT:
      case STRING:
        return resultSet.getString(index);
      case BLOB:
        return resultSet.getBinary(index);
      default:
        throw new IllegalArgumentException("Unsupported data type " + fixtureCase.dataType);
    }
  }

  private static void assertValue(
      FixtureCase fixtureCase, Object expected, Object actual, long time) {
    String message = fixtureCase.id + " value at t=" + time;
    switch (fixtureCase.dataType) {
      case BOOLEAN:
        assertEquals(message, expected, actual);
        break;
      case INT32:
        assertEquals(message, expected, actual);
        break;
      case DATE:
        assertEquals(message, expected, actual);
        break;
      case INT64:
      case TIMESTAMP:
        assertEquals(message, expected, actual);
        break;
      case FLOAT:
        assertEquals(
            message,
            Float.floatToIntBits((Float) expected),
            Float.floatToIntBits((Float) actual));
        break;
      case DOUBLE:
        assertEquals(
            message,
            Double.doubleToLongBits((Double) expected),
            Double.doubleToLongBits((Double) actual));
        break;
      case TEXT:
      case STRING:
        assertEquals(message, expected, actual);
        break;
      case BLOB:
        assertTrue(message, Arrays.equals((byte[]) expected, (byte[]) actual));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type " + fixtureCase.dataType);
    }
  }

  private static void assertFieldValue(
      FixtureCase fixtureCase, Object expected, Field field, long time) {
    String message = fixtureCase.id + " value at t=" + time;
    assertEquals(message + " type", fixtureCase.dataType, field.getDataType());
    switch (fixtureCase.dataType) {
      case BOOLEAN:
        assertEquals(message, expected, field.getBoolV());
        break;
      case INT32:
        assertEquals(message, expected, field.getIntV());
        break;
      case DATE:
        assertEquals(message, expected, field.getDateV());
        break;
      case INT64:
      case TIMESTAMP:
        assertEquals(message, expected, field.getLongV());
        break;
      case FLOAT:
        assertEquals(
            message,
            Float.floatToIntBits((Float) expected),
            Float.floatToIntBits(field.getFloatV()));
        break;
      case DOUBLE:
        assertEquals(
            message,
            Double.doubleToLongBits((Double) expected),
            Double.doubleToLongBits(field.getDoubleV()));
        break;
      case TEXT:
      case STRING:
        assertEquals(message, expected, field.getBinaryV().getStringValue(StandardCharsets.UTF_8));
        break;
      case BLOB:
        assertTrue(
            message, Arrays.equals((byte[]) expected, field.getBinaryV().getValues()));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type " + fixtureCase.dataType);
    }
  }
}
