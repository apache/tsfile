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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.encoding.encoder.AClusterAlgorithm;
import org.apache.tsfile.encoding.encoder.AClusterEncoder;
import org.apache.tsfile.encoding.encoder.ClusterEncoder;
import org.apache.tsfile.encoding.encoder.ClusterResult;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.KClusterAlgorithm;
import org.apache.tsfile.encoding.encoder.KClusterEncoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.page.PageWriter;
import org.apache.tsfile.write.page.ValuePageWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ClusterCodecTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> encodings() {
    return Arrays.asList(new Object[][] {{TSEncoding.ACLUSTER}, {TSEncoding.KCLUSTER}});
  }

  private final TSEncoding encoding;

  public ClusterCodecTest(TSEncoding encoding) {
    this.encoding = encoding;
  }

  private Encoder encoder(TSDataType type) {
    return encoding == TSEncoding.ACLUSTER
        ? new AClusterEncoder(type)
        : new KClusterEncoder(type, 4, 3, 42);
  }

  private void encode(Encoder encoder, TSDataType type, long value, ByteArrayOutputStream out) {
    switch (type) {
      case INT32:
      case DATE:
        encoder.encode((int) value, out);
        break;
      case INT64:
      case TIMESTAMP:
        encoder.encode(value, out);
        break;
      case FLOAT:
        encoder.encode(Float.intBitsToFloat((int) value), out);
        break;
      case DOUBLE:
        encoder.encode(Double.longBitsToDouble(value), out);
        break;
      default:
        throw new AssertionError(type);
    }
  }

  private long read(Decoder decoder, TSDataType type, ByteBuffer buffer) {
    switch (type) {
      case INT32:
      case DATE:
        return decoder.readInt(buffer);
      case INT64:
      case TIMESTAMP:
        return decoder.readLong(buffer);
      case FLOAT:
        return Float.floatToRawIntBits(decoder.readFloat(buffer)) & 0xffffffffL;
      case DOUBLE:
        return Double.doubleToRawLongBits(decoder.readDouble(buffer));
      default:
        throw new AssertionError(type);
    }
  }

  private byte[] roundTrip(TSDataType type, long[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder writer = encoder(type);
    for (long value : input) {
      encode(writer, type, value, out);
    }
    writer.flush(out);
    int bytes = out.size();
    writer.flush(out);
    assertEquals(bytes, out.size());
    Decoder reader = Decoder.getDecoderByType(encoding, type);
    assertEquals(encoding, reader.getType());
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    long[] decoded = new long[input.length];
    // Real TsFile page readers call readXXX directly without first calling hasNext.
    for (int i = 0; i < input.length; i++) {
      if (i > 0) {
        assertTrue(reader.hasNext(buffer));
        assertTrue(reader.hasNext(buffer));
      }
      decoded[i] = read(reader, type, buffer);
    }
    assertFalse(reader.hasNext(buffer));
    assertEquals(0, buffer.remaining());
    assertEquals(frequencies(input), frequencies(decoded));
    return out.toByteArray();
  }

  private Map<Long, Integer> frequencies(long[] values) {
    Map<Long, Integer> result = new HashMap<>();
    for (long value : values) {
      result.put(value, result.getOrDefault(value, 0) + 1);
    }
    return result;
  }

  @Test
  public void testGroupedIntegerMultisets() throws IOException {
    long[] values = new long[600];
    for (int i = 0; i < values.length; i++) {
      values[i] = new long[] {-5, 100, -4, 10000, -5, 10000}[i % 6];
    }
    for (TSDataType type :
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.DATE, TSDataType.TIMESTAMP
        }) {
      byte[] encoded = roundTrip(type, values);
      assertTrue("Test must exercise cluster storage, not RAW", encoded[7] != 0);
    }
  }

  @Test
  public void testEmptyConstantAndShortBlocks() throws IOException {
    roundTrip(TSDataType.INT64, new long[0]);
    roundTrip(TSDataType.INT64, new long[] {-5});
    roundTrip(TSDataType.INT32, new long[] {0, 0, 0});
    roundTrip(TSDataType.INT64, new long[] {Long.MIN_VALUE});
    long[] same = new long[200];
    Arrays.fill(same, -5);
    assertTrue(roundTrip(TSDataType.INT64, same).length < 32);
  }

  @Test
  public void testIntegerExtremes() throws IOException {
    roundTrip(
        TSDataType.INT32,
        new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, Integer.MIN_VALUE});
    byte[] raw =
        roundTrip(
            TSDataType.INT64,
            new long[] {Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 1, Long.MIN_VALUE});
    assertEquals(0, raw[7]);
    roundTrip(
        TSDataType.INT64, new long[] {Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 3});
  }

  @Test
  public void testExactDecimalScaling() throws IOException {
    for (TSDataType type : new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE}) {
      long[] values = new long[500];
      for (int i = 0; i < values.length; i++) {
        double value = new double[] {-5.25, 0.1, 100.125, 7.75}[i % 4];
        values[i] =
            type == TSDataType.FLOAT
                ? Float.floatToRawIntBits((float) value) & 0xffffffffL
                : Double.doubleToRawLongBits(value);
      }
      assertTrue(roundTrip(type, values)[7] != 0);
    }
  }

  @Test
  public void testRawFloatingBits() throws IOException {
    long[] doubles = {
      0L,
      Long.MIN_VALUE,
      0x7ff0000000000000L,
      0xfff0000000000000L,
      0x7ff8000000000001L,
      0x7ff8000000000002L,
      1L,
      0x7fefffffffffffffL,
      Double.doubleToRawLongBits(0.12345678901234567)
    };
    assertEquals(0, roundTrip(TSDataType.DOUBLE, doubles)[7]);
    long[] floats = {
      0, 0x80000000L, 0x7f800000L, 0xff800000L, 0x7fc00001L, 0x7fc00002L, 1, 0x7f7fffffL
    };
    assertEquals(0, roundTrip(TSDataType.FLOAT, floats)[7]);
    // Finite values alone can also require RAW, without NaN or infinity.
    assertEquals(
        0,
        roundTrip(
            TSDataType.DOUBLE,
            new long[] {Double.doubleToRawLongBits(1e300), Double.doubleToRawLongBits(1e-300)})[7]);
  }

  @Test
  public void testSeededRandomRoundTrips() throws IOException {
    Random random = new Random(123);
    for (int trial = 0; trial < 12; trial++) {
      long[] integers = new long[1 + random.nextInt(150)];
      long[] doubles = new long[integers.length];
      for (int i = 0; i < integers.length; i++) {
        integers[i] = random.nextInt(1000) - 500;
        doubles[i] = Double.doubleToRawLongBits(integers[i] / 100.0);
      }
      roundTrip(TSDataType.INT64, integers);
      roundTrip(TSDataType.DOUBLE, doubles);
    }
  }

  @Test
  public void testBoundedBlocksBeyondUnsignedShortCount() throws IOException {
    long[] values = new long[65537];
    Arrays.fill(values, -7);
    assertTrue(roundTrip(TSDataType.INT64, values).length < 256);
    Encoder writer = encoder(TSDataType.INT64);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < 3 * ClusterEncoder.MAX_BLOCK_VALUES; i++) {
      writer.encode(7L, out);
    }
    assertTrue(writer.getMaxByteSize() <= 64L + 8L * ClusterEncoder.MAX_BLOCK_VALUES);
    writer.flush(out);
  }

  @Test
  public void testReuseAndReset() throws IOException {
    Encoder writer = encoder(TSDataType.INT64);
    Decoder reader = Decoder.getDecoderByType(encoding, TSDataType.INT64);
    for (long value : new long[] {-5, 0, 100}) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writer.encode(value, out);
      writer.flush(out);
      reader.reset();
      ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
      assertEquals(value, reader.readLong(buffer));
      assertFalse(reader.hasNext(buffer));
    }
  }

  @Test
  public void testMalformedAndTruncatedBlocks() throws IOException {
    byte[] raw = roundTrip(TSDataType.INT64, new long[] {Long.MIN_VALUE, Long.MAX_VALUE, 3});
    for (int end = 1; end < raw.length; end++) {
      expectInvalid(Arrays.copyOf(raw, end));
    }
    byte[] wrongVersion = raw.clone();
    wrongVersion[8] = 2;
    expectInvalid(wrongVersion);
    byte[] wrongType = raw.clone();
    wrongType[9] = TSDataType.FLOAT.serialize();
    expectInvalid(wrongType);
    byte[] zeroPack = raw.clone();
    zeroPack[5] = zeroPack[6] = 0;
    expectInvalid(zeroPack);
    long[] repeated = new long[100];
    for (int i = 0; i < repeated.length; i++) {
      repeated[i] = i % 2 == 0 ? -3 : 1000;
    }
    byte[] clustered = roundTrip(TSDataType.INT64, repeated);
    clustered[4]++; // Header no longer matches the encoded group counts.
    expectInvalid(clustered);
  }

  private void expectInvalid(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try {
      Decoder.getDecoderByType(encoding, TSDataType.INT64).readLong(buffer);
      fail("Malformed block should fail");
    } catch (TsFileDecodingException expected) {
      assertEquals("Failed parsing must not consume caller bytes", 0, buffer.position());
    }
  }

  @Test
  public void testTypeMismatchIsRejected() {
    try {
      encoder(TSDataType.INT64).encode(1.0, new ByteArrayOutputStream());
      fail("Mismatched writes must not be silently dropped");
    } catch (TsFileEncodingException expected) {
      // expected
    }
  }

  @Test
  public void testRealPageReaderDirectReads() throws IOException {
    PageWriter writer =
        new PageWriter(
            new MeasurementSchema("v", TSDataType.INT64, encoding, CompressionType.UNCOMPRESSED));
    long[] input = new long[100];
    for (int i = 0; i < input.length; i++) {
      input[i] = i % 3 == 0 ? -5 : 100;
      writer.write(i, input[i]);
    }
    PageReader reader =
        new PageReader(
            writer.getUncompressedBytes(),
            TSDataType.INT64,
            Decoder.getDecoderByType(encoding, TSDataType.INT64),
            Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT64));
    assertEquals(frequencies(input), batchFrequencies(reader.getAllSatisfiedPageData(true)));
  }

  @Test
  public void testRealValuePageReaderWithNulls() throws IOException {
    ValuePageWriter writer =
        new ValuePageWriter(
            encoder(TSDataType.INT64),
            ICompressor.getCompressor(CompressionType.UNCOMPRESSED),
            TSDataType.INT64);
    long[] times = new long[120];
    long[] nonNull = new long[80];
    int index = 0;
    for (int i = 0; i < times.length; i++) {
      times[i] = i;
      long value = i % 2 == 0 ? -5 : 100;
      boolean isNull = i % 3 == 0;
      writer.write(i, value, isNull);
      if (!isNull) {
        nonNull[index++] = value;
      }
    }
    ValuePageReader reader =
        new ValuePageReader(
            null,
            writer.getUncompressedBytes(),
            TSDataType.INT64,
            Decoder.getDecoderByType(encoding, TSDataType.INT64));
    assertEquals(frequencies(nonNull), batchFrequencies(reader.nextBatch(times, true, null)));
  }

  private Map<Long, Integer> batchFrequencies(BatchData batch) {
    Map<Long, Integer> result = new HashMap<>();
    while (batch.hasCurrent()) {
      long value = ((Number) batch.currentValue()).longValue();
      result.put(value, result.getOrDefault(value, 0) + 1);
      batch.next();
    }
    return result;
  }

  @Test
  public void testLegacyClusterFixture() throws IOException {
    // Legacy layout: one reference at offset 0, minimum 5, frequency 3, zero residuals.
    byte[] legacy = {
      0, 0, 1, 0, 3, 0, 10, 3, 80, 24, 4, 0, 2, 5, (byte) 128, 0, 0, 0, (byte) 128, (byte) 128
    };
    Decoder reader = Decoder.getDecoderByType(encoding, TSDataType.INT64);
    ByteBuffer input = ByteBuffer.wrap(legacy);
    for (int i = 0; i < 3; i++) {
      assertEquals(5L, reader.readLong(input));
    }
    assertFalse(reader.hasNext(input));
  }

  @Test
  public void testAlgorithmResultContracts() {
    if (encoding == TSEncoding.ACLUSTER) {
      // The new point still pays its self-residual cost; opening a second reference loses.
      assertEquals(1, AClusterAlgorithm.run(new long[] {0, 2, 3}).references.length);
      return;
    }
    Random random = new Random(12);
    for (int trial = 0; trial < 20; trial++) {
      long[] data = new long[45];
      for (int i = 0; i < data.length; i++) {
        data[i] = random.nextInt(500);
      }
      ClusterResult result = KClusterAlgorithm.run(data, 5, 1, trial);
      result.validate(data.length);
      ClusterResult again = KClusterAlgorithm.run(data, 5, 1, trial);
      assertArrayEquals(result.references, again.references);
      assertArrayEquals(result.assignments, again.assignments);
      for (int i = 0; i < data.length; i++) {
        long assignedDistance = Math.abs(data[i] - result.references[result.assignments[i]]);
        int assignedBits = Math.max(1, 64 - Long.numberOfLeadingZeros(assignedDistance));
        for (long reference : result.references) {
          long distance = Math.abs(data[i] - reference);
          int bits = Math.max(1, 64 - Long.numberOfLeadingZeros(distance));
          assertTrue("Assignments must match the final references", assignedBits <= bits);
          if (data[i] == reference) {
            assertEquals(reference, result.references[result.assignments[i]]);
          }
        }
      }
    }
  }

  @Test
  public void testBuilderPropertiesAndAliases() throws IOException {
    Map<String, String> props = new HashMap<>();
    props.put("k", "2");
    props.put("max_iterations", "1");
    props.put("seed", "123");
    TSEncodingBuilder builder = TSEncodingBuilder.getEncodingBuilder(encoding);
    builder.initFromProps(props);
    for (TSDataType type : new TSDataType[] {TSDataType.DATE, TSDataType.TIMESTAMP}) {
      Encoder writer = builder.getEncoder(type);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      encode(writer, type, -5, out);
      writer.flush(out);
      assertEquals(
          -5L,
          read(Decoder.getDecoderByType(encoding, type), type, ByteBuffer.wrap(out.toByteArray())));
    }
    if (encoding == TSEncoding.KCLUSTER) {
      props.put("max_iterations", "0");
      try {
        builder.initFromProps(props);
        fail("Invalid iteration limit must be rejected");
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testFileCloseReopenAcrossChunks() throws Exception {
    File file = new File(temporaryFolder.getRoot(), "cluster.tsfile");
    long[] input = new long[1200];
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(
          "root.table",
          new MeasurementSchema("v", TSDataType.INT64, encoding, CompressionType.UNCOMPRESSED));
      for (int i = 0; i < input.length; i++) {
        input[i] =
            i < 600
                ? new long[] {-5, 100, 10000}[i % 3]
                : new long[] {Long.MIN_VALUE, Long.MAX_VALUE, 0}[i % 3];
        writer.writeRecord(new TSRecord("root.table", i).addPoint("v", input[i]));
        if ((i + 1) % 200 == 0) {
          writer.flush();
        }
      }
    }
    Map<Long, Integer> result = new HashMap<>();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader reader = new TsFileReader(sequence)) {
      QueryDataSet rows =
          reader.query(
              QueryExpression.create(
                  Collections.singletonList(new Path("root.table", "v", true)), null));
      while (rows.hasNext()) {
        long value = rows.next().getFields().get(0).getLongV();
        result.put(value, result.getOrDefault(value, 0) + 1);
      }
    }
    assertEquals(frequencies(input), result);
  }
}
