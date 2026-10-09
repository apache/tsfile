/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.encoding.table;

import org.apache.tsfile.enums.TSDataType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ClusterTableCodecTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> methods() {
    return Arrays.asList(
        new Object[][] {
          {ClusterTableOptions.Method.ACLUSTER}, {ClusterTableOptions.Method.KCLUSTER}
        });
  }

  private final ClusterTableOptions options;

  public ClusterTableCodecTest(ClusterTableOptions.Method method) {
    options = new ClusterTableOptions(method, 4, 3, 42);
  }

  static ClusterTable data(int n) {
    Object[][] rows = new Object[n][4];
    long[] times = new long[n];
    Random random = new Random(19);
    for (int r = 0; r < n; r++) {
      int v = (r * 17) % 31;
      times[r] = 1700000000000L + (r / 2); // duplicates are intentional
      rows[r][0] = (double) v;
      rows[r][1] = (double) (v * 2 + 1);
      rows[r][2] = random.nextGaussian();
      rows[r][3] = r % 13 == 0 ? null : (double) (r % 7) / 10;
    }
    return new ClusterTable(
        times,
        new String[] {"x", "y", "noise", "nullable"},
        new TSDataType[] {
          TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE
        },
        rows);
  }

  static Map<String, Integer> records(ClusterTable table) {
    Map<String, Integer> result = new HashMap<>();
    addRecords(result, table);
    return result;
  }

  static void addRecords(Map<String, Integer> result, ClusterTable table) {
    for (int r = 0; r < table.rowCount(); r++) {
      StringBuilder key = new StringBuilder().append(table.timestamp(r));
      for (int c = 0; c < table.columnCount(); c++) {
        key.append('/').append(table.isNull(r, c) ? "null" : Long.toHexString(table.rawBits(r, c)));
      }
      result.merge(key.toString(), 1, Integer::sum);
    }
  }

  @Test
  public void jointColumnsAndAlpKeepWholeRecords() throws Exception {
    ClusterTable input = data(320);
    ClusterTableCodec.Encoded encoded = ClusterTableCodec.encode(input, options, new int[] {0, 1});
    assertArrayEquals(new int[] {0, 1}, encoded.clusteredColumns());
    assertTrue(encoded.referenceCount > 0);
    assertTrue(encoded.clusterBytes > 0);
    assertTrue(encoded.alpBytes > 0);
    ClusterTable output = ClusterTableCodec.decode(encoded.bytes());
    assertArrayEquals(input.columnNames(), output.columnNames());
    assertArrayEquals(input.columnTypes(), output.columnTypes());
    assertEquals(records(input), records(output));
    assertEquals(
        records(input.timeRange(1700000000020L, 1700000000040L)),
        records(output.timeRange(1700000000020L, 1700000000040L)));
    assertArrayEquals(
        encoded.bytes(), ClusterTableCodec.encode(input, options, new int[] {0, 1}).bytes());
  }

  @Test
  public void automaticSelectorAndSchemaFreeze() throws Exception {
    ClusterTable input = data(256);
    assertArrayEquals(new int[] {0, 1}, ClusterColumnSelector.select(input));
    ClusterTableEncoder stream = new ClusterTableEncoder(options);
    stream.fit(input);
    ClusterTable page = input.slice(30, 80);
    assertArrayEquals(new int[] {0, 1}, stream.encode(page).selectedColumns());
    ClusterTable other =
        new ClusterTable(
            new long[] {0},
            new String[] {"other"},
            new TSDataType[] {TSDataType.DOUBLE},
            new Object[][] {{1.0}});
    assertThrows(IllegalArgumentException.class, () -> stream.encode(other));
  }

  @Test
  public void exactTypesNullsAndUnsafeNumbers() throws Exception {
    long nan = 0x7ff8000000000042L;
    Object[][] rows = {
      {Integer.MIN_VALUE, Long.MIN_VALUE, -0.0f, -0.0},
      {
        Integer.MAX_VALUE,
        Long.MAX_VALUE,
        Float.intBitsToFloat(0x7fc00042),
        Double.longBitsToDouble(nan)
      },
      {0, 9007199254740993L, Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY},
      {null, null, null, null},
      {7, -9007199254740993L, Float.MIN_VALUE, Double.MIN_VALUE},
      {-7, -1L, 1.25f, 1.25}
    };
    ClusterTable input =
        new ClusterTable(
            new long[] {4, 3, 2, 1, 1, Long.MIN_VALUE},
            new String[] {"i", "l", "f", "d"},
            new TSDataType[] {
              TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
            },
            rows);
    ClusterTableCodec.Encoded encoded =
        ClusterTableCodec.encode(input, options, new int[] {0, 1, 2, 3});
    assertEquals(0, encoded.clusteredColumns().length);
    assertEquals(records(input), records(ClusterTableCodec.decode(encoded.bytes())));
  }

  @Test
  public void longValuesNeverPassThroughDouble() throws Exception {
    ClusterTable input =
        new ClusterTable(
            new long[] {8, 8, 9, 10},
            new String[] {"large", "float"},
            new TSDataType[] {TSDataType.INT64, TSDataType.FLOAT},
            new Object[][] {
              {Long.MAX_VALUE, 1.2f},
              {Long.MAX_VALUE - 1, 1.3f},
              {Long.MAX_VALUE - 2, 1.4f},
              {Long.MAX_VALUE, 1.2f}
            });
    ClusterTableCodec.Encoded encoded = ClusterTableCodec.encode(input, options, new int[] {0, 1});
    assertEquals(2, encoded.clusteredColumns().length);
    assertEquals(records(input), records(ClusterTableCodec.decode(encoded.bytes())));
  }

  @Test
  public void emptyAndConstantTables() throws Exception {
    ClusterTable empty = data(0);
    assertEquals(
        0, ClusterTableCodec.decode(ClusterTableCodec.encode(empty, options).bytes()).rowCount());
    Object[][] rows = new Object[64][2];
    for (Object[] row : rows) {
      row[0] = 0.0;
      row[1] = -12.5;
    }
    ClusterTable constant =
        new ClusterTable(
            new long[64],
            new String[] {"a", "b"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.DOUBLE},
            rows);
    ClusterTableCodec.Encoded encoded =
        ClusterTableCodec.encode(constant, options, new int[] {0, 1});
    assertEquals(1, encoded.referenceCount);
    assertEquals(records(constant), records(ClusterTableCodec.decode(encoded.bytes())));
  }

  @Test
  public void serializedBlockIsIndependentAndRejectsDamage() throws Exception {
    byte[] original = ClusterTableCodec.encode(data(70), options).bytes();
    for (int cut : new int[] {0, 1, 8, original.length - 1}) {
      assertThrows(
          java.io.IOException.class, () -> ClusterTableCodec.decode(Arrays.copyOf(original, cut)));
    }
    byte[] bad = original.clone();
    bad[bad.length / 2] ^= 1;
    assertThrows(java.io.IOException.class, () -> ClusterTableCodec.decode(bad));
    byte[] shape = original.clone();
    ByteBuffer.wrap(shape).putInt(6, Integer.MAX_VALUE);
    CRC32 crc = new CRC32();
    crc.update(shape, 0, shape.length - 4);
    ByteBuffer.wrap(shape).putInt(shape.length - 4, (int) crc.getValue());
    assertThrows(java.io.IOException.class, () -> ClusterTableCodec.decode(shape));
    assertEquals(records(data(70)), records(ClusterTableCodec.decode(original)));
  }

  @Test
  public void badInputRejectedWithoutSilentConversion() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ClusterTable(
                new long[] {0},
                new String[] {"v"},
                new TSDataType[] {TSDataType.INT64},
                new Object[][] {{9007199254740993.0}}));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterTableCodec.encode(data(2), options, new int[] {0, 0}));
    assertThrows(
        IllegalArgumentException.class, () -> ClusterTableCodec.encode(data(10001), options));
  }

  @Test
  public void alpCompressedAndRawVectorsAreExact() throws Exception {
    long[] raw = new long[2051];
    for (int i = 0; i < raw.length; i++) raw[i] = Double.doubleToRawLongBits((i % 31) / 10.0);
    raw[12] = Double.doubleToRawLongBits(-0.0);
    raw[19] = 0x7ff800000000007aL;
    byte[] encoded = AlpColumnCodec.encode(raw, TSDataType.DOUBLE);
    assertEquals(1, encoded[0]);
    assertTrue(encoded.length < raw.length * 8);
    assertArrayEquals(
        raw,
        AlpColumnCodec.decode(
            new DataInputStream(new ByteArrayInputStream(encoded)), raw.length, TSDataType.DOUBLE));
    long[] integers = {Long.MIN_VALUE, Long.MAX_VALUE, 9007199254740993L};
    byte[] fallback = AlpColumnCodec.encode(integers, TSDataType.INT64);
    assertEquals(0, fallback[0]);
    assertArrayEquals(
        integers,
        AlpColumnCodec.decode(
            new DataInputStream(new ByteArrayInputStream(fallback)),
            integers.length,
            TSDataType.INT64));
  }

  @Test
  public void actualTsFileCloseReopenAndMultiplePages() throws Exception {
    ClusterTable input = data(10007);
    File file = new File(temporary.getRoot(), "records.tsfile");
    try (ClusterTableTsFile.Writer writer = new ClusterTableTsFile.Writer(file, options)) {
      writer.append(input);
      writer.flush();
      writer.append(input.slice(0, 9));
    }
    Map<String, Integer> expected = records(input);
    addRecords(expected, input.slice(0, 9));
    Map<String, Integer> actual = new HashMap<>();
    int[] pages = {0};
    ClusterTableTsFile.read(
        file,
        page -> {
          pages[0]++;
          addRecords(actual, page);
        });
    assertEquals(3, pages[0]);
    assertEquals(expected, actual);
    assertThrows(java.io.IOException.class, () -> new ClusterTableTsFile.Writer(file, options));
  }

  @Test
  public void randomizedRecordsAndAlpCompanions() throws Exception {
    Random random = new Random(38);
    for (int trial = 0; trial < 12; trial++) {
      int n = 1 + random.nextInt(100);
      Object[][] rows = new Object[n][3];
      long[] times = new long[n];
      for (int r = 0; r < n; r++) {
        times[r] = random.nextLong();
        rows[r][0] = random.nextInt(31);
        rows[r][1] = random.nextLong();
        rows[r][2] = Double.longBitsToDouble(random.nextLong());
      }
      ClusterTable table =
          new ClusterTable(
              times,
              new String[] {"i", "l", "d"},
              new TSDataType[] {TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE},
              rows);
      assertEquals(
          records(table),
          records(
              ClusterTableCodec.decode(
                  ClusterTableCodec.encode(table, options, new int[] {0}).bytes())));
    }
  }
}
