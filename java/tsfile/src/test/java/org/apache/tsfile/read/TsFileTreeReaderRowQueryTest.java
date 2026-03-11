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

package org.apache.tsfile.read;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileTreeReader;
import org.apache.tsfile.read.v4.TsFileTreeReaderBuilder;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.v4.TsFileTreeWriterBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link ITsFileTreeReader#queryByRow} (tree-model row-range query).
 *
 * <p>Each device has {@value #TOTAL} rows with timestamps 0..TOTAL-1 and values timestamp * 10.
 */
public class TsFileTreeReaderRowQueryTest {

  private static final String FILE_PATH = "test_tree_reader_row_query.tsfile";
  private static final int TOTAL = 50;
  private static final String DEVICE = "device";
  private static final String MEA = "s1";

  private ITsFileTreeReader reader;

  @Before
  public void setUp() throws IOException, WriteProcessException {
    writeTreeFile(FILE_PATH, Collections.singletonList(DEVICE), MEA, TOTAL);
    reader = new TsFileTreeReaderBuilder().file(new File(FILE_PATH)).build();
  }

  @After
  public void tearDown() throws IOException {
    if (reader != null) {
      reader.close();
    }
    new File(FILE_PATH).delete();
  }

  // ① limit=0 → empty result
  @Test
  public void testLimitZeroReturnsEmpty() throws IOException {
    Assert.assertEquals(0, countRows(reader, DEVICE, MEA, 0, 0));
  }

  // ② limit < total → exactly `limit` rows
  @Test
  public void testLimitLessThanTotal() throws IOException {
    Assert.assertEquals(20, countRows(reader, DEVICE, MEA, 0, 20));
  }

  // ③ limit > total → all rows
  @Test
  public void testLimitExceedsTotal() throws IOException {
    Assert.assertEquals(TOTAL, countRows(reader, DEVICE, MEA, 0, 1000));
  }

  // ④ limit=-1 → unlimited, returns all rows
  @Test
  public void testNegativeLimitMeansUnlimited() throws IOException {
    Assert.assertEquals(TOTAL, countRows(reader, DEVICE, MEA, 0, -1));
  }

  // ⑤ offset + limit in the middle
  @Test
  public void testOffsetPlusLimit() throws IOException {
    Assert.assertEquals(15, countRows(reader, DEVICE, MEA, 10, 15));
  }

  // ⑥ offset >= total → empty result
  @Test
  public void testOffsetBeyondTotal() throws IOException {
    Assert.assertEquals(0, countRows(reader, DEVICE, MEA, 1000, 10));
  }

  // ⑦ offset + limit > total → return remaining rows from offset
  @Test
  public void testOffsetPlusLimitExceedsTotal() throws IOException {
    // offset=40, limit=20 → only 10 rows remain
    Assert.assertEquals(10, countRows(reader, DEVICE, MEA, 40, 20));
  }

  // ⑧ data correctness: timestamps and values start from `offset`
  @Test
  public void testDataCorrectness() throws IOException {
    List<String> deviceIds = Collections.singletonList(DEVICE);
    List<String> measurements = Collections.singletonList(MEA);
    ResultSet rs = reader.queryByRow(deviceIds, measurements, 5, 10);
    int count = 0;
    while (rs.next()) {
      long ts = rs.getLong(1); // column 1 = Time
      long val = rs.getLong(2); // column 2 = measurement
      Assert.assertEquals(5 + count, ts);
      Assert.assertEquals((5 + count) * 10L, val);
      count++;
    }
    rs.close();
    Assert.assertEquals(10, count);
  }

  // ⑨ metadata is accessible via the result set
  @Test
  public void testMetadataAccessible() throws IOException {
    List<String> deviceIds = Collections.singletonList(DEVICE);
    List<String> measurements = Collections.singletonList(MEA);
    ResultSet rs = reader.queryByRow(deviceIds, measurements, 0, 5);
    Assert.assertNotNull(rs.getMetadata());
    // Column 1 is always "Time"; column 2 is the full path "device.s1"
    Assert.assertEquals("Time", rs.getMetadata().getColumnName(1));
    Assert.assertEquals(DEVICE + "." + MEA, rs.getMetadata().getColumnName(2));
    rs.close();
  }

  // ⑩ paging consistency: two pages together equal the full result
  @Test
  public void testPaginationConsistency() throws IOException {
    int page1 = countRows(reader, DEVICE, MEA, 0, 25);
    int page2 = countRows(reader, DEVICE, MEA, 25, 25);
    Assert.assertEquals(TOTAL, page1 + page2);
  }

  // ⑪ multiple devices: offset/limit applied to merged result
  @Test
  public void testMultipleDevices() throws IOException, WriteProcessException {
    String filePath = "test_tree_reader_row_query_multi.tsfile";
    writeTreeFile(filePath, Arrays.asList("dev1", "dev2"), MEA, 20);
    try (ITsFileTreeReader r = new TsFileTreeReaderBuilder().file(new File(filePath)).build()) {
      int count = countRows(r, null, MEA, 5, 10);
      Assert.assertEquals(10, count);
    } finally {
      new File(filePath).delete();
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Count rows returned by {@code queryByRow}.
   *
   * @param r reader (already open)
   * @param device device ID; if {@code null}, both "dev1" and "dev2" are queried
   * @param mea measurement name
   * @param offset row offset
   * @param limit row limit
   */
  private int countRows(ITsFileTreeReader r, String device, String mea, int offset, int limit)
      throws IOException {
    List<String> deviceIds =
        device != null ? Collections.singletonList(device) : Arrays.asList("dev1", "dev2");
    List<String> measurements = Collections.singletonList(mea);
    ResultSet rs = r.queryByRow(deviceIds, measurements, offset, limit);
    int count = 0;
    while (rs.next()) {
      count++;
    }
    rs.close();
    return count;
  }

  /**
   * Write a tree-model TsFile with the given devices and measurement. Timestamps are 0..numRows-1,
   * values are timestamp * 10 (INT64).
   */
  private static void writeTreeFile(
      String filePath, List<String> deviceIds, String measurement, int numRows)
      throws IOException, WriteProcessException {
    File file = new File(filePath);
    MeasurementSchema schema = new MeasurementSchema(measurement, TSDataType.INT64);
    try (TsFileTreeWriter writer = new TsFileTreeWriterBuilder().file(file).build()) {
      for (String deviceId : deviceIds) {
        writer.registerTimeseries(deviceId, schema);
      }
      for (String deviceId : deviceIds) {
        for (int i = 0; i < numRows; i++) {
          TSRecord record = new TSRecord(deviceId, i);
          record.addPoint(measurement, (long) i * 10);
          writer.write(record);
        }
      }
    }
  }
}
