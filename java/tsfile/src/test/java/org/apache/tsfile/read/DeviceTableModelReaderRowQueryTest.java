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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link DeviceTableModelReader#queryByRow} (table-model row-range query).
 *
 * <p>The test table has {@value #TOTAL} rows with timestamps 0..TOTAL-1 and field values equal to
 * their timestamps.
 */
public class DeviceTableModelReaderRowQueryTest {

  private static final String FILE_PATH = "test_table_reader_row_query.tsfile";
  private static final int TOTAL = 50;
  private static final String TABLE = "t1";
  private static final String FIELD = "s0";

  private DeviceTableModelReader reader;

  @Before
  public void setUp() throws IOException, WriteProcessException {
    writeTableFile(FILE_PATH, TABLE, FIELD, TOTAL);
    reader = new DeviceTableModelReader(new File(FILE_PATH));
  }

  @After
  public void tearDown() {
    if (reader != null) {
      reader.close();
    }
    new File(FILE_PATH).delete();
  }

  // ① limit=0 → empty result
  @Test
  public void testLimitZeroReturnsEmpty()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    Assert.assertEquals(0, countRows(0, 0));
  }

  // ② limit < total → exactly `limit` rows
  @Test
  public void testLimitLessThanTotal()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    Assert.assertEquals(10, countRows(0, 10));
  }

  // ③ limit > total → all rows
  @Test
  public void testLimitExceedsTotal()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    Assert.assertEquals(TOTAL, countRows(0, 9999));
  }

  // ④ limit=-1 → unlimited, returns all rows
  @Test
  public void testNegativeLimitMeansUnlimited()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    Assert.assertEquals(TOTAL, countRows(0, -1));
  }

  // ⑤ offset + limit in the middle
  @Test
  public void testOffsetPlusLimit()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    Assert.assertEquals(15, countRows(10, 15));
  }

  // ⑥ offset >= total → empty result
  @Test
  public void testOffsetBeyondTotal()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    Assert.assertEquals(0, countRows(1000, 10));
  }

  // ⑦ offset + limit > total → return remaining rows from offset
  @Test
  public void testOffsetPlusLimitExceedsTotal()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    // offset=40, limit=20 → only 10 rows remain
    Assert.assertEquals(10, countRows(40, 20));
  }

  // ⑧ data correctness: timestamps and values start from `offset`
  @Test
  public void testDataCorrectness()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    List<String> columns = Collections.singletonList(FIELD);
    ResultSet rs = reader.queryByRow(TABLE, columns, 5, 10);
    int count = 0;
    while (rs.next()) {
      long ts = rs.getLong(1); // column 1 = Time
      long val = rs.getLong(2); // column 2 = s0
      Assert.assertEquals(5 + count, ts);
      Assert.assertEquals(5 + count, val);
      count++;
    }
    rs.close();
    Assert.assertEquals(10, count);
  }

  // ⑨ metadata is accessible via the result set
  @Test
  public void testMetadataAccessible()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    List<String> columns = Collections.singletonList(FIELD);
    ResultSet rs = reader.queryByRow(TABLE, columns, 0, 5);
    Assert.assertNotNull(rs.getMetadata());
    Assert.assertEquals("Time", rs.getMetadata().getColumnName(1));
    Assert.assertEquals(FIELD, rs.getMetadata().getColumnName(2));
    rs.close();
  }

  // ⑩ paging consistency: two pages together equal the full result
  @Test
  public void testPaginationConsistency()
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    int page1 = countRows(0, 25);
    int page2 = countRows(25, 25);
    Assert.assertEquals(TOTAL, page1 + page2);
  }

  // ⑪ multiple chunks: offset/limit spans chunk boundary correctly
  @Test
  public void testMultipleChunksCorrectness()
      throws IOException,
          WriteProcessException,
          ReadProcessException,
          NoTableException,
          NoMeasurementException {
    String filePath = "test_table_reader_row_query_multi_chunk.tsfile";
    writeTableFileMultiChunk(filePath, TABLE, FIELD, 30, 30);
    try (DeviceTableModelReader r = new DeviceTableModelReader(new File(filePath))) {
      // offset=25, limit=20 → rows 25..44
      List<String> columns = Collections.singletonList(FIELD);
      ResultSet rs = r.queryByRow(TABLE, columns, 25, 20);
      int count = 0;
      while (rs.next()) {
        long ts = rs.getLong(1);
        Assert.assertEquals(25 + count, ts);
        count++;
      }
      rs.close();
      Assert.assertEquals(20, count);
    } finally {
      new File(filePath).delete();
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────────

  private int countRows(int offset, int limit)
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    List<String> columns = Collections.singletonList(FIELD);
    ResultSet rs = reader.queryByRow(TABLE, columns, offset, limit);
    int count = 0;
    while (rs.next()) {
      count++;
    }
    rs.close();
    return count;
  }

  /**
   * Write a single-chunk table file with {@code numRows} rows. Timestamps are 0..numRows-1 and
   * field values equal their timestamps.
   */
  private static void writeTableFile(
      String filePath, String tableName, String fieldName, int numRows)
      throws IOException, WriteProcessException {
    TableSchema tableSchema =
        new TableSchema(
            tableName,
            Collections.singletonList(new MeasurementSchema(fieldName, TSDataType.INT64)),
            Collections.singletonList(ColumnCategory.FIELD));
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(new File(filePath)).tableSchema(tableSchema).build()) {
      Tablet tablet =
          new Tablet(
              tableName,
              Collections.singletonList(fieldName),
              Collections.singletonList(TSDataType.INT64),
              Collections.singletonList(ColumnCategory.FIELD),
              numRows);
      for (int i = 0; i < numRows; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(fieldName, i, (long) i);
      }
      writer.write(tablet);
    }
  }

  /**
   * Write a two-chunk table file by using a tiny memory threshold to force a flush between the two
   * tablets. First chunk has rows 0..chunk1Rows-1, second chunk has rows
   * chunk1Rows..chunk1Rows+chunk2Rows-1. Field values equal their timestamps.
   */
  private static void writeTableFileMultiChunk(
      String filePath, String tableName, String fieldName, int chunk1Rows, int chunk2Rows)
      throws IOException, WriteProcessException {
    TableSchema tableSchema =
        new TableSchema(
            tableName,
            Collections.singletonList(new MeasurementSchema(fieldName, TSDataType.INT64)),
            Collections.singletonList(ColumnCategory.FIELD));
    // memoryThreshold(1) forces a flush after every write, producing multiple chunks.
    try (ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(new File(filePath))
            .tableSchema(tableSchema)
            .memoryThreshold(1)
            .build()) {
      Tablet tablet1 =
          new Tablet(
              tableName,
              Collections.singletonList(fieldName),
              Collections.singletonList(TSDataType.INT64),
              Collections.singletonList(ColumnCategory.FIELD),
              chunk1Rows);
      for (int i = 0; i < chunk1Rows; i++) {
        tablet1.addTimestamp(i, i);
        tablet1.addValue(fieldName, i, (long) i);
      }
      writer.write(tablet1);

      Tablet tablet2 =
          new Tablet(
              tableName,
              Collections.singletonList(fieldName),
              Collections.singletonList(TSDataType.INT64),
              Collections.singletonList(ColumnCategory.FIELD),
              chunk2Rows);
      for (int i = 0; i < chunk2Rows; i++) {
        tablet2.addTimestamp(i, chunk1Rows + i);
        tablet2.addValue(fieldName, i, (long) (chunk1Rows + i));
      }
      writer.write(tablet2);
    }
  }
}
