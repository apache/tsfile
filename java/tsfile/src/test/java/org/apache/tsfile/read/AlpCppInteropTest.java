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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.read;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;

public class AlpCppInteropTest {

  private static final int PAGE_ROWS = 1103;
  private static final int ROWS = 3 * PAGE_ROWS + 32;

  private static float expectedFloat(int row) {
    if (row >= 3 * PAGE_ROWS) {
      row = row % 4 == 0 ? 7 : row % 4 == 1 ? 8 : 1023;
    } else if (row >= 2 * PAGE_ROWS) {
      return -7.25f;
    }
    switch (row) {
      case 7:
        return Float.intBitsToFloat(0x7fc12345);
      case 8:
        return Float.POSITIVE_INFINITY;
      case 1023:
        return Float.NEGATIVE_INFINITY;
      case 1024:
        return -0.0f;
      case 1099:
        return Float.MIN_VALUE;
      case 600:
        return 0.1234567f;
      default:
        return ((float) (row % 61 - 30)) * 0.01f;
    }
  }

  private static double expectedDouble(int row) {
    if (row >= 3 * PAGE_ROWS) {
      row = row % 4 == 0 ? 7 : row % 4 == 1 ? 8 : 1023;
    } else if (row >= 2 * PAGE_ROWS) {
      return 42.125;
    }
    switch (row) {
      case 7:
        return Double.longBitsToDouble(0xfff8123456789abcL);
      case 8:
        return Double.POSITIVE_INFINITY;
      case 1023:
        return Double.NEGATIVE_INFINITY;
      case 1024:
        return -0.0;
      case 1099:
        return Double.MIN_VALUE;
      case 600:
        return 0.123456789012345;
      default:
        return ((double) (row % 127 - 63)) * 0.0001;
    }
  }

  @Test
  public void readsCppAlpPagesAndMatchesEveryValueBitForBit() throws Exception {
    File tsFile = copyFixture();
    try {
      try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsFile.getPath())) {
        Map<String, TableSchema> schemas = sequenceReader.getTableSchemaMap();
        Assert.assertTrue("C++ table schema was not read", schemas.containsKey("alp_table"));
        Assert.assertEquals(
            TSDataType.FLOAT, schemas.get("alp_table").getColumnSchemas().get(2).getType());
        Assert.assertEquals(
            TSDataType.DOUBLE, schemas.get("alp_table").getColumnSchemas().get(3).getType());
      }

      try (DeviceTableModelReader reader = new DeviceTableModelReader(tsFile);
          ResultSet rows = reader.query("alp_table", Arrays.asList("keep", "f", "d"), 0, ROWS)) {
        int rowCount = 0;
        while (rows.next()) {
          int row = (int) rows.getLong(1);
          Assert.assertEquals("timestamp and keep column differ", row, rows.getInt(2));
          Assert.assertTrue("row ordering changed", row == rowCount);
          boolean expectNull = row >= PAGE_ROWS && row < 2 * PAGE_ROWS;
          Assert.assertEquals("FLOAT null bitmap at row " + row, expectNull, rows.isNull(3));
          Assert.assertEquals("DOUBLE null bitmap at row " + row, expectNull, rows.isNull(4));
          if (!expectNull) {
            Assert.assertEquals(
                "FLOAT bits at row " + row,
                Float.floatToRawIntBits(expectedFloat(row)),
                Float.floatToRawIntBits(rows.getFloat(3)));
            Assert.assertEquals(
                "DOUBLE bits at row " + row,
                Double.doubleToRawLongBits(expectedDouble(row)),
                Double.doubleToRawLongBits(rows.getDouble(4)));
          }
          rowCount++;
        }
        Assert.assertEquals(ROWS, rowCount);
      }
    } finally {
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  @Test
  public void decoderFactoryAcceptsCppEmptyPage() throws Exception {
    byte[] bytes;
    try (InputStream input =
        AlpCppInteropTest.class.getResourceAsStream("/alp/alp-empty-float.bin")) {
      Assert.assertNotNull("Missing C++ empty ALP page", input);
      bytes = input.readAllBytes();
    }
    Assert.assertEquals(16, bytes.length);
    ByteBuffer page = ByteBuffer.wrap(bytes);
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.ALP, TSDataType.FLOAT);
    Assert.assertFalse(decoder.hasNext(page));
    Assert.assertFalse(page.hasRemaining());
  }

  private static File copyFixture() throws IOException {
    File file = Files.createTempFile("tsfile-cpp-alp-", ".tsfile").toFile();
    try (InputStream input =
        AlpCppInteropTest.class.getResourceAsStream("/alp/alp-vector-pages.tsfile")) {
      Assert.assertNotNull("Missing checked-in C++ fixture", input);
      Files.copy(input, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
    return file;
  }
}
