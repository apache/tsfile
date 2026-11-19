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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.bitpacking.IntPacker;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/** Encoder for int value using rle or bit-packing. */
public class IntRleEncoder extends RleEncoder<Integer> {

  private static final Logger logger = LoggerFactory.getLogger(IntRleEncoder.class);

  /** Packer for packing int values. */
  private IntPacker packer;

  /** Scratch array for packing one group, reused so each group does not allocate it. */
  private final int[] packBuffer = new int[TSFileConfig.RLE_MIN_REPEATED_NUM];

  /**
   * Values buffered until {@link #flush(ByteArrayOutputStream)}. A primitive array avoids boxing
   * every dictionary index (the dictionary encoder feeds one index per value).
   */
  private int[] intValues = new int[TSFileConfig.RLE_MIN_REPEATED_NUM * 4];

  private int intValueCount;

  /** Primitive mirror of {@code bufferedValues} so encoding an int never boxes it. */
  private final int[] intBufferedValues = new int[TSFileConfig.RLE_MIN_REPEATED_NUM];

  private int intPreValue;

  public IntRleEncoder() {
    super();
    bufferedValues = new Integer[TSFileConfig.RLE_MIN_REPEATED_NUM];
    preValue = 0;
    // Kept non-null for RleEncoder.reset(); the values themselves are buffered in intValues.
    values = new ArrayList<>();
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    if (intValueCount == intValues.length) {
      int[] grown = new int[intValues.length << 1];
      System.arraycopy(intValues, 0, grown, 0, intValueCount);
      intValues = grown;
    }
    intValues[intValueCount++] = value;
  }

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    if (value) {
      this.encode(1, out);
    } else {
      this.encode(0, out);
    }
  }

  /**
   * write all values buffered in the cache to an OutputStream.
   *
   * @param out - byteArrayOutputStream
   * @throws IOException cannot flush to OutputStream
   */
  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    // we get bit width after receiving all data
    this.bitWidth = getIntMaxBitWidth(intValues, intValueCount);
    packer = new IntPacker(bitWidth);
    for (int i = 0; i < intValueCount; i++) {
      encodeIntValue(intValues[i]);
    }
    super.flush(out);
  }

  /** Primitive version of {@link RleEncoder#encodeValue(Comparable)} that avoids boxing. */
  @SuppressWarnings("squid:S3776")
  private void encodeIntValue(int value) {
    if (!isBitWidthSaved) {
      byteCache.write(bitWidth);
      isBitWidthSaved = true;
    }
    if (value == intPreValue) {
      repeatCount++;
      if (repeatCount >= TSFileConfig.RLE_MIN_REPEATED_NUM
          && repeatCount <= TSFileConfig.RLE_MAX_REPEATED_NUM) {
        return;
      } else if (repeatCount == TSFileConfig.RLE_MAX_REPEATED_NUM + 1) {
        repeatCount = TSFileConfig.RLE_MAX_REPEATED_NUM;
        try {
          writeRleRun();
        } catch (IOException e) {
          logger.error(Messages.get("log.encoding.rle_full_rle_run_error"), repeatCount, e);
        }
        repeatCount = 1;
        intPreValue = value;
      }
    } else {
      if (repeatCount >= TSFileConfig.RLE_MIN_REPEATED_NUM) {
        try {
          writeRleRun();
        } catch (IOException e) {
          logger.error(Messages.get("log.encoding.rle_write_num_error"), repeatCount, e);
        }
      }
      repeatCount = 1;
      intPreValue = value;
    }
    intBufferedValues[numBufferedValues] = value;
    numBufferedValues++;
    if (numBufferedValues == TSFileConfig.RLE_MIN_REPEATED_NUM) {
      writeOrAppendBitPackedRun();
    }
  }

  private static int getIntMaxBitWidth(int[] values, int count) {
    int max = 1;
    for (int i = 0; i < count; i++) {
      max = Math.max(max, Integer.SIZE - Integer.numberOfLeadingZeros(values[i]));
    }
    return max;
  }

  @Override
  protected void reset() {
    super.reset();
    preValue = 0;
    intPreValue = 0;
    intValueCount = 0;
  }

  /** write bytes to an outputStream using rle format: [header][value]. */
  @Override
  protected void writeRleRun() throws IOException {
    endPreviousBitPackedRun(TSFileConfig.RLE_MIN_REPEATED_NUM);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(repeatCount << 1, byteCache);
    ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(
        intPreValue, byteCache, bitWidth);
    repeatCount = 0;
    numBufferedValues = 0;
  }

  @Override
  protected void clearBuffer() {
    for (int i = numBufferedValues; i < TSFileConfig.RLE_MIN_REPEATED_NUM; i++) {
      intBufferedValues[i] = 0;
    }
  }

  @Override
  protected void convertBuffer() {
    byte[] bytes = packScratch(bitWidth);
    for (int i = 0; i < TSFileConfig.RLE_MIN_REPEATED_NUM; i++) {
      packBuffer[i] = intBufferedValues[i];
    }
    packer.pack8Values(packBuffer, 0, bytes);
    // the packed group is appended to the reusable bit-packing buffer
    appendBitPackedGroup(bytes, bitWidth);
  }

  @Override
  public int getOneItemMaxSize() {
    // The meaning of 45 is:
    // 4 + 4 + max(4+4,1 + 4 + 4 * 8)
    // length + bitwidth + max(rle-header + num, bit-header + lastNum + 8packer)
    return 45;
  }

  @Override
  public long getMaxByteSize() {
    // try to calculate max value
    int groupNum = (intValueCount / 8 + 1) / 63 + 1;
    return (long) 8 + groupNum * 5 + (long) intValueCount * 4;
  }
}
