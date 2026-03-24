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

import org.apache.tsfile.encoding.bitpacking.LongPacker;
import org.apache.tsfile.encoding.fire.LongFire;
import org.apache.tsfile.encoding.optimal.SprintzOptimalPackSize;
import org.apache.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Vector;

public class LongSprintzEncoder extends SprintzEncoder {

  private static final int OPTIMAL_CHUNK_MIN_SIZE = 32;
  private static final byte OPTIMAL_MODE_MARKER = 0;
  private static final byte OPTIMAL_MODE_VERSION = 1;

  LongPacker packer;
  LongFire firePred;
  protected Vector<Long> values;

  /** For optimal mode: buffer to collect values before finding optimal pack size. */
  private final ArrayList<Long> chunkBuffer = new ArrayList<>();

  /** For optimal mode: whether we've written the mode marker. */
  private boolean optimalModeMarkerWritten = false;

  public LongSprintzEncoder() {
    super();
    Block_size = config.getSprintzBlockSize();
    values = new Vector<>();
    firePred = new LongFire(3);
  }

  @Override
  protected void reset() {
    super.reset();
    values.clear();
    chunkBuffer.clear();
    optimalModeMarkerWritten = false;
  }

  @Override
  public int getOneItemMaxSize() {
    return 1 + (1 + 32) * Long.BYTES;
  }

  @Override
  public long getMaxByteSize() {
    return 1 + (1L + values.size() + chunkBuffer.size()) * Long.BYTES;
  }

  protected Long predict(Long value, Long preVlaue) throws TsFileEncodingException {
    Long pred;
    if (predictMethod.equals("delta")) {
      pred = delta(value, preVlaue);
    } else if (predictMethod.equals("fire")) {
      pred = fire(value, preVlaue);
    } else {
      throw new TsFileEncodingException(
          "Config: Predict Method {} of SprintzEncoder is not supported.");
    }
    if (pred <= 0) {
      pred = -2 * pred;
    } else {
      pred = 2 * pred - 1;
    }
    return pred;
  }

  @Override
  protected void bitPack() throws IOException {
    final long preValue = values.get(0);
    values.remove(0);
    this.bitWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(values);
    packer = new LongPacker(this.bitWidth);
    int packedBytes = (Block_size * bitWidth + 7) / 8;
    byte[] bytes = new byte[packedBytes];
    long[] tmpBuffer = new long[Block_size];
    for (int i = 0; i < Block_size; i++) {
      tmpBuffer[i] = values.get(i);
    }
    packer.packNValues(tmpBuffer, 0, Block_size, bytes);
    ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(bitWidth, byteCache, 1);
    byteCache.write(ByteBuffer.allocate(8).putLong(preValue).array());
    byteCache.write(bytes, 0, bytes.length);
  }

  protected Long delta(Long value, Long preValue) {
    return value - preValue;
  }

  protected Long fire(Long value, Long preValue) {
    long pred = firePred.predict(preValue);
    long err = value - pred;
    firePred.train(preValue, value, err);
    return err;
  }

  /**
   * Encode a chunk with optimal pack size. Each block finds its own optimal pack size.
   */
  private void encodeChunkWithOptimalPackSize(long[] originals, long[] residuals) throws IOException {
    int n = residuals.length;
    if (n == 0) {
      return;
    }

    int packSize = SprintzOptimalPackSize.findOptimalPackSize(residuals);
    packSize = Math.max(1, Math.min(32, packSize));

    int numPacks = (n + packSize - 1) / packSize;

    for (int p = 0; p < numPacks; p++) {
      int start = p * packSize;
      int end = Math.min(start + packSize, n);
      int actualPackSize = end - start;

      long preValue = originals[start];
      long[] packResiduals = new long[actualPackSize];
      for (int i = 0; i < actualPackSize; i++) {
        packResiduals[i] = residuals[start + i];
      }

      int packBitWidth = getLongArrayMaxBitWidth(packResiduals);
      packBitWidth = Math.max(1, packBitWidth);

      packer = new LongPacker(packBitWidth);
      int packedBytes = (actualPackSize * packBitWidth + 7) / 8;
      byte[] bytes = new byte[packedBytes];
      packer.packNValues(packResiduals, 0, actualPackSize, bytes);

      byteCache.write(actualPackSize);
      ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(packBitWidth, byteCache, 1);
      byteCache.write(ByteBuffer.allocate(8).putLong(preValue).array());
      byteCache.write(bytes, 0, bytes.length);
    }
  }

  private static int getLongArrayMaxBitWidth(long[] arr) {
    int max = 1;
    for (long num : arr) {
      int bw = 64 - Long.numberOfLeadingZeros(Math.max(1, num));
      max = Math.max(max, bw);
    }
    return max;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (config.isSprintzUseOptimalPackSize() && !chunkBuffer.isEmpty()) {
      encodeRemainingChunkWithOptimal();
    }
    if (byteCache.size() > 0) {
      byteCache.writeTo(out);
    }
    if (!values.isEmpty()) {
      int n = values.size();
      if (config.isSprintzUseOptimalPackSize()) {
        // Encode trailing values as one optimal-style block, no RLE
        long[] originals = new long[n];
        for (int i = 0; i < n; i++) {
          originals[i] = values.get(i);
        }
        if (n == 1) {
          // Single value: [0][0][preValue 8 bytes] so decoder treats packSize=0, next=0 as one value
          out.write(0);
          out.write(0);
          out.write(ByteBuffer.allocate(8).putLong(originals[0]).array());
        } else {
          long[] residuals = new long[n - 1];
          firePred.reset();
          long pre = originals[0];
          for (int i = 1; i < n; i++) {
            try {
              residuals[i - 1] = predict(originals[i], pre);
            } catch (TsFileEncodingException e) {
              throw new IOException(e);
            }
            pre = originals[i];
          }
          int packBitWidth = getLongArrayMaxBitWidth(residuals);
          packBitWidth = Math.max(1, packBitWidth);
          int actualPackSize = n - 1;
          int packedBytes = (actualPackSize * packBitWidth + 7) / 8;
          byte[] bytes = new byte[packedBytes];
          packer = new LongPacker(packBitWidth);
          packer.packNValues(residuals, 0, actualPackSize, bytes);
          out.write(n - 1);
          ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(packBitWidth, out, 1);
          out.write(ByteBuffer.allocate(8).putLong(originals[0]).array());
          out.write(bytes, 0, bytes.length);
        }
      } else {
        // Legacy: [0][size] then RLE for trailing (decoder expects bitWidth=0 for this)
        out.write(0);
        ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(n, out, 1);
        LongRleEncoder encoder = new LongRleEncoder();
        for (long val : values) {
          encoder.encode(val, out);
        }
        encoder.flush(out);
      }
    }
    reset();
  }

  private void encodeRemainingChunkWithOptimal() throws IOException {
    if (chunkBuffer.size() < 2) {
      values.addAll(chunkBuffer);
      chunkBuffer.clear();
      return;
    }

    int n = chunkBuffer.size();
    long[] originals = new long[n];
    for (int i = 0; i < n; i++) {
      originals[i] = chunkBuffer.get(i);
    }

    long[] residuals = new long[n - 1];
    firePred.reset();
    long pre = originals[0];
    for (int i = 1; i < n; i++) {
      try {
        residuals[i - 1] = predict(originals[i], pre);
      } catch (TsFileEncodingException e) {
        throw new IOException(e);
      }
      pre = originals[i];
    }

    if (!optimalModeMarkerWritten) {
      byteCache.write(OPTIMAL_MODE_MARKER);
      byteCache.write(OPTIMAL_MODE_VERSION);
      optimalModeMarkerWritten = true;
    }
    encodeChunkWithOptimalPackSize(originals, residuals);
    chunkBuffer.clear();
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    if (config.isSprintzUseOptimalPackSize()) {
      encodeOptimalMode(value, out);
      return;
    }
    encodeLegacyMode(value, out);
  }

  private void encodeOptimalMode(long value, ByteArrayOutputStream out) {
    chunkBuffer.add(value);

    if (chunkBuffer.size() >= OPTIMAL_CHUNK_MIN_SIZE) {
      try {
        if (!optimalModeMarkerWritten) {
          byteCache.write(OPTIMAL_MODE_MARKER);
          byteCache.write(OPTIMAL_MODE_VERSION);
          optimalModeMarkerWritten = true;
        }

        int n = chunkBuffer.size();
        long[] originals = new long[n];
        for (int i = 0; i < n; i++) {
          originals[i] = chunkBuffer.get(i);
        }

        long[] residuals = new long[n - 1];
        firePred.reset();
        long pre = originals[0];
        for (int i = 1; i < n; i++) {
          try {
            residuals[i - 1] = predict(originals[i], pre);
          } catch (TsFileEncodingException e) {
            logger.error("Error in optimal Sprintz encoding", e);
            throw new TsFileEncodingException("Sprintz optimal encoding failed", e);
          }
          pre = originals[i];
        }

        encodeChunkWithOptimalPackSize(originals, residuals);
        chunkBuffer.clear();
        groupNum++;
        if (groupNum == groupMax) {
          // Write accumulated chunks to out but do NOT call full flush(): reset() would set
          // optimalModeMarkerWritten=false and we'd write [0][1] again, which the decoder
          // would misinterpret as RLE and desync (e.g. "need 20 bytes for packed data, have 5").
          byteCache.writeTo(out);
          byteCache.reset();
          groupNum = 0;
        }
      } catch (IOException e) {
        logger.error("Error occurred when encoding with optimal Sprintz", e);
        throw new TsFileEncodingException("Sprintz optimal encoding failed", e);
      }
    }
  }

  private void encodeLegacyMode(long value, ByteArrayOutputStream out) {
    if (!isFirstCached) {
      values.add(value);
      isFirstCached = true;
      return;
    }
    values.add(value);
    if (values.size() == Block_size + 1) {
      try {
        long pre = values.get(0);
        firePred.reset();
        for (int i = 1; i <= Block_size; i++) {
          long tmp = values.get(i);
          values.set(i, predict(values.get(i), pre));
          pre = tmp;
        }
        bitPack();
        isFirstCached = false;
        values.clear();
        groupNum++;
        if (groupNum == groupMax) {
          flush(out);
        }
      } catch (IOException e) {
        logger.error("Error occurred when encoding INT64 Type value with Sprintz", e);
        throw new TsFileEncodingException("Sprintz legacy encoding failed", e);
      }
    }
  }
}
