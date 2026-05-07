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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.bitpacking.LongPacker;
import org.apache.tsfile.encoding.fire.LongFire;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class LongSprintzDecoder extends SprintzDecoder {

  private static final byte OPTIMAL_MODE_MARKER = 0;
  private static final byte OPTIMAL_MODE_VERSION = 1;

  LongPacker packer;
  LongFire firePred;
  private long preValue;
  private long[] currentBuffer;
  private long currentValue;
  private final String predictScheme =
      TSFileDescriptor.getInstance().getConfig().getSprintzPredictScheme();

  /** Whether we're decoding optimal mode (per-block pack size). */
  private Boolean optimalMode = null;

  /** Number of residuals in current block (for optimal mode). */
  private int currentBlockSize;

  public LongSprintzDecoder() {
    super();
    Block_size = TSFileDescriptor.getInstance().getConfig().getSprintzBlockSize();
    firePred = new LongFire(3);
    currentBuffer = new long[Block_size + 1];
    reset();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (isBlockReaded && currentCount < decodeSize) || buffer.remaining() > 0;
  }

  @Override
  public void reset() {
    super.reset();
    preValue = 0;
    currentValue = 0;
    currentCount = 0;
    optimalMode = null;
    if (currentBuffer != null) {
      Arrays.fill(currentBuffer, 0);
    }
  }

  private void ensureOptimalModeDetermined(ByteBuffer in) throws IOException {
    if (optimalMode != null) {
      return;
    }
    if (in.remaining() < 2) {
      return;
    }
    byte first = in.get();
    byte second = in.get();
    optimalMode = (first == OPTIMAL_MODE_MARKER && second == OPTIMAL_MODE_VERSION);
    if (!optimalMode) {
      in.position(in.position() - 2);
    }
  }

  @Override
  protected void decodeBlock(ByteBuffer in) throws IOException {
    ensureOptimalModeDetermined(in);

    if (Boolean.TRUE.equals(optimalMode)) {
      decodeOptimalBlock(in);
    } else {
      decodeLegacyBlock(in);
    }
    isBlockReaded = true;
  }

  private void decodeOptimalBlock(ByteBuffer in) throws IOException {
    int packSize = in.get() & 0xFF;
    if (packSize == 0) {
      if (in.remaining() < 1) {
        throw new IOException(
            "Sprintz optimal block: need at least 1 byte after packSize=0");
      }
      int next = in.get() & 0xFF;
      if (next == 0) {
        // Single-value block: [0][0][preValue 8 bytes], no RLE
        if (in.remaining() < 8) {
          throw new IOException(
              "Sprintz optimal single-value block: need 8 bytes for preValue, have " + in.remaining());
        }
        decodeSize = 1;
        currentBlockSize = 0;
        if (currentBuffer == null || currentBuffer.length < 1) {
          currentBuffer = new long[33];
        }
        currentBuffer[0] = in.getLong();
        return;
      }
      // RLE block: [0][size] format (legacy / backward compat)
      decodeSize = next;
      LongRleDecoder decoder = new LongRleDecoder();
      if (currentBuffer == null || currentBuffer.length < decodeSize) {
        currentBuffer = new long[Math.max(decodeSize, 33)];
      }
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readLong(in);
      }
      currentBlockSize = 0;
      return;
    }

    packSize = Math.min(packSize, 32);
    if (in.remaining() < 1 + 8) {
      throw new IOException(
          "Sprintz optimal block: need 9 bytes for bitWidth+preValue, have " + in.remaining());
    }
    bitWidth = ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(in, 1);
    preValue = in.getLong();
    decodeSize = packSize + 1;
    currentBlockSize = packSize;

    int packedBytes = (packSize * bitWidth + 7) / 8;
    if (in.remaining() < packedBytes) {
      throw new IOException(
          "Sprintz optimal block: need " + packedBytes + " bytes for packed data, have "
              + in.remaining() + " (packSize=" + packSize + ", bitWidth=" + bitWidth + ")");
    }

    if (currentBuffer == null || currentBuffer.length < decodeSize) {
      currentBuffer = new long[Math.max(decodeSize, 33)];
    }
    currentBuffer[0] = preValue;

    long[] tmpBuffer = new long[packSize];
    packer = new LongPacker(bitWidth);
    byte[] packcle = new byte[packedBytes];
    for (int i = 0; i < packedBytes; i++) {
      packcle[i] = in.get();
    }
    packer.unpackNValues(packcle, 0, packSize, tmpBuffer);
    for (int i = 0; i < packSize; i++) {
      currentBuffer[i + 1] = tmpBuffer[i];
    }
    recalculate(currentBlockSize);
  }

  private void decodeLegacyBlock(ByteBuffer in) throws IOException {
    if (in.remaining() < 1) {
      throw new IOException("Sprintz legacy block: no data remaining");
    }
    bitWidth = ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(in, 1);
    // Encoder writes trailing RLE as [0][size] so bitWidth=0 means RLE block
    if (bitWidth == 0) {
      if (in.remaining() < 1) {
        throw new IOException("Sprintz legacy RLE block: need 1 byte for size, have " + in.remaining());
      }
      decodeSize = in.get() & 0xFF;
      LongRleDecoder decoder = new LongRleDecoder();
      if (currentBuffer == null || currentBuffer.length < decodeSize) {
        currentBuffer = new long[Math.max(decodeSize, Block_size + 1)];
      }
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readLong(in);
      }
      currentBlockSize = 0;
      return;
    }
    if ((bitWidth & (1 << 7)) != 0) {
      decodeSize = bitWidth & ~(1 << 7);
      LongRleDecoder decoder = new LongRleDecoder();
      if (currentBuffer == null || currentBuffer.length < decodeSize) {
        currentBuffer = new long[Math.max(decodeSize, Block_size + 1)];
      }
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readLong(in);
      }
      currentBlockSize = 0;
      return;
    }

    if (in.remaining() < 8) {
      throw new IOException(
          "Sprintz legacy block: need 8 bytes for preValue, have " + in.remaining());
    }
    decodeSize = Block_size + 1;
    currentBlockSize = Block_size;
    preValue = in.getLong();
    if (currentBuffer == null || currentBuffer.length < decodeSize) {
      currentBuffer = new long[decodeSize];
    }
    currentBuffer[0] = preValue;

    long[] tmpBuffer = new long[Block_size];
    packer = new LongPacker(bitWidth);
    int packedBytes = (Block_size * bitWidth + 7) / 8;
    byte[] packcle = new byte[packedBytes];
    for (int i = 0; i < packedBytes; i++) {
      packcle[i] = in.get();
    }
    packer.unpackNValues(packcle, 0, Block_size, tmpBuffer);
    for (int i = 0; i < Block_size; i++) {
      currentBuffer[i + 1] = tmpBuffer[i];
    }
    recalculate(currentBlockSize);
  }

  private void recalculate(int blockSize) {
    for (int i = 1; i <= blockSize; i++) {
      if (currentBuffer[i] % 2 == 0) {
        currentBuffer[i] = -currentBuffer[i] / 2;
      } else {
        currentBuffer[i] = (currentBuffer[i] + 1) / 2;
      }
    }
    if (predictScheme.equals("delta")) {
      for (int i = 1; i <= blockSize; i++) {
        currentBuffer[i] += currentBuffer[i - 1];
      }
    } else if (predictScheme.equals("fire")) {
      firePred.reset();
      for (int i = 1; i <= blockSize; i++) {
        long pred = firePred.predict(currentBuffer[i - 1]);
        long err = currentBuffer[i];
        currentBuffer[i] = pred + err;
        firePred.train(currentBuffer[i - 1], currentBuffer[i], err);
      }
    } else {
      throw new UnsupportedOperationException("Sprintz predictive method {} is not supported.");
    }
  }

  @Override
  protected void recalculate() {
    recalculate(currentBlockSize > 0 ? currentBlockSize : Block_size);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    if (!isBlockReaded || currentCount >= decodeSize) {
      try {
        decodeBlock(buffer);
      } catch (IOException e) {
        logger.error("Error occurred when readLong with Sprintz Decoder.", e);
      }
    }
    currentValue = currentBuffer[currentCount++];
    if (currentCount == decodeSize) {
      isBlockReaded = false;
      currentCount = 0;
    }
    return currentValue;
  }
}
