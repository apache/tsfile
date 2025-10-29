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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * AClusterEncoder is a batch-based compressor for numerical data (INT32, INT64, FLOAT, DOUBLE). It
 * buffers all data points within a page, then on flush(), applies a clustering algorithm to find
 * optimal global reference points and encodes residuals.
 */
public class AClusterEncoder extends Encoder {

  private static final int DEFAULT_PACK_SIZE = 10;

  /** A buffer to store all values of a page before flushing. We use long to unify all types. */
  private interface ValueBuffer {
    /** Adds a value to the buffer. */
    void add(int value);

    void add(long value);

    void add(float value);

    void add(double value);

    /** Clears the internal buffer. */
    void clear();

    /** Checks if the buffer is empty. */
    boolean isEmpty();

    /** Gets the current size of the buffer. */
    int size();

    ProcessingResult processAndGet();
  }

  private static class Int64Buffer implements ValueBuffer {
    private final List<Long> values = new ArrayList<>();

    @Override
    public void add(int value) {
      values.add((long) value);
    }

    @Override
    public void add(long value) {
      values.add(value);
    }

    @Override
    public void add(float value) {
      /* Do nothing, type mismatch */
    }

    @Override
    public void add(double value) {
      /* Do nothing, type mismatch */
    }

    @Override
    public ProcessingResult processAndGet() { // <--- 实现新方法
      long[] data = values.stream().mapToLong(l -> l).toArray();
      return new ProcessingResult(data, 0); // Exponent is 0 for integers
    }

    @Override
    public void clear() {
      values.clear();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public int size() {
      return values.size();
    }
  }

  /** A buffer for FLOAT and DOUBLE types. It performs scaling in processAndGetLongs(). */
  private static class DoubleBuffer implements ValueBuffer {
    private final List<Double> values = new ArrayList<>();

    @Override
    public void add(int value) {
      /* Do nothing, type mismatch */
    }

    @Override
    public void add(long value) {
      /* Do nothing, type mismatch */
    }

    @Override
    public void add(float value) {
      values.add((double) value);
    } // Store as double to unify

    @Override
    public void add(double value) {
      values.add(value);
    }

    @Override
    public ProcessingResult processAndGet() {
      // --- Edge Case: Handle empty buffer ---
      if (values.isEmpty()) {
        return new ProcessingResult(new long[0], 0);
      }
      int maxDecimalPlaces = 0;
      for (double v : values) {
        String s = BigDecimal.valueOf(v).toPlainString();
        int dotIndex = s.indexOf('.');
        if (dotIndex != -1) {
          int decimalPlaces = s.length() - dotIndex - 1;
          if (decimalPlaces > maxDecimalPlaces) {
            maxDecimalPlaces = decimalPlaces;
          }
        }
      }

      double scalingFactor = Math.pow(10, maxDecimalPlaces);

      long[] scaledLongs = new long[values.size()];
      for (int i = 0; i < values.size(); i++) {
        scaledLongs[i] = Math.round(values.get(i) * scalingFactor);
      }

      return new ProcessingResult(scaledLongs, maxDecimalPlaces);
    }

    @Override
    public void clear() {
      values.clear();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public int size() {
      return values.size();
    }
  }

  private static class ProcessingResult {
    final long[] scaledLongs;
    final int scalingExponent; // e.g., 3 for a scaling factor of 1000

    ProcessingResult(long[] scaledLongs, int scalingExponent) {
      this.scaledLongs = scaledLongs;
      this.scalingExponent = scalingExponent;
    }

    long[] getScaledLongs() {
      return this.scaledLongs;
    }

    int getScalingExponent() {
      return this.scalingExponent;
    }
  }

  private final ValueBuffer buffer;

  /**
   * Constructor for AClusterEncoder. It's called by AClusterEncodingBuilder.
   *
   * @param dataType The data type of the time series, used for potential type-specific logic.
   */
  public AClusterEncoder(TSDataType dataType) {
    super(TSEncoding.ACLUSTER);
    switch (dataType) {
      case INT32:
      case INT64:
        this.buffer = new Int64Buffer();
        break;
      case FLOAT:
      case DOUBLE:
        this.buffer = new DoubleBuffer();
        break;
      default:
        throw new TsFileEncodingException(
            "AClusterEncoder does not support data type: " + dataType);
    }
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    buffer.add(value);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    buffer.add(value);
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    buffer.add(value);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    buffer.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (buffer.isEmpty()) {
      return;
    }

    ProcessingResult procResult = buffer.processAndGet();
    long[] originalData = procResult.getScaledLongs();
    int scalingExponent = procResult.getScalingExponent();
    if (originalData.length == 0) return;
    long minVal = findMin(originalData);
    long[] data = new long[originalData.length];
    for (int i = 0; i < data.length; i++) {
      data[i] = originalData[i] - minVal;
    }

    Object[] clusterResult = AClusterAlgorithm.run(data);
    long[] sortedMedoids = (long[]) clusterResult[0];
    int[] clusterAssignments = (int[]) clusterResult[1];
    long[] clusterFrequencies = (long[]) clusterResult[2];

    long[] sortedZigzagResiduals =
        calculateSortedZigzagResiduals(data, sortedMedoids, clusterAssignments, clusterFrequencies);

    encodeResults(
        out, scalingExponent, minVal, sortedMedoids, clusterFrequencies, sortedZigzagResiduals);

    buffer.clear();
  }

  private static class MedoidFreqPair {
    long medoid;
    long frequency;
    int originalIndex;

    MedoidFreqPair(long medoid, long frequency, int originalIndex) {
      this.medoid = medoid;
      this.frequency = frequency;
      this.originalIndex = originalIndex;
    }
  }

  /**
   * A direct translation of your `residualCalculationZigzagNoHuff_sorted` logic. It computes
   * residuals, applies zigzag encoding, and groups them by cluster.
   */
  private long[] calculateSortedZigzagResiduals(
      long[] data, long[] medoids, int[] assignments, long[] frequencies) {
    int n = data.length;
    int k = medoids.length;
    if (n == 0) return new long[0];

    long[] sortedResiduals = new long[n];
    int[] writePointers = new int[k];
    int cumulativeCount = 0;
    for (int i = 0; i < k; i++) {
      writePointers[i] = cumulativeCount;
      cumulativeCount += (int) frequencies[i];
    }

    for (int i = 0; i < n; i++) {
      int clusterId = assignments[i];
      long medoid = medoids[clusterId];
      long residual = data[i] - medoid;
      long zigzagResidual = (residual << 1) ^ (residual >> 63); // Zigzag Encoding

      int targetIndex = writePointers[clusterId];
      sortedResiduals[targetIndex] = zigzagResidual;
      writePointers[clusterId]++;
    }
    return sortedResiduals;
  }

  private void encodeResults(
      ByteArrayOutputStream out,
      int scalingExponent,
      long minVal,
      long[] medoids,
      long[] frequencies,
      long[] residuals)
      throws IOException {

    ClusterSupport writer = new ClusterSupport(out);
    int numPoints = residuals.length;
    int k = medoids.length;

    writer.write(scalingExponent, 8);
    writer.write(k, 16);
    writer.write(numPoints, 16);
    writer.write(DEFAULT_PACK_SIZE, 16);

    int minValBit = ClusterSupport.bitsRequired(Math.abs(minVal));
    writer.write(minValBit, 8);
    writer.write(minVal >= 0 ? 0 : 1, 1);
    writer.write(minVal, minValBit);

    if (k == 0) {
      writer.flush();
      return;
    }

    long minMedoid = findMin(medoids);
    long[] medoidOffsets = new long[k];
    int maxMedoidOffsetBits = 0;
    for (int i = 0; i < k; i++) {
      medoidOffsets[i] = medoids[i] - minMedoid;
      maxMedoidOffsetBits =
          Math.max(maxMedoidOffsetBits, ClusterSupport.bitsRequired(medoidOffsets[i]));
    }
    int minMedoidBit = ClusterSupport.bitsRequired(Math.abs(minMedoid));
    writer.write(minMedoidBit, 8);
    writer.write(minMedoid > 0 ? 0 : 1, 1);
    writer.write(minMedoid, minMedoidBit);

    writer.write(maxMedoidOffsetBits, 8);
    for (long offset : medoidOffsets) {
      writer.write(offset, maxMedoidOffsetBits);
    }

    long[] freqDeltas = new long[k];
    if (k > 0) {
      freqDeltas[0] = frequencies[0];
      for (int i = 1; i < k; i++) {
        freqDeltas[i] = frequencies[i] - frequencies[i - 1];
      }
    }
    int numFreqBlocks = (k + DEFAULT_PACK_SIZE - 1) / DEFAULT_PACK_SIZE;
    writer.write(numFreqBlocks, 16);

    int[] freqBlockMaxBits = new int[numFreqBlocks];
    // Metadata pass for frequencies
    for (int i = 0; i < numFreqBlocks; i++) {
      int start = i * DEFAULT_PACK_SIZE;
      int end = Math.min(start + DEFAULT_PACK_SIZE, k);
      long maxDelta = 0;
      for (int j = start; j < end; j++) {
        maxDelta = Math.max(maxDelta, freqDeltas[j]);
      }
      freqBlockMaxBits[i] = ClusterSupport.bitsRequired(maxDelta);
      writer.write(freqBlockMaxBits[i], 8);
    }
    // Data pass for frequencies
    for (int i = 0; i < numFreqBlocks; i++) {
      int start = i * DEFAULT_PACK_SIZE;
      int end = Math.min(start + DEFAULT_PACK_SIZE, k);
      int bitsForBlock = freqBlockMaxBits[i];
      for (int j = start; j < end; j++) {
        writer.write(freqDeltas[j], bitsForBlock);
      }
    }

    int numPacks = (numPoints + DEFAULT_PACK_SIZE - 1) / DEFAULT_PACK_SIZE;
    writer.write(numPacks, 32);

    int[] resPackMaxBits = new int[numPacks];
    // Metadata pass for residuals
    for (int i = 0; i < numPacks; i++) {
      int start = i * DEFAULT_PACK_SIZE;
      int end = Math.min(start + DEFAULT_PACK_SIZE, numPoints);
      long maxOffset = 0;
      for (int j = start; j < end; j++) {
        maxOffset = Math.max(maxOffset, residuals[j]);
      }
      resPackMaxBits[i] = ClusterSupport.bitsRequired(maxOffset);
      writer.write(resPackMaxBits[i], 8);
    }
    // Data pass for residuals
    for (int i = 0; i < numPacks; i++) {
      int start = i * DEFAULT_PACK_SIZE;
      int end = Math.min(start + DEFAULT_PACK_SIZE, numPoints);
      int bitsForPack = resPackMaxBits[i];
      if (bitsForPack > 0) {
        for (int j = start; j < end; j++) {
          writer.write(residuals[j], bitsForPack);
        }
      }
    }

    writer.flush();
  }

  private long findMin(long[] data) {
    if (data == null || data.length == 0) {
      throw new IllegalArgumentException("Data array cannot be null or empty.");
    }
    long min = data[0];
    for (int i = 1; i < data.length; i++) {
      if (data[i] < min) {
        min = data[i];
      }
    }
    return min;
  }

  @Override
  public int getOneItemMaxSize() {
    return 8;
  }

  @Override
  public long getMaxByteSize() {
    if (this.buffer.isEmpty()) {
      return 0;
    }
    return (long) this.buffer.size() * getOneItemMaxSize() * 3 / 2;
  }

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("AClusterEncoder does not support boolean values.");
  }

  @Override
  public void encode(short value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("AClusterEncoder does not support short values.");
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("AClusterEncoder does not support Binary values.");
  }

  @Override
  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("AClusterEncoder does not support BigDecimal values.");
  }
}
