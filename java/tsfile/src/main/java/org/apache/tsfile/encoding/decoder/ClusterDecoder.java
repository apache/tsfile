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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ClusterDecoder extends Decoder {

  private final TSDataType dataType;

  private long[] longValues;
  private double[] doubleValues;
  private int readIndex = 0;
  private int count = 0;
  private boolean hasDecoded = false;

  public ClusterDecoder(TSDataType dataType) {
    super(TSEncoding.ACLUSTER);
    this.dataType = dataType;
  }

  /** check has next */
  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (!hasDecoded) {
      decodeInternally(buffer);
    }
    return readIndex < count;
  }

  /** reset */
  @Override
  public void reset() {
    this.hasDecoded = false;
    this.readIndex = 0;
    this.count = 0;
    this.longValues = null;
    this.doubleValues = null;
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    return (int) longValues[readIndex++];
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    return longValues[readIndex++];
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    return (float) doubleValues[readIndex++];
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    return doubleValues[readIndex++];
  }

  /** Internal decode method Decode ACluster */
  private void decodeInternally(ByteBuffer buffer) {
    if (hasDecoded) {
      return;
    }

    ClusterReader reader = new ClusterReader(buffer);

    // --- Header ---
    int scalingExponent = (int) reader.read(8);
    int k = (int) reader.read(16);
    this.count = (int) reader.read(16);
    int packSize = (int) reader.read(16);

    // --- Global Minimum Value (minVal) ---
    int minValBit = (int) reader.read(8);
    long minValSign = reader.read(1);
    long absMinVal = reader.read(minValBit); // Read the absolute value part
    long minVal = (minValSign == 1) ? -absMinVal : absMinVal; // Apply the sign

    // Allocate memory based on count and dataType
    if (this.count > 0) {
      if (dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE) {
        this.doubleValues = new double[this.count];
      } else {
        this.longValues = new long[this.count];
      }
    }

    // Handle case where all values are the same (k=0)
    if (k == 0) {
      if (this.count > 0) {
        reconstructFromMinValOnly(minVal, scalingExponent);
      }
      this.hasDecoded = true;
      return;
    }

    // --- Medoids ---
    long[] medoids = new long[k];
    int minMedoidBit = (int) reader.read(8);
    long minMedoidSign = reader.read(1);
    long absMinMedoid = reader.read(minMedoidBit); // Read the absolute value part
    long minMedoid = (minMedoidSign == 1) ? -absMinMedoid : absMinMedoid; // Apply the sign

    int maxMedoidOffsetBits = (int) reader.read(8);
    for (int i = 0; i < k; i++) {
      long offset = reader.read(maxMedoidOffsetBits);
      medoids[i] = minMedoid + offset;
    }

    // --- Frequencies (Cluster Sizes) ---
    // The encoder wrote deltas (cluster sizes), so we read them and rebuild the cumulative array
    long[] cumulativeFrequencies = new long[k];
    int numFreqBlocks = (int) reader.read(16);

    // Metadata pass for frequencies
    int[] freqBlockMaxBits = new int[numFreqBlocks];
    for (int i = 0; i < numFreqBlocks; i++) {
      freqBlockMaxBits[i] = (int) reader.read(8);
    }

    // Data pass for frequencies - Reconstruct cumulative frequencies
    long currentCumulativeFreq = 0;
    int freqIndex = 0;
    for (int i = 0; i < numFreqBlocks; i++) {
      int start = i * packSize;
      int end = Math.min(start + packSize, k);
      int bitsForBlock = freqBlockMaxBits[i];
      for (int j = start; j < end; j++) {
        long delta = reader.read(bitsForBlock); // This delta is the actual cluster size
        currentCumulativeFreq += delta;
        cumulativeFrequencies[freqIndex++] = currentCumulativeFreq;
      }
    }

    // --- Residuals ---
    long[] residuals = new long[this.count];
    int numPacks = (int) reader.read(32);

    // Metadata pass for residuals
    int[] resPackMaxBits = new int[numPacks];
    for (int i = 0; i < numPacks; i++) {
      resPackMaxBits[i] = (int) reader.read(8);
    }

    // Data pass for residuals
    int residualIdx = 0;
    for (int i = 0; i < numPacks; i++) {
      int start = i * packSize;
      int end = Math.min(start + packSize, this.count);
      int bitsForPack = resPackMaxBits[i];
      if (bitsForPack > 0) {
        for (int j = start; j < end; j++) {
          residuals[residualIdx++] = reader.read(bitsForPack);
        }
      } else {
        // If bitsForPack is 0, all residuals in this pack are 0.
        // We just need to advance the index, as the array is already initialized to 0.
        residualIdx += (end - start);
      }
    }

    // --- Final Data Reconstruction ---
    // Use the correctly reconstructed cumulativeFrequencies array
    reconstructData(medoids, cumulativeFrequencies, residuals, minVal, scalingExponent);

    this.hasDecoded = true;
  }

  private void reconstructData(
      long[] medoids, long[] frequencies, long[] residuals, long minVal, int scalingExponent) {
    int residualReadPos = 0;
    int dataWritePos = 0;
    double scalingFactor = Math.pow(10, scalingExponent);

    for (int clusterId = 0; clusterId < medoids.length; clusterId++) {
      long pointsInThisCluster = frequencies[clusterId];
      long medoid = medoids[clusterId];

      for (int i = 0; i < pointsInThisCluster; i++) {
        long zigzagResidual = residuals[residualReadPos++];
        long residual = (zigzagResidual >>> 1) ^ -(zigzagResidual & 1);
        long scaledDataPoint = medoid + residual + minVal;

        if (dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE) {
          doubleValues[dataWritePos++] = scaledDataPoint / scalingFactor;
        } else {
          longValues[dataWritePos++] = scaledDataPoint;
        }
      }
    }
  }

  private void reconstructFromMinValOnly(long minVal, int scalingExponent) {
    if (dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE) {
      double scalingFactor = Math.pow(10, scalingExponent);
      double finalValue = minVal / scalingFactor;
      for (int i = 0; i < this.count; i++) doubleValues[i] = finalValue;
    } else {
      for (int i = 0; i < this.count; i++) longValues[i] = minVal;
    }
  }
}
