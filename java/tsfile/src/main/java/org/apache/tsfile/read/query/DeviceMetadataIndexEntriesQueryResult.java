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

package org.apache.tsfile.read.query;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;

import static org.apache.tsfile.read.query.DeviceMetadataIndexNodeOffsetsQueryContext.MAX_UNSIGNED_BYTE;
import static org.apache.tsfile.read.query.DeviceMetadataIndexNodeOffsetsQueryContext.MAX_UNSIGNED_INTEGER;
import static org.apache.tsfile.read.query.DeviceMetadataIndexNodeOffsetsQueryContext.MAX_UNSIGNED_SHORT;

public interface DeviceMetadataIndexEntriesQueryResult extends Accountable {
  long[] getDeviceMetadataIndexNodeOffset(int deviceIndex);

  int length();
}

class ArrDeviceMetadataIndexEntriesQueryResult implements DeviceMetadataIndexEntriesQueryResult {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ArrDeviceMetadataIndexEntriesQueryResult.class);

  private final Object offsetDeltaArr;
  private final Object nodeSizeArr;
  private final long standardOffset;
  private final int length;

  ArrDeviceMetadataIndexEntriesQueryResult(
      Object offsetDeltaArr, Object nodeSizeArr, long standardOffset, int length) {
    this.offsetDeltaArr = offsetDeltaArr;
    this.nodeSizeArr = nodeSizeArr;
    this.standardOffset = standardOffset;
    this.length = length;
  }

  @Override
  public long[] getDeviceMetadataIndexNodeOffset(int deviceIndex) {
    if (deviceIndex >= length) {
      return null;
    }
    long nodeSize;
    long startOffset;
    if (nodeSizeArr instanceof byte[]) {
      nodeSize = (((byte[]) nodeSizeArr)[deviceIndex]) & MAX_UNSIGNED_BYTE;
    } else if (nodeSizeArr instanceof short[]) {
      nodeSize = (((short[]) nodeSizeArr)[deviceIndex]) & MAX_UNSIGNED_SHORT;
    } else if (nodeSizeArr instanceof int[]) {
      nodeSize = (((int[]) nodeSizeArr)[deviceIndex]) & MAX_UNSIGNED_INTEGER;
    } else {
      nodeSize = ((long[]) nodeSizeArr)[deviceIndex];
    }
    if (nodeSize <= 0) {
      return null;
    }
    if (offsetDeltaArr instanceof short[]) {
      startOffset = standardOffset + (((short[]) offsetDeltaArr)[deviceIndex] & MAX_UNSIGNED_SHORT);
    } else if (offsetDeltaArr instanceof int[]) {
      startOffset = standardOffset + (((int[]) offsetDeltaArr)[deviceIndex] & MAX_UNSIGNED_INTEGER);
    } else {
      startOffset = standardOffset + ((long[]) offsetDeltaArr)[deviceIndex];
    }

    return new long[] {startOffset, startOffset + nodeSize};
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + getRamBytesUsedOfArr(nodeSizeArr) + getRamBytesUsedOfArr(offsetDeltaArr);
  }

  private long getRamBytesUsedOfArr(Object arr) {
    if (arr instanceof long[]) {
      return RamUsageEstimator.sizeOfLongArray(length);
    } else if (arr instanceof int[]) {
      return RamUsageEstimator.sizeOfIntArray(length);
    } else if (arr instanceof short[]) {
      return RamUsageEstimator.sizeOfShortArray(length);
    } else {
      return RamUsageEstimator.sizeOfByteArray(length);
    }
  }
}

class MapDeviceMetadataIndexEntriesQueryResult implements DeviceMetadataIndexEntriesQueryResult {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MapDeviceMetadataIndexEntriesQueryResult.class);
  private final Object indexMap;
  private final ArrDeviceMetadataIndexEntriesQueryResult arrResult;

  public MapDeviceMetadataIndexEntriesQueryResult(
      Object indexMap, Object offsetDeltaArr, Object nodeSizeArr, long standardOffset, int length) {
    this.indexMap = indexMap;
    this.arrResult =
        new ArrDeviceMetadataIndexEntriesQueryResult(
            offsetDeltaArr, nodeSizeArr, standardOffset, length);
  }

  @Override
  public long[] getDeviceMetadataIndexNodeOffset(int deviceIndex) {
    if (arrResult.length() == 0) {
      return null;
    }
    int idx;
    if (indexMap instanceof int[]) {
      idx =
          Arrays.binarySearch(
              (int[]) indexMap, 0, Math.min(deviceIndex + 1, arrResult.length()), deviceIndex);
    } else {
      idx =
          unsignedBinarySearch(
              (short[]) indexMap, 0, Math.min(deviceIndex + 1, arrResult.length()), deviceIndex);
    }
    if (idx < 0) {
      return null;
    }
    return arrResult.getDeviceMetadataIndexNodeOffset(idx);
  }

  private int unsignedBinarySearch(short[] arr, int fromIndex, int toIndex, int key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = arr[mid] & (int) MAX_UNSIGNED_SHORT;

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid;
      } // key found
    }
    return -(low + 1); // key not found.
  }

  @Override
  public int length() {
    return arrResult.length();
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + arrResult.ramBytesUsed()
        + (indexMap instanceof int[]
            ? RamUsageEstimator.sizeOfIntArray(arrResult.length())
            : RamUsageEstimator.sizeOfShortArray(arrResult.length()));
  }
}
