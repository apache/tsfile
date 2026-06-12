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

import org.apache.tsfile.utils.RamUsageEstimator;

public class DeviceMetadataIndexNodeOffsetsQueryContext {
  public static final long MAX_UNSIGNED_INTEGER = 0XFFFFFFFFL;
  public static final long MAX_UNSIGNED_SHORT = 0XFFFFL;
  public static final long MAX_UNSIGNED_BYTE = 0XFFL;

  private long standardStartOffset;
  private long minStartOffset = Long.MAX_VALUE;
  private long maxStartOffset = Long.MIN_VALUE;
  private long maxStartOffsetDelta;
  private long maxNodeSize;
  private final int length;
  private int used;

  private long[] longStartOffsetArr;
  private long[] longNodeSizeArr;
  private int[] intNodeSizeArr;
  private short[] shortNodeSizeArr;
  private byte[] byteNodeSizeArr;

  private Object startOffsetDeltaArr;
  private Object nodeSizeArr;

  public DeviceMetadataIndexNodeOffsetsQueryContext(int length) {
    this(length, -1);
  }

  public DeviceMetadataIndexNodeOffsetsQueryContext(int length, long metadataSize) {
    this.length = length;
    this.used = 0;
    longStartOffsetArr = new long[length];
    if (metadataSize <= 0 || metadataSize > MAX_UNSIGNED_INTEGER) {
      longNodeSizeArr = new long[length];
    } else if (metadataSize > MAX_UNSIGNED_SHORT) {
      intNodeSizeArr = new int[length];
    } else if (metadataSize > MAX_UNSIGNED_BYTE) {
      shortNodeSizeArr = new short[length];
    } else {
      byteNodeSizeArr = new byte[length];
    }
  }

  public void addDeviceMetadataIndexNodeOffset(int i, long startOffset, long endOffset) {
    minStartOffset = Math.min(startOffset, minStartOffset);
    maxStartOffset = Math.max(startOffset, maxStartOffset);
    longStartOffsetArr[i] = startOffset;
    long nodeSize = endOffset - startOffset;
    maxNodeSize = Math.max(nodeSize, maxNodeSize);
    if (longNodeSizeArr != null) {
      longNodeSizeArr[i] = nodeSize;
    } else if (intNodeSizeArr != null) {
      intNodeSizeArr[i] = (int) (nodeSize & MAX_UNSIGNED_INTEGER);
    } else if (shortNodeSizeArr != null) {
      shortNodeSizeArr[i] = (short) (nodeSize & MAX_UNSIGNED_SHORT);
    } else {
      byteNodeSizeArr[i] = (byte) (nodeSize & MAX_UNSIGNED_BYTE);
    }
    used++;
  }

  public DeviceMetadataIndexEntriesQueryResult compact() {
    maxStartOffsetDelta = maxStartOffset - minStartOffset;
    boolean compactToMap = estimateMapRamBytes() < estimateArrRamBytes(length);
    return compactToMap ? compactToIntMap() : compactToArr();
  }

  private long estimateMapRamBytes() {
    return estimateArrRamBytes(used)
        + (length <= MAX_UNSIGNED_SHORT
            ? RamUsageEstimator.sizeOfShortArray(used)
            : RamUsageEstimator.sizeOfIntArray(used));
  }

  private long estimateArrRamBytes(int arrLength) {
    long cost = 0;
    if (maxStartOffsetDelta <= MAX_UNSIGNED_SHORT) {
      cost += RamUsageEstimator.sizeOfShortArray(arrLength);
    } else if (maxStartOffsetDelta <= MAX_UNSIGNED_INTEGER) {
      cost += RamUsageEstimator.sizeOfIntArray(arrLength);
    } else {
      cost += RamUsageEstimator.sizeOfLongArray(arrLength);
    }
    if (maxNodeSize <= MAX_UNSIGNED_BYTE) {
      cost += RamUsageEstimator.sizeOfByteArray(arrLength);
    } else if (maxNodeSize <= MAX_UNSIGNED_SHORT) {
      cost += RamUsageEstimator.sizeOfShortArray(arrLength);
    } else if (maxNodeSize <= MAX_UNSIGNED_INTEGER) {
      cost += RamUsageEstimator.sizeOfIntArray(arrLength);
    } else {
      cost += RamUsageEstimator.sizeOfLongArray(arrLength);
    }
    return cost;
  }

  private DeviceMetadataIndexEntriesQueryResult compactToArr() {
    standardStartOffset = minStartOffset;
    if (maxStartOffsetDelta <= MAX_UNSIGNED_SHORT) {
      short[] shortStartOffsetDeltaArr = new short[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        shortStartOffsetDeltaArr[i] =
            (short) (MAX_UNSIGNED_SHORT & (longStartOffsetArr[i] - standardStartOffset));
      }
      startOffsetDeltaArr = shortStartOffsetDeltaArr;
    } else if (maxStartOffsetDelta < Integer.MAX_VALUE) {
      int[] intStartOffsetDeltaArr = new int[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        intStartOffsetDeltaArr[i] =
            (int) (MAX_UNSIGNED_INTEGER & (longStartOffsetArr[i] - standardStartOffset));
      }
      startOffsetDeltaArr = intStartOffsetDeltaArr;
    } else {
      standardStartOffset = 0;
      startOffsetDeltaArr = longStartOffsetArr;
    }
    longStartOffsetArr = null;
    if (maxNodeSize <= MAX_UNSIGNED_BYTE && byteNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToByteArr();
    } else if (maxNodeSize <= MAX_UNSIGNED_SHORT && shortNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToShortArr();
    } else if (maxNodeSize <= MAX_UNSIGNED_INTEGER && intNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToIntArr();
    } else {
      nodeSizeArr = longNodeSizeArr;
    }
    clearDeprecatedNodeSizeArr();

    return new ArrDeviceMetadataIndexEntriesQueryResult(
        startOffsetDeltaArr, nodeSizeArr, standardStartOffset, length);
  }

  private DeviceMetadataIndexEntriesQueryResult compactToIntMap() {
    int[] intIndexMap = null;
    short[] shortIndexMap = null;
    if (length <= Short.MAX_VALUE) {
      shortIndexMap = new short[used];
    } else {
      intIndexMap = new int[used];
    }
    int mapSize = 0;
    standardStartOffset = minStartOffset;
    if (maxStartOffsetDelta <= MAX_UNSIGNED_SHORT) {
      short[] shortStartOffsetDeltaArr = new short[used];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        shortStartOffsetDeltaArr[mapSize] =
            (short) (MAX_UNSIGNED_SHORT & (longStartOffsetArr[i] - standardStartOffset));
        if (intIndexMap != null) {
          intIndexMap[mapSize++] = i;
        } else {
          shortIndexMap[mapSize++] = (short) (i & MAX_UNSIGNED_SHORT);
        }
      }
      startOffsetDeltaArr = shortStartOffsetDeltaArr;
    } else if (maxStartOffsetDelta <= MAX_UNSIGNED_INTEGER) {
      int[] intStartOffsetDeltaArr = new int[used];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        intStartOffsetDeltaArr[mapSize] =
            (int) (MAX_UNSIGNED_INTEGER & (longStartOffsetArr[i] - standardStartOffset));
        if (intIndexMap != null) {
          intIndexMap[mapSize++] = i;
        } else {
          shortIndexMap[mapSize++] = (short) (i & MAX_UNSIGNED_SHORT);
        }
      }
      startOffsetDeltaArr = intStartOffsetDeltaArr;
    } else {
      long[] newLongStartOffsetDeltaArr = new long[used];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        newLongStartOffsetDeltaArr[mapSize] = longStartOffsetArr[i];
        if (intIndexMap != null) {
          intIndexMap[mapSize++] = i;
        } else {
          shortIndexMap[mapSize++] = (short) (i & MAX_UNSIGNED_SHORT);
        }
      }
      startOffsetDeltaArr = newLongStartOffsetDeltaArr;
      standardStartOffset = 0;
    }
    longStartOffsetArr = null;

    if (maxNodeSize <= MAX_UNSIGNED_BYTE && byteNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToByteArr(used);
    } else if (maxNodeSize <= MAX_UNSIGNED_SHORT && shortNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToShortArr(used);
    } else if (maxNodeSize <= MAX_UNSIGNED_INTEGER && intNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToIntArr(used);
    } else {
      nodeSizeArr = compactNodeSizeToLongArr(used);
    }
    clearDeprecatedNodeSizeArr();

    return new MapDeviceMetadataIndexEntriesQueryResult(
        intIndexMap == null ? shortIndexMap : intIndexMap,
        startOffsetDeltaArr,
        nodeSizeArr,
        standardStartOffset,
        used);
  }

  private void clearDeprecatedNodeSizeArr() {
    longNodeSizeArr = null;
    intNodeSizeArr = null;
    shortNodeSizeArr = null;
    byteNodeSizeArr = null;
  }

  private int[] compactNodeSizeArrToIntArr() {
    if (intNodeSizeArr != null) {
      return intNodeSizeArr;
    }
    int[] newIntNodeSizeArr = new int[length];
    for (int i = 0; i < length; i++) {
      newIntNodeSizeArr[i] = (int) (longNodeSizeArr[i] & MAX_UNSIGNED_INTEGER);
    }
    return newIntNodeSizeArr;
  }

  private short[] compactNodeSizeArrToShortArr() {
    shortNodeSizeArr = shortNodeSizeArr == null ? new short[length] : shortNodeSizeArr;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        shortNodeSizeArr[i] = (short) (longNodeSizeArr[i] & MAX_UNSIGNED_SHORT);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        shortNodeSizeArr[i] = (short) (intNodeSizeArr[i] & MAX_UNSIGNED_SHORT);
      }
    }
    return shortNodeSizeArr;
  }

  private byte[] compactNodeSizeArrToByteArr() {
    byteNodeSizeArr = byteNodeSizeArr == null ? new byte[length] : byteNodeSizeArr;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        byteNodeSizeArr[i] = (byte) (longNodeSizeArr[i] & MAX_UNSIGNED_BYTE);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        byteNodeSizeArr[i] = (byte) (intNodeSizeArr[i] & MAX_UNSIGNED_BYTE);
      }
      intNodeSizeArr = null;
    } else if (shortNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        byteNodeSizeArr[i] = (byte) (shortNodeSizeArr[i] & MAX_UNSIGNED_BYTE);
      }
      shortNodeSizeArr = null;
    }
    return byteNodeSizeArr;
  }

  private long[] compactNodeSizeToLongArr(int newLength) {
    long[] newLongNodeSizeArr = new long[newLength];
    int j = 0;
    for (int i = 0; i < length; i++) {
      if (longNodeSizeArr[i] == 0) {
        continue;
      }
      newLongNodeSizeArr[j++] = longNodeSizeArr[i];
    }
    return newLongNodeSizeArr;
  }

  private int[] compactNodeSizeArrToIntArr(int newLength) {
    int[] intNodeSizeArr = new int[newLength];
    int j = 0;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (longNodeSizeArr[i] == 0) {
          continue;
        }
        intNodeSizeArr[j++] = (int) (longNodeSizeArr[i] & MAX_UNSIGNED_INTEGER);
      }
    } else {
      for (int i = 0; i < length; i++) {
        if (intNodeSizeArr[i] == 0) {
          continue;
        }
        intNodeSizeArr[j++] = intNodeSizeArr[i];
      }
    }
    return intNodeSizeArr;
  }

  private short[] compactNodeSizeArrToShortArr(int newLength) {
    short[] newShortNodeSizeArr = new short[newLength];
    int j = 0;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (longNodeSizeArr[i] == 0) {
          continue;
        }
        newShortNodeSizeArr[j++] = (short) (longNodeSizeArr[i] & MAX_UNSIGNED_SHORT);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (intNodeSizeArr[i] == 0) {
          continue;
        }
        newShortNodeSizeArr[j++] = (short) (intNodeSizeArr[i] & MAX_UNSIGNED_SHORT);
      }
    } else {
      for (int i = 0; i < length; i++) {
        if (shortNodeSizeArr[i] == 0) {
          continue;
        }
        newShortNodeSizeArr[j++] = shortNodeSizeArr[i];
      }
    }
    return newShortNodeSizeArr;
  }

  private byte[] compactNodeSizeArrToByteArr(int newLength) {
    byte[] newByteNodeSizeArr = new byte[newLength];
    int j = 0;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (longNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = (byte) (longNodeSizeArr[i] & MAX_UNSIGNED_BYTE);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (intNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = (byte) (intNodeSizeArr[i] & MAX_UNSIGNED_BYTE);
      }
      intNodeSizeArr = null;
    } else if (shortNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (shortNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = (byte) (shortNodeSizeArr[i] & MAX_UNSIGNED_BYTE);
      }
      shortNodeSizeArr = null;
    } else {
      for (int i = 0; i < length; i++) {
        if (byteNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = byteNodeSizeArr[i];
      }
    }
    return newByteNodeSizeArr;
  }
}
