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

public class DeviceMetadataIndexNodeOffsetsQueryContext {

  private long standardStartOffset;
  private long minStartOffset = Long.MAX_VALUE;
  private long maxStartOffset = Long.MIN_VALUE;
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
    if (metadataSize <= 0 || metadataSize > 0XFFFFFFFFL) {
      longNodeSizeArr = new long[length];
    } else if (metadataSize > 0XFFFFL) {
      intNodeSizeArr = new int[length];
    } else if (metadataSize > 0XFFL) {
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
      intNodeSizeArr[i] = (int) (nodeSize & 0XFFFFFFFFL);
    } else if (shortNodeSizeArr != null) {
      shortNodeSizeArr[i] = (short) (nodeSize & 0XFFFFL);
    } else {
      byteNodeSizeArr[i] = (byte) (nodeSize & 0XFFL);
    }
    used++;
  }

  public DeviceMetadataIndexEntriesQueryResult compact() {
    boolean compactToMap = true;
    return compactToMap ? compactToMap() : compactToArr();
  }

  private DeviceMetadataIndexEntriesQueryResult compactToArr() {
    long maxStartOffsetDelta = (maxStartOffset - minStartOffset) / 2;
    standardStartOffset = minStartOffset + maxStartOffsetDelta;
    if (maxStartOffsetDelta < Short.MAX_VALUE) {
      short[] shortStartOffsetDeltaArr = new short[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        shortStartOffsetDeltaArr[i] = (short) (longStartOffsetArr[i] - standardStartOffset);
      }
      startOffsetDeltaArr = shortStartOffsetDeltaArr;
    } else if (maxStartOffsetDelta < Integer.MAX_VALUE) {
      int[] intStartOffsetDeltaArr = new int[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        intStartOffsetDeltaArr[i] = (int) (longStartOffsetArr[i] - standardStartOffset);
      }
      startOffsetDeltaArr = intStartOffsetDeltaArr;
    } else {
      standardStartOffset = 0;
      startOffsetDeltaArr = longStartOffsetArr;
    }
    if (maxNodeSize <= 0XFFL && byteNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToByteArr();
    } else if (maxNodeSize <= 0XFFFFL && shortNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToShortArr();
    } else if (maxNodeSize <= 0XFFFFFFFFL && intNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToIntArr();
    } else {
      nodeSizeArr = longNodeSizeArr;
    }

    longStartOffsetArr = null;
    longNodeSizeArr = null;
    return new ArrDeviceMetadataIndexEntriesQueryResult(
        startOffsetDeltaArr, nodeSizeArr, standardStartOffset, length);
  }

  private DeviceMetadataIndexEntriesQueryResult compactToMap() {
    int[] map = new int[used];
    int mapSize = 0;
    long maxStartOffsetDelta = (maxStartOffset - minStartOffset) / 2;
    standardStartOffset = minStartOffset + maxStartOffsetDelta;
    if (maxStartOffsetDelta < Short.MAX_VALUE) {
      short[] shortStartOffsetDeltaArr = new short[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        shortStartOffsetDeltaArr[mapSize] = (short) (longStartOffsetArr[i] - standardStartOffset);
        map[mapSize++] = i;
      }
      startOffsetDeltaArr = shortStartOffsetDeltaArr;
    } else if (maxStartOffsetDelta < Integer.MAX_VALUE) {
      int[] intStartOffsetDeltaArr = new int[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        intStartOffsetDeltaArr[mapSize] = (int) (longStartOffsetArr[i] - standardStartOffset);
        map[mapSize++] = i;
      }
      startOffsetDeltaArr = intStartOffsetDeltaArr;
    } else {
      long[] newLongStartOffsetDeltaArr = new long[length];
      for (int i = 0; i < length; i++) {
        if (longStartOffsetArr[i] <= 0) {
          continue;
        }
        newLongStartOffsetDeltaArr[mapSize] = longStartOffsetArr[i];
        map[mapSize++] = i;
      }
      startOffsetDeltaArr = newLongStartOffsetDeltaArr;
      standardStartOffset = 0;
    }

    if (maxNodeSize <= 0XFFL && byteNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToByteArr(used);
    } else if (maxNodeSize <= 0XFFFFL && shortNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToShortArr(used);
    } else if (maxNodeSize <= 0XFFFFFFFFL && intNodeSizeArr == null) {
      nodeSizeArr = compactNodeSizeArrToIntArr(used);
    } else {
      nodeSizeArr = compactNodeSizeToLongArr(used);
    }

    return new MapDeviceMetadataIndexEntriesQueryResult(
        map, startOffsetDeltaArr, nodeSizeArr, standardStartOffset, used);
  }

  private int[] compactNodeSizeArrToIntArr() {
    if (intNodeSizeArr != null) {
      return intNodeSizeArr;
    }
    int[] intNodeSizeArr = new int[length];
    for (int i = 0; i < length; i++) {
      intNodeSizeArr[i] = (int) (longNodeSizeArr[i] & 0XFFFFFFFFL);
    }
    return intNodeSizeArr;
  }

  private short[] compactNodeSizeArrToShortArr() {
    shortNodeSizeArr = shortNodeSizeArr == null ? new short[length] : shortNodeSizeArr;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        shortNodeSizeArr[i] = (short) (longNodeSizeArr[i] & 0XFFFFL);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        shortNodeSizeArr[i] = (short) (intNodeSizeArr[i] & 0XFFFFL);
      }
    }
    return shortNodeSizeArr;
  }

  private byte[] compactNodeSizeArrToByteArr() {
    byteNodeSizeArr = byteNodeSizeArr == null ? new byte[length] : byteNodeSizeArr;
    if (longNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        byteNodeSizeArr[i] = (byte) (longNodeSizeArr[i] & 0XFFL);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        byteNodeSizeArr[i] = (byte) (intNodeSizeArr[i] & 0XFFL);
      }
      intNodeSizeArr = null;
    } else if (shortNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        byteNodeSizeArr[i] = (byte) (shortNodeSizeArr[i] & 0XFFL);
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
        intNodeSizeArr[j++] = (int) (longNodeSizeArr[i] & 0XFFFFFFFFL);
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
        newShortNodeSizeArr[j++] = (short) (longNodeSizeArr[i] & 0XFFFFL);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (intNodeSizeArr[i] == 0) {
          continue;
        }
        newShortNodeSizeArr[j++] = (short) (intNodeSizeArr[i] & 0XFFFFL);
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
        newByteNodeSizeArr[j++] = (byte) (longNodeSizeArr[i] & 0XFFL);
      }
      longNodeSizeArr = null;
    } else if (intNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (intNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = (byte) (intNodeSizeArr[i] & 0XFFL);
      }
      intNodeSizeArr = null;
    } else if (shortNodeSizeArr != null) {
      for (int i = 0; i < length; i++) {
        if (shortNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = (byte) (shortNodeSizeArr[i] & 0XFFL);
      }
      shortNodeSizeArr = null;
    } else {
      for (int i = 0; i < length; i++) {
        if (byteNodeSizeArr[i] == 0) {
          continue;
        }
        newByteNodeSizeArr[j++] = newByteNodeSizeArr[i];
      }
    }
    return newByteNodeSizeArr;
  }
}
