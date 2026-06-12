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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetadataIndexNode {

  private static final double LOG2 = Math.log(2);
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  protected final List<IMetadataIndexEntry> children;
  protected long endOffset;

  /** type of the child node at offset. */
  private final MetadataIndexNodeType nodeType;

  public MetadataIndexNode(MetadataIndexNodeType nodeType) {
    children = new ArrayList<>();
    endOffset = -1L;
    this.nodeType = nodeType;
  }

  public MetadataIndexNode(
      List<IMetadataIndexEntry> children, long endOffset, MetadataIndexNodeType nodeType) {
    this.children = children;
    this.endOffset = endOffset;
    this.nodeType = nodeType;
  }

  public List<IMetadataIndexEntry> getChildren() {
    return children;
  }

  public long getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(long endOffset) {
    this.endOffset = endOffset;
  }

  public MetadataIndexNodeType getNodeType() {
    return nodeType;
  }

  public void addEntry(IMetadataIndexEntry metadataIndexEntry) {
    this.children.add(metadataIndexEntry);
  }

  public boolean isFull() {
    return children.size() >= config.getMaxDegreeOfIndexNode();
  }

  IMetadataIndexEntry peek() {
    if (children.isEmpty()) {
      return null;
    }
    return children.get(0);
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(children.size(), outputStream);
    for (IMetadataIndexEntry metadataIndexEntry : children) {
      byteLen += metadataIndexEntry.serializeTo(outputStream);
    }
    byteLen += ReadWriteIOUtils.write(endOffset, outputStream);
    byteLen += ReadWriteIOUtils.write(nodeType.serialize(), outputStream);
    return byteLen;
  }

  public static MetadataIndexNode deserializeFrom(
      ByteBuffer buffer, boolean isDeviceLevel, DeserializeConfig context) {
    List<IMetadataIndexEntry> children = new ArrayList<>();
    int size = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    for (int i = 0; i < size; i++) {
      children.add(context.deserializeMetadataIndexEntry(buffer, isDeviceLevel));
    }
    long offset = ReadWriteIOUtils.readLong(buffer);
    MetadataIndexNodeType nodeType =
        MetadataIndexNodeType.deserialize(ReadWriteIOUtils.readByte(buffer));
    return new MetadataIndexNode(children, offset, nodeType);
  }

  public static MetadataIndexNode deserializeFrom(
      InputStream inputStream, boolean isDeviceLevel, DeserializeConfig config) throws IOException {
    List<IMetadataIndexEntry> children = new ArrayList<>();
    int size = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    for (int i = 0; i < size; i++) {
      children.add(config.deserializeMetadataIndexEntry(inputStream, isDeviceLevel));
    }
    long offset = ReadWriteIOUtils.readLong(inputStream);
    MetadataIndexNodeType nodeType =
        MetadataIndexNodeType.deserialize(ReadWriteIOUtils.readByte(inputStream));
    return new MetadataIndexNode(children, offset, nodeType);
  }

  public Pair<IMetadataIndexEntry, Long> getChildIndexEntry(Comparable key, boolean exactSearch) {
    int index = binarySearchInChildren(key, exactSearch);
    return getChildIndexEntry(index);
  }

  private Pair<IMetadataIndexEntry, Long> getChildIndexEntry(int index) {
    if (index == -1) {
      return null;
    }
    long childEndOffset;
    if (index != children.size() - 1) {
      childEndOffset = children.get(index + 1).getOffset();
    } else {
      childEndOffset = this.endOffset;
    }
    return new Pair<>(children.get(index), childEndOffset);
  }

  int binarySearchInChildren(Comparable key, boolean exactSearch) {
    int low = 0;
    int high = children.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      IMetadataIndexEntry midVal = children.get(mid);
      int cmp = midVal.getCompareKey().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    // key not found
    if (exactSearch) {
      return -1;
    } else {
      return low == 0 ? low : low - 1;
    }
  }

  public List<Pair<IMetadataIndexEntry, Long>> getChildIndexEntries(
      List<? extends Comparable> keys, boolean exactSearch) {
    int[] indexArr =
        keys.size() >= children.size()
                || (keys.size() * Math.log(children.size()) / LOG2)
                    > (keys.size() + children.size())
            ? mergeSearchInChildren(keys, exactSearch)
            : binarySearchInChildren(keys, exactSearch);
    List<Pair<IMetadataIndexEntry, Long>> pairs = new ArrayList<>();
    int previousIndex = -1;
    Pair<IMetadataIndexEntry, Long> previousPair = null;
    for (int idx : indexArr) {
      if (previousIndex == idx) {
        pairs.add(previousPair);
      } else {
        Pair<IMetadataIndexEntry, Long> current = getChildIndexEntry(idx);
        pairs.add(current);
        previousIndex = idx;
        previousPair = current;
      }
    }
    return pairs;
  }

  int[] binarySearchInChildren(List<? extends Comparable> keys, boolean exactSearch) {
    int[] results = new int[keys.size()];
    Arrays.fill(results, -1);
    int currentLow = 0;
    int high = children.size() - 1;

    for (int i = 0; i < keys.size(); i++) {
      Comparable key = keys.get(i);
      if (currentLow > high) {
        Arrays.fill(results, i, keys.size(), exactSearch ? -1 : currentLow - 1);
        return results;
      }

      int foundIndex = -1;
      int start = currentLow;
      int end = high;

      while (start <= end) {
        int mid = (start + end) >>> 1;
        IMetadataIndexEntry midVal = children.get(mid);
        int cmp = midVal.getCompareKey().compareTo(key);

        if (cmp < 0) {
          start = mid + 1;
        } else if (cmp > 0) {
          end = mid - 1;
        } else {
          foundIndex = mid;
          break;
        }
      }

      if (foundIndex >= 0) {
        results[i] = foundIndex;
        currentLow = foundIndex + 1;
      } else {
        if (exactSearch) {
          results[i] = -1;
        } else {
          int insertPos = start - 1;
          results[i] = insertPos;
          currentLow = start;
        }
      }
    }
    return results;
  }

  int[] mergeSearchInChildren(List<? extends Comparable> keys, boolean exactSearch) {
    int[] results = new int[keys.size()];
    int i = 0;
    int j = 0;
    while (i < keys.size() && j < children.size()) {
      Comparable currentKey = keys.get(i);
      Comparable currentChild = children.get(j).getCompareKey();
      int cmp = currentKey.compareTo(currentChild);
      if (cmp == 0) {
        results[i] = j;
        i++;
        j++;
      } else if (cmp > 0) {
        j++;
      } else {
        if (exactSearch) {
          results[i] = -1;
        } else {
          results[i] = j - 1;
        }
        i++;
      }
    }
    Arrays.fill(results, i, keys.size(), exactSearch ? -1 : children.size() - 1);
    return results;
  }

  public boolean isDeviceLevel() {
    return this.nodeType == MetadataIndexNodeType.INTERNAL_DEVICE
        || this.nodeType == MetadataIndexNodeType.LEAF_DEVICE;
  }
}
