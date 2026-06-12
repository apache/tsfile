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

package org.apache.tsfile.read;

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.LongConsumer;

public class LazyTsFileDeviceIteratorWithDevices extends LazyTsFileDeviceIterator {

  private final List<IDeviceID> sortedDevices;

  public LazyTsFileDeviceIteratorWithDevices(
      TsFileSequenceReader reader,
      String tableName,
      LongConsumer ioSizeRecorder,
      List<IDeviceID> sortedDevices)
      throws IOException {
    super(reader, tableName, ioSizeRecorder);
    this.sortedDevices = sortedDevices;
  }

  protected Iterator<Pair<DeviceMetadataIndexEntry, Long>> constructDeviceEntryIterator(
      MetadataIndexNode node, int deviceStartIdx, int deviceEndIdx) {
    return new MetadataIndexNodeIteratorWithDevices(node, deviceStartIdx, deviceEndIdx);
  }

  @Override
  protected void prepareNextTable() throws IOException {
    while (queue.isEmpty()
        && levelInternalDeviceNodeIterators.isEmpty()
        && tableMetadataIndexNodeIterator.hasNext()) {
      MetadataIndexNode nextTableMetadataIndexNode = tableMetadataIndexNodeIterator.next();

      if (nextTableMetadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
        getDevicesOfLeafNode(nextTableMetadataIndexNode, 0, sortedDevices.size(), queue);
      } else {
        levelInternalDeviceNodeIterators.push(
            constructDeviceEntryIterator(nextTableMetadataIndexNode, 0, sortedDevices.size()));
      }
    }
  }

  @Override
  protected void advanceInternalIterators() throws IOException {
    while (!levelInternalDeviceNodeIterators.isEmpty() && queue.isEmpty()) {
      Iterator<Pair<DeviceMetadataIndexEntry, Long>> iterator =
          levelInternalDeviceNodeIterators.peek();

      if (!iterator.hasNext()) {
        levelInternalDeviceNodeIterators.pop();
        continue;
      }

      Pair<DeviceMetadataIndexEntry, Long> childEntryPair = iterator.next();
      MetadataIndexNode node =
          reader.readMetadataIndexNode(
              childEntryPair.getLeft().getOffset(),
              childEntryPair.getRight(),
              true,
              ioSizeRecorder);

      MetadataIndexNodeIteratorWithDevices iteratorWithDevices =
          (MetadataIndexNodeIteratorWithDevices) iterator;
      if (node.getNodeType() == MetadataIndexNodeType.LEAF_DEVICE) {
        getDevicesOfLeafNode(
            node,
            iteratorWithDevices.getCurrentChildDeviceStartIdx(),
            iteratorWithDevices.getCurrentDeviceEndIdx(),
            queue);
      } else {
        levelInternalDeviceNodeIterators.push(
            constructDeviceEntryIterator(
                node,
                iteratorWithDevices.getCurrentChildDeviceStartIdx(),
                iteratorWithDevices.getCurrentDeviceEndIdx()));
      }
    }
  }

  protected void getDevicesOfLeafNode(
      MetadataIndexNode deviceLeafNode,
      int deviceStartIdx,
      int deviceEndIdx,
      Queue<Pair<IDeviceID, long[]>> measurementNodeOffsetQueue) {
    if (!deviceLeafNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      throw new IllegalStateException("the first param should be device leaf node.");
    }
    List<Pair<IMetadataIndexEntry, Long>> childrenEntries =
        deviceLeafNode.getChildIndexEntries(
            sortedDevices.subList(deviceStartIdx, deviceEndIdx), true);
    for (Pair<IMetadataIndexEntry, Long> pair : childrenEntries) {
      if (pair == null) {
        continue;
      }
      IMetadataIndexEntry deviceEntry = pair.getLeft();
      long childStartOffset = deviceEntry.getOffset();
      long childEndOffset = pair.getRight();
      long[] offset = {childStartOffset, childEndOffset};
      measurementNodeOffsetQueue.offer(
          new Pair<>(((DeviceMetadataIndexEntry) deviceEntry).getDeviceID(), offset));
    }
  }

  private class MetadataIndexNodeIteratorWithDevices
      implements Iterator<Pair<DeviceMetadataIndexEntry, Long>> {
    private final int deviceStartIdx;
    private final List<Pair<IMetadataIndexEntry, Long>> childIndexEntries;

    private int currentIdx;
    private int currentChildDeviceStartIdx;

    private MetadataIndexNodeIteratorWithDevices(
        MetadataIndexNode node, int deviceStartIdx, int deviceEndIdx) {
      this.childIndexEntries =
          node.getChildIndexEntries(sortedDevices.subList(deviceStartIdx, deviceEndIdx), false);
      this.currentIdx = deviceStartIdx;
      this.deviceStartIdx = deviceStartIdx;
    }

    @Override
    public boolean hasNext() {
      while (currentIdx - deviceStartIdx < childIndexEntries.size()) {
        if (childIndexEntries.get(currentIdx - deviceStartIdx) != null) {
          break;
        }
        currentIdx++;
      }
      return currentIdx - deviceStartIdx < childIndexEntries.size();
    }

    @Override
    public Pair<DeviceMetadataIndexEntry, Long> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Pair<IMetadataIndexEntry, Long> previous = childIndexEntries.get(currentIdx - deviceStartIdx);
      currentChildDeviceStartIdx = currentIdx;
      currentIdx++;
      while (currentIdx < deviceStartIdx + childIndexEntries.size()) {
        // comparing reference here is ok because they share the same reference
        if (previous != childIndexEntries.get(currentIdx - deviceStartIdx)) {
          break;
        }
        currentIdx++;
      }
      return new Pair<>((DeviceMetadataIndexEntry) previous.left, previous.right);
    }

    private int getCurrentChildDeviceStartIdx() {
      return currentChildDeviceStartIdx;
    }

    private int getCurrentDeviceEndIdx() {
      return currentIdx;
    }
  }
}
