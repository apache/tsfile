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

import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.LongConsumer;

public class LazyTsFileDeviceIterator {
  protected final TsFileSequenceReader reader;
  protected final Iterator<MetadataIndexNode> tableMetadataIndexNodeIterator;
  protected final ArrayDeque<Pair<IDeviceID, long[]>> queue = new ArrayDeque<>();
  protected final ArrayDeque<Iterator<Pair<DeviceMetadataIndexEntry, Long>>>
      levelInternalDeviceNodeIterators = new ArrayDeque<>(4);
  protected final LongConsumer ioSizeRecorder;
  protected Pair<IDeviceID, long[]> currentDeviceAndMeasurementNodeOffsetPair;
  protected MetadataIndexNode firstMeasurementNodeOfCurrentDevice;

  protected static final Logger logger = LoggerFactory.getLogger(LazyTsFileDeviceIterator.class);

  public LazyTsFileDeviceIterator(TsFileSequenceReader reader) throws IOException {
    this(reader, null);
  }

  public LazyTsFileDeviceIterator(TsFileSequenceReader reader, LongConsumer ioSizeRecorder)
      throws IOException {
    this.reader = reader;
    this.tableMetadataIndexNodeIterator =
        reader.readFileMetadata(ioSizeRecorder).getTableMetadataIndexNodeMap().values().iterator();
    this.ioSizeRecorder = ioSizeRecorder;
  }

  public LazyTsFileDeviceIterator(
      TsFileSequenceReader reader, String tableName, LongConsumer ioSizeRecorder)
      throws IOException {
    this.reader = reader;
    this.ioSizeRecorder = ioSizeRecorder;
    MetadataIndexNode tableMetadataIndexNode =
        reader.readFileMetadata(ioSizeRecorder).getTableMetadataIndexNode(tableName);
    this.tableMetadataIndexNodeIterator =
        tableMetadataIndexNode == null
            ? Collections.emptyIterator()
            : Collections.singleton(tableMetadataIndexNode).iterator();
  }

  public boolean hasNext() {
    try {
      while (true) {
        if (!queue.isEmpty()) {
          return true;
        }

        if (!levelInternalDeviceNodeIterators.isEmpty()) {
          advanceInternalIterators();
          continue;
        }

        if (!tableMetadataIndexNodeIterator.hasNext()) {
          return false;
        }
        prepareNextTable();
      }
    } catch (IOException e) {
      throw new TsFileRuntimeException(e);
    }
  }

  private void advanceInternalIterators() throws IOException {
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

      if (node.getNodeType() == MetadataIndexNodeType.LEAF_DEVICE) {
        getDevicesOfLeafNode(node, queue);
      } else {
        levelInternalDeviceNodeIterators.push(constructDeviceEntryIterator(node));
      }
    }
  }

  public IDeviceID next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    this.currentDeviceAndMeasurementNodeOffsetPair = queue.remove();
    this.firstMeasurementNodeOfCurrentDevice = null;
    return currentDeviceAndMeasurementNodeOffsetPair.getLeft();
  }

  public boolean hasCurrent() {
    return this.currentDeviceAndMeasurementNodeOffsetPair != null;
  }

  public IDeviceID getCurrentDeviceID() {
    if (currentDeviceAndMeasurementNodeOffsetPair == null) {
      throw new IllegalStateException("next() must be called before accessing current device");
    }
    return currentDeviceAndMeasurementNodeOffsetPair.getLeft();
  }

  public long[] getCurrentDeviceMeasurementNodeOffset() {
    if (currentDeviceAndMeasurementNodeOffsetPair == null) {
      throw new IllegalStateException("next() must be called before accessing current device");
    }
    return this.currentDeviceAndMeasurementNodeOffsetPair.getRight();
  }

  public boolean isCurrentDeviceAligned() throws IOException {
    return reader.isAlignedDevice(getFirstMeasurementNodeOfCurrentDevice());
  }

  public MetadataIndexNode getFirstMeasurementNodeOfCurrentDevice() throws IOException {
    if (currentDeviceAndMeasurementNodeOffsetPair == null) {
      throw new IllegalStateException("next() must be called before accessing current device");
    }
    if (this.firstMeasurementNodeOfCurrentDevice != null) {
      return this.firstMeasurementNodeOfCurrentDevice;
    }
    long[] offsetArr = currentDeviceAndMeasurementNodeOffsetPair.getRight();
    this.firstMeasurementNodeOfCurrentDevice =
        reader.readMetadataIndexNode(offsetArr[0], offsetArr[1], false, ioSizeRecorder);
    return this.firstMeasurementNodeOfCurrentDevice;
  }

  private void prepareNextTable() throws IOException {
    while (queue.isEmpty()
        && levelInternalDeviceNodeIterators.isEmpty()
        && tableMetadataIndexNodeIterator.hasNext()) {
      MetadataIndexNode nextTableMetadataIndexNode = tableMetadataIndexNodeIterator.next();

      if (nextTableMetadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
        getDevicesOfLeafNode(nextTableMetadataIndexNode, queue);
      } else {
        levelInternalDeviceNodeIterators.push(
            constructDeviceEntryIterator(nextTableMetadataIndexNode));
      }
    }
  }

  protected Iterator<Pair<DeviceMetadataIndexEntry, Long>> constructDeviceEntryIterator(
      MetadataIndexNode node) {
    return new Iterator<Pair<DeviceMetadataIndexEntry, Long>>() {

      int index = 0;

      @Override
      public boolean hasNext() {
        return index < node.getChildren().size();
      }

      @Override
      public Pair<DeviceMetadataIndexEntry, Long> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        IMetadataIndexEntry entry = node.getChildren().get(index++);
        if (index == node.getChildren().size()) {
          return new Pair<>((DeviceMetadataIndexEntry) entry, node.getEndOffset());
        }
        return new Pair<>(
            (DeviceMetadataIndexEntry) entry, node.getChildren().get(index).getOffset());
      }
    };
  }

  protected void getDevicesOfLeafNode(
      MetadataIndexNode deviceLeafNode, Queue<Pair<IDeviceID, long[]>> measurementNodeOffsetQueue) {
    if (!deviceLeafNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      throw new IllegalStateException("the first param should be device leaf node.");
    }
    List<IMetadataIndexEntry> childrenEntries = deviceLeafNode.getChildren();
    for (int i = 0; i < childrenEntries.size(); i++) {
      IMetadataIndexEntry deviceEntry = childrenEntries.get(i);
      long childStartOffset = deviceEntry.getOffset();
      long childEndOffset =
          i == childrenEntries.size() - 1
              ? deviceLeafNode.getEndOffset()
              : childrenEntries.get(i + 1).getOffset();
      long[] offset = {childStartOffset, childEndOffset};
      measurementNodeOffsetQueue.offer(
          new Pair<>(((DeviceMetadataIndexEntry) deviceEntry).getDeviceID(), offset));
    }
  }

  public TsFileSequenceReader getReader() {
    return reader;
  }

  public LongConsumer getIoSizeRecorder() {
    return ioSizeRecorder;
  }
}
