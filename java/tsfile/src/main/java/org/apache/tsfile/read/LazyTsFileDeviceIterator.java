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

import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.LongConsumer;

public class LazyTsFileDeviceIterator {
  protected final TsFileSequenceReader reader;
  protected final Iterator<MetadataIndexNode> tableMetadataIndexNodeIterator;
  protected final Queue<Pair<IDeviceID, long[]>> queue = new LinkedList<>();
  protected final ArrayDeque<Iterator<Pair<DeviceMetadataIndexEntry, Long>>>
      levelInternalDeviceNodeIterators;
  protected final LongConsumer ioSizeRecorder;
  protected Pair<IDeviceID, long[]> currentDeviceAndMeasurementNodeOffsetPair;

  protected static final Logger logger = LoggerFactory.getLogger(LazyTsFileDeviceIterator.class);

  public LazyTsFileDeviceIterator(TsFileSequenceReader reader) throws IOException {
    this(reader, null);
  }

  public LazyTsFileDeviceIterator(TsFileSequenceReader reader, LongConsumer ioSizeRecorder)
      throws IOException {
    this.reader = reader;
    this.tableMetadataIndexNodeIterator =
        reader.readFileMetadata().getTableMetadataIndexNodeMap().values().iterator();
    this.levelInternalDeviceNodeIterators = new ArrayDeque<>(4);
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
    this.levelInternalDeviceNodeIterators = new ArrayDeque<>(4);
  }

  public boolean hasNext() {
    try {
      prepareNextTable();
      if (!queue.isEmpty()) {
        return true;
      } else if (levelInternalDeviceNodeIterators.isEmpty()) {
        return false;
      } else {
        while (!levelInternalDeviceNodeIterators.isEmpty()) {
          Iterator<Pair<DeviceMetadataIndexEntry, Long>> childIterator =
              levelInternalDeviceNodeIterators.peek();
          if (childIterator.hasNext()) {
            Pair<DeviceMetadataIndexEntry, Long> childEntryPair = childIterator.next();
            MetadataIndexNode node =
                readMetadataIndexNode(
                    childEntryPair.getLeft().getOffset(), childEntryPair.getRight());
            if (node.getNodeType() == MetadataIndexNodeType.LEAF_DEVICE) {
              getDevicesOfLeafNode(node, queue);
              return true;
            } else {
              levelInternalDeviceNodeIterators.push(constructDeviceEntryIterator(node));
            }
          } else {
            levelInternalDeviceNodeIterators.pop();
          }
        }
        return false;
      }
    } catch (IOException e) {
      throw new TsFileRuntimeException(e);
    }
  }

  public IDeviceID next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    this.currentDeviceAndMeasurementNodeOffsetPair = queue.remove();
    return currentDeviceAndMeasurementNodeOffsetPair.getLeft();
  }

  public IDeviceID getCurrentDeviceID() {
    return currentDeviceAndMeasurementNodeOffsetPair.getLeft();
  }

  public long[] getCurrentDeviceMeasurementNodeOffset() {
    return this.currentDeviceAndMeasurementNodeOffsetPair.getRight();
  }

  private void prepareNextTable() throws IOException {
    if (!queue.isEmpty() || !levelInternalDeviceNodeIterators.isEmpty()) {
      return;
    }
    if (!tableMetadataIndexNodeIterator.hasNext()) {
      return;
    }
    MetadataIndexNode nextTableMetadataIndexNode = tableMetadataIndexNodeIterator.next();

    if (nextTableMetadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      getDevicesOfLeafNode(nextTableMetadataIndexNode, queue);
    } else {
      levelInternalDeviceNodeIterators.push(
          constructDeviceEntryIterator(nextTableMetadataIndexNode));
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
      measurementNodeOffsetQueue.add(
          new Pair<>(((DeviceMetadataIndexEntry) deviceEntry).getDeviceID(), offset));
    }
  }

  public MetadataIndexNode readMetadataIndexNode(Long startOffset, Long endOffset)
      throws IOException {
    try {
      ByteBuffer nextBuffer = reader.readData(startOffset, endOffset, ioSizeRecorder);
      DeserializeConfig deserializeConfig = reader.getDeserializeContext();
      return deserializeConfig.deviceMetadataIndexNodeBufferDeserializer.deserialize(
          nextBuffer, deserializeConfig);
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error(
          "Something error happened while getting all devices of file {}", reader.getFileName());
      throw e;
    }
  }
}
