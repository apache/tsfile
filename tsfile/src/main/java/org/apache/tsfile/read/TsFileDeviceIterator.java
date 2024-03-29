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
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class TsFileDeviceIterator implements Iterator<Pair<IDeviceID, Boolean>> {
  private final TsFileSequenceReader reader;

  // device -> firstMeasurmentNode offset
  private final Queue<Pair<IDeviceID, long[]>> queue;
  private Pair<IDeviceID, Boolean> currentDevice = null;
  private MetadataIndexNode measurementNode;

  // <startOffset, endOffset>, device leaf node offset in this file
  private final List<long[]> leafDeviceNodeOffsetList;

  public TsFileDeviceIterator(
      TsFileSequenceReader reader,
      List<long[]> leafDeviceNodeOffsetList,
      Queue<Pair<IDeviceID, long[]>> queue) {
    this.reader = reader;
    this.queue = queue;
    this.leafDeviceNodeOffsetList = leafDeviceNodeOffsetList;
  }

  public Pair<IDeviceID, Boolean> current() {
    return currentDevice;
  }

  @Override
  public boolean hasNext() {
    if (!queue.isEmpty()) {
      return true;
    } else if (leafDeviceNodeOffsetList.isEmpty()) {
      // device queue is empty and all device leaf node has been read
      return false;
    } else {
      // queue is empty but there are still some devices on leaf node not being read yet
      long[] nextDeviceLeafNodeOffset = leafDeviceNodeOffsetList.remove(0);
      try {
        reader.getDevicesAndEntriesOfOneLeafNode(
            nextDeviceLeafNodeOffset[0], nextDeviceLeafNodeOffset[1], queue);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return true;
    }
  }

  @Override
  public Pair<IDeviceID, Boolean> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Pair<IDeviceID, long[]> startEndPair = queue.remove();
    try {
      // get the first measurement node of this device, to know if the device is aligned
      this.measurementNode =
          reader.readMetadataIndexNode(startEndPair.right[0], startEndPair.right[1], false);
      boolean isAligned = reader.isAlignedDevice(measurementNode);
      currentDevice = new Pair<>(startEndPair.left, isAligned);
      return currentDevice;
    } catch (IOException e) {
      throw new TsFileRuntimeException(
          "Error occurred while reading a time series metadata block.");
    }
  }

  public MetadataIndexNode getFirstMeasurementNodeOfCurrentDevice() {
    return measurementNode;
  }
}
