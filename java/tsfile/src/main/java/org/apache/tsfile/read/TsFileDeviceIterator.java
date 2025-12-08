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
import java.util.function.LongConsumer;

public class TsFileDeviceIterator implements Iterator<Pair<IDeviceID, Boolean>> {
  private final LazyTsFileDeviceIterator lazyTsFileDeviceIterator;

  public TsFileDeviceIterator(TsFileSequenceReader reader) throws IOException {
    this.lazyTsFileDeviceIterator = new LazyTsFileDeviceIterator(reader);
  }

  public TsFileDeviceIterator(
      TsFileSequenceReader reader, String tableName, LongConsumer ioSizeRecorder)
      throws IOException {
    this.lazyTsFileDeviceIterator = new LazyTsFileDeviceIterator(reader, tableName, ioSizeRecorder);
  }

  public Pair<IDeviceID, Boolean> current() throws IOException {
    return new Pair<>(
        lazyTsFileDeviceIterator.getCurrentDeviceID(),
        lazyTsFileDeviceIterator.isCurrentDeviceAligned());
  }

  @Override
  public boolean hasNext() {
    return lazyTsFileDeviceIterator.hasNext();
  }

  @Override
  public Pair<IDeviceID, Boolean> next() {
    IDeviceID deviceId = lazyTsFileDeviceIterator.next();
    try {
      // get the first measurement node of this device, to know if the device is aligned
      return new Pair<>(deviceId, lazyTsFileDeviceIterator.isCurrentDeviceAligned());
    } catch (IOException e) {
      throw new TsFileRuntimeException(
          "Error occurred while reading a time series metadata block.");
    }
  }

  public MetadataIndexNode getFirstMeasurementNodeOfCurrentDevice() throws IOException {
    return lazyTsFileDeviceIterator.getFirstMeasurementNodeOfCurrentDevice();
  }

  public long[] getCurrentDeviceMeasurementNodeOffset() {
    return lazyTsFileDeviceIterator.getCurrentDeviceMeasurementNodeOffset();
  }
}
