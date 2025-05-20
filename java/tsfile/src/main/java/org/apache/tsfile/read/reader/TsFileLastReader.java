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

package org.apache.tsfile.read.reader;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

/** Conveniently retrieve last points of all timeseries from a TsFile. */
public class TsFileLastReader
    implements AutoCloseable, Iterator<Pair<IDeviceID, List<Pair<String, TimeValuePair>>>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileLastReader.class);

  private final TsFileSequenceReader sequenceReader;
  private boolean asyncIO = true;
  private Iterator<Pair<IDeviceID, List<TimeseriesMetadata>>> timeseriesMetadataIter;
  private Pair<IDeviceID, List<Pair<String, TimeValuePair>>> nextValue;

  private BlockingQueue<Pair<IDeviceID, List<Pair<String, TimeValuePair>>>> lastValueQueue;
  private ForkJoinTask<Void> asyncTask;

  public TsFileLastReader(String filePath) throws IOException {
    sequenceReader = new TsFileSequenceReader(filePath);
  }

  public TsFileLastReader(String filePath, boolean asyncIO) throws IOException {
    this(filePath);
    this.asyncIO = asyncIO;
  }

  @Override
  public boolean hasNext() {
    if (timeseriesMetadataIter == null) {
      try {
        init();
      } catch (IOException e) {
        LOGGER.error("Cannot read timeseries metadata from {}", sequenceReader.getFileName(), e);
        return false;
      }
    }

    // already meet the terminator
    if (nextValue != null) {
      return nextValue.getLeft() != null;
    }

    if (asyncIO) {
      try {
        nextValue = lastValueQueue.take();
        if (nextValue.getLeft() == null) {
          // the terminator
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    } else {
      if (!timeseriesMetadataIter.hasNext()) {
        nextValue = new Pair<>(null, null);
      } else {
        Pair<IDeviceID, List<TimeseriesMetadata>> next = timeseriesMetadataIter.next();
        nextValue = new Pair<>(next.left, convertToLastPoints(next.right));
      }
    }
    return nextValue.left != null;
  }

  /**
   * @return (deviceId, measurementId, lastPoint)
   */
  @Override
  public Pair<IDeviceID, List<Pair<String, TimeValuePair>>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Pair<IDeviceID, List<Pair<String, TimeValuePair>>> ret = nextValue;
    nextValue = null;
    return ret;
  }

  private List<Pair<String, TimeValuePair>> convertToLastPoints(
      List<TimeseriesMetadata> timeseriesMetadataList) {
    return timeseriesMetadataList.stream()
        .map(
            seriesMeta ->
                new Pair<>(
                    seriesMeta.getMeasurementId(),
                    new TimeValuePair(
                        seriesMeta.getStatistics().getEndTime(),
                        TsPrimitiveType.getByType(
                            seriesMeta.getTsDataType() == TSDataType.VECTOR
                                ? TSDataType.INT64
                                : seriesMeta.getTsDataType(),
                            seriesMeta.getTsDataType() == TSDataType.VECTOR
                                ? seriesMeta.getStatistics().getEndTime()
                                : seriesMeta.getStatistics().getLastValue()))))
        .collect(Collectors.toList());
  }

  private void init() throws IOException {
    timeseriesMetadataIter = sequenceReader.iterAllTimeseriesMetadata(false);
    if (asyncIO) {
      int queueCapacity = 1024;
      lastValueQueue = new ArrayBlockingQueue<>(queueCapacity);
      asyncTask =
          ForkJoinPool.commonPool()
              .submit(
                  () -> {
                    try {
                      while (timeseriesMetadataIter.hasNext()) {
                        Pair<IDeviceID, List<TimeseriesMetadata>> deviceSeriesMetadata =
                            timeseriesMetadataIter.next();
                        lastValueQueue.put(
                            new Pair<>(
                                deviceSeriesMetadata.left,
                                convertToLastPoints(deviceSeriesMetadata.right)));
                      }
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    } catch (Exception e) {
                      LOGGER.error("Error while reading timeseries metadata", e);
                    } finally {
                      try {
                        lastValueQueue.put(new Pair<>(null, null));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }
                    return null;
                  });
    }
  }

  @Override
  public void close() throws Exception {
    if (asyncIO) {
      asyncTask.cancel(true);
    }
    sequenceReader.close();
  }
}
