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

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/** Conveniently retrieve last points of all timeseries from a TsFile. */
public class TsFileLastReader
    implements AutoCloseable, Iterator<Pair<IDeviceID, List<Pair<String, TimeValuePair>>>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileLastReader.class);

  private final TsFileSequenceReader sequenceReader;
  private boolean asyncIO = true;
  // when true, blob series will return a null TimeValuePair
  private boolean ignoreBlob = false;
  private Iterator<Pair<IDeviceID, List<TimeseriesMetadata>>> timeseriesMetadataIter;
  private Pair<IDeviceID, List<Pair<String, TimeValuePair>>> nextValue;

  private BlockingQueue<Pair<IDeviceID, List<Pair<String, TimeValuePair>>>> lastValueQueue;
  private ForkJoinTask<Void> asyncTask;

  public TsFileLastReader(String filePath) throws IOException {
    sequenceReader = new TsFileSequenceReader(filePath);
  }

  /**
   * @param filePath path of the TsFile
   * @param asyncIO use asynchronous IO or not
   * @param ignoreBlob whether to ignore series with blob type (the returned TimeValuePair will be
   *     null)
   */
  public TsFileLastReader(String filePath, boolean asyncIO, boolean ignoreBlob) throws IOException {
    this(filePath);
    this.asyncIO = asyncIO;
    this.ignoreBlob = ignoreBlob;
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
      return hasNextAsync();
    } else {
      return hasNextSync();
    }
  }

  private boolean hasNextSync() {
    if (!timeseriesMetadataIter.hasNext()) {
      nextValue = new Pair<>(null, null);
    } else {
      Pair<IDeviceID, List<TimeseriesMetadata>> next = timeseriesMetadataIter.next();
      try {
        nextValue = new Pair<>(next.left, convertToLastPoints(next.right));
      } catch (IOException e) {
        LOGGER.error("Cannot read timeseries metadata from {}", sequenceReader.getFileName(), e);
        return false;
      }
    }
    return nextValue.left != null;
  }

  private boolean hasNextAsync() {
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
      List<TimeseriesMetadata> timeseriesMetadataList) throws IOException {
    boolean isAligned = timeseriesMetadataList.get(0).getTsDataType() == TSDataType.VECTOR;
    List<Pair<String, TimeValuePair>> list = new ArrayList<>();
    for (TimeseriesMetadata meta : timeseriesMetadataList) {
      Pair<String, TimeValuePair> stringTimeValuePairPair = convertToLastPoint(meta, isAligned);
      list.add(stringTimeValuePairPair);
    }
    return list;
  }

  private TimeValuePair readNonAlignedLastPoint(Chunk chunk) throws IOException {
    ChunkReader chunkReader = new ChunkReader(chunk);
    BatchData batchData = null;
    while (chunkReader.hasNextSatisfiedPage()) {
      batchData = chunkReader.nextPageData();
    }
    if (batchData != null) {
      return batchData.getLastPairBeforeOrEqualTimestamp(Long.MAX_VALUE);
    } else {
      return null;
    }
  }

  private TimeValuePair readAlignedLastPoint(Chunk chunk, ChunkMetadata chunkMetadata, long endTime)
      throws IOException {
    ByteBuffer chunkData = chunk.getData();
    PageHeader lastPageHeader = null;
    ByteBuffer lastPageData = null;
    while (chunkData.hasRemaining()) {
      PageHeader pageHeader;
      if (chunk.isSinglePageChunk()) {
        pageHeader = PageHeader.deserializeFrom(chunkData, chunkMetadata.getStatistics());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkData, TSDataType.BLOB);
      }
      ByteBuffer pageData = chunkData.slice();
      pageData.limit(pageData.position() + pageHeader.getCompressedSize());
      chunkData.position(chunkData.position() + pageHeader.getCompressedSize());

      if ((pageHeader.getStatistics() == null && pageHeader.getUncompressedSize() != 0)
          || (pageHeader.getStatistics() != null && pageHeader.getStatistics().getCount() > 0)) {
        lastPageHeader = pageHeader;
        lastPageData = pageData;
      }
    }

    if (lastPageHeader != null) {
      CompressionType compressionType = chunk.getHeader().getCompressionType();
      if (compressionType != CompressionType.UNCOMPRESSED) {
        ByteBuffer uncompressedPage = ByteBuffer.allocate(lastPageHeader.getUncompressedSize());
        IUnCompressor.getUnCompressor(compressionType).uncompress(lastPageData, uncompressedPage);
        lastPageData = uncompressedPage;
        lastPageData.flip();
      }

      ValuePageReader valuePageReader =
          new ValuePageReader(
              lastPageHeader,
              lastPageData,
              TSDataType.BLOB,
              Decoder.getDecoderByType(chunk.getHeader().getEncodingType(), TSDataType.BLOB));
      TsPrimitiveType lastValue = null;
      for (int i = 0; i < valuePageReader.getSize(); i++) {
        // the timestamp here is not necessary
        lastValue = valuePageReader.nextValue(0, i);
      }
      return new TimeValuePair(endTime, lastValue);
    } else {
      return null;
    }
  }

  private Pair<String, TimeValuePair> convertToLastPoint(
      TimeseriesMetadata seriesMeta, boolean isAligned) throws IOException {
    if (seriesMeta.getTsDataType() != TSDataType.BLOB) {
      return new Pair<>(
          seriesMeta.getMeasurementId(),
          new TimeValuePair(
              seriesMeta.getStatistics().getEndTime(),
              seriesMeta.getTsDataType() == TSDataType.VECTOR
                  ? TsPrimitiveType.getByType(
                      TSDataType.INT64, seriesMeta.getStatistics().getEndTime())
                  : TsPrimitiveType.getByType(
                      seriesMeta.getTsDataType(), seriesMeta.getStatistics().getLastValue())));
    } else {
      return readLastPoint(seriesMeta, isAligned);
    }
  }

  private Pair<String, TimeValuePair> readLastPoint(
      TimeseriesMetadata seriesMeta, boolean isAligned) throws IOException {
    if (seriesMeta.getChunkMetadataList() == null) {
      return new Pair<>(seriesMeta.getMeasurementId(), null);
    }

    ChunkMetadata lastNonEmptyChunkMetadata = null;
    for (int i = seriesMeta.getChunkMetadataList().size() - 1; i >= 0; i--) {
      ChunkMetadata chunkMetadata = (ChunkMetadata) seriesMeta.getChunkMetadataList().get(i);
      if (chunkMetadata.getStatistics() == null || chunkMetadata.getStatistics().getCount() > 0) {
        // the chunk of a single chunk series must not be empty
        lastNonEmptyChunkMetadata = chunkMetadata;
        break;
      }
    }

    if (lastNonEmptyChunkMetadata == null) {
      return new Pair<>(seriesMeta.getMeasurementId(), null);
    }

    Chunk chunk = sequenceReader.readMemChunk(lastNonEmptyChunkMetadata);

    if (!isAligned) {
      return new Pair<>(seriesMeta.getMeasurementId(), readNonAlignedLastPoint(chunk));
    } else {
      return new Pair<>(
          seriesMeta.getMeasurementId(),
          readAlignedLastPoint(
              chunk, lastNonEmptyChunkMetadata, seriesMeta.getStatistics().getEndTime()));
    }
  }

  private void init() throws IOException {
    timeseriesMetadataIter = sequenceReader.iterAllTimeseriesMetadata(false, !ignoreBlob);
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
    if (asyncIO && asyncTask != null) {
      asyncTask.cancel(true);
    }
    sequenceReader.close();
  }
}
