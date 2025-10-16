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
package org.apache.tsfile.write.writer.tsmiterator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.FullPath;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * TSMIterator returns full path of series and its TimeseriesMetadata iteratively. It accepts data
 * source from memory or disk. Static method getTSMIteratorInMemory returns a TSMIterator that reads
 * from memory, and static method getTSMIteratorInDisk returns a TSMIterator that reads from disk.
 */
public class TSMIterator {
  private static final Logger LOG = LoggerFactory.getLogger(TSMIterator.class);
  protected Iterable<Pair<Path, List<IChunkMetadata>>> sortedChunkMetadataList;
  protected Iterator<Pair<Path, List<IChunkMetadata>>> iterator;

  protected TSMIterator(List<ChunkGroupMetadata> chunkGroupMetadataList) {
    this.sortedChunkMetadataList = sortChunkMetadata(chunkGroupMetadataList, null, null);
    this.iterator = sortedChunkMetadataList.iterator();
  }

  public static TSMIterator getTSMIteratorInMemory(
      List<ChunkGroupMetadata> chunkGroupMetadataList) {
    return new TSMIterator(chunkGroupMetadataList);
  }

  public static TSMIterator getTSMIteratorInDisk(
      File cmtFile, List<ChunkGroupMetadata> chunkGroupMetadataList, LinkedList<Long> serializePos)
      throws IOException {
    return new DiskTSMIterator(cmtFile, chunkGroupMetadataList, serializePos);
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Pair<Path, TimeseriesMetadata> next() throws IOException {
    Pair<Path, List<IChunkMetadata>> nextPair = iterator.next();
    return new Pair<>(
        nextPair.left,
        constructOneTimeseriesMetadata(nextPair.left.getMeasurement(), nextPair.right));
  }

  public static TimeseriesMetadata constructOneTimeseriesMetadata(
      String measurementId, List<IChunkMetadata> chunkMetadataList) throws IOException {
    // create TimeseriesMetaData
    PublicBAOS publicBAOS = new PublicBAOS();
    TSDataType dataType = chunkMetadataList.get(chunkMetadataList.size() - 1).getDataType();
    Statistics seriesStatistics = Statistics.getStatsByType(dataType);

    int chunkMetadataListLength = 0;
    boolean serializeStatistic = (chunkMetadataList.size() > 1);
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.getDataType().equals(dataType)) {
        continue;
      }
      chunkMetadataListLength += chunkMetadata.serializeTo(publicBAOS, serializeStatistic);
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    TimeseriesMetadata timeseriesMetadata =
        new TimeseriesMetadata(
            (byte)
                ((serializeStatistic ? (byte) 1 : (byte) 0) | chunkMetadataList.get(0).getMask()),
            chunkMetadataListLength,
            measurementId,
            dataType,
            seriesStatistics,
            publicBAOS);
    return timeseriesMetadata;
  }

  // entries in a device map have the same device, so only compare measurement
  private static final Comparator<Path> deviceMapComparator =
      Comparator.comparing(Path::getMeasurement);

  public static Iterable<Pair<Path, List<IChunkMetadata>>> sortChunkMetadata(
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      IDeviceID currentDevice,
      List<ChunkMetadata> chunkMetadataList) {

    SortedMap<IDeviceID, SortedMap<Path, List<IChunkMetadata>>> chunkMetadataMap = new TreeMap<>();
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      SortedMap<Path, List<IChunkMetadata>> deviceMap =
          chunkMetadataMap.computeIfAbsent(
              chunkGroupMetadata.getDevice(), x -> new TreeMap<>(deviceMapComparator));
      for (IChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        deviceMap
            .computeIfAbsent(
                new FullPath(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid()),
                x -> new ArrayList<>(1))
            .add(chunkMetadata);
      }
    }

    if (currentDevice != null) {
      SortedMap<Path, List<IChunkMetadata>> deviceMap =
          chunkMetadataMap.computeIfAbsent(currentDevice, x -> new TreeMap<>());
      for (IChunkMetadata chunkMetadata : chunkMetadataList) {
        deviceMap
            .computeIfAbsent(
                new FullPath(currentDevice, chunkMetadata.getMeasurementUid()),
                x -> new ArrayList<>(1))
            .add(chunkMetadata);
      }
    }

    //      SortedMap<Path, List<IChunkMetadata>>
    return () ->
        chunkMetadataMap.values().stream()
            //  Pair<Path, List<IChunkMetadata>>
            .flatMap(deviceMap -> deviceMap.entrySet().stream())
            .map(e -> new Pair<>(e.getKey(), e.getValue()))
            .iterator();
  }

  private static void testSortChunkMetadata() {
    int deviceNum = 100;
    int measurementNum = 10000;

    List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
    for (int i = 0; i < deviceNum; i++) {
      List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
      for (int j = 0; j < measurementNum; j++) {
        chunkMetadataList.add(
            new ChunkMetadata(
                "s" + j,
                TSDataType.INT32,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED,
                0,
                null));
      }
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.db1.d" + i);
      chunkGroupMetadataList.add(new ChunkGroupMetadata(deviceID, chunkMetadataList));
    }

    int repeat = 100;
    long start = System.currentTimeMillis();
    for (int i = 0; i < repeat; i++) {
      long sortStart = System.nanoTime();
      Iterable<Pair<Path, List<IChunkMetadata>>> pairs =
          sortChunkMetadata(chunkGroupMetadataList, null, null);
      long sortEnd = System.nanoTime();
      for (Pair<Path, List<IChunkMetadata>> pair : pairs) {}

      long iterationEnd = System.nanoTime();
      System.out.println(
          "Sort " + (sortEnd - sortStart) + ", iteration" + (iterationEnd - sortEnd));
    }
    System.out.println(System.currentTimeMillis() - start);
  }

  public static void main(String[] args) throws IOException {
    int deviceNum = 100;
    int measurementNum = 10000;

    List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
    for (int i = 0; i < deviceNum; i++) {
      List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
      for (int j = 0; j < measurementNum; j++) {
        chunkMetadataList.add(
            new ChunkMetadata(
                "s" + j,
                TSDataType.INT64,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED,
                0,
                Statistics.getStatsByType(TSDataType.INT64)));
      }
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.db1.d" + i);
      chunkGroupMetadataList.add(new ChunkGroupMetadata(deviceID, chunkMetadataList));
    }

    int repeat = 100;
    long start = System.currentTimeMillis();
    for (int i = 0; i < repeat; i++) {
      TSMIterator tsmIterator = new TSMIterator(chunkGroupMetadataList);
      while (tsmIterator.hasNext()) {
        tsmIterator.next();
      }
    }
    System.out.println(System.currentTimeMillis() - start);
  }
}
