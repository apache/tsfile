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

package org.apache.tsfile.write;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.executor.QueryExecutor;
import org.apache.tsfile.read.query.executor.TsFileExecutor;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

public class PerformanceTest {

  private final String testDir = "target" + File.separator + "tableViewTest";
  private final int idSchemaCnt = 3;
  private final int measurementSchemaCnt = 100;
  private final int tableCnt = 100;
  private final int devicePerTable = 10;
  private final int pointPerSeries = 100;
  private final int tabletCnt = 10;

  private List<MeasurementSchema> measurementSchemas;

  private List<Long> registerTimeList = new ArrayList<>();
  private List<Long> writeTimeList = new ArrayList<>();
  private List<Long> closeTimeList = new ArrayList<>();
  private List<Long> queryTimeList = new ArrayList<>();
  private List<Long> fileSizeList = new ArrayList<>();

  public static void main(String[] args) throws Exception{
    final PerformanceTest test = new PerformanceTest();
    test.initSchemas();

    int repetitionCnt = 10;
    for (int i = 0; i < repetitionCnt; i++) {
      test.testTree();
    }

    final double registerTime = test.registerTimeList.subList(repetitionCnt / 2, repetitionCnt).stream()
        .mapToLong(l -> l).average().orElse(
            0.0f);
    final double writeTime = test.writeTimeList.subList(repetitionCnt / 2, repetitionCnt).stream()
        .mapToLong(l -> l).average().orElse(
            0.0f);
    final double closeTime = test.closeTimeList.subList(repetitionCnt / 2, repetitionCnt).stream()
        .mapToLong(l -> l).average().orElse(
            0.0f);
    final double queryTime = test.queryTimeList.subList(repetitionCnt / 2, repetitionCnt).stream()
        .mapToLong(l -> l).average().orElse(
            0.0f);
    final double fileSize = test.fileSizeList.subList(repetitionCnt / 2, repetitionCnt).stream()
        .mapToLong(l -> l).average().orElse(
            0.0f);
    System.out.printf("Register %fns, write %fns, close %fns, query %fns, fileSize %f %n",
        registerTime,
        writeTime, closeTime, queryTime, fileSize);
  }

  private void initSchemas() {
    measurementSchemas = new ArrayList<>();
    for (int i = 0; i < measurementSchemaCnt; i++) {
      measurementSchemas.add(new MeasurementSchema(
          "s" + i, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4));
    }
  }

  private void testTree() throws IOException, WriteProcessException {
    long registerTimeSum = 0;
    long writeTimeSum = 0;
    long closeTimeSum = 0;
    long queryTimeSum = 0;
    long startTime;
    final File file = initFile();
    TsFileWriter tsFileWriter = new TsFileWriter(file);
    try {
      startTime = System.nanoTime();
      registerTree(tsFileWriter);
      registerTimeSum = System.nanoTime() - startTime;
      Tablet tablet = initTreeTablet();
      for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
        for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
          for (int tabletNum = 0; tabletNum < tabletCnt; tabletNum++) {
            fillTreeTablet(tablet, tableNum, deviceNum, tabletNum);
            startTime = System.nanoTime();
            tsFileWriter.writeAligned(tablet);
            writeTimeSum += System.nanoTime() - startTime;
          }
        }
      }
    } finally {
      startTime = System.nanoTime();
      tsFileWriter.close();
      closeTimeSum = System.nanoTime() - startTime;
    }
    long fileSize = file.length();

    startTime = System.nanoTime();
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(file.getAbsolutePath())) {
      QueryExecutor queryExecutor =
          new TsFileExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader));

      List<Path> selectedSeries = new ArrayList<>();
      for (int i = 0; i < measurementSchemaCnt; i++) {
        for (int j = 0; j < devicePerTable; j++) {
          selectedSeries.add(new Path(genTreeDeviceId(tableCnt / 2, j).getDevice(), "s" + i,
              false));
        }
      }
      final QueryExpression queryExpression = QueryExpression.create(selectedSeries, null);
      final QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
      int cnt = 0;
      while (queryDataSet.hasNext()) {
        queryDataSet.next();
        cnt++;
      }
    }
    queryTimeSum = System.nanoTime() - startTime;
    file.delete();

    System.out.printf("Tree register %dns, write %dns, close %dns, query %dns, fileSize %d %n",
        registerTimeSum,
        writeTimeSum, closeTimeSum, queryTimeSum, fileSize);
    registerTimeList.add(registerTimeSum);
    writeTimeList.add(writeTimeSum);
    closeTimeList.add(closeTimeSum);
    queryTimeList.add(queryTimeSum);
    fileSizeList.add(fileSize);

  }

  private File initFile() throws IOException {
    File dir = new File(testDir);
    dir.mkdirs();
    return new File(dir, "testTsFile");
  }

  private Tablet initTreeTablet() {
    return new Tablet(null, measurementSchemas, pointPerSeries);
  }

  private void fillTreeTablet(Tablet tablet, int tableNum, int deviceNum, int tabletNum) {
    tablet.deviceId = genTreeDeviceId(tableNum, deviceNum).getDevice();
    for (int i = 0; i < measurementSchemaCnt; i++) {
      long[] values = (long[]) tablet.values[i];
      for (int valNum = 0; valNum < pointPerSeries; valNum++) {
        values[valNum] = (long) tabletNum * pointPerSeries + valNum;
      }
    }
    for (int valNum = 0; valNum < pointPerSeries; valNum++) {
      tablet.timestamps[valNum] = (long) tabletNum * pointPerSeries + valNum;
    }
    tablet.rowSize = pointPerSeries;
  }

  private void registerTree(TsFileWriter writer) throws WriteProcessException {
    for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
      for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
        writer.registerAlignedTimeseries(genTreeDeviceId(tableNum, deviceNum),
            measurementSchemas);
      }
    }
  }

  private IMeasurementSchema genMeasurementSchema(int measurementNum) {
    return measurementSchemas.get(measurementNum);
  }

  private String genTableName(int tableNum) {
    return "table_" + tableNum;
  }

  private String genDeviceId(int tableNum, int deviceNum) {
    String[] idSegments = new String[idSchemaCnt + 1];
    idSegments[0] = genTableName(tableNum);
    for (int i = 0; i < idSchemaCnt; i++) {
      idSegments[i + 1] = "0";
    }
    idSegments[idSchemaCnt] = Integer.toString(deviceNum);
    return String.join(".", idSegments);
  }

  private Path genTreeDeviceId(int tableNum, int deviceNum) {
    return new Path(genDeviceId(tableNum, deviceNum), "", false);
  }

}
