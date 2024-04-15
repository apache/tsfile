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

package org.apache.tsfile.tableview;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

public class PerformanceTest {

  private final String testDir = "target" + File.separator + "tableViewTest";
  private final int idSchemaCnt = 3;
  private final int measurementSchemaCnt = 100;
  private final int tableCnt = 100;
  private final int devicePerTable = 100;
  private final int pointPerSeries = 100;
  private final int tabletCnt = 10;

  private List<IMeasurementSchema> idSchemas;
  private List<IMeasurementSchema> measurementSchemas;

  private void testTree() throws IOException, WriteProcessException {
    long registerTimeSum = 0;
    long writeTimeSum = 0;
    long startTime;
    try (TsFileWriter tsFileWriter = initTsFileWriter()) {
      startTime = System.nanoTime();
      registerTree(tsFileWriter);
      registerTimeSum = System.nanoTime() - startTime;
      Tablet tablet = initTreeTablet();
      for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
        for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
          for (int tabletNum = 0; tabletNum < tabletCnt; tabletNum++) {
            fillTreeTablet(tablet, tabletNum, deviceNum, tabletNum);
            startTime = System.nanoTime();
            tsFileWriter.write(tablet);
            writeTimeSum += System.nanoTime() - startTime;
          }
        }
      }
    }

    System.out.printf("Tree register {}ns, ");
  }

  private TsFileWriter initTsFileWriter() throws IOException {
    File dir = new File(testDir);
    dir.mkdirs();
    File tsFile = new File(dir, "testTsFile");
    return new TsFileWriter(tsFile);
  }

  private Tablet initTreeTablet() {
    return new Tablet(null, measurementSchemas, pointPerSeries);
  }

  private void fillTreeTablet(Tablet tablet, int tableNum, int deviceNum, int tabletNum) {
    tablet.insertTargetName = genDeviceId(tableNum, deviceNum).toString();
    for (int i = 0; i < measurementSchemaCnt; i++) {
      long[] values = (long[]) tablet.values[i];
      for (int valNum = 0; valNum < pointPerSeries; valNum++) {
        values[valNum] = (long) tabletNum * pointPerSeries + valNum;
      }
    }
    for (int valNum = 0; valNum < pointPerSeries; valNum++) {
      tablet.timestamps[valNum] = (long) tabletNum * pointPerSeries + valNum;
    }
  }

  private Tablet initTableTablet() {
    List<IMeasurementSchema> allSchema = new ArrayList<>(idSchemas);
    allSchema.addAll(measurementSchemas);
    return new Tablet(null, allSchema, pointPerSeries);
  }

  private void fillTableTablet(Tablet tablet, int tableNum, int deviceNum, int tabletNum) {
    IDeviceID deviceID = genDeviceId(tableNum, deviceNum);
    tablet.insertTargetName = deviceID.segment(0).toString();
    for (int i = 0; i < idSchemaCnt; i++) {
      Binary[] binaries = ((Binary[]) tablet.values[i]);
      for (int rowNum = 0; rowNum < pointPerSeries; rowNum++) {
        binaries[rowNum] = new Binary(deviceID.segment(i + 1).toString().getBytes());
      }
    }
    for (int i = 0; i < measurementSchemaCnt; i++) {
      long[] values = (long[]) tablet.values[i + idSchemaCnt];
      for (int valNum = 0; valNum < pointPerSeries; valNum++) {
        values[valNum] = (long) tabletNum * pointPerSeries + valNum;
      }
    }
    for (int valNum = 0; valNum < pointPerSeries; valNum++) {
      tablet.timestamps[valNum] = (long) tabletNum * pointPerSeries + valNum;
    }
  }

  private void registerTree(TsFileWriter writer) throws WriteProcessException {
    for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
      for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
        for (int measurementNum = 0; measurementNum < measurementSchemaCnt; measurementNum++) {
          writer.registerTimeseries(genDeviceId(tableNum, deviceNum), genMeasurementSchema(measurementNum));
        }
      }
    }
  }

  private IMeasurementSchema genMeasurementSchema(int measurementNum) {
    if (measurementSchemas == null) {
      measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementSchemaCnt; i++) {
        measurementSchemas.add(new MeasurementSchema(
            "s" + measurementNum, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4));
      }
    }

    return measurementSchemas.get(measurementNum);
  }

  private IMeasurementSchema genIdSchema(int idNum) {
    if (idSchemas == null) {
      idSchemas = new ArrayList<>(idSchemaCnt);
      for (int i = 0; i < idSchemaCnt; i++) {
        idSchemas.add(new MeasurementSchema(
            "id" + i, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      }
    }
    return idSchemas.get(idNum);
  }

  private String genTableName(int tableNum) {
    return "table_" + tableNum;
  }

  private IDeviceID genDeviceId(int tableNum, int deviceNum) {
    String[] idSegments = new String[idSchemaCnt + 1];
    idSegments[0] = genTableName(tableNum);
    for (int i = 0; i < idSchemaCnt - 1; i++) {
      idSegments[i + 1] = "0";
    }
    idSegments[idSchemaCnt - 1] = Integer.toString(deviceNum);
    return new StringArrayDeviceID(idSegments);
  }

  private void registerTable(TsFileWriter writer) {
    for (int i = 0; i < tableCnt; i++) {
      TableSchema tableSchema = genTableSchema(i);
      writer.registerTableSchema(tableSchema);
    }
  }

  private TableSchema genTableSchema(int tableNum) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnType> columnTypes = new ArrayList<>();

    for (int i = 0; i < idSchemaCnt; i++) {
      measurementSchemas.add(genIdSchema(i));
      columnTypes.add(ColumnType.ID);
    }
    for (int i = 0; i < measurementSchemaCnt; i++) {
      measurementSchemas.add(genMeasurementSchema(i));
      columnTypes.add(ColumnType.MEASUREMENT);
    }
    return new TableSchema("testTable" + tableNum, measurementSchemas, columnTypes);
  }

}
