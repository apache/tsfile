/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.v4;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileTreeReader;
import org.apache.tsfile.read.v4.TsFileTreeReaderBuilder;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.v4.TsFileTreeWriterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Example for writing and reading TsFile in tree model.
 *
 * <p>Demonstrates: - Defining schemas for multiple devices - Writing data using both Tablet and
 * TSRecord - Reading back with TsFileTreeReader - Retrieving device IDs and schema info - Using
 * ResultSet and ResultSetMetadata
 */
public class TsFileTreeReaderExample {

  private static final String FILE_PATH = "tree_model_example.tsfile";

  public static void main(String[] args) throws IOException {
    File file = new File(FILE_PATH);
    if (file.exists()) {
      file.delete();
    }

    // Step 1. Generate TsFile with tree writer
    generateTestTsFile(file);

    List<String> deviceIds = Arrays.asList("d1", "d2");
    List<String> measurements = Arrays.asList("s1", "s2");

    // Step 2. Build tree reader
    try (ITsFileTreeReader treeReader = new TsFileTreeReaderBuilder().file(file).build()) {

      // === Basic query by devices and measurements ===

      try (ResultSet resultSet =
          treeReader.query(deviceIds, measurements, Long.MIN_VALUE, Long.MAX_VALUE)) {

        System.out.println("=== Query results ===");
        while (resultSet.next()) {
          long time = resultSet.getLong(1);
          Integer d1s1 = resultSet.isNull("d1.s1") ? null : resultSet.getInt("d1.s1");
          Long d1s2 = resultSet.isNull("d1.s2") ? null : resultSet.getLong("d1.s2");
          Integer d2s1 = resultSet.isNull("d2.s1") ? null : resultSet.getInt("d2.s1");

          System.out.printf("time=%d, d1.s1=%s, d1.s2=%s, d2.s1=%s%n", time, d1s1, d1s2, d2s1);
        }
      }
      // === Retrieve all device IDs ===
      List<String> allDeviceIds = treeReader.getAllDeviceIds();
      System.out.println("=== All device IDs === " + allDeviceIds);

      // === Retrieve schema of device d1 ===
      List<MeasurementSchema> schemas = treeReader.getDeviceSchema("d1");
      List<String> measurementNames =
          schemas.stream().map(IMeasurementSchema::getMeasurementName).collect(Collectors.toList());
      System.out.println("=== Schema of d1 === " + measurementNames);

      // === ResultSet metadata ===
      try (ResultSet resultSet =
          treeReader.query(deviceIds, measurements, Long.MIN_VALUE, Long.MAX_VALUE)) {
        ResultSetMetadata metadata = resultSet.getMetadata();
        System.out.println("=== Metadata columns ===");
        for (int i = 1; i <= 4; i++) {
          System.out.printf("Column %d: %s%n", i, metadata.getColumnName(i));
        }
      }
    }

    // === Iterator-based query example ===
    try (ITsFileTreeReader treeReader = new TsFileTreeReaderBuilder().file(file).build()) {

      long startTime = Long.MIN_VALUE;
      long endTime = Long.MAX_VALUE;

      try (ResultSet resultSet = treeReader.query(deviceIds, measurements, startTime, endTime)) {
        System.out.println("=== Iterator-based query results ===");
        Iterator<TSRecord> recordIterator = resultSet.iterator();
        int countVal = 0;

        while (recordIterator.hasNext()) {
          TSRecord record = recordIterator.next();
          System.out.println(
              "Device: " + record.deviceId.toString() + ", Timestamp: " + record.time);

          if (record.deviceId.toString().equals("d1")) {
            System.out.println(
                "  Measurement at index 1: "
                    + record.dataPointList.get(1).getMeasurementId()
                    + " = "
                    + record.dataPointList.get(1).getValue());
            System.out.println(
                "  Measurement at index 2: "
                    + record.dataPointList.get(2).getMeasurementId()
                    + " = "
                    + record.dataPointList.get(2).getValue());
          } else if (record.deviceId.toString().equals("d2")) {
            System.out.println(
                "  Measurement at index 1: "
                    + record.dataPointList.get(1).getMeasurementId()
                    + " = "
                    + record.dataPointList.get(1).getValue());
            System.out.println("  Measurement at index 2 is null");
          } else {
            System.out.println("  Unexpected deviceId: " + record.deviceId.toString());
          }

          countVal++;
        }

        System.out.println("Total records iterated: " + countVal);
      }
    }
    new File(FILE_PATH).delete();
  }

  private static void generateTestTsFile(File file) throws IOException {
    try (TsFileTreeWriter writer = new TsFileTreeWriterBuilder().file(file).build()) {
      // Define schemas
      IMeasurementSchema schema1 = new MeasurementSchema("s1", TSDataType.INT32);
      IMeasurementSchema schema2 = new MeasurementSchema("s2", TSDataType.INT64);

      // Register schemas for devices
      writer.registerTimeseries("d1", schema1);
      writer.registerTimeseries("d1", schema2);
      writer.registerTimeseries("d2", schema1);

      // Write data for device d1 with Tablet
      Tablet d1Tablet = new Tablet("d1", Arrays.asList(schema1, schema2));
      for (int i = 0; i < 5; i++) {
        d1Tablet.addTimestamp(i, i);
        d1Tablet.addValue(i, 0, i); // s1
        d1Tablet.addValue(i, 1, (long) i); // s2
      }
      writer.write(d1Tablet);

      // Write data for device d2 with TSRecord
      for (int i = 0; i < 5; i++) {
        TSRecord record = new TSRecord("d2", i);
        record.addPoint("s1", i);
        writer.write(record);
      }
    } catch (WriteProcessException e) {
      throw new RuntimeException(e);
    }
  }
}
