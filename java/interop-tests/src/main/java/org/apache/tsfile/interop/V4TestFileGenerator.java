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

package org.apache.tsfile.interop;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * Generates V4 format test files for C# interoperability testing. Creates simple test files that
 * can be read by C# implementation.
 */
public class V4TestFileGenerator {

  public static void main(String[] args) {
    try {
      String outputDir = args.length > 0 ? args[0] : "/tmp/v4-interop-test";
      File dir = new File(outputDir);
      if (!dir.exists()) {
        dir.mkdirs();
      }

      // Generate simple test file
      generateSimpleV4File(outputDir);

      // Generate test file with multiple devices
      generateMultiDeviceV4File(outputDir);

      System.out.println("Successfully generated V4 test files in: " + outputDir);
    } catch (Exception e) {
      System.err.println("Error generating test files: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void generateSimpleV4File(String outputDir) throws Exception {
    String path = outputDir + "/simple_v4.tsfile";
    File f = new File(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }

    String tableName = "sensor_data";

    TableSchema tableSchema =
        new TableSchema(
            tableName,
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("region")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("device")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("temperature")
                    .dataType(TSDataType.DOUBLE)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("humidity")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));

    try (ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(tableSchema)
            .memoryThreshold(1024 * 1024)
            .build()) {

      Tablet tablet =
          new Tablet(
              Arrays.asList("region", "device", "temperature", "humidity"),
              Arrays.asList(
                  TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT32));

      // Add data for device 1
      for (int row = 0; row < 10; row++) {
        long timestamp = row * 1000L;
        tablet.addTimestamp(row, timestamp);
        tablet.addValue(row, "region", "Beijing");
        tablet.addValue(row, "device", "D1");
        tablet.addValue(row, "temperature", 25.0 + row * 0.5);
        tablet.addValue(row, "humidity", 60 + row);
      }

      writer.write(tablet);
    }

    System.out.println("Generated: " + path);
  }

  private static void generateMultiDeviceV4File(String outputDir) throws Exception {
    String path = outputDir + "/multi_device_v4.tsfile";
    File f = new File(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }

    String tableName = "iot_data";

    TableSchema tableSchema =
        new TableSchema(
            tableName,
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("factory")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("line")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("machine")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("speed")
                    .dataType(TSDataType.INT64)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("power")
                    .dataType(TSDataType.FLOAT)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("status")
                    .dataType(TSDataType.BOOLEAN)
                    .category(ColumnCategory.FIELD)
                    .build()));

    try (ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(tableSchema)
            .memoryThreshold(1024 * 1024)
            .build()) {

      Tablet tablet =
          new Tablet(
              Arrays.asList("factory", "line", "machine", "speed", "power", "status"),
              Arrays.asList(
                  TSDataType.STRING,
                  TSDataType.STRING,
                  TSDataType.STRING,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.BOOLEAN));

      // Add data for multiple devices
      String[] factories = {"F1", "F2"};
      String[] lines = {"L1", "L2"};
      String[] machines = {"M1", "M2", "M3"};

      int rowIndex = 0;
      for (String factory : factories) {
        for (String line : lines) {
          for (String machine : machines) {
            for (int i = 0; i < 5; i++) {
              long timestamp = rowIndex * 100L;
              tablet.addTimestamp(rowIndex, timestamp);
              tablet.addValue(rowIndex, "factory", factory);
              tablet.addValue(rowIndex, "line", line);
              tablet.addValue(rowIndex, "machine", machine);
              tablet.addValue(rowIndex, "speed", 1000L + rowIndex * 10);
              tablet.addValue(rowIndex, "power", 100.0f + rowIndex * 0.5f);
              tablet.addValue(rowIndex, "status", rowIndex % 2 == 0);
              rowIndex++;
            }
          }
        }
      }

      writer.write(tablet);
    }

    System.out.println("Generated: " + path);
  }
}
