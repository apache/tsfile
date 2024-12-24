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

package org.apache.tsfile.v4;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.StringJoiner;

public class ITsFileReaderAndITsFileWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ITsFileReaderAndITsFileWriter.class);

  public static void main(String[] args) throws IOException {
    String path = "test.tsfile";
    File f = FSFactoryProducer.getFSFactory().getFile(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }

    String tableName = "table1";

    TableSchema tableSchema =
        new TableSchema(
            tableName,
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("tag1")
                    .dataType(TSDataType.STRING)
                    .category(Tablet.ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("tag2")
                    .dataType(TSDataType.STRING)
                    .category(Tablet.ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(Tablet.ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder().name("s2").dataType(TSDataType.BOOLEAN).build()));

    Tablet tablet =
        new Tablet(
            Arrays.asList("tag1", "tag2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN));
    for (int row = 0; row < 5; row++) {
      long timestamp = row;
      tablet.addTimestamp(row, timestamp);
      tablet.addValue(row, "tag1", "tag1_value_1");
      tablet.addValue(row, "tag2", "tag2_value_1");
      tablet.addValue(row, "s1", row);
      // null value
      // tablet.addValue(row, "s2", true);
    }
    for (int row = 5; row < 10; row++) {
      long timestamp = row;
      tablet.addTimestamp(row, timestamp);

      // tag1 column
      tablet.addValue(row, 0, "tag1_value_2");

      // tag2 column
      tablet.addValue(row, 1, "tag1_value_2");

      // s1 column: null value
      // tablet.addValue(row, 2, row);

      // s2 column
      tablet.addValue(row, 3, false);
    }

    long memoryThreshold = 10 * 1024 * 1024;
    // tableSchema and file are required. memoryThreshold is an optional parameter, default value is
    // 32 * 1024 * 1024 byte.
    try (ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(tableSchema)
            .memoryThreshold(memoryThreshold)
            .build()) {
      writer.write(tablet);
    } catch (WriteProcessException e) {
      LOGGER.error("meet error in TsFileWrite ", e);
    }

    // file is a required parameter
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet resultSet =
            reader.query(tableName, Arrays.asList("tag1", "tag2", "s1", "s2"), 2, 8)) {
      // first column is Time
      ResultSetMetadata metadata = resultSet.getMetadata();
      System.out.println(metadata);
      StringJoiner sj = new StringJoiner(" ");
      for (int column = 1; column <= 5; column++) {
        sj.add(metadata.getColumnName(column) + "(" + metadata.getColumnType(column) + ") ");
      }
      System.out.println(sj.toString());
      while (resultSet.next()) {
        // columnIndex starts from 1
        // Time tag1 tag2 s1 s2
        Long timeField = resultSet.getLong("Time");
        String tag1 = resultSet.isNull("tag1") ? null : resultSet.getString("tag1");
        String tag2 = resultSet.isNull("tag2") ? null : resultSet.getString("tag2");
        Integer s1Field = resultSet.isNull("s1") ? null : resultSet.getInt(4);
        Boolean s2Field = resultSet.isNull("s2") ? null : resultSet.getBoolean(5);
        sj = new StringJoiner(" ");
        System.out.println(
            sj.add(timeField + "")
                .add(tag1)
                .add(tag2)
                .add(s1Field + "")
                .add(s2Field + "")
                .toString());
      }
    } catch (Exception e) {
      LOGGER.error("meet error in TsFileRead ", e);
    }
  }
}
