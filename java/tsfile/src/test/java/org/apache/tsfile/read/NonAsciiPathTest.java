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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Device and measurement names containing multi-byte UTF-8 characters take part in the persisted
 * bloom filter positions, so they need explicit coverage.
 */
public class NonAsciiPathTest {

  private static final String DEVICE = "root.测试.设备1";
  private static final String TEMPERATURE = "温度";
  private static final String HUMIDITY = "湿度";
  private static final int ROWS = 20;

  @Test
  public void treeModelReadWrite() throws Exception {
    File file = File.createTempFile("non_ascii_tree", ".tsfile");
    Files.deleteIfExists(file.toPath());
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                TEMPERATURE, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.LZ4),
            new MeasurementSchema(
                HUMIDITY, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.LZ4));
    try {
      try (TsFileWriter writer = new TsFileWriter(file)) {
        writer.registerTimeseries(new Path(DEVICE), schemas);
        Tablet tablet = new Tablet(DEVICE, schemas);
        for (int i = 0; i < ROWS; i++) {
          tablet.addTimestamp(i, 1000L + i);
          tablet.addValue(i, 0, 20.0 + i);
          tablet.addValue(i, 1, 50.0 + i);
        }
        writer.writeTree(tablet);
      }

      try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(file.getPath());
          TsFileReader reader = new TsFileReader(sequenceReader)) {
        Assert.assertEquals(1, sequenceReader.getAllDevices().size());

        List<Path> paths = new ArrayList<>();
        paths.add(new Path(DEVICE, TEMPERATURE, true));
        paths.add(new Path(DEVICE, HUMIDITY, true));
        try (QueryDataSet dataSet = reader.query(QueryExpression.create(paths, null))) {
          int rowIndex = 0;
          while (dataSet.hasNext()) {
            RowRecord row = dataSet.next();
            List<Field> fields = row.getFields();
            Assert.assertEquals(1000L + rowIndex, row.getTimestamp());
            Assert.assertEquals(20.0 + rowIndex, fields.get(0).getDoubleV(), 1e-9);
            Assert.assertEquals(50.0 + rowIndex, fields.get(1).getDoubleV(), 1e-9);
            rowIndex++;
          }
          Assert.assertEquals(ROWS, rowIndex);
        }
      }
    } finally {
      Files.deleteIfExists(file.toPath());
    }
  }
}
