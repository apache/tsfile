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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.util.Arrays;

/** Generates aligned tree fixtures for the Python sequence training tests. */
public class TreeDatasetFixtureGenerator {
  public static void main(String[] args) throws Exception {
    TSFileDescriptor.getInstance()
        .getConfig()
        .setTimeEncoder(args.length > 1 ? args[1] : "PLAIN");
    TSFileDescriptor.getInstance().getConfig().setCompressor("UNCOMPRESSED");
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(2);
    try (TsFileWriter writer = new TsFileWriter(new File(args[0]))) {
      writer.registerAlignedTimeseries(
          new Path("root.training.d1"),
          Arrays.asList(
              schema("temperature", TSDataType.DOUBLE), schema("status", TSDataType.INT32)));
      writer.registerAlignedTimeseries(
          new Path("root.training.d2.deep"),
          Arrays.asList(
              schema("temperature", TSDataType.INT32), schema("status", TSDataType.BOOLEAN)));
      for (int t = 0; t < 6; t++) {
        TSRecord first = new TSRecord("root.training.d1", t).addPoint("status", t);
        if (t != 2) {
          first.addPoint("temperature", t + 0.25);
        }
        writer.writeRecord(first);
        TSRecord second =
            new TSRecord("root.training.d2.deep", t).addPoint("status", t % 2 == 0);
        if (t != 3) {
          second.addPoint("temperature", t * 10 + 100);
        }
        writer.writeRecord(second);
      }
    }
  }

  private static MeasurementSchema schema(String name, TSDataType type) {
    return new MeasurementSchema(name, type, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
  }
}
