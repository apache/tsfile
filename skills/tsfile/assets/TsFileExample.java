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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.v4.TsFileTreeWriterBuilder;

import java.io.File;
import java.nio.file.Files;

public class TsFileExample {
  public static void main(String[] args) throws Exception {
    File file = new File("example.tsfile");
    Files.deleteIfExists(file.toPath());

    String deviceId = "sensor_01";
    try (TsFileTreeWriter writer = new TsFileTreeWriterBuilder().file(file).build()) {
      writer.registerTimeseries(
          deviceId, new MeasurementSchema("temperature", TSDataType.FLOAT));
      writer.registerTimeseries(deviceId, new MeasurementSchema("humidity", TSDataType.FLOAT));
      writer.registerTimeseries(deviceId, new MeasurementSchema("pressure", TSDataType.DOUBLE));

      for (int i = 0; i < 100; i++) {
        long timestamp = System.currentTimeMillis() + i * 1000L;
        TSRecord record =
            new TSRecord(deviceId, timestamp)
                .addPoint("temperature", 20.0f + i * 0.1f)
                .addPoint("humidity", 50.0f + i * 0.5f)
                .addPoint("pressure", 1013.25 + i * 0.01);
        writer.write(record);
      }
    }

    System.out.println("TsFile written successfully: " + file.getAbsolutePath());
  }
}
