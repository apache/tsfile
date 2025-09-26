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

import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileReaderEmptyChunkTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileReaderEmptyChunkTest.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH).delete();
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(
      final TsFileIOWriter writer,
      final List<String> measurementNames,
      final TimeRange[] toGenerateChunkTimeRanges,
      final int emptyChunkIndex)
      throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(
          new MeasurementSchema(
              measurementName, TSDataType.INT64, TSEncoding.RLE, CompressionType.LZ4));
    }

    for (TimeRange toGenerateChunk : toGenerateChunkTimeRanges) {
      final AlignedChunkWriterImpl alignedChunkWriter =
          new AlignedChunkWriterImpl(measurementSchemas);
      for (long time = toGenerateChunk.getMin(); time <= toGenerateChunk.getMax(); time++) {
        alignedChunkWriter.getTimeChunkWriter().write(time);
        for (int i = 0; i < measurementNames.size(); i++) {
          if (i == emptyChunkIndex) {
            continue;
          }

          alignedChunkWriter.getValueChunkWriterByIndex(i).write(time, time, false);
        }
      }
      alignedChunkWriter.writeToFileWriter(writer);
      writer.writeEmptyValueChunk(
          measurementNames.get(emptyChunkIndex),
          CompressionType.LZ4,
          TSDataType.INT64,
          TSEncoding.PLAIN,
          new LongStatistics());
    }
  }
}
