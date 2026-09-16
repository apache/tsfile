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
package org.apache.tsfile.write.chunk;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.PlainDecoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class AlignedNullWriteTest {
  @Test
  public void lateColumnsAndMissingRowsKeepPageBoundaries() throws Exception {
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int oldLimit = config.getMaxNumberOfPointsInPage();
    try {
      config.setMaxNumberOfPointsInPage(8);
      AlignedChunkGroupWriterImpl group =
          new AlignedChunkGroupWriterImpl(IDeviceID.Factory.DEFAULT_FACTORY.create("root.nulls"));
      for (int i = 0; i < 11; i++) {
        group.write(i, Collections.emptyList());
      }
      TSDataType[] types = {
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.DATE,
        TSDataType.INT64,
        TSDataType.TIMESTAMP,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.OBJECT
      };
      // Each newly added column must catch up one sealed page and three rows of the current page.
      for (TSDataType type : types) {
        ValueChunkWriter writer =
            group.tryToAddSeriesWriterInternal(
                new MeasurementSchema(
                    type.name(), type, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        assertEquals(1, writer.getNumOfPages());
        assertEquals(3, writer.getPageWriter().getSize());
        assertEquals(0, writer.getPageWriter().getStatistics().getCount());
      }
      for (int i = 11; i < 16; i++) {
        group.write(i, Collections.emptyList());
      }
      assertEquals(2, group.timeChunkWriter.getNumOfPages());
      for (ValueChunkWriter writer : group.valueChunkWriterMap.values()) {
        assertEquals(2, writer.getNumOfPages());
        assertEquals(0, writer.getPageWriter().getSize());
      }
      group.write(16, Collections.singletonList(new IntDataPoint("INT32", 42)));
      for (ValueChunkWriter writer : group.valueChunkWriterMap.values()) {
        assertEquals(1, writer.getPageWriter().getSize());
        boolean present = writer.getDataType() == TSDataType.INT32;
        assertEquals(present ? 1 : 0, writer.getPageWriter().getStatistics().getCount());
        ByteBuffer data = writer.getPageWriter().getUncompressedBytes();
        assertEquals(1, data.getInt());
        assertEquals(present ? (byte) 0x80 : 0, data.get());
        if (present) {
          assertEquals(42, new PlainDecoder().readInt(data));
        }
        assertEquals(0, data.remaining());
      }
    } finally {
      config.setMaxNumberOfPointsInPage(oldLimit);
    }
  }
}
