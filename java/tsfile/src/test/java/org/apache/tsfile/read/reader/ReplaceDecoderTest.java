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
import org.apache.tsfile.encoding.decoder.DecoderWrapper;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ReplaceDecoderTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("ReplaceDecoder.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH).delete();
  }

  @Test
  public void testNonAligned() throws IOException, WriteProcessException {
    IDeviceID deviceID = new StringArrayDeviceID("root.test.d1");
    try (TsFileWriter writer = new TsFileWriter(new File(FILE_PATH))) {
      writer.registerTimeseries(deviceID, new MeasurementSchema("s1", TSDataType.INT32));
      Tablet tablet =
          new Tablet(
              deviceID.toString(),
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)));
      for (int i = 0; i < 10; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(i, 0, i);
      }
      writer.writeTree(tablet);
      writer.flush();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      TimeseriesMetadata timeseriesMetadata = reader.getDeviceTimeseriesMetadata(deviceID).get(0);
      for (IChunkMetadata iChunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
        Chunk chunk = reader.readMemChunk((ChunkMetadata) iChunkMetadata);
        chunk
            .getHeader()
            .setReplaceDecoder(
                decoder ->
                    new DecoderWrapper(decoder) {
                      @Override
                      public int readInt(ByteBuffer buffer) {
                        return decoder.readInt(buffer) + 10;
                      }
                    });
        ChunkReader chunkReader = new ChunkReader(chunk);
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          IPointReader pointReader = batchData.getBatchDataIterator();
          while (pointReader.hasNextTimeValuePair()) {
            TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
            Assert.assertEquals(
                (int) timeValuePair.getTimestamp(), (Integer) timeValuePair.getValues()[0] - 10);
          }
        }
      }
    }
  }

  @Test
  public void testAligned() throws IOException, WriteProcessException {
    IDeviceID deviceID = new StringArrayDeviceID("root.test.d1");
    try (TsFileWriter writer = new TsFileWriter(new File(FILE_PATH))) {
      writer.registerAlignedTimeseries(
          deviceID, Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)));
      Tablet tablet =
          new Tablet(
              deviceID.toString(),
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)));
      for (int i = 0; i < 10; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(i, 0, i);
      }
      writer.writeTree(tablet);
      writer.flush();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, true);

      for (AbstractAlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        Chunk timeChunk =
            reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
        Chunk valueChunk =
            reader.readMemChunk(
                (ChunkMetadata) alignedChunkMetadata.getValueChunkMetadataList().get(0));
        valueChunk
            .getHeader()
            .setReplaceDecoder(
                decoder ->
                    new DecoderWrapper(decoder) {
                      @Override
                      public int readInt(ByteBuffer buffer) {
                        return decoder.readInt(buffer) + 10;
                      }
                    });
        AlignedChunkReader chunkReader =
            new AlignedChunkReader(timeChunk, Collections.singletonList(valueChunk));
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          IPointReader pointReader = batchData.getBatchDataIterator();
          while (pointReader.hasNextTimeValuePair()) {
            TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
            Assert.assertEquals(
                (int) timeValuePair.getTimestamp(), (Integer) timeValuePair.getValues()[0] - 10);
          }
        }
      }
    }
  }
}
