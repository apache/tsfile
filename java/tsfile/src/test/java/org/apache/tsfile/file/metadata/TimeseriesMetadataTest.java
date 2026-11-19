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
package org.apache.tsfile.file.metadata;

import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.utils.TestHelper;
import org.apache.tsfile.read.reader.LocalTsFileInput;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.PublicBAOS;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

public class TimeseriesMetadataTest {

  public static final String measurementUID = "sensor01";
  public static final int typeLength = 1024;
  final String PATH = TestConstant.BASE_OUTPUT_PATH.concat("outputTimeSeries.tsfile");

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testWriteIntoFile() {
    TimeseriesMetadata timeseriesMetadata =
        TestHelper.createSimpleTimeseriesMetaData(measurementUID);
    serialized(timeseriesMetadata);
    TimeseriesMetadata readMetadata = deSerialized();
    Assert.assertEquals(timeseriesMetadata.toString(), readMetadata.toString());
    readMetadata = deSerializedByTsFileInput();
    Assert.assertEquals(timeseriesMetadata.toString(), readMetadata.toString());
    serialized(readMetadata);
  }

  private TimeseriesMetadata deSerialized() {
    FileInputStream fis = null;
    TimeseriesMetadata metaData = null;
    try {
      fis = new FileInputStream(PATH);
      FileChannel fch = fis.getChannel();
      ByteBuffer buffer = ByteBuffer.allocate((int) fch.size());
      fch.read(buffer);
      buffer.flip();
      metaData = TimeseriesMetadata.deserializeFrom(buffer, true);
      metaData.setChunkMetadataListBuffer(new PublicBAOS());
      return metaData;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return metaData;
  }

  private TimeseriesMetadata deSerializedByTsFileInput() {
    TsFileInput tsFileInput = null;
    TimeseriesMetadata metaData = null;
    try {
      tsFileInput = new LocalTsFileInput(Paths.get(PATH));
      metaData = TimeseriesMetadata.deserializeFrom(tsFileInput, true);
      metaData.setChunkMetadataListBuffer(new PublicBAOS());
      return metaData;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (tsFileInput != null) {
        try {
          tsFileInput.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return metaData;
  }

  private void serialized(TimeseriesMetadata metaData) {
    File file = new File(PATH);
    if (file.exists()) {
      file.delete();
    }
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      metaData.serializeTo(fos);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testStreamingDeserializerSkipsRejectedChunkList() throws IOException {
    File file = new File(TestConstant.BASE_OUTPUT_PATH.concat("streaming-predicate.tsfile"));
    file.delete();
    try {
      long firstChunkOffset = 1024L;
      long secondChunkOffset = 2048L;
      TimeseriesMetadata first = metadataWithOneChunk("s1", firstChunkOffset, 3);
      TimeseriesMetadata second = metadataWithOneChunk("s2", secondChunkOffset, 5);
      Assert.assertTrue(first.getDataSizeOfChunkMetaDataList() > 0);
      int firstLength;
      try (FileOutputStream out = new FileOutputStream(file)) {
        firstLength = first.serializeTo(out);
        second.serializeTo(out);
      }
      TsFileInput input = new LocalTsFileInput(Paths.get(file.getPath()));
      try {
        TimeseriesMetadata rejected =
            TimeseriesMetadata.deserializeFrom(input, name -> false, true, true);
        Assert.assertEquals("s1", rejected.getMeasurementId());
        // the rejected series' chunk list is skipped, not decoded
        Assert.assertNull(rejected.getChunkMetadataList());
        // the input advanced exactly to the next record
        Assert.assertEquals(firstLength, input.position());

        TimeseriesMetadata accepted =
            TimeseriesMetadata.deserializeFrom(input, name -> true, true, true);
        Assert.assertEquals("s2", accepted.getMeasurementId());
        Assert.assertNotNull(accepted.getChunkMetadataList());
        Assert.assertEquals(1, accepted.getChunkMetadataList().size());
        ChunkMetadata chunk = (ChunkMetadata) accepted.getChunkMetadataList().get(0);
        Assert.assertEquals(secondChunkOffset, chunk.getOffsetOfChunkHeader());
        Assert.assertEquals(5, chunk.getStatistics().getCount());
        Assert.assertEquals(0L, chunk.getStatistics().getStartTime());
        Assert.assertEquals(4L, chunk.getStatistics().getEndTime());
      } finally {
        input.close();
      }
    } finally {
      file.delete();
    }
  }

  private static TimeseriesMetadata metadataWithOneChunk(
      String measurementId, long chunkOffset, int chunkCount) throws IOException {
    Statistics<? extends Serializable> seriesStatistics =
        Statistics.getStatsByType(TSDataType.INT64);
    seriesStatistics.setCount(chunkCount);
    seriesStatistics.setStartTime(0);
    seriesStatistics.setEndTime(chunkCount - 1);
    Statistics<? extends Serializable> chunkStatistics =
        Statistics.getStatsByType(TSDataType.INT64);
    chunkStatistics.setCount(chunkCount);
    chunkStatistics.setStartTime(0);
    chunkStatistics.setEndTime(chunkCount - 1);
    ChunkMetadata chunk =
        new ChunkMetadata(
            measurementId,
            TSDataType.INT64,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            chunkOffset,
            chunkStatistics);
    PublicBAOS chunkBuffer = new PublicBAOS();
    chunk.serializeTo(chunkBuffer, true);
    return new TimeseriesMetadata(
        (byte) 1,
        chunkBuffer.size(),
        measurementId,
        TSDataType.INT64,
        seriesStatistics,
        chunkBuffer);
  }
}
