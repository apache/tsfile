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
package org.apache.tsfile.write;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.chunk.TableChunkReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.TimeChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ChunkRewriteTest {

  @Test
  public void AlignedChunkSinglePageTest() throws IOException {
    String[] measurements = new String[] {"s1", "s2", "s3"};
    TSDataType[] types = new TSDataType[] {TSDataType.FLOAT, TSDataType.INT32, TSDataType.DOUBLE};
    VectorMeasurementSchema measurementSchema =
        new VectorMeasurementSchema("root.sg.d1", measurements, types);
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();

    TimeChunkWriter timeChunkWriter = chunkWriter.getTimeChunkWriter();
    List<ValueChunkWriter> valueChunkWriters = chunkWriter.getValueChunkWriterList();

    Chunk timeChunk = getTimeChunk(measurementSchema, timeChunkWriter);

    List<Chunk> valueChunks = getValueChunks(valueChunkWriters);

    AlignedChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks);
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    for (IPageReader page : pageReaders) {
      IPointReader pointReader = ((AlignedPageReader) page).getLazyPointReader();
      int i = 1;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals((float) i, point.getValue().getVector()[0].getValue());
        assertEquals(i, point.getValue().getVector()[1].getValue());
        assertEquals((double) i, point.getValue().getVector()[2].getValue());
        i++;
      }
    }
    timeChunk.getData().flip();
    valueChunks.get(0).getData().flip();
    valueChunks.get(1).getData().flip();
    valueChunks.get(2).getData().flip();
    // rewrite INT32->DOUBLE
    Chunk newValueChunk = valueChunks.get(1).rewrite(TSDataType.DOUBLE, timeChunk);
    valueChunks.set(1, newValueChunk);
    AlignedChunkReader newChunkReader = new AlignedChunkReader(timeChunk, valueChunks);
    List<IPageReader> newPageReaders = newChunkReader.loadPageReaderList();
    for (IPageReader page : newPageReaders) {
      IPointReader pointReader = ((AlignedPageReader) page).getLazyPointReader();
      int i = 1;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals((float) i, point.getValue().getVector()[0].getValue());
        assertEquals((double) i, point.getValue().getVector()[1].getValue());
        assertEquals((double) i, point.getValue().getVector()[2].getValue());
        i++;
      }
      assertEquals(20, i - 1);
    }
    timeChunk.getData().flip();
    valueChunks.get(0).getData().flip();
    valueChunks.get(1).getData().flip();
    valueChunks.get(2).getData().flip();

    //

  }

  @Test
  public void AlignedChunkMultiPagesTest() throws IOException {
    String[] measurements = new String[] {"s1", "s2", "s3"};
    TSDataType[] types = new TSDataType[] {TSDataType.FLOAT, TSDataType.INT32, TSDataType.DOUBLE};
    VectorMeasurementSchema measurementSchema =
        new VectorMeasurementSchema("root.sg.d1", measurements, types);
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();

    for (int time = 21; time <= 40; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();

    TimeChunkWriter timeChunkWriter = chunkWriter.getTimeChunkWriter();
    List<ValueChunkWriter> valueChunkWriters = chunkWriter.getValueChunkWriterList();

    Chunk timeChunk = getTimeChunk(measurementSchema, timeChunkWriter);
    List<Chunk> valueChunks = getValueChunks(valueChunkWriters);

    AlignedChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks);
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    int i = 1;
    for (IPageReader page : pageReaders) {
      IPointReader pointReader = ((AlignedPageReader) page).getLazyPointReader();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals((float) i, point.getValue().getVector()[0].getValue());
        assertEquals(i, point.getValue().getVector()[1].getValue());
        assertEquals((double) i, point.getValue().getVector()[2].getValue());
        i++;
      }
    }
    assertEquals(40, i - 1);
    timeChunk.getData().flip();
    valueChunks.get(0).getData().flip();
    valueChunks.get(1).getData().flip();
    valueChunks.get(2).getData().flip();
    // rewrite INT32->DOUBLE
    Chunk newValueChunk = valueChunks.get(1).rewrite(TSDataType.DOUBLE, timeChunk);
    valueChunks.set(1, newValueChunk);
    AlignedChunkReader newChunkReader = new AlignedChunkReader(timeChunk, valueChunks);
    i = 1;
    List<IPageReader> newPageReaders = newChunkReader.loadPageReaderList();
    for (IPageReader page : newPageReaders) {
      IPointReader pointReader = ((AlignedPageReader) page).getLazyPointReader();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals((float) i, point.getValue().getVector()[0].getValue());
        assertEquals((double) i, point.getValue().getVector()[1].getValue());
        assertEquals((double) i, point.getValue().getVector()[2].getValue());
        i++;
      }
    }
    assertEquals(40, i - 1);
    timeChunk.getData().flip();
    valueChunks.get(0).getData().flip();
    valueChunks.get(1).getData().flip();
    valueChunks.get(2).getData().flip();
  }

  @Test
  public void AlignedChunkWithNullTest() throws IOException {
    String[] measurements = new String[] {"s1", "s2", "s3"};
    TSDataType[] types = new TSDataType[] {TSDataType.FLOAT, TSDataType.INT32, TSDataType.DOUBLE};
    VectorMeasurementSchema measurementSchema =
        new VectorMeasurementSchema("root.sg.d1", measurements, types);
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 30; time = time + 3) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);

      chunkWriter.write(time + 1, (float) (time + 1), true);
      chunkWriter.write(time + 1, time + 1, false);
      chunkWriter.write(time + 1, (double) (time + 1), true);
      chunkWriter.write(time + 1);

      chunkWriter.write(time + 2, (float) (time + 1), true);
      chunkWriter.write(time + 2, time + 1, true);
      chunkWriter.write(time + 2, (double) (time + 1), true);
      chunkWriter.write(time + 2);
    }
    chunkWriter.sealCurrentPage();

    TimeChunkWriter timeChunkWriter = chunkWriter.getTimeChunkWriter();
    List<ValueChunkWriter> valueChunkWriters = chunkWriter.getValueChunkWriterList();

    Chunk timeChunk = getTimeChunk(measurementSchema, timeChunkWriter);
    List<Chunk> valueChunks = getValueChunks(valueChunkWriters);

    TableChunkReader chunkReader = new TableChunkReader(timeChunk, valueChunks, null);
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    int i = 1;
    for (IPageReader page : pageReaders) {
      IPointReader pointReader = page.getAllSatisfiedPageData().getBatchDataIterator();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        if (i % 3 == 1) {
          assertEquals((long) i, point.getTimestamp());
          assertEquals((float) i, point.getValue().getVector()[0].getValue());
          assertEquals(i, point.getValue().getVector()[1].getValue());
          assertEquals((double) i, point.getValue().getVector()[2].getValue());
        } else if (i % 3 == 2) {
          assertEquals((long) i, point.getTimestamp());
          assertNull(point.getValue().getVector()[0]);
          assertEquals(i, point.getValue().getVector()[1].getValue());
          assertNull(point.getValue().getVector()[2]);
        } else {
          assertEquals((long) i, point.getTimestamp());
          assertNull(point.getValue().getVector()[0]);
          assertNull(point.getValue().getVector()[1]);
          assertNull(point.getValue().getVector()[2]);
        }
        i++;
      }
    }
    assertEquals(30, i - 1);
    timeChunk.getData().flip();
    valueChunks.get(0).getData().flip();
    valueChunks.get(1).getData().flip();
    valueChunks.get(2).getData().flip();
    // rewrite INT32->DOUBLE
    Chunk newValueChunk = valueChunks.get(1).rewrite(TSDataType.DOUBLE, timeChunk);
    valueChunks.set(1, newValueChunk);
    TableChunkReader newChunkReader = new TableChunkReader(timeChunk, valueChunks, null);
    i = 1;
    List<IPageReader> newPageReaders = newChunkReader.loadPageReaderList();
    for (IPageReader page : newPageReaders) {
      IPointReader pointReader = page.getAllSatisfiedPageData().getBatchDataIterator();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        if (i % 3 == 1) {
          assertEquals((long) i, point.getTimestamp());
          assertEquals((float) i, point.getValue().getVector()[0].getValue());
          assertEquals((double) i, point.getValue().getVector()[1].getValue());
          assertEquals((double) i, point.getValue().getVector()[2].getValue());
        } else if (i % 3 == 2) {
          assertEquals((long) i, point.getTimestamp());
          assertNull(point.getValue().getVector()[0]);
          assertEquals((double) i, point.getValue().getVector()[1].getValue());
          assertNull(point.getValue().getVector()[2]);
        } else {
          assertEquals((long) i, point.getTimestamp());
          assertNull(point.getValue().getVector()[0]);
          assertNull(point.getValue().getVector()[1]);
          assertNull(point.getValue().getVector()[2]);
        }
        i++;
      }
    }
    assertEquals(30, i - 1);
    timeChunk.getData().flip();
    valueChunks.get(0).getData().flip();
    valueChunks.get(1).getData().flip();
    valueChunks.get(2).getData().flip();
  }

  @Test
  public void NonAlignedChunkMultiPagesTest() throws IOException {
    IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.PLAIN);
    ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, time);
    }
    chunkWriter.sealCurrentPage();
    for (int time = 21; time <= 40; time++) {
      chunkWriter.write(time, time);
    }
    chunkWriter.sealCurrentPage();
    Chunk newChunk = getChunk(schema, chunkWriter);
    ChunkReader chunkReader = new ChunkReader(newChunk);
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    int i = 1;
    for (IPageReader page : pageReaders) {
      BatchData data = page.getAllSatisfiedPageData(true);
      IPointReader pointReader = data.getBatchDataIterator();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals(i, point.getValue().getValue());
        i++;
      }
    }
    assertEquals(40, i - 1);
    newChunk.getData().flip();
    // rewrite INT32->DOUBLE
    Chunk newChunk2 = newChunk.rewrite(TSDataType.DOUBLE);
    ChunkReader chunkReader2 = new ChunkReader(newChunk2);
    List<IPageReader> pageReaders2 = chunkReader2.loadPageReaderList();
    i = 1;
    for (IPageReader page : pageReaders2) {
      BatchData data = page.getAllSatisfiedPageData(true);
      IPointReader pointReader = data.getBatchDataIterator();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals((double) i, point.getValue().getValue());
        i++;
      }
    }
  }

  @Test
  public void NonAlignedChunkSinglePageTest() throws IOException {
    IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.PLAIN);
    ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, time);
    }
    chunkWriter.sealCurrentPage();

    Chunk newChunk = getChunk(schema, chunkWriter);
    ChunkReader chunkReader = new ChunkReader(newChunk);
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    for (IPageReader page : pageReaders) {
      BatchData data = page.getAllSatisfiedPageData(true);
      IPointReader pointReader = data.getBatchDataIterator();
      int i = 1;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals(i, point.getValue().getValue());
        i++;
      }
      assertEquals(20, i - 1);
    }
    newChunk.getData().flip();
    // rewrite FLOAT->DOUBLE
    Chunk newChunk2 = newChunk.rewrite(TSDataType.DOUBLE);
    ChunkReader chunkReader2 = new ChunkReader(newChunk2);
    List<IPageReader> pageReaders2 = chunkReader2.loadPageReaderList();
    for (IPageReader page : pageReaders2) {
      BatchData data = page.getAllSatisfiedPageData(true);
      IPointReader pointReader = data.getBatchDataIterator();
      int i = 1;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        assertEquals((long) i, point.getTimestamp());
        assertEquals((double) i, point.getValue().getValue());
        i++;
      }
    }
  }

  public Chunk getTimeChunk(
      VectorMeasurementSchema measurementSchema, TimeChunkWriter timeChunkWriter) {
    ByteBuffer newChunkData = timeChunkWriter.getByteBuffer();
    ChunkHeader newChunkHeader =
        new ChunkHeader(
            measurementSchema.getMeasurementName(),
            newChunkData.capacity(),
            TSDataType.VECTOR,
            measurementSchema.getCompressor(),
            measurementSchema.getTimeTSEncoding(),
            timeChunkWriter.getNumOfPages());
    return new Chunk(newChunkHeader, newChunkData, null, timeChunkWriter.getStatistics());
  }

  public List<Chunk> getValueChunks(List<ValueChunkWriter> valueChunkWriters) {
    List<Chunk> valueChunks = new ArrayList<>();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriters) {
      ByteBuffer valueChunkData = valueChunkWriter.getByteBuffer();
      ChunkHeader valueChunkHeader =
          new ChunkHeader(
              valueChunkWriter.getMeasurementId(),
              valueChunkData.capacity(),
              valueChunkWriter.getDataType(),
              valueChunkWriter.getCompressionType(),
              valueChunkWriter.getEncodingType(),
              valueChunkWriter.getNumOfPages());
      Chunk valueChunk =
          new Chunk(valueChunkHeader, valueChunkData, null, valueChunkWriter.getStatistics());
      valueChunks.add(valueChunk);
    }
    return valueChunks;
  }

  public Chunk getChunk(IMeasurementSchema schema, ChunkWriterImpl chunkWriter) {
    ByteBuffer newChunkData = chunkWriter.getByteBuffer();
    ChunkHeader newChunkHeader =
        new ChunkHeader(
            schema.getMeasurementName(),
            newChunkData.capacity(),
            schema.getType(),
            schema.getCompressor(),
            schema.getEncodingType(),
            chunkWriter.getNumOfPages());
    return new Chunk(newChunkHeader, newChunkData, null, chunkWriter.getStatistics());
  }
}
