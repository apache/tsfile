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

package org.apache.tsfile.read.common;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.chunk.TableChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfByteArray;

/** used in query. */
public class Chunk {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Chunk.class)
          + ChunkHeader.INSTANCE_SIZE
          + RamUsageEstimator.shallowSizeOfInstance(ByteBuffer.class);

  private final ChunkHeader chunkHeader;
  private ByteBuffer chunkData;
  private Statistics chunkStatistic;

  private EncryptParameter encryptParam;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  public Chunk(
      ChunkHeader header,
      ByteBuffer buffer,
      List<TimeRange> deleteIntervalList,
      Statistics chunkStatistic) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deleteIntervalList = deleteIntervalList;
    this.chunkStatistic = chunkStatistic;
    this.encryptParam = EncryptUtils.encryptParam;
  }

  public Chunk(
      ChunkHeader header,
      ByteBuffer buffer,
      List<TimeRange> deleteIntervalList,
      Statistics chunkStatistic,
      EncryptParameter encryptParam) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deleteIntervalList = deleteIntervalList;
    this.chunkStatistic = chunkStatistic;
    this.encryptParam = encryptParam;
  }

  public Chunk(ChunkHeader header, ByteBuffer buffer) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.encryptParam = EncryptUtils.encryptParam;
  }

  public Chunk(ChunkHeader header, ByteBuffer buffer, EncryptParameter encryptParam) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.encryptParam = encryptParam;
  }

  public EncryptParameter getEncryptParam() {
    return encryptParam;
  }

  public ChunkHeader getHeader() {
    return chunkHeader;
  }

  public ByteBuffer getData() {
    return chunkData;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public void mergeChunkByAppendPage(Chunk chunk) throws IOException {
    int dataSize = 0;
    // from where the page data of the merged chunk starts, if -1, it means the merged chunk has
    // more than one page
    int offset1 = -1;
    // if the merged chunk has only one page, after merge with current chunk ,it will have more than
    // page
    // so we should add page statistics for it
    if (((byte) (chunk.chunkHeader.getChunkType() & 0x3F))
        == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      // read the uncompressedSize and compressedSize of this page
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      // record the position from which we can reuse
      offset1 = chunk.chunkData.position();
      chunk.chunkData.flip();
      // the actual size should add another page statistics size
      dataSize += (chunk.chunkData.array().length + chunk.chunkStatistic.getSerializedSize());
    } else {
      // if the merge chunk already has more than one page, we can reuse all the part of its data
      // the dataSize is equal to the before
      dataSize += chunk.chunkData.array().length;
    }
    // from where the page data of the current chunk starts, if -1, it means the current chunk has
    // more than one page
    int offset2 = -1;
    // if the current chunk has only one page, after merge with the merged chunk ,it will have more
    // than page
    // so we should add page statistics for it
    if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      // change the chunk type
      chunkHeader.setChunkType(MetaMarker.CHUNK_HEADER);
      // read the uncompressedSize and compressedSize of this page
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      // record the position from which we can reuse
      offset2 = chunkData.position();
      chunkData.flip();
      // the actual size should add another page statistics size
      dataSize += (chunkData.array().length + chunkStatistic.getSerializedSize());
    } else {
      // if the current chunk already has more than one page, we can reuse all the part of its data
      // the dataSize is equal to the before
      dataSize += chunkData.array().length;
    }
    chunkHeader.setDataSize(dataSize);
    ByteBuffer newChunkData = ByteBuffer.allocate(dataSize);
    // the current chunk has more than one page, we can use its data part directly without any
    // changes
    if (offset2 == -1) {
      newChunkData.put(chunkData.array());
    } else { // the current chunk has only one page, we need to add one page statistics for it
      byte[] b = chunkData.array();
      // put the uncompressedSize and compressedSize of this page
      newChunkData.put(b, 0, offset2);
      // add page statistics
      PublicBAOS a = new PublicBAOS();
      chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      // put the remaining page data
      newChunkData.put(b, offset2, b.length - offset2);
    }
    // the merged chunk has more than one page, we can use its data part directly without any
    // changes
    if (offset1 == -1) {
      newChunkData.put(chunk.chunkData.array());
    } else {
      // put the uncompressedSize and compressedSize of this page
      byte[] b = chunk.chunkData.array();
      newChunkData.put(b, 0, offset1);
      // add page statistics
      PublicBAOS a = new PublicBAOS();
      chunk.chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      // put the remaining page data
      newChunkData.put(b, offset1, b.length - offset1);
    }
    chunkData = newChunkData;
  }

  public Statistics getChunkStatistic() {
    return chunkStatistic;
  }

  /**
   * it's only used for query cache, and assuming that we use HeapByteBuffer, if we use Pooled
   * DirectByteBuffer in the future, we need to change the calculation logic here. chunkStatistic
   * and deleteIntervalList are all null in cache
   */
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + sizeOfByteArray(chunkData.capacity());
  }

  public Chunk rewrite(TSDataType newType, Chunk timeChunk) throws IOException {
    if (newType == null || newType == chunkHeader.getDataType()) {
      return this;
    }
    IMeasurementSchema schema =
        new MeasurementSchema(
            chunkHeader.getMeasurementID(),
            newType,
            chunkHeader.getEncodingType(),
            chunkHeader.getCompressionType());

    ValueChunkWriter chunkWriter =
        new ValueChunkWriter(
            chunkHeader.getMeasurementID(),
            chunkHeader.getCompressionType(),
            newType,
            chunkHeader.getEncodingType(),
            schema.getValueEncoder(),
            encryptParam);
    List<Chunk> valueChunks = new ArrayList<>();
    valueChunks.add(this);
    TableChunkReader chunkReader = new TableChunkReader(timeChunk, valueChunks, null);
    List<IPageReader> pages = chunkReader.loadPageReaderList();
    for (IPageReader page : pages) {
      IPointReader pointReader = page.getAllSatisfiedPageData().getBatchDataIterator();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        Object convertedValue = null;
        if (point.getValue().getVector()[0] != null) {
          convertedValue =
              newType.castFromSingleValue(
                  chunkHeader.getDataType(), point.getValue().getVector()[0].getValue());
        }
        long timestamp = point.getTimestamp();
        switch (newType) {
          case BOOLEAN:
            chunkWriter.write(
                timestamp,
                convertedValue == null ? true : (boolean) convertedValue,
                convertedValue == null);
            break;
          case DATE:
          case INT32:
            chunkWriter.write(
                timestamp,
                convertedValue == null ? Integer.MAX_VALUE : (int) convertedValue,
                convertedValue == null);
            break;
          case TIMESTAMP:
          case INT64:
            chunkWriter.write(
                timestamp,
                convertedValue == null ? (long) Integer.MAX_VALUE : (long) convertedValue,
                convertedValue == null);
            break;
          case FLOAT:
            chunkWriter.write(
                timestamp,
                convertedValue == null ? (float) Integer.MAX_VALUE : (float) convertedValue,
                convertedValue == null);
            break;
          case DOUBLE:
            chunkWriter.write(
                timestamp,
                convertedValue == null ? (double) Integer.MAX_VALUE : (double) convertedValue,
                convertedValue == null);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            chunkWriter.write(
                timestamp,
                convertedValue == null ? Binary.EMPTY_VALUE : (Binary) convertedValue,
                convertedValue == null);
            break;
          default:
            throw new IOException("Unsupported data type: " + newType);
        }
      }
      chunkWriter.sealCurrentPage();
    }
    ByteBuffer newChunkData = chunkWriter.getByteBuffer();
    ChunkHeader newChunkHeader =
        new ChunkHeader(
            chunkHeader.getChunkType(),
            chunkHeader.getMeasurementID(),
            newChunkData.capacity(),
            newType,
            chunkHeader.getCompressionType(),
            chunkHeader.getEncodingType());
    chunkData.flip();
    timeChunk.chunkData.flip();
    return new Chunk(
        newChunkHeader,
        newChunkData,
        deleteIntervalList,
        chunkWriter.getStatistics(),
        encryptParam);
  }

  public Chunk rewrite(TSDataType newType) throws IOException {
    if (newType == null || newType == chunkHeader.getDataType()) {
      return this;
    }
    IMeasurementSchema schema =
        new MeasurementSchema(
            chunkHeader.getMeasurementID(),
            newType,
            chunkHeader.getEncodingType(),
            chunkHeader.getCompressionType());
    ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema, encryptParam);
    ChunkReader chunkReader = new ChunkReader(this);
    List<IPageReader> pages = chunkReader.loadPageReaderList();
    for (IPageReader page : pages) {
      BatchData batchData = page.getAllSatisfiedPageData();
      IPointReader pointReader = batchData.getBatchDataIterator();
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair point = pointReader.nextTimeValuePair();
        Object convertedValue =
            newType.castFromSingleValue(chunkHeader.getDataType(), point.getValue().getValue());
        long timestamp = point.getTimestamp();
        if (convertedValue == null) {
          throw new IOException("NonAlignedChunk contains null, timestamp: " + timestamp);
        }
        switch (newType) {
          case BOOLEAN:
            chunkWriter.write(timestamp, (boolean) convertedValue);
            break;
          case DATE:
          case INT32:
            chunkWriter.write(timestamp, (int) convertedValue);
            break;
          case TIMESTAMP:
          case INT64:
            chunkWriter.write(timestamp, (long) convertedValue);
            break;
          case FLOAT:
            chunkWriter.write(timestamp, (float) convertedValue);
            break;
          case DOUBLE:
            chunkWriter.write(timestamp, (double) convertedValue);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            chunkWriter.write(timestamp, (Binary) convertedValue);
            break;
          default:
            throw new IOException("Unsupported data type: " + newType);
        }
      }
      chunkWriter.sealCurrentPage();
    }
    ByteBuffer newChunkData = chunkWriter.getByteBuffer();
    ChunkHeader newChunkHeader =
        new ChunkHeader(
            chunkHeader.getChunkType(),
            chunkHeader.getMeasurementID(),
            newChunkData.capacity(),
            newType,
            chunkHeader.getCompressionType(),
            chunkHeader.getEncodingType());
    chunkData.flip();
    return new Chunk(
        newChunkHeader,
        newChunkData,
        deleteIntervalList,
        chunkWriter.getStatistics(),
        encryptParam);
  }
}
