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

package org.apache.tsfile.write.v4;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.chunk.AlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.tsfile.write.chunk.NonAlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.chunk.TableChunkGroupWriterImpl;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

abstract class AbstractTableModelTsFileWriter implements ITsFileWriter {

  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractTableModelTsFileWriter.class);

  /** IO writer of this TsFile. */
  protected final TsFileIOWriter fileWriter;

  protected EncryptParameter encryptParam;

  protected final int pageSize;
  protected long recordCount = 0;

  // deviceId -> measurementIdList
  protected Map<IDeviceID, List<String>> flushedMeasurementsInDeviceMap = new HashMap<>();

  // DeviceId -> LastTime
  protected Map<IDeviceID, Long> alignedDeviceLastTimeMap = new HashMap<>();

  // TimeseriesId -> LastTime
  protected Map<IDeviceID, Map<String, Long>> nonAlignedTimeseriesLastTimeMap = new HashMap<>();

  protected Map<IDeviceID, IChunkGroupWriter> groupWriters = new TreeMap<>();

  /** min value of threshold of data points num check. */
  protected long recordCountForNextMemCheck = 100;

  protected long chunkGroupSizeThreshold;

  /**
   * init this Writer.
   *
   * @param file the File to be written by this TsFileWriter
   */
  @TsFileApi
  protected AbstractTableModelTsFileWriter(File file, long chunkGroupSizeThreshold)
      throws IOException {
    Schema schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    this.fileWriter = new TsFileIOWriter(file);
    fileWriter.setSchema(schema);

    this.pageSize = conf.getPageSizeInByte();
    this.chunkGroupSizeThreshold = chunkGroupSizeThreshold;
    if (this.pageSize >= chunkGroupSizeThreshold) {
      LOG.warn(
          "TsFile's page size {} is greater than chunk group size {}, please enlarge the chunk group"
              + " size or decrease page size. ",
          pageSize,
          chunkGroupSizeThreshold);
    }

    String encryptLevel;
    byte[] encryptKey;
    byte[] dataEncryptKey;
    String encryptType;
    if (config.getEncryptFlag()) {
      encryptLevel = "2";
      encryptType = config.getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("IoTDB is the best".getBytes());
        md.update(config.getEncryptKey().getBytes());
        dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
        encryptKey =
            IEncryptor.getEncryptor(config.getEncryptType(), config.getEncryptKey().getBytes())
                .encrypt(dataEncryptKey);
      } catch (Exception e) {
        throw new EncryptException(
            "SHA-256 function not found while using SHA-256 to generate data key");
      }
    } else {
      encryptLevel = "0";
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      encryptKey = null;
      dataEncryptKey = null;
    }
    this.encryptParam = new EncryptParameter(encryptType, dataEncryptKey);
    if (encryptKey != null) {
      StringBuilder valueStr = new StringBuilder();

      for (byte b : encryptKey) {
        valueStr.append(b).append(",");
      }

      valueStr.deleteCharAt(valueStr.length() - 1);
      String str = valueStr.toString();

      fileWriter.setEncryptParam(encryptLevel, encryptType, str);
    } else {
      fileWriter.setEncryptParam(encryptLevel, encryptType, "");
    }
  }

  protected IChunkGroupWriter tryToInitialGroupWriter(
      IDeviceID deviceId, boolean isAligned, boolean isTableModel) {
    IChunkGroupWriter groupWriter = groupWriters.get(deviceId);
    if (groupWriter == null) {
      if (isAligned) {
        groupWriter =
            isTableModel
                ? new TableChunkGroupWriterImpl(deviceId, encryptParam)
                : new AlignedChunkGroupWriterImpl(deviceId, encryptParam);
        ((AlignedChunkGroupWriterImpl) groupWriter)
            .setLastTime(alignedDeviceLastTimeMap.get(deviceId));
      } else {
        groupWriter = new NonAlignedChunkGroupWriterImpl(deviceId, encryptParam);
        ((NonAlignedChunkGroupWriterImpl) groupWriter)
            .setLastTimeMap(
                nonAlignedTimeseriesLastTimeMap.getOrDefault(deviceId, new HashMap<>()));
      }
      groupWriters.put(deviceId, groupWriter);
    }
    return groupWriter;
  }

  /**
   * calculate total memory size occupied by all ChunkGroupWriter instances currently.
   *
   * @return total memory size used
   */
  protected long calculateMemSizeForAllGroup() {
    long memTotalSize = 0;
    for (IChunkGroupWriter group : groupWriters.values()) {
      memTotalSize += group.updateMaxGroupMemSize();
    }
    return memTotalSize;
  }

  /**
   * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush them to given
   * OutputStream.
   *
   * @throws IOException exception in IO
   */
  protected void checkMemorySizeAndMayFlushChunks() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) {
      long memSize = calculateMemSizeForAllGroup();
      if (memSize > chunkGroupSizeThreshold) {
        LOG.debug("start to flush chunk groups, memory space occupy:{}", memSize);
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
        flush();
      } else {
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
      }
    }
  }

  /**
   * flush the data in all series writers of all chunk group writers and their page writers to
   * outputStream.
   *
   * @throws IOException exception in IO
   */
  @TsFileApi
  protected void flush() throws IOException {
    if (recordCount > 0) {
      for (Map.Entry<IDeviceID, IChunkGroupWriter> entry : groupWriters.entrySet()) {
        IDeviceID deviceId = entry.getKey();
        IChunkGroupWriter groupWriter = entry.getValue();
        fileWriter.startChunkGroup(deviceId);
        long pos = fileWriter.getPos();
        long dataSize = groupWriter.flushToFileWriter(fileWriter);
        if (fileWriter.getPos() - pos != dataSize) {
          throw new IOException(
              String.format(
                  "Flushed data size is inconsistent with computation! Estimated: %d, Actual: %d",
                  dataSize, fileWriter.getPos() - pos));
        }
        fileWriter.endChunkGroup();
        if (groupWriter instanceof AlignedChunkGroupWriterImpl) {
          // add flushed measurements
          List<String> measurementList =
              flushedMeasurementsInDeviceMap.computeIfAbsent(deviceId, p -> new ArrayList<>());
          ((AlignedChunkGroupWriterImpl) groupWriter)
              .getMeasurements()
              .forEach(
                  measurementId -> {
                    if (!measurementList.contains(measurementId)) {
                      measurementList.add(measurementId);
                    }
                  });
          // add lastTime
          this.alignedDeviceLastTimeMap.put(
              deviceId, ((AlignedChunkGroupWriterImpl) groupWriter).getLastTime());
        } else {
          // add lastTime
          this.nonAlignedTimeseriesLastTimeMap.put(
              deviceId, ((NonAlignedChunkGroupWriterImpl) groupWriter).getLastTimeMap());
        }
      }
      reset();
    }
  }

  protected void reset() {
    groupWriters.clear();
    recordCount = 0;
  }

  protected TsFileIOWriter getIOWriter() {
    return this.fileWriter;
  }

  protected Schema getSchema() {
    return fileWriter.getSchema();
  }

  /**
   * calling this method to write the last data remaining in memory and close the normal and error
   * OutputStream.
   */
  @Override
  @TsFileApi
  public void close() {
    LOG.info("start close file");
    try {
      flush();
      fileWriter.endFile();
    } catch (IOException e) {
      LOG.warn("Meet exception when close file writer. ", e);
    }
  }
}
