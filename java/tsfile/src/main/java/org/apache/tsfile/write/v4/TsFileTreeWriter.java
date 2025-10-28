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

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * TsFileTreeWriter provides a tree-based interface for writing data to TsFiles. It simplifies the
 * writing process by handling schema registration and data writing with a focus on tree model
 * operations.
 */
public class TsFileTreeWriter implements AutoCloseable {

  private final TsFileWriter tsFileWriter;

  /**
   * Constructs a TsFileTreeWriter with the specified TsFileWriter and memory threshold.
   *
   * @param file the output file
   * @param memoryThreshold the memory threshold for flushing data
   */
  TsFileTreeWriter(File file, int memoryThreshold) throws IOException {
    this.tsFileWriter = new TsFileWriter(file);
    tsFileWriter.setMemoryThreshold(memoryThreshold);
  }

  /**
   * Registers a non-aligned timeseries for the specified device.
   *
   * @param deviceId the device identifier
   * @param schema the measurement schema to register
   * @throws WriteProcessException if registration fails
   */
  public void registerTimeseries(String deviceId, IMeasurementSchema schema)
      throws WriteProcessException {
    try {
      tsFileWriter.registerTimeseries(deviceId, schema);
    } catch (WriteProcessException e) {
      throw new WriteProcessException("Failed to register timeseries for device " + deviceId, e);
    }
  }

  /**
   * Registers a group of aligned timeseries for the specified device.
   *
   * @param deviceId the device identifier
   * @param schemas list of measurement schemas to register as aligned
   * @throws WriteProcessException if registration fails
   */
  public void registerAlignedTimeseries(String deviceId, List<IMeasurementSchema> schemas)
      throws WriteProcessException {
    try {
      tsFileWriter.registerAlignedTimeseries(deviceId, schemas);
    } catch (WriteProcessException e) {
      throw new WriteProcessException(
          "Failed to register aligned timeseries for device " + deviceId, e);
    }
  }

  /**
   * Writes a Tablet (batch of data points) to the TsFile.
   *
   * @param tablet the tablet containing data points to write
   * @throws IOException if an I/O error occurs
   * @throws WriteProcessException if writing fails
   */
  public void write(Tablet tablet) throws IOException, WriteProcessException {
    try {
      tsFileWriter.writeTree(tablet);
    } catch (IOException | WriteProcessException e) {
      throw new WriteProcessException("Failed to write tablet data", e);
    }
  }

  /**
   * Writes a TSRecord (single record) to the TsFile.
   *
   * @param record the TSRecord to write
   * @throws IOException if an I/O error occurs
   * @throws WriteProcessException if writing fails
   */
  public void write(TSRecord record) throws IOException, WriteProcessException {
    try {
      tsFileWriter.writeRecord(record);
    } catch (IOException | WriteProcessException e) {
      throw new WriteProcessException("Failed to write TSRecord", e);
    }
  }

  /**
   * Flushes any buffered data and closes the writer. After closing, the writer cannot be used
   * anymore.
   *
   * @throws IOException if an I/O error occurs during closing
   */
  @Override
  public void close() throws IOException {
    try {
      tsFileWriter.close();
    } catch (IOException e) {
      throw new IOException("Failed to close TsFileTreeWriter", e);
    }
  }
}
