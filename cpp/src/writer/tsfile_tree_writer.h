/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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
#ifndef WRITER_TSFILE_TREE_WRITER_H
#define WRITER_TSFILE_TREE_WRITER_H

#include <memory>
#include <string>
#include <vector>

#include "tsfile_writer.h"

namespace storage {
class RestorableTsFileIOWriter;

/**
 * @brief Provides an interface for writing hierarchical (tree-structured)
 * time-series data into a TsFile.
 *
 * The TsFileTreeWriter class is designed for writing time-series data organized
 * in a device–measurement hierarchy into a TsFile. It allows registration of
 * single or aligned time series and supports writing data in both batch
 * (Tablet) and row (TsRecord) formats.
 *
 * This writer is particularly suitable when data is naturally organized by
 * device paths, and measurements are associated with those devices. The class
 * also allows controlling memory usage via a configurable threshold.
 */
class TsFileTreeWriter {
   public:
    /**
     * Constructs a TsFileTreeWriter instance to write hierarchical time-series
     * data into the specified file, with an optional memory threshold for
     * buffered data.
     *
     * @param writer_file Target file where the TsFile data will be written.
     * Must not be null.
     * @param memory_threshold Optional parameter specifying the memory usage
     * threshold for buffered data before automatic flush occurs. Default is
     * 128MB.
     */
    explicit TsFileTreeWriter(storage::WriteFile* writer_file,
                              uint64_t memory_threshold = 128 * 1024 * 1024);

    /**
     * Constructs a TsFileTreeWriter from a RestorableTsFileIOWriter so that
     * data can be appended after recovery (schema and alignment are taken from
     * the restored file).
     *
     * @param restorable_writer Restored I/O writer; must not be null and must
     * have been opened and scanned (e.g. after truncate recovery).
     * @param memory_threshold Optional memory threshold for buffered data.
     */
    explicit TsFileTreeWriter(
        storage::RestorableTsFileIOWriter* restorable_writer,
        uint64_t memory_threshold = 128 * 1024 * 1024);

    /**
     * Registers a single (non-aligned) time series under the given device ID.
     *
     * @param device_id The ID or path of the device to which the time series
     * belongs.
     * @param schema The measurement schema defining the data type and encoding
     * of the time series. Must not be null.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int register_timeseries(std::string& device_id, MeasurementSchema* schema);

    /**
     * Registers multiple aligned time series under the same device ID.
     *
     * @param device_id The ID or path of the device to which the aligned time
     * series belong.
     * @param schemas A vector of measurement schema pointers representing the
     * aligned measurements. Must not be empty or contain null pointers.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int register_timeseries(std::string& device_id,
                            std::vector<MeasurementSchema*> schemas);

    /**
     * Writes a batch of data points (tablet) for one or more time series into
     * the TsFile.
     *
     * @param tablet The Tablet object containing multiple rows of data to be
     * written. Must not be null.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int write(const Tablet& tablet);

    /**
     * Writes a single record (row) of data for a specific device and timestamp
     * into the TsFile.
     *
     * @param record The TsRecord object containing the device ID, timestamp,
     * and measurement values. Must not be null.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int write(const TsRecord& record);

    /**
     * @brief Flushes all buffered data to the underlying storage
     *
     * This method forces any buffered data to be written to the TsFile. It is
     * useful for ensuring data persistence at specific points in the writing
     * process, such as after writing a significant amount of data or before
     * performing critical operations.
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     * @note After a successful flush, buffered data is cleared and memory
     *       usage is reduced. The writer continues to accept new data after
     *       flushing.
     */
    int flush();

    /**
     * Closes the writer and ensures all buffered data is flushed to disk.
     * After this method is called, no further operations should be performed on
     * this writer instance.
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int close();

   private:
    // Underlying TsFile writer responsible for the actual file I/O and
    // serialization logic.
    std::shared_ptr<TsFileWriter> tsfile_writer_;
};

}  // namespace storage

#endif  // WRITER_TSFILE_TREE_WRITER_H