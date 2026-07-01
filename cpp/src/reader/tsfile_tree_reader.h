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

#ifndef READER_TSFILE_TREE_READER_H
#define READER_TSFILE_TREE_READER_H

#include <memory>
#include <string>
#include <vector>

#include "tsfile_reader.h"

namespace storage {

class TsFileTreeReader {
   public:
    TsFileTreeReader();
    ~TsFileTreeReader();

    /**
     * @brief open the tsfile
     *
     * @param file_path the path of the tsfile which will be opened
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int open(const std::string &file_path);

    /**
     * @brief close the tsfile, this method should be called after the
     * query is finished
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int close();

    /**
     * @brief Query time series data for specified devices and measurements
     * within a time range
     *
     * @param device_ids List of device identifiers to query
     * @param measurement_names List of measurement names to query
     * @param start_time Start timestamp of the query range (inclusive)
     * @param end_time End timestamp of the query range (inclusive)
     * @param[out] result_set Pointer to the ResultSet that will contain query
     * results
     * @return Returns 0 on success, or a non-zero error code on failure.
     *         The caller is responsible for destroying the result set using
     * destroy_query_data_set()
     */
    int query(const std::vector<std::string> &device_ids,
              const std::vector<std::string> &measurement_names,
              int64_t start_time, int64_t end_time, ResultSet *&result_set);

    /**
     * @brief Destroy and deallocate the query result set
     *
     * @param result_set Pointer to the ResultSet to be destroyed
     * @note This method should be called after the result set is no longer
     * needed to prevent memory leaks
     */
    void destroy_query_data_set(ResultSet *result_set);

    /**
     * @brief Get the measurement schema for a specific device
     *
     * @param device_id The device identifier
     * @return Pointer to the MeasurementSchema for the device, or nullptr if
     * not found
     * @note The caller should not delete the returned pointer as it's managed
     * by the reader
     */
    std::vector<MeasurementSchema> get_device_schema(
        const std::string &device_id);

    /**
     * @brief Get all device identifiers in the TsFile
     *
     * @return Vector containing all device identifiers found in the TsFile
     * @note The returned vector will be empty if no devices are found or file
     * is not opened
     */
    std::vector<std::string> get_all_device_ids();

   private:
    std::shared_ptr<TsFileReader>
        tsfile_reader_;  ///< Underlying TsFile reader implementation
};

}  // namespace storage

#endif  // READER_TSFILE_TREE_READER_H