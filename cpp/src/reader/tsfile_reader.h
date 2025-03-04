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

#ifndef READER_TSFILE_READER_H
#define READER_TSFILE_READER_H

#include "common/row_record.h"
#include "common/tsfile_common.h"
#include "expression.h"
#include "file/read_file.h"
#include "reader/table_query_executor.h"
namespace storage {
class TsFileExecutor;
class ReadFile;
class ResultSet;
struct MeasurementSchema;
}  // namespace storage

namespace storage {

extern int libtsfile_init();
extern void libtsfile_destroy();
/**
 * @brief TsfileReader provides the ability to query all files with the suffix
 * .tsfile
 *
 * TsfileReader is designed to query .tsfile files, it accepts tree model
 * queries and table model queries, and supports querying metadata such as
 * TableSchema and TimeseriesSchema.
 */
class TsFileReader {
   public:
    TsFileReader();
    ~TsFileReader();
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
     * @brief query the tsfile by the query expression,Users can construct
     * their own query expressions to query tsfile
     *
     * @param [in] qe the query expression
     * @param [out] ret_qds the result set
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int query(storage::QueryExpression *qe, ResultSet *&ret_qds);
    /**
     * @brief query the tsfile by the path list, start time and end time
     * this method is used to query the tsfile by the tree model.
     *
     * @param [in] path_list the path list
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [out] result_set the result set
     */
    int query(std::vector<std::string> &path_list, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    /**
     * @brief query the tsfile by the table name, columns names, start time
     * and end time. this method is used to query the tsfile by the table
     * model.
     *
     * @param [in] table_name the table name
     * @param [in] columns_names the columns names
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [out] result_set the result set
     */
    int query(const std::string &table_name,
              const std::vector<std::string> &columns_names, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    /**
     * @brief destroy the result set, this method should be called after the
     * query is finished and result_set
     *
     * @param qds the result set
     */
    void destroy_query_data_set(ResultSet *qds);
    ResultSet *read_timeseries(
        const std::shared_ptr<IDeviceID> &device_id,
        const std::vector<std::string> &measurement_name);
    /**
     * @brief get all devices in the tsfile
     *
     * @param table_name the table name
     * @return std::vector<std::shared_ptr<IDeviceID>> the device id list
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(
        std::string table_name);
    /**
     * @brief get the timeseries schema by the device id and measurement name
     *
     * @param [in] device_id the device id
     * @param [out] result std::vector<MeasurementSchema> the measurement schema
     * list
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema> &result);
    /**
     * @brief get the table schema by the table name
     *
     * @param table_name the table name
     * @return std::shared_ptr<TableSchema> the table schema
     */
    std::shared_ptr<TableSchema> get_table_schema(
        const std::string &table_name);
    /**
     * @brief get all table schemas in the tsfile
     *
     * @return std::vector<std::shared_ptr<TableSchema>> the table schema list
     */
    std::vector<std::shared_ptr<TableSchema>> get_all_table_schemas();

   private:
    int get_all_devices(std::vector<std::shared_ptr<IDeviceID>> &device_ids,
                        std::shared_ptr<MetaIndexNode> index_node,
                        common::PageArena &pa);
    storage::ReadFile *read_file_;
    storage::TsFileExecutor *tsfile_executor_;
    storage::TableQueryExecutor *table_query_executor_;
};

}  // namespace storage

#endif  // READER_TSFILE_READER
