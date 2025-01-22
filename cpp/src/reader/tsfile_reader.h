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
namespace storage {
class TsFileExecutor;
class ReadFile;
class ResultSet;
struct MeasurementSchema;
}  // namespace storage

namespace storage {

extern int libtsfile_init();
extern void libtsfile_destroy();

class TsFileReader {
   public:
    TsFileReader();
    ~TsFileReader();
    int open(const std::string &file_path);
    int close();
    int query(storage::QueryExpression *qe, ResultSet *&ret_qds);
    int query(std::vector<std::string> &path_list, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    void destroy_query_data_set(ResultSet *qds);
    ResultSet *read_timeseries(const std::shared_ptr<IDeviceID>& device_id,
                               const std::vector<std::string>& measurement_name);
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(std::string table_name);
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema> &result);

   private:
    int get_all_devices(std::vector<std::shared_ptr<IDeviceID>> &device_ids,
                        std::shared_ptr<MetaIndexNode> index_node, common::PageArena &pa);
    storage::ReadFile *read_file_;
    storage::TsFileExecutor *tsfile_executor_;
};

}  // namespace storage

#endif  // READER_TSFILE_READER
