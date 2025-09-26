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
#include "expression.h"
#include "file/read_file.h"

namespace storage {
class TsFileExecutor;
class ReadFile;
class QueryDataSet;
}  // namespace storage

namespace storage {

extern int libtsfile_init();
extern void libtsfile_destroy();

class TsFileReader {
   public:
    TsFileReader();
    ~TsFileReader();
    int open(const std::string &file_path);
    int query(storage::QueryExpression *qe, QueryDataSet *&ret_qds);
    void destroy_query_data_set(QueryDataSet *qds);
    QueryDataSet *read_timeseries(const std::string &device_name,
                                  std::vector<std::string> measurement_name);

   private:
    storage::ReadFile *read_file_;
    storage::TsFileExecutor *tsfile_executor_;
};

}  // namespace storage

#endif  // READER_TSFILE_READER
