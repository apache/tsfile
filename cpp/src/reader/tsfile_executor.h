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
#ifndef READER_TSFILE_EXECUTOR_H
#define READER_TSFILE_EXECUTOR_H

#include <map>

#include "file/read_file.h"
#include "query_data_set.h"
#include "query_executor.h"

namespace storage {

class TsFileExecutor  // : public QueryExecutor
{
   public:
    TsFileExecutor();
    ~TsFileExecutor();
    int init(ReadFile *read_file);
    int init(const std::string &file_path);
    int execute(QueryExpression *query_expr, QueryDataSet *&ret_qds);
    void destroy_query_data_set(QueryDataSet *qds);

   private:
    int execute_may_with_global_timefilter(QueryExpression *qe,
                                           QueryDataSet *&ret_qds);
    int execute_with_timegenerator(QueryExpression *qe, QueryDataSet *&ret_qds);

   private:
    TsFileIOReader io_reader_;
    QueryExpression *query_exprs_;
    std::vector<TsFileSeriesScanIterator *> data_scan_iter_;
    std::vector<common::TsBlock *> tsblocks_;
    std::vector<common::ColIterator *> time_iters_;
    std::vector<common::ColIterator *> value_iters_;
    bool is_inited_;
};

}  // namespace storage

#endif  // READER_TSFILE_EXECUTOR_H
