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
#ifndef READER_QUERY_EXECUTOR_H
#define READER_QUERY_EXECUTOR_H

#include <vector>

#include "common/row_record.h"
#include "expression.h"
#include "file/read_file.h"
#include "reader/tsfile_series_scan_iterator.h"

namespace storage {

class QueryExecutor {
   public:
    QueryExecutor() { query_exprs_ = nullptr; }

    virtual ~QueryExecutor() {
        int size = data_scan_iter_.size();
        for (int i = 0; i < size; ++i) {
            delete data_scan_iter_[i];
            data_scan_iter_[i] = nullptr;
        }
    }

    // virtual int init(QueryExpression *query_expr, ReadFile *read_file) {
    // ASSERT(false); return 0; };

    virtual RowRecord* execute() {
        ASSERT(false);
        return nullptr;
    };

    virtual void end() { ASSERT(false); };

   protected:
    QueryExpression* query_exprs_;
    std::vector<TsFileSeriesScanIterator*> data_scan_iter_;
    std::vector<common::TsBlock*> tsblocks_;
    std::vector<common::ColIterator*> time_iters_;
    std::vector<common::ColIterator*> value_iters_;
};

}  // namespace storage

#endif  // READER_QUERY_EXECUTOR_H
