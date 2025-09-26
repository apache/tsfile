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

#ifndef READER_QDS_WITHOUT_TIMEGENERATOR_H
#define READER_QDS_WITHOUT_TIMEGENERATOR_H

#include <map>
#include <vector>

#include "expression.h"
#include "file/tsfile_io_reader.h"
#include "result_set.h"

namespace storage {

class QDSWithoutTimeGenerator : public ResultSet {
   public:
    QDSWithoutTimeGenerator()
        : row_record_(nullptr),
          result_set_metadata_(nullptr),
          io_reader_(nullptr),
          qe_(nullptr),
          ssi_vec_(),
          tsblocks_(),
          time_iters_(),
          value_iters_(),
          heap_time_() {}
    ~QDSWithoutTimeGenerator() { close(); }
    int init(TsFileIOReader *io_reader, QueryExpression *qe);
    void close();
    int next(bool &has_next);
    bool is_null(const std::string &column_name);
    bool is_null(uint32_t column_index);
    RowRecord *get_row_record();
    std::shared_ptr<ResultSetMetadata> get_metadata();

   private:
    int get_next_tsblock(uint32_t index, bool alloc_mem);

   private:
    RowRecord *row_record_;
    std::shared_ptr<ResultSetMetadata> result_set_metadata_;
    TsFileIOReader *io_reader_;
    QueryExpression *qe_;
    std::vector<TsFileSeriesScanIterator *> ssi_vec_;
    std::vector<common::TsBlock *> tsblocks_;
    std::vector<common::ColIterator *> time_iters_;
    std::vector<common::ColIterator *> value_iters_;
    std::multimap<int64_t, uint32_t>
        heap_time_;  // key-->time, value-->path_index
};

}  // namespace storage

#endif  // READER_QDS_WITHOUT_TIMEGENERATOR_H
