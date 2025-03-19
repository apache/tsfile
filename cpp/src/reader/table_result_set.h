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
#ifndef READER_TABLE_RESULT_SET_H
#define READER_TABLE_RESULT_SET_H
#include <memory>

#include "reader/block/tsblock_reader.h"
#include "reader/result_set.h"

namespace storage {
class TableResultSet : public ResultSet {
   public:
    explicit TableResultSet(std::unique_ptr<TsBlockReader> tsblock_reader,
                            std::vector<std::string> column_names,
                            std::vector<common::TSDataType> data_types)
        : tsblock_reader_(std::move(tsblock_reader)),
          column_names_(column_names),
          data_types_(data_types) {
        init();
    }
    ~TableResultSet();
    int next(bool& has_next) override;
    bool is_null(const std::string& column_name) override;
    bool is_null(uint32_t column_index) override;
    RowRecord* get_row_record() override;
    std::shared_ptr<ResultSetMetadata> get_metadata() override;
    void close() override;

   private:
    void init();
    std::unique_ptr<TsBlockReader> tsblock_reader_;
    common::RowIterator* row_iterator_ = nullptr;
    common::TsBlock* tsblock_ = nullptr;
    RowRecord* row_record_ = nullptr;
    std::shared_ptr<ResultSetMetadata> result_set_metadata_;
    std::vector<std::unique_ptr<TsBlockReader>> tsblock_readers_;
    std::vector<std::string> column_names_;
    std::vector<common::TSDataType> data_types_;
};
}  // namespace storage
#endif  // TABLE_RESULT_SET_H