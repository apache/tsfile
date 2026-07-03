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

enum ReturnMode { RETURN_ROW = 0, RETURN_BATCH = 1 };

class TableResultSet : public ResultSet {
   public:
    explicit TableResultSet(std::unique_ptr<TsBlockReader> tsblock_reader,
                            std::vector<std::string> column_names,
                            std::vector<common::TSDataType> data_types,
                            int return_mode = RETURN_ROW)
        : tsblock_reader_(std::move(tsblock_reader)),
          column_names_(column_names),
          data_types_(data_types),
          return_mode_(return_mode) {
        init();
    }
    ~TableResultSet() override;
    int next(bool& has_next) override;
    bool is_null(const std::string& column_name) override;
    bool is_null(uint32_t column_index) override;
    RowRecord* get_row_record() override;
    std::shared_ptr<ResultSetMetadata> get_metadata() override;
    void close() override;
    int get_next_tsblock(common::TsBlock*& block) override;

    // Fast typed accessors: read straight from the current TsBlock vector
    // without going through RowRecord/Field.  Caller is expected to have
    // checked is_null() — when the cell is null the underlying buffer pointer
    // is nullptr and these return a default (0 / 0.0 / false) without
    // dereferencing it.
    bool get_bool_at(uint32_t column_index) override;
    int32_t get_int32_at(uint32_t column_index) override;
    int64_t get_int64_at(uint32_t column_index) override;
    float get_float_at(uint32_t column_index) override;
    double get_double_at(uint32_t column_index) override;

   private:
    void init();
    // Lazy materialization: fill row_record_ from the current row when a
    // caller actually requests the RowRecord (or a non-fast accessor).
    void materialize_current_row();

    std::unique_ptr<TsBlockReader> tsblock_reader_;
    common::RowIterator* row_iterator_ = nullptr;
    common::TsBlock* tsblock_ = nullptr;
    std::shared_ptr<ResultSetMetadata> result_set_metadata_;
    std::vector<std::unique_ptr<TsBlockReader>> tsblock_readers_;
    std::vector<std::string> column_names_;
    std::vector<common::TSDataType> data_types_;
    const int return_mode_;
    bool closed_ = false;
    // True when row_iterator_ points at a row that hasn't been consumed yet.
    bool row_ready_ = false;
    // True when row_record_ has been populated for the current row.
    bool row_materialized_ = false;
};
}  // namespace storage
#endif  // TABLE_RESULT_SET_H
