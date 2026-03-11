/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#ifndef READER_ROW_RANGE_RESULT_SET_H
#define READER_ROW_RANGE_RESULT_SET_H

#include <memory>
#include <string>

#include "reader/result_set.h"

namespace storage {

/**
 * @brief A ResultSet wrapper that applies row-level offset and limit.
 *
 * Takes ownership of the inner ResultSet and releases it on close().
 * Once the limit is reached, next() returns has_next=false immediately
 * without calling the underlying ResultSet, avoiding unnecessary data loading.
 *
 * @param offset  Number of leading rows to skip (must be >= 0).
 * @param limit   Maximum number of rows to return. A value < 0 means
 *                no limit (all remaining rows are returned).
 */
class RowRangeResultSet : public ResultSet {
   public:
    RowRangeResultSet(ResultSet* inner, int offset, int limit);
    ~RowRangeResultSet() override;

    int next(bool& has_next) override;
    bool is_null(const std::string& column_name) override;
    bool is_null(uint32_t column_index) override;
    RowRecord* get_row_record() override;
    std::shared_ptr<ResultSetMetadata> get_metadata() override;
    void close() override;

   private:
    ResultSet* inner_;
    int offset_;
    int limit_;
    int returned_count_;
    bool offset_skipped_;
};

}  // namespace storage
#endif  // READER_ROW_RANGE_RESULT_SET_H
