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

#ifndef READER_QUERY_DATA_SET_H
#define READER_QUERY_DATA_SET_H

#include <unordered_map>

#include "common/row_record.h"

namespace storage {

class ResultSetMetadata {
   public:
    ResultSetMetadata(const std::vector<std::string>& column_names,
                      const std::vector<common::TSDataType>& column_types) {
        this->column_names_.emplace_back("time");
        this->column_types_.emplace_back(common::INT64);
        for (size_t i = 0; i < column_names.size(); ++i) {
            this->column_names_.emplace_back(column_names[i]);
            this->column_types_.emplace_back(column_types[i]);
        }
    }
    common::TSDataType get_column_type(uint32_t column_index) {
        ASSERT(column_index >= 1 && column_index <= column_types_.size());
        return column_types_[column_index - 1];
    }
    std::string get_column_name(uint32_t column_index) {
        ASSERT(column_index >= 1 && column_index <= column_names_.size());
        return column_names_[column_index - 1];
    }
    uint32_t get_column_count() { return column_names_.size(); }

   private:
    std::vector<std::string> column_names_;
    std::vector<common::TSDataType> column_types_;
};

class ResultSet {
   public:
    ResultSet() {}
    virtual ~ResultSet() {}
    virtual int next(bool& has_next) = 0;
    virtual bool is_null(const std::string& column_name) = 0;
    virtual bool is_null(uint32_t column_index) = 0;

    template <typename T>
    T get_value(const std::string& column_name) {
        RowRecord* row_record = get_row_record();
        ASSERT(index_lookup_.count(column_name));
        uint32_t index = index_lookup_[column_name];
        ASSERT(index >= 0 && index < row_record->get_col_num());
        return row_record->get_field(index)->get_value<T>();
    }
    template <typename T>
    T get_value(uint32_t column_index) {
        column_index--;
        RowRecord* row_record = get_row_record();
        ASSERT(column_index >= 0 && column_index < row_record->get_col_num());
        return row_record->get_field(column_index)->get_value<T>();
    }
    virtual RowRecord* get_row_record() = 0;
    virtual std::shared_ptr<ResultSetMetadata> get_metadata() = 0; /*Use a
     share_ptr instead of a bare pointer avoid user delete metadata*/
    virtual void close() = 0;

   protected:
    std::unordered_map<std::string, uint32_t> index_lookup_;
    common::PageArena pa_;
};

template <>
inline common::String* ResultSet::get_value(const std::string& full_name) {
    RowRecord* row_record = get_row_record();
    ASSERT(index_lookup_.count(full_name));
    uint32_t index = index_lookup_[full_name];
    ASSERT(index >= 0 && index < row_record->get_col_num());
    return row_record->get_field(index)->get_string_value();
}
template <>
inline common::String* ResultSet::get_value(uint32_t column_index) {
    column_index--;
    RowRecord* row_record = get_row_record();
    ASSERT(column_index >= 0 && column_index < row_record->get_col_num());
    return row_record->get_field(column_index)->get_string_value();
}

}  // namespace storage

#endif  // READER_QUERY_DATA_SET_H
