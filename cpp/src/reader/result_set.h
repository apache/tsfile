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

#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_map>

#include "common/row_record.h"

namespace storage {
/**
 * @brief metadata of result set
 *
 * user can obtain the metadata from ResultSetMetadata, including all column
 * names and data types. When a user uses the table model, the first column
 * defaults to the time column.
 */
class ResultSetMetadata {
   public:
    /**
     * @brief constructor of ResultSetMetadata
     *
     * @param column_names the column names
     * @param column_types the column types
     */
    ResultSetMetadata(const std::vector<std::string>& column_names,
                      const std::vector<common::TSDataType>& column_types) {
        this->column_names_.emplace_back("time");
        this->column_types_.emplace_back(common::INT64);
        for (size_t i = 0; i < column_names.size(); ++i) {
            this->column_names_.emplace_back(column_names[i]);
            this->column_types_.emplace_back(column_types[i]);
        }
    }
    /**
     * @brief get the column type
     *
     * @param column_index the column index starting from 1
     * @return the column type
     */
    common::TSDataType get_column_type(uint32_t column_index) {
        ASSERT(column_index >= 1 && column_index <= column_types_.size());
        return column_types_[column_index - 1];
    }
    /**
     * @brief get the column name
     *
     * @param column_index the column index starting from 1
     * @return the column name
     */
    std::string get_column_name(uint32_t column_index) {
        ASSERT(column_index >= 1 && column_index <= column_names_.size());
        return column_names_[column_index - 1];
    }
    /**
     * @brief get the column count
     *
     * @return the column count by uint32_t
     */
    uint32_t get_column_count() { return column_names_.size(); }

   private:
    std::vector<std::string> column_names_;
    std::vector<common::TSDataType> column_types_;
};

/**
 * @brief ResultSet is the query result of the TsfileReader. It provides access
 * to the results.
 *
 * ResultSet is a virtual class. Convert it to the corresponding implementation
 * class when used
 * @note When using the tree model and the filter is a global time filter,
 * it should be cast as QDSWithoutTimeGenerator.
 * @note When using the tree model and the filter is not a global time filter,
 * it should be QDSWithTimeGenerator.
 * @note If the query uses the table model, the cast should be TableResultSet
 */
class ResultSet {
   public:
    ResultSet() {}
    virtual ~ResultSet() {}
    /**
     * @brief Get the next row of the result set
     *
     * @param[out] has_next a boolean value indicating if there is a next row
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    virtual int next(bool& has_next) = 0;
    /**
     * @brief Check if the value of the column is null by column name
     *
     * @param column_name the name of the column
     * @return true if the value is null, false otherwise
     */
    virtual bool is_null(const std::string& column_name) = 0;
    /**
     * @brief Check if the value of the column is null by column index
     *
     * @param column_index the index of the column starting from 1
     * @return true if the value is null, false otherwise
     */
    virtual bool is_null(uint32_t column_index) = 0;

    /**
     * @brief Get the value of the column by column name
     *
     * @param column_name the name of the column
     * @return the value of the column
     */
    template <typename T>
    T get_value(const std::string& column_name) {
        RowRecord* row_record = get_row_record();
        ASSERT(index_lookup_.count(column_name));
        uint32_t index = index_lookup_[column_name];
        ASSERT(index >= 0 && index < row_record->get_col_num());
        return row_record->get_field(index)->get_value<T>();
    }
    /**
     * @brief Get the value of the column by column index
     *
     * @param column_index the index of the column starting from 1
     * @return the value of the column
     */
    template <typename T>
    T get_value(uint32_t column_index) {
        column_index--;
        RowRecord* row_record = get_row_record();
        ASSERT(column_index >= 0 && column_index < row_record->get_col_num());
        return row_record->get_field(column_index)->get_value<T>();
    }
    /**
     * @brief Get the row record of the result set
     *
     * @return the row record
     */
    virtual RowRecord* get_row_record() = 0;
    /**
     * @brief Get the metadata of the result set
     *
     * @return std::shared_ptr<ResultSetMetadata> the metadata of the result set
     */
    virtual std::shared_ptr<ResultSetMetadata> get_metadata() = 0;
    /**
     * @brief Close the result set
     *
     * @note this method should be called after the result set is no longer
     * needed.
     */
    virtual void close() = 0;

   protected:
    struct CaseInsensitiveHash {
        std::size_t operator()(const std::string& str) const {
            std::string lowerStr = str;
            std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            return std::hash<std::string>()(lowerStr);
        }
    };

    struct CaseInsensitiveEqual {
        bool operator()(const std::string& lhs, const std::string& rhs) const {
            if (lhs.size() != rhs.size()) {
                return false;
            }
            for (size_t i = 0; i < lhs.size(); ++i) {
                if (std::tolower(lhs[i]) != std::tolower(rhs[i])) {
                    return false;
                }
            }
            return true;
        }
    };

    std::unordered_map<std::string, uint32_t, CaseInsensitiveHash,
                       CaseInsensitiveEqual>
        index_lookup_;
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

template <>
inline std::tm ResultSet::get_value(const std::string& full_name) {
    RowRecord* row_record = get_row_record();
    ASSERT(index_lookup_.count(full_name));
    uint32_t index = index_lookup_[full_name];
    ASSERT(index >= 0 && index < row_record->get_col_num());
    return row_record->get_field(index)->get_date_value();
}
template <>
inline std::tm ResultSet::get_value(uint32_t column_index) {
    column_index--;
    RowRecord* row_record = get_row_record();
    ASSERT(column_index >= 0 && column_index < row_record->get_col_num());
    return row_record->get_field(column_index)->get_date_value();
}

}  // namespace storage

#endif  // READER_QUERY_DATA_SET_H
