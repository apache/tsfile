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

class ResultSetIterator;

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
class ResultSet : std::enable_shared_from_this<ResultSet> {
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
     * @brief Simple iterator for ResultSet with smart pointers
     */
    virtual ResultSetIterator iterator();

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
    RowRecord* row_record_ = nullptr;
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

/**
 * @brief Simple iterator for ResultSet with smart pointers
 */
class ResultSetIterator {
   public:
    explicit ResultSetIterator(ResultSet* result_set)
        : result_set_(result_set) {}

    /**
     * @brief Check if there is a next row available
     */
    bool hasNext() {
        if (cached_record_ != nullptr) {
            return true;
        }
        if (exhausted_) {
            return false;
        }

        bool has_next = false;
        if (result_set_) {
            int ret = result_set_->next(has_next);
            ASSERT(ret == 0);
            // TODO:handle error in hasNext.
            (void)ret;
            if (has_next) {
                cached_record_ = result_set_->get_row_record();
            } else {
                exhausted_ = true;
            }
        }
        return has_next;
    }

    /**
     * @brief Get the next row record
     */
    RowRecord* next() {
        if (!hasNext()) {
            return nullptr;
        }
        RowRecord* ret = cached_record_;
        cached_record_ = nullptr;
        return ret;
    }

    /**
     * @brief Get the underlying ResultSet for direct access
     */
    ResultSet* getResultSet() const { return result_set_; }

   private:
    ResultSet* result_set_ = nullptr;
    RowRecord* cached_record_ = nullptr;
    bool exhausted_ = false;
};

inline ResultSetIterator ResultSet::iterator() {
    return ResultSetIterator(this);
}

static MAYBE_UNUSED void print_table_result_set(
    storage::ResultSet* table_result_set) {
    if (table_result_set == nullptr) {
        std::cout << "TableResultSet is nullptr" << std::endl;
        return;
    }

    auto metadata = table_result_set->get_metadata();
    if (metadata == nullptr) {
        std::cout << "Metadata is nullptr" << std::endl;
        return;
    }

    uint32_t column_count = metadata->get_column_count();
    if (column_count == 0) {
        std::cout << "No columns in result set" << std::endl;
        return;
    }

    for (uint32_t i = 1; i <= column_count; i++) {
        std::cout << metadata->get_column_name(i);
        if (i < column_count) {
            std::cout << "\t";
        }
    }
    std::cout << std::endl;

    bool has_next = false;
    int row_count = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        for (uint32_t i = 1; i <= column_count; i++) {
            if (table_result_set->is_null(i)) {
                std::cout << "NULL";
            } else {
                common::TSDataType col_type = metadata->get_column_type(i);
                switch (col_type) {
                    case common::INT64: {
                        int64_t val = table_result_set->get_value<int64_t>(i);
                        std::cout << val;
                        break;
                    }
                    case common::INT32: {
                        int32_t val = table_result_set->get_value<int32_t>(i);
                        std::cout << val;
                        break;
                    }
                    case common::FLOAT: {
                        float val = table_result_set->get_value<float>(i);
                        std::cout << val;
                        break;
                    }
                    case common::DOUBLE: {
                        double val = table_result_set->get_value<double>(i);
                        std::cout << val;
                        break;
                    }
                    case common::BOOLEAN: {
                        bool val = table_result_set->get_value<bool>(i);
                        std::cout << (val ? "true" : "false");
                        break;
                    }
                    case common::TEXT:
                    case common::STRING: {
                        common::String* str =
                            table_result_set->get_value<common::String*>(i);
                        if (str == nullptr) {
                            std::cout << "null";
                        } else {
                            std::cout << std::string(str->buf_, str->len_);
                        }
                        break;
                    }
                    default: {
                        std::cout << "<UNKNOWN>";
                        break;
                    }
                }
            }
            if (i < column_count) {
                std::cout << "\t";
            }
        }
        std::cout << std::endl;
        row_count++;
    }
    std::cout << "Total rows: " << row_count << std::endl;
}

}  // namespace storage

#endif  // READER_QUERY_DATA_SET_H
