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
#ifndef COMMON_TSBLOCK_TUPLE_DESC_H
#define COMMON_TSBLOCK_TUPLE_DESC_H

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "utils/db_utils.h"
#include "utils/errno_define.h"

namespace common {

/*
 * Anyway, a default length of text type is needed for easy calculation,
 * and it will be changed to a configurable item later
 */
#define DEFAULT_RESERVED_SIZE_OF_TEXT 16
#define TEXT_LEN 4

extern uint32_t get_len(TSDataType type);

// Describe merged information of multiple timeseries
class TupleDesc {
   public:
    TupleDesc() {}
    virtual ~TupleDesc() {}

    FORCE_INLINE void reset() { column_list_.clear(); }

    FORCE_INLINE void push_back(ColumnDesc &desc) {
        column_list_.push_back(desc);
    }

    FORCE_INLINE uint32_t get_column_count() const {
        return column_list_.size();
    }

    FORCE_INLINE ColumnDesc &operator[](uint32_t index) {
        ASSERT(index < column_list_.size());
        return column_list_[index];
    }

    FORCE_INLINE ColumnDesc &get_column_desc(uint32_t index) {
        ASSERT(index < column_list_.size());
        return column_list_[index];
    }

    FORCE_INLINE common::TSDataType get_column_type(uint32_t index) {
        return column_list_[index].type_;
    }

    FORCE_INLINE std::string get_column_name(uint32_t index) {
        return column_list_[index].column_name_;
    }

    FORCE_INLINE void remove_column(uint32_t idx) {
        column_list_.erase(column_list_.begin() + idx);
    }

    // get the single row len, ignore nulls and select-list memory for the
    // moment
    uint32_t get_single_row_len(int *erro_code);

    bool equal_to(const TupleDesc &that) const {
        if (column_list_.size() != that.column_list_.size()) {
            return false;
        }
        for (uint32_t i = 0; i < column_list_.size(); i++) {
            const ColumnDesc &this_col_desc = column_list_[i];
            const ColumnDesc &that_col_desc = that.column_list_[i];
            if (this_col_desc != that_col_desc) {
                return false;
            }
        }
        return true;
    }

    void clone_from(TupleDesc *that) {
        ASSERT(column_list_.size() == 0);
        column_list_ = that->column_list_;  // deep copy
    }

    std::string debug_string()  // for debug
    {
        std::stringstream out;
        for (size_t i = 0; i < column_list_.size(); ++i) {
            out << column_list_[i].debug_string() << std::endl;
        }
        return out.str();
    }

   private:
    std::vector<ColumnDesc> column_list_;
};

}  // namespace common

#endif  // COMMON_TSBLOCK_TUPLE_DESC_H
