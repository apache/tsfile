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
#ifndef READER_TABLE_FIELD_FILTER_H
#define READER_TABLE_FIELD_FILTER_H

#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#include "reader/filter/filter.h"
#include "utils/errno_define.h"

namespace storage {

// Borrowed views: names and bytes are valid only during satisfy_row().
// A null data pointer denotes NULL; size preserves embedded NUL bytes.
struct TableFieldValue {
    const std::string& name;
    common::TSDataType type;
    const char* data;
    uint32_t size;

    bool is_null() const { return data == nullptr; }

    // Exact native primitive types; string-like values use data and size.
    template <typename T>
    bool get(T& value) const {
        const bool matches =
            (std::is_same<T, bool>::value && type == common::BOOLEAN) ||
            (std::is_same<T, int32_t>::value &&
             (type == common::INT32 || type == common::DATE)) ||
            (std::is_same<T, int64_t>::value &&
             (type == common::INT64 || type == common::TIMESTAMP)) ||
            (std::is_same<T, float>::value && type == common::FLOAT) ||
            (std::is_same<T, double>::value && type == common::DOUBLE);
        if (is_null() || !matches || size != sizeof(T)) {
            return false;
        }
        std::memcpy(&value, data, sizeof(T));
        return true;
    }
};

struct TableFieldRow {
    int64_t timestamp;
    // Unique projected FIELDs with normalized names, in unspecified order.
    const std::vector<TableFieldValue>& fields;

    const TableFieldValue* find(const std::string& name) const {
        for (const auto& field : fields) {
            if (field.name == name) return &field;
        }
        return nullptr;
    }
};

// Row predicates can inspect all projected FIELD columns, including NULLs.
// TAG predicates belong in query()'s tag_filter argument. Unprojected fields
// are not loaded automatically. An error is propagated through ResultSet.
class TableFieldFilter : public Filter {
   public:
    virtual int satisfy_row(const TableFieldRow& row,
                            bool& satisfied) const = 0;
};

// Legacy Filter callbacks support one selected integer/Boolean or string-like
// FIELD. NULL does not satisfy a legacy value predicate. Other legacy shapes
// return E_NOT_SUPPORT; use TableFieldFilter for floats or multiple fields.
int evaluate_table_field_filter(Filter& filter, const TableFieldRow& row,
                                bool& satisfied);

}  // namespace storage
#endif  // READER_TABLE_FIELD_FILTER_H
