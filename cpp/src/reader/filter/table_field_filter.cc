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
#include "reader/filter/table_field_filter.h"

namespace storage {
namespace {
template <typename T>
int evaluate_integer(Filter& filter, const TableFieldRow& row,
                     bool& satisfied) {
    T value;
    if (!row.fields[0].get(value)) return common::E_DECODE_ERR;
    satisfied = filter.satisfy(row.timestamp, static_cast<int64_t>(value));
    return common::E_OK;
}
}  // namespace

int evaluate_table_field_filter(Filter& filter, const TableFieldRow& row,
                                bool& satisfied) {
    satisfied = false;
    if (auto* row_filter = dynamic_cast<TableFieldFilter*>(&filter)) {
        return row_filter->satisfy_row(row, satisfied);
    }
    if (row.fields.size() != 1) return common::E_NOT_SUPPORT;
    const auto& field = row.fields[0];
    if (field.is_null()) return common::E_OK;
    switch (field.type) {
        case common::BOOLEAN:
            return evaluate_integer<bool>(filter, row, satisfied);
        case common::DATE:
        case common::INT32:
            return evaluate_integer<int32_t>(filter, row, satisfied);
        case common::TIMESTAMP:
        case common::INT64:
            return evaluate_integer<int64_t>(filter, row, satisfied);
        case common::TEXT:
        case common::STRING:
        case common::BLOB:
            satisfied = filter.satisfy(row.timestamp,
                                       common::String(field.data, field.size));
            return common::E_OK;
        default:
            return common::E_NOT_SUPPORT;
    }
}
}  // namespace storage
