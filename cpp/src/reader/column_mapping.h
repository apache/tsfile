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
#ifndef READER_COLUMN_MAPPING_H
#define READER_COLUMN_MAPPING_H

#include "common/schema.h"
#include "expression.h"
namespace storage {
class ColumnMapping {
   public:
    int add(const std::string &column_name, int index, TableSchema &schema) {
        int column_index = schema.find_column_index(column_name);
        if (column_index < 0) {
            return common::E_COLUMN_NOT_EXIST;
        }

        common::ColumnCategory column_category =
            schema.get_column_categories()[column_index];
        column_pos_map[column_name].push_back(index);

        if (column_category == common::ColumnCategory::TAG) {
            tag_columns_.insert(column_name);
        } else {
            field_columns_.insert(column_name);
        }

        return common::E_OK;
    }

    int add(const Expression &measurementFilter) {
        // TODO: get measurements in the filter and add them to
        // field_columns_
        return common::E_OK;
    }

    const std::vector<int> &get_column_pos(
        const std::string &column_name) const {
        static const std::vector<int> empty;
        auto it = column_pos_map.find(column_name);
        return it != column_pos_map.end() ? it->second : empty;
    }

    bool is_tag(const std::string &column_name) const {
        return tag_columns_.find(column_name) != tag_columns_.end();
    }

    bool is_field(const std::string &column_name) const {
        return field_columns_.find(column_name) != field_columns_.end();
    }

    const std::unordered_set<std::string> &get_id_columns() const {
        return tag_columns_;
    }

    const std::unordered_set<std::string> &get_measurement_columns() const {
        return field_columns_;
    }

   private:
    std::unordered_map<std::string, std::vector<int>> column_pos_map;
    std::unordered_set<std::string> tag_columns_;
    std::unordered_set<std::string> field_columns_;
};

}  // namespace storage

#endif  // READER_COLUMN_MAPPING_H