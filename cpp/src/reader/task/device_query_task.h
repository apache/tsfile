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
#ifndef READER_TASK_DEVICE_QUERY_TASK_H
#define READER_TASK_DEVICE_QUERY_TASK_H

#include "common/device_id.h"
#include "reader/column_mapping.h"

namespace storage {
class DeviceQueryTask {
   public:
    DeviceQueryTask(std::shared_ptr<IDeviceID> device_id,
                    std::vector<std::string> column_names,
                    std::shared_ptr<ColumnMapping> column_mapping,
                    MetaIndexNode *index_root,
                    std::shared_ptr<TableSchema> table_schema)
        : device_id_(device_id),
          column_names_(column_names),
          column_mapping_(column_mapping),
          index_root_(index_root),
          table_schema_(table_schema) {}
    ~DeviceQueryTask();

    static DeviceQueryTask *create_device_query_task(
        std::shared_ptr<IDeviceID> device_id,
        std::vector<std::string> column_names,
        std::shared_ptr<ColumnMapping> column_mapping,
        MetaIndexNode *index_root, std::shared_ptr<TableSchema> table_schema,
        common::PageArena &pa);

    const std::vector<std::string> &get_column_names() const {
        return column_names_;
    }

    std::shared_ptr<TableSchema> get_table_schema() const {
        return table_schema_;
    }

    const MetaIndexNode *get_index_root() const { return index_root_; }

    const std::shared_ptr<ColumnMapping> &get_column_mapping() const {
        return column_mapping_;
    }

    std::shared_ptr<IDeviceID> get_device_id() const { return device_id_; }

   private:
    std::shared_ptr<IDeviceID> device_id_;
    std::vector<std::string> column_names_;
    std::shared_ptr<ColumnMapping> column_mapping_;
    MetaIndexNode *index_root_;
    std::shared_ptr<TableSchema> table_schema_;
};

}  // namespace storage

#endif  // READER_TASK_DEVICE_QUERY_TASK_H