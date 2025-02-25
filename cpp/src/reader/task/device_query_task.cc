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

#include "reader/task/device_query_task.h"

namespace storage {
DeviceQueryTask *DeviceQueryTask::create_device_query_task(
    std::shared_ptr<IDeviceID> device_id, std::vector<std::string> column_names,
    std::shared_ptr<ColumnMapping> column_mapping, MetaIndexNode *index_root,
    std::shared_ptr<TableSchema> table_schema, common::PageArena &pa) {
    void *buf = pa.alloc(sizeof(DeviceQueryTask));
    if (UNLIKELY(buf == nullptr)) {
        return nullptr;
    }
    DeviceQueryTask *task = new (buf) DeviceQueryTask(
        device_id, column_names, column_mapping, index_root, table_schema);
    return task;
}

 DeviceQueryTask::~DeviceQueryTask() {
    if (index_root_) {
        index_root_->~MetaIndexNode();
    }
}

}  // namespace storage
