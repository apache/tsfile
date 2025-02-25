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
#ifndef READER_TASK_DEVICE_TASK_ITERATOR_H
#define READER_TASK_DEVICE_TASK_ITERATOR_H

#include "common/device_id.h"
#include "reader/imeta_data_querier.h"
#include "reader/task/device_query_task.h"

namespace storage {

class ColumnMapping;
class DeviceQueryTask;

class DeviceTaskIterator {
   public:
    explicit DeviceTaskIterator(std::vector<std::string> column_names,
                                MetaIndexNode *index_root,
                                std::shared_ptr<ColumnMapping> column_mapping,
                                IMetadataQuerier *metadata_querier,
                                const Filter *id_filter,
                                std::shared_ptr<TableSchema> table_schema)
        : column_names_(column_names),
          column_mapping_(column_mapping),
          device_meta_iterator_(
              metadata_querier->device_iterator(index_root, id_filter)),
          table_schema_(table_schema) {
        pa_.init(512, common::MOD_DEVICE_TASK_ITER);
    }
    ~DeviceTaskIterator() { pa_.destroy(); }

    bool has_next() const;

    int next(DeviceQueryTask *&task);

   private:
    std::vector<std::string> column_names_;
    std::shared_ptr<ColumnMapping> column_mapping_;
    std::unique_ptr<DeviceMetaIterator> device_meta_iterator_;
    std::shared_ptr<TableSchema> table_schema_;
    common::PageArena pa_;
};

}  // namespace storage

#endif  // READER_TASK_DEVICE_TASK_ITERATOR_H