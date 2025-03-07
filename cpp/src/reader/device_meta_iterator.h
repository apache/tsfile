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

#ifndef READER_DEVICE_META_ITERATOR_H
#define READER_DEVICE_META_ITERATOR_H

#include <queue>

#include "file/tsfile_io_reader.h"
#include "reader/expression.h"

namespace storage {

class DeviceMetaIterator {
   public:
    explicit DeviceMetaIterator(TsFileIOReader *io_reader,
                                MetaIndexNode *meat_index_node,
                                const Filter *id_filter)
        : io_reader_(io_reader), id_filter_(id_filter) {
        meta_index_nodes_.push(meat_index_node);
        pa_.init(512, common::MOD_DEVICE_META_ITER);
    }

    ~DeviceMetaIterator() { pa_.destroy(); }

    bool has_next();

    int next(std::pair<std::shared_ptr<IDeviceID>, MetaIndexNode *> &ret_meta);

   private:
    int load_results();
    int load_leaf_device(MetaIndexNode *meta_index_node);
    int load_internal_node(MetaIndexNode *meta_index_node);
    TsFileIOReader *io_reader_;
    std::queue<MetaIndexNode *> meta_index_nodes_;
    std::queue<std::pair<std::shared_ptr<IDeviceID>, MetaIndexNode *>>
        result_cache_;
    const Filter *id_filter_;
    common::PageArena pa_;
};

}  // end namespace storage
#endif  // READER_DEVICE_META_ITERATOR_H