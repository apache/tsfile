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
#include <string>
#include <vector>

#include "file/tsfile_io_reader.h"
#include "reader/expression.h"

namespace storage {

class DeviceMetaIterator {
   public:
    explicit DeviceMetaIterator(TsFileIOReader* io_reader,
                                MetaIndexNode* meat_index_node,
                                const Filter* id_filter)
        : io_reader_(io_reader),
          id_filter_(id_filter),
          should_split_device_name(false),
          direct_lookup_done_(false) {
        // A valid schema-only table has no device index.  Treat a null root as
        // an empty iterator instead of dereferencing it during has_next().
        if (meat_index_node != nullptr) {
            meta_index_nodes_.push(meat_index_node);
        }
        pa_.init(512, common::MOD_DEVICE_META_ITER);
        try_setup_direct_lookup(meat_index_node);
    }

    DeviceMetaIterator(TsFileIOReader* io_reader,
                       const std::vector<MetaIndexNode*>& meta_index_node_list,
                       const Filter* id_filter)
        : io_reader_(io_reader),
          id_filter_(id_filter),
          direct_lookup_done_(false) {
        for (auto meta_index_node : meta_index_node_list) {
            meta_index_nodes_.push(meta_index_node);
        }
        should_split_device_name = true;
        pa_.init(512, common::MOD_DEVICE_META_ITER);
    }

    ~DeviceMetaIterator();

    void destroy_remaining_cached_devices();

    bool has_next();

    int next(std::pair<std::shared_ptr<IDeviceID>, MetaIndexNode*>& ret_meta);

   private:
    int load_results();
    int load_leaf_device(MetaIndexNode* meta_index_node);
    int load_internal_node(MetaIndexNode* meta_index_node);

    void try_setup_direct_lookup(MetaIndexNode* root_node);
    int load_results_direct();

    TsFileIOReader* io_reader_;
    std::queue<MetaIndexNode*> meta_index_nodes_;
    std::queue<std::pair<std::shared_ptr<IDeviceID>, MetaIndexNode*>>
        result_cache_;
    const Filter* id_filter_;
    common::PageArena pa_;
    bool should_split_device_name;

    bool direct_lookup_done_;
    std::shared_ptr<IDeviceID> direct_device_id_;
    MetaIndexNode* direct_root_node_ = nullptr;
};

}  // end namespace storage
#endif  // READER_DEVICE_META_ITERATOR_H
