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

#include "device_meta_iterator.h"

namespace storage {
bool DeviceMetaIterator::has_next() {
    if (!result_cache_.empty()) {
        return true;
    }

    if (load_results() != common::E_OK) {
        return false;
    }

    return !result_cache_.empty();
}

int DeviceMetaIterator::next(
    std::pair<std::shared_ptr<IDeviceID>, MetaIndexNode*>& ret_meta) {
    if (!has_next()) {
        return common::E_NO_MORE_DATA;
    }

    ret_meta = result_cache_.front();
    result_cache_.pop();
    return common::E_OK;
}

int DeviceMetaIterator::load_results() {
    while (!meta_index_nodes_.empty()) {
        // To avoid ASan overflow.
        // using `const auto&` creates a reference
        // to a queue element that may become invalid.
        auto meta_data_index_node = meta_index_nodes_.front();
        meta_index_nodes_.pop();
        const auto& node_type = meta_data_index_node->node_type_;
        if (node_type == MetaIndexNodeType::LEAF_DEVICE) {
            load_leaf_device(meta_data_index_node);
        } else if (node_type == MetaIndexNodeType::INTERNAL_DEVICE) {
            load_internal_node(meta_data_index_node);
        } else {
            return common::E_INVALID_NODE_TYPE;
        }
    }

    return common::E_OK;
}

int DeviceMetaIterator::load_leaf_device(MetaIndexNode* meta_index_node) {
    int ret = common::E_OK;
    const auto& leaf_children = meta_index_node->children_;
    for (size_t i = 0; i < leaf_children.size(); i++) {
        std::shared_ptr<IMetaIndexEntry> child = leaf_children[i];
        // const auto& device_id = child->name_;
        if (id_filter_ != nullptr /*TODO: !id_filter_->satisfy(device_id)*/) {
            continue;
        }
        int32_t start_offset = child->get_offset();
        int32_t end_offset = i + 1 < leaf_children.size()
                                 ? leaf_children[i + 1]->get_offset()
                                 : meta_index_node->end_offset_;
        MetaIndexNode* child_node = nullptr;
        if (RET_FAIL(io_reader_->read_device_meta_index(
                start_offset, end_offset, pa_, child_node))) {
            return ret;
        } else {
            result_cache_.push(
                std::make_pair(child->get_device_id(), child_node));
        }
    }
    return ret;
}

int DeviceMetaIterator::load_internal_node(MetaIndexNode* meta_index_node) {
    int ret = common::E_OK;
    const auto& internal_children = meta_index_node->children_;

    for (size_t i = 0; i < internal_children.size(); i++) {
        std::shared_ptr<IMetaIndexEntry> child = internal_children[i];
        int32_t start_offset = child->get_offset();
        int32_t end_offset = (i + 1 < internal_children.size())
                                 ? internal_children[i + 1]->get_offset()
                                 : meta_index_node->end_offset_;

        MetaIndexNode* child_node = nullptr;
        if (RET_FAIL(io_reader_->read_device_meta_index(
                start_offset, end_offset, pa_, child_node))) {
            return ret;
        } else {
            meta_index_nodes_.push(child_node);
        }
    }
    return ret;
}
}  // namespace storage