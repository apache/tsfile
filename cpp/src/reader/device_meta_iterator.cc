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

#include "filter/tag_filter.h"

namespace storage {

void DeviceMetaIterator::destroy_remaining_cached_devices() {
    while (!result_cache_.empty()) {
        auto p = result_cache_.front();
        result_cache_.pop();
        if (p.second != nullptr) {
            p.second->~MetaIndexNode();
        }
    }
}

DeviceMetaIterator::~DeviceMetaIterator() {
    destroy_remaining_cached_devices();
    while (!meta_index_nodes_.empty()) {
        auto pending = meta_index_nodes_.front();
        meta_index_nodes_.pop();
        if (pending.second) {
            pending.first->~MetaIndexNode();
        }
    }
    pa_.destroy();
}

int DeviceMetaIterator::has_next(bool& has_next) {
    has_next = false;
    if (read_error_ != common::E_OK) {
        return read_error_;
    }
    if (!result_cache_.empty()) {
        has_next = true;
        return common::E_OK;
    }

    if (direct_device_id_ != nullptr) {
        if (direct_lookup_done_) {
            return common::E_OK;
        }
        read_error_ = load_results_direct();
    } else {
        read_error_ = load_results();
    }
    if (read_error_ == common::E_OK) {
        has_next = !result_cache_.empty();
    }
    return read_error_;
}

int DeviceMetaIterator::next(
    std::pair<std::shared_ptr<IDeviceID>, MetaIndexNode*>& ret_meta) {
    bool available = false;
    const int ret = has_next(available);
    if (ret != common::E_OK) {
        return ret;
    }
    if (!available) {
        return common::E_NO_MORE_DATA;
    }

    ret_meta = result_cache_.front();
    result_cache_.pop();
    return common::E_OK;
}

int DeviceMetaIterator::load_results() {
    while (!meta_index_nodes_.empty()) {
        auto pending = meta_index_nodes_.front();
        meta_index_nodes_.pop();
        auto* node = pending.first;
        int ret = common::E_OK;
        if (node->node_type_ == MetaIndexNodeType::LEAF_DEVICE) {
            ret = load_leaf_device(node);
        } else if (node->node_type_ == MetaIndexNodeType::INTERNAL_DEVICE) {
            ret = load_internal_node(node);
        } else {
            ret = common::E_INVALID_NODE_TYPE;
        }
        if (pending.second) {
            node->~MetaIndexNode();
        }
        if (ret != common::E_OK) {
            return ret;
        }
    }
    return common::E_OK;
}

int DeviceMetaIterator::load_leaf_device(MetaIndexNode* meta_index_node) {
    int ret = common::E_OK;
    const auto& leaf_children = meta_index_node->children_;
    for (size_t i = 0; i < leaf_children.size(); i++) {
        std::shared_ptr<IMetaIndexEntry> child = leaf_children[i];
        if (id_filter_ != nullptr) {
            if (!id_filter_->satisfyRow(
                    0, child->get_device_id()->get_segments())) {
                continue;
            }
        }
        int64_t start_offset = child->get_offset();
        int64_t end_offset = i + 1 < leaf_children.size()
                                 ? leaf_children[i + 1]->get_offset()
                                 : meta_index_node->end_offset_;
        MetaIndexNode* child_node = nullptr;
        if (RET_FAIL(io_reader_->read_device_meta_index(
                start_offset, end_offset, pa_, child_node, true))) {
            return ret;
        } else {
            auto device_id = child->get_device_id();
            if (should_split_device_name) {
                device_id->split_table_name();
            }
            result_cache_.push(std::make_pair(device_id, child_node));
        }
    }
    return ret;
}

int DeviceMetaIterator::load_internal_node(MetaIndexNode* meta_index_node) {
    int ret = common::E_OK;
    const auto& internal_children = meta_index_node->children_;

    for (size_t i = 0; i < internal_children.size(); i++) {
        std::shared_ptr<IMetaIndexEntry> child = internal_children[i];
        int64_t start_offset = child->get_offset();
        int64_t end_offset = (i + 1 < internal_children.size())
                                 ? internal_children[i + 1]->get_offset()
                                 : meta_index_node->end_offset_;

        MetaIndexNode* child_node = nullptr;
        if (RET_FAIL(io_reader_->read_device_meta_index(
                start_offset, end_offset, pa_, child_node, false))) {
            return ret;
        } else {
            meta_index_nodes_.push({child_node, true});
        }
    }
    return ret;
}

void DeviceMetaIterator::try_setup_direct_lookup(MetaIndexNode* root_node) {
    if (id_filter_ == nullptr || root_node == nullptr) return;

    const auto* eq = dynamic_cast<const TagEq*>(id_filter_);
    if (eq == nullptr) return;

    if (root_node->children_.empty()) return;

    auto first_device = root_node->children_[0]->get_device_id();
    if (first_device == nullptr) return;

    auto first_segments = first_device->get_segments();
    int actual_segment_count = static_cast<int>(first_segments.size());

    if (actual_segment_count != 2) return;

    std::string table_name = first_device->get_table_name();
    std::vector<std::string> segs(actual_segment_count);
    segs[0] = table_name;
    for (int i = 1; i < actual_segment_count; i++) {
        segs[i] = "";
    }
    segs[eq->col_idx_] = eq->value_;
    direct_device_id_ = std::make_shared<StringArrayDeviceID>(segs);
    direct_root_node_ = root_node;
}

int DeviceMetaIterator::load_results_direct() {
    int ret = common::E_OK;
    direct_lookup_done_ = true;

    if (direct_device_id_ == nullptr) {
        return common::E_OK;
    }

    auto device_comparable =
        std::make_shared<DeviceIDComparable>(direct_device_id_);

    std::shared_ptr<IMetaIndexEntry> device_index_entry;
    int64_t end_offset = 0;

    ret = io_reader_->load_device_index_entry(device_comparable,
                                              device_index_entry, end_offset);

    // "Device not present in this file" is the only ret value we should
    // suppress.  Read failures and corrupt index entries used to be folded
    // into "no matches"; the caller then couldn't distinguish a clean miss
    // from a partial read that silently dropped real data.  Surface them.
    if (ret == common::E_DEVICE_NOT_EXIST || ret == common::E_NOT_EXIST) {
        return common::E_OK;
    }
    if (ret != common::E_OK) {
        return ret;
    }
    if (device_index_entry == nullptr) {
        return common::E_OK;
    }

    int64_t start_offset = device_index_entry->get_offset();
    MetaIndexNode* child_node = nullptr;
    if (RET_FAIL(io_reader_->read_device_meta_index(start_offset, end_offset,
                                                    pa_, child_node, true))) {
        return ret;
    }

    auto device_id = device_index_entry->get_device_id();
    if (should_split_device_name) {
        device_id->split_table_name();
    }
    result_cache_.push(std::make_pair(device_id, child_node));

    return common::E_OK;
}

}  // namespace storage
