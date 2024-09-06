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

#include "common/tsfile_common.h"

#include <algorithm>
#include <map>

#include "common/logger/elog.h"

using namespace common;
namespace storage {

const char *MAGIC_STRING_TSFILE = "TsFile";
const int MAGIC_STRING_TSFILE_LEN = 6;
const char VERSION_NUM_BYTE = 0x03;
const char CHUNK_GROUP_HEADER_MARKER = 0;
const char CHUNK_HEADER_MARKER = 1;
const char ONLY_ONE_PAGE_CHUNK_HEADER_MARKER = 5;
const char SEPARATOR_MARKER = 2;
const char OPERATION_INDEX_RANGE = 4;

/* ================ TimeseriesIndex ================ */
int TimeseriesIndex::add_chunk_meta(ChunkMeta *chunk_meta,
                                    bool serialize_statistic) {
    int ret = E_OK;
    if (IS_NULL(chunk_meta)) {
        ret = E_INVALID_ARG;
    } else if (RET_FAIL(chunk_meta->serialize_to(
                   chunk_meta_list_serialized_buf_, serialize_statistic))) {
    } else if (RET_FAIL(statistic_->merge_with(chunk_meta->statistic_))) {
    }
    return ret;
}

/* ================ TSMIterator ================ */
int TSMIterator::init() {
    // sort chunk_group_meta_list_ ： {[measurementA, offsetA1], [measurementB,
    // offsetB1], [measurementA, offsetA2], [measurementB, offsetB2]} ->
    // {[measurementA, offsetA1], [measurementA, offsetA2], [measurementB,
    // offsetB1], [measurementB, offsetB2]}
    for (auto chunk_group_meta_iter = chunk_group_meta_list_.begin();
         chunk_group_meta_iter != chunk_group_meta_list_.end();
         chunk_group_meta_iter++) {
        auto chunk_meta_list = chunk_group_meta_iter.get()->chunk_meta_list_;
        // Use a map to group chunks by measurement_name_
        std::map<common::String, std::vector<ChunkMeta *>> groups;
        std::vector<common::String> order;
        for (auto it = chunk_meta_list.begin(); it != chunk_meta_list.end();
             it++) {
            auto *chunk_meta = it.get();
            if (groups.find(chunk_meta->measurement_name_) == groups.end()) {
                order.push_back(chunk_meta->measurement_name_);
            }
            groups[chunk_meta->measurement_name_].push_back(chunk_meta);
        }

        // Sort each group of chunk metas by offset
        for (auto it = groups.begin(); it != groups.end(); ++it) {
            std::vector<ChunkMeta *> &group = it->second;
            std::sort(group.begin(), group.end(),
                      [](ChunkMeta *a, ChunkMeta *b) {
                          return a->offset_of_chunk_header_ <
                                 b->offset_of_chunk_header_;
                      });
        }
        // Clear and refill chunk_group_meta_list
        chunk_group_meta_iter.get()->chunk_meta_list_.clear();
        for (const auto &measurement_name : order) {
            for (auto chunk_meta : groups[measurement_name]) {
                chunk_group_meta_iter.get()->chunk_meta_list_.push_back(
                    chunk_meta);
            }
        }
    }

    // FIXME empty list
    chunk_group_meta_iter_ = chunk_group_meta_list_.begin();
    if (chunk_group_meta_iter_ == chunk_group_meta_list_.end()) {
        return E_NOT_EXIST;
    }
    while (chunk_group_meta_iter_ != chunk_group_meta_list_.end()) {
        chunk_meta_iter_ =
            chunk_group_meta_iter_.get()->chunk_meta_list_.begin();
        std::map<common::String, std::vector<ChunkMeta *>> tmp;
        while (chunk_meta_iter_ !=
               chunk_group_meta_iter_.get()->chunk_meta_list_.end()) {
            tmp[chunk_meta_iter_.get()->measurement_name_].emplace_back(
                chunk_meta_iter_.get());
            chunk_meta_iter_++;
        }
        if (!tmp.empty()) {
            tsm_chunk_meta_info_[chunk_group_meta_iter_.get()->device_name_] =
                tmp;
        }

        chunk_group_meta_iter_++;
    }
    tsm_measurement_iter_ = tsm_chunk_meta_info_.begin()->second.begin();
    tsm_device_iter_ = tsm_chunk_meta_info_.begin();
    return E_OK;
}

bool TSMIterator::has_next() const {
    return tsm_device_iter_ != tsm_chunk_meta_info_.end();
}

int TSMIterator::get_next(String &ret_device_name, String &ret_measurement_name,
                          TimeseriesIndex &ret_ts_index) {
    int ret = E_OK;
    SimpleList<ChunkMeta *> chunk_meta_list_of_this_ts(
        1024, MOD_TIMESERIES_INDEX_OBJ);  // FIXME
    if (tsm_measurement_iter_ == tsm_device_iter_->second.end()) {
        tsm_device_iter_++;
        if (!has_next()) {
            return E_NO_MORE_DATA;
        } else {
            tsm_measurement_iter_ = tsm_device_iter_->second.begin();
        }
    }
    ret_device_name.shallow_copy_from(tsm_device_iter_->first);
    ret_measurement_name.shallow_copy_from(tsm_measurement_iter_->first);
    for (auto meta : tsm_measurement_iter_->second) {
        chunk_meta_list_of_this_ts.push_back(meta);
    }
    if (chunk_meta_list_of_this_ts.size() == 0) {
        return E_TSFILE_WRITER_META_ERR;
    }

    const bool multi_chunks = chunk_meta_list_of_this_ts.size() > 1;
    ChunkMeta *first_chunk_meta = chunk_meta_list_of_this_ts.front();
    const char meta_type = (multi_chunks ? 1 : 0) | (first_chunk_meta->mask_);
    const TSDataType data_type = first_chunk_meta->data_type_;
    const TsID ts_id = first_chunk_meta->ts_id_;

    ret_ts_index.set_ts_meta_type(meta_type);
    ret_ts_index.set_measurement_name(ret_measurement_name);
    ret_ts_index.set_data_type(data_type);
    ret_ts_index.init_statistic(data_type);
    ret_ts_index.set_ts_id(ts_id);

    SimpleList<ChunkMeta *>::Iterator ts_chunk_meta_iter =
        chunk_meta_list_of_this_ts.begin();
    for (;
         IS_SUCC(ret) && ts_chunk_meta_iter != chunk_meta_list_of_this_ts.end();
         ts_chunk_meta_iter++) {
        ChunkMeta *chunk_meta = ts_chunk_meta_iter.get();
        if (RET_FAIL(ret_ts_index.add_chunk_meta(chunk_meta, multi_chunks))) {
        }
    }
    if (IS_SUCC(ret)) {
        ret_ts_index.finish();
    }
    if (UNLIKELY(ret_device_name.is_null())) {
        ret = E_TSFILE_WRITER_META_ERR;
        // log_err("null device name from chunk_group_meta_iter, ret=%d", ret);
        ASSERT(false);
    }
    tsm_measurement_iter_++;
    return ret;
}

/* ================ MetaIndexNode ================ */

struct MetaIndexEntryComp {
    bool operator()(MetaIndexEntry *entry, const String &target_name) {
        return entry->name_.less_than(target_name);
    }
};

int MetaIndexNode::binary_search_children(const String &name, bool exact_search,
                                          MetaIndexEntry &ret_index_entry,
                                          int64_t &ret_end_offset) {
#if DEBUG_SE
    std::cout << "MetaIndexNode::binary_search_children start, name=" << name
              << ", exact_search=" << exact_search
              << ", children_.size=" << children_.size() << std::endl;
    for (int i = 0; i < (int)children_.size(); i++) {
        std::cout << "Iterating children: " << children_[i]->name_ << std::endl;
    }
#endif
    bool is_aligned = false;
    if (node_type_ == LEAF_MEASUREMENT && children_.size() == 1 &&
        children_[0]->name_.len_ == 0) {
        is_aligned = true;
    }
    // children_[l] <= name < children_[h]
    int l = -1;
    if (is_aligned) {
        l = 0;
    } else {
        int h = (int)children_.size();
        bool found = false;
        while (l < h - 1) {
            int m = (l + h) / 2;
            int cmp = children_[m]->name_.compare(name);
#if DEBUG_SE
            std::cout
                << "MetaIndexNode::binary_search_children doing, cmp: cur="
                << children_[m]->name_ << ", name=" << name
                << ", exact_search=" << exact_search
                << ", children_.size=" << children_.size() << std::endl;
#endif
            if (cmp == 0) {
                l = m;
                found = true;
                break;
            } else if (cmp > 0) {  // children_[m] > name
                h = m;
            } else {  // children_[m] < name
                l = m;
            }
        }
        if ((l == -1) || (exact_search && !found)) {
#if DEBUG_SE
            std::cout << "MetaIndexNode::binary_search_children end, "
                         "ret=E_NOT_EXIST, name="
                      << name << ", exact_search=" << exact_search << std::endl;
#endif
            return E_NOT_EXIST;
        }
    }
    ret_index_entry = *children_[l];
    if (l == (int)children_.size() - 1) {
        ret_end_offset = this->end_offset_;
    } else {
        ret_end_offset = children_[l + 1]->offset_;
    }
#if DEBUG_SE
    std::cout << "MetaIndexNode::binary_search_children end, ret_index_entry="
              << ret_index_entry << ", ret_end_offset=" << ret_end_offset
              << ", name=" << name << ", exact_search=" << exact_search
              << std::endl;
#endif
    return E_OK;
}

#if 0
int MetaIndexNode::binary_search_children(const String &name,
                                          bool exact_search,
                                          MetaIndexEntry &ret_index_entry,
                                          int64_t &ret_end_offset)
{
  // TODO currently, we do sequence search.
  // We will change it to binary search after replacing SimpleList with SimpleVector
  SimpleList<MetaIndexEntry*>::Iterator it;
  SimpleList<MetaIndexEntry*>::Iterator prev_it;
  SimpleList<MetaIndexEntry*>::Iterator target_it;
  for (it = children_.begin(); it != children_.end(); it++) {
    int cmp_res = it.get()->name_.compare(name);
    if (cmp_res == 0) {
      target_it = it;
      break;
    } else if (cmp_res < 0) {
      prev_it = it;
    } else {
      break;
    }
  } // end for

  if (exact_search && target_it == children_.end()) {
    return E_NOT_EXIST;
  } else {
    if (target_it == children_.end()) {
      target_it = prev_it;
    }
    ret_index_entry = *(target_it.get());
    target_it++;
    if (target_it == children_.end()) {
      ret_end_offset = this->end_offset_;
    } else {
      ret_end_offset = target_it.get()->offset_;
    }
  }
  return E_OK;
}
#endif

}  // end namespace storage