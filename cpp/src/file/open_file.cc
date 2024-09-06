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

#include "open_file.h"

using namespace common;

namespace storage {

int OpenFile::init() {
    void *buf = mem_alloc(sizeof(TsTimeRangeMap), MOD_OPEN_FILE_OBJ);
    if (IS_NULL(buf)) {
        return E_OOM;
    }
    ts_time_range_map_ = new (buf) TsTimeRangeMap;
    return E_OK;
}

void OpenFile::reset() {
    MutexGuard mg(mutex_);
    if (ts_time_range_map_ != nullptr) {
        ts_time_range_map_->clear();
        mem_free(ts_time_range_map_);
        ts_time_range_map_ = nullptr;
    }
    if (bloom_filter_ != nullptr) {
        mem_free(bloom_filter_);
        bloom_filter_ = nullptr;
    }
}

void OpenFile::set_file_id_and_path(const FileID &file_id,
                                    const std::string &file_path) {
    ASSERT(file_id.is_valid());
    file_id_ = file_id;
    file_path_ = file_path;
}

int OpenFile::build_from(
    const std::vector<TimeseriesTimeIndexEntry> &time_index_vec) {
    MutexGuard mg(mutex_);
    ASSERT(ts_time_range_map_ != nullptr);
    for (size_t i = 0; i < time_index_vec.size(); i++) {
        const TimeseriesTimeIndexEntry &ti_entry = time_index_vec[i];
        std::pair<TsID, TimeRange> pair;
        pair.first = ti_entry.ts_id_;
        pair.second = ti_entry.time_range_;
        std::pair<TsTimeRangeMapIterator, bool> ins_res =
            ts_time_range_map_->insert(pair);
        if (!ins_res.second) {
            return E_OOM;
        }
    }
    return E_OK;
}

int OpenFile::add(const TsID &ts_id, const TimeRange &time_range) {
    MutexGuard mg(mutex_);
    TsTimeRangeMapIterator find_iter = ts_time_range_map_->find(ts_id);
    if (find_iter != ts_time_range_map_->end()) {
        ASSERT(false);
        return E_ALREADY_EXIST;
    }

    std::pair<TsID, TimeRange> pair;
    pair.first = ts_id;
    pair.second = time_range;
    std::pair<TsTimeRangeMapIterator, bool> ins_res =
        ts_time_range_map_->insert(pair);
    if (!ins_res.second) {
        return E_OOM;
    }
    return E_OK;
}

bool OpenFile::contain_timeseries(const TsID &ts_id) const {
    MutexGuard mg(mutex_);
    TsTimeRangeMapIterator find_iter = ts_time_range_map_->find(ts_id);
    return find_iter != ts_time_range_map_->end();
}

int OpenFile::get_time_range(const TsID &ts_id,
                             TimeRange &ret_time_range) const {
    int ret = E_OK;
    MutexGuard mg(mutex_);
    TsTimeRangeMapIterator find_iter = ts_time_range_map_->find(ts_id);
    if (find_iter == ts_time_range_map_->end()) {
        ret = E_NOT_EXIST;
    } else {
        ret_time_range = find_iter->second;
    }
    return ret;
}

}  // end namespace storage
