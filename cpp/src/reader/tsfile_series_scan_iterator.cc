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

#include "reader/tsfile_series_scan_iterator.h"

using namespace common;

namespace storage {

void TsFileSeriesScanIterator::destroy() {
    timeseries_index_pa_.destroy();
    if (chunk_reader_ != nullptr) {
        chunk_reader_->destroy();
        common::mem_free(chunk_reader_);
        chunk_reader_ = nullptr;
    }
    if (tsblock_ != nullptr) {
        delete tsblock_;
        tsblock_ = nullptr;
    }
}

bool TsFileSeriesScanIterator::should_skip_chunk_by_time(
    ChunkMeta* cm, int64_t min_time_hint) {
    if (min_time_hint == std::numeric_limits<int64_t>::min() ||
        cm->statistic_ == nullptr) {
        return false;
    }
    return cm->statistic_->end_time_ < min_time_hint;
}

bool TsFileSeriesScanIterator::should_skip_chunk_by_offset(ChunkMeta* cm) {
    if (row_offset_ <= 0) {
        return false;
    }
    if (cm->statistic_ == nullptr || cm->statistic_->count_ == 0) {
        return false;
    }
    int32_t count = cm->statistic_->count_;
    if (row_offset_ >= count) {
        row_offset_ -= count;
        return true;
    }
    return false;
}

bool TsFileSeriesScanIterator::should_skip_aligned_chunk_by_offset(
    ChunkMeta* time_cm, ChunkMeta* value_cm) {
    if (row_offset_ <= 0) {
        return false;
    }
    if (time_cm->statistic_ == nullptr || value_cm->statistic_ == nullptr) {
        return false;
    }
    int32_t tc = time_cm->statistic_->count_;
    int32_t vc = value_cm->statistic_->count_;
    if (tc <= 0 || vc <= 0) {
        return false;
    }
    if (tc != vc) {
        return false;
    }
    int32_t count = tc;
    if (row_offset_ >= count) {
        row_offset_ -= count;
        return true;
    }
    return false;
}

int TsFileSeriesScanIterator::get_next(TsBlock*& ret_tsblock, bool alloc,
                                       Filter* oneshoot_filter,
                                       int64_t min_time_hint) {
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr) ? oneshoot_filter : time_filter_;

    while (true) {
        if (!chunk_reader_->has_more_data()) {
            while (true) {
                if (!has_next_chunk()) {
                    return E_NO_MORE_DATA;
                } else {
                    if (!is_aligned_) {
                        ChunkMeta* cm = get_current_chunk_meta();
                        advance_to_next_chunk();
                        // Skip by time filter.
                        if (filter != nullptr && cm->statistic_ != nullptr &&
                            !filter->satisfy(cm->statistic_)) {
                            continue;
                        }
                        // Skip by min_time_hint (merge cursor).
                        if (should_skip_chunk_by_time(cm, min_time_hint)) {
                            continue;
                        }
                        // Single-path: skip entire chunk by offset using count.
                        if (should_skip_chunk_by_offset(cm)) {
                            continue;
                        }
                        chunk_reader_->reset();
                        if (RET_FAIL(chunk_reader_->load_by_meta(cm))) {
                        }
                        break;
                    } else {
                        ChunkMeta* value_cm = value_chunk_meta_cursor_.get();
                        ChunkMeta* time_cm = time_chunk_meta_cursor_.get();
                        advance_to_next_chunk();
                        if (filter != nullptr &&
                            value_cm->statistic_ != nullptr &&
                            !filter->satisfy(value_cm->statistic_)) {
                            continue;
                        }
                        if (should_skip_chunk_by_time(value_cm,
                                                      min_time_hint)) {
                            continue;
                        }
                        if (should_skip_aligned_chunk_by_offset(time_cm,
                                                                value_cm)) {
                            continue;
                        }
                        chunk_reader_->reset();
                        if (RET_FAIL(chunk_reader_->load_by_aligned_meta(
                                time_cm, value_cm))) {
                        }
                        break;
                    }
                }
            }
        }
        if (IS_SUCC(ret)) {
            if (alloc && ret_tsblock == nullptr) {
                ret_tsblock = alloc_tsblock();
            }
            ret = chunk_reader_->get_next_page(ret_tsblock, filter, *data_pa_,
                                               min_time_hint, row_offset_,
                                               row_limit_);
        }
        // When current chunk is exhausted (e.g. all pages skipped by offset)
        // but there are more chunks, load next chunk and retry.
        if (ret == common::E_NO_MORE_DATA && has_next_chunk()) {
            ret = E_OK;
            continue;
        }
        return ret;
    }
}

void TsFileSeriesScanIterator::revert_tsblock() {
    if (tsblock_ == nullptr) {
        return;
    }
    delete tsblock_;
    tsblock_ = nullptr;
}

int TsFileSeriesScanIterator::init_chunk_reader() {
    int ret = E_OK;
    is_aligned_ = itimeseries_index_->is_aligned();
    if (!is_aligned_) {
        void* buf =
            common::mem_alloc(sizeof(ChunkReader), common::MOD_CHUNK_READER);
        chunk_reader_ = new (buf) ChunkReader;
        chunk_meta_cursor_ = itimeseries_index_->get_chunk_meta_list()->begin();
        if (RET_FAIL(chunk_reader_->init(
                read_file_, itimeseries_index_->get_measurement_name(),
                itimeseries_index_->get_data_type(), time_filter_))) {
        }
    } else {
        void* buf = common::mem_alloc(sizeof(AlignedChunkReader),
                                      common::MOD_CHUNK_READER);
        chunk_reader_ = new (buf) AlignedChunkReader;
        time_chunk_meta_cursor_ =
            itimeseries_index_->get_time_chunk_meta_list()->begin();
        value_chunk_meta_cursor_ =
            itimeseries_index_->get_value_chunk_meta_list()->begin();
        if (RET_FAIL(chunk_reader_->init(
                read_file_, itimeseries_index_->get_measurement_name(),
                itimeseries_index_->get_data_type(), time_filter_))) {
        }
    }

    return ret;
}

TsBlock* TsFileSeriesScanIterator::alloc_tsblock() {
    ChunkHeader& ch = chunk_reader_->get_chunk_header();

    // TODO config
    ColumnSchema time_cd("time", common::INT64, common::SNAPPY,
                         common::TS_2DIFF);
    ColumnSchema value_cd(ch.measurement_name_, ch.data_type_,
                          ch.compression_type_, ch.encoding_type_);

    tuple_desc_.push_back(time_cd);
    tuple_desc_.push_back(value_cd);

    tsblock_ = new TsBlock(&tuple_desc_);
    if (E_OK != tsblock_->init()) {
        delete tsblock_;
        tsblock_ = nullptr;
    }
    return tsblock_;
}

}  // end namespace storage