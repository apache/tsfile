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

#include <iostream>

#include "common/global.h"
#ifdef ENABLE_THREADS
#include "common/thread_pool.h"
#endif

using namespace common;

namespace storage {

void TsFileSeriesScanIterator::destroy() {
    // MultiAlignedTimeseriesIndex is placement-new'd inside
    // timeseries_index_pa_ (see TsFileIOReader::alloc_multi_ssi).  The arena's
    // destroy() frees raw memory without running destructors, so its
    // value_ts_idxs_ std::vector backing buffer would leak.  Release it
    // explicitly before tearing down the arena.  dynamic_cast is null-safe and
    // returns nullptr for the single-value / non-aligned index types, which own
    // no separate heap storage.
    if (auto* multi =
            dynamic_cast<MultiAlignedTimeseriesIndex*>(itimeseries_index_)) {
        std::vector<TimeseriesIndex*>().swap(multi->value_ts_idxs_);
    }
    itimeseries_index_ = nullptr;
    timeseries_index_pa_.destroy();
    if (chunk_reader_ != nullptr) {
        // destroy() already runs manual destructors on internal members
        // (chunk_header_, decoders, compressor, ...), so calling
        // chunk_reader_->~IChunkReader() here would double-destruct them.
        // The vector-buffer leaks (e.g. chunk_pages_) are released inside
        // AlignedChunkReader::destroy() via vector<>{}.swap().
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
    // Aligned value chunks' statistic_->count_ only counts non-null rows,
    // not total rows.  Using value_cm alone could skip an entire 100-row
    // chunk for an offset of 10 just because it has 10 non-null values.
    // Only apply the whole-chunk shortcut when time and value statistics
    // agree on the row count (i.e. no sparse nulls in this chunk); fall
    // through to per-page/per-row handling otherwise so the offset is
    // applied against the real row stream.
    if (time_cm == nullptr || value_cm == nullptr ||
        time_cm->statistic_ == nullptr || value_cm->statistic_ == nullptr) {
        return false;
    }
    int32_t tc = time_cm->statistic_->count_;
    int32_t vc = value_cm->statistic_->count_;
    if (tc <= 0 || vc <= 0 || tc != vc) {
        return false;
    }
    if (row_offset_ >= tc) {
        row_offset_ -= tc;
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

    // When get_next_page() reports E_NO_MORE_DATA but the chunk reader
    // still claims has_more_data() (an aligned-chunk artifact where time
    // and value pages report state differently), a bare `continue` would
    // retry the exhausted chunk forever.  Force the next iteration to
    // advance to the next chunk-meta cursor instead.
    bool force_load_next_chunk = false;
    while (true) {
        if (!chunk_reader_->has_more_data() || force_load_next_chunk) {
            force_load_next_chunk = false;
            while (true) {
                if (!has_next_chunk()) {
                    return E_NO_MORE_DATA;
                } else if (is_multi_value_) {
                    // Multi-value aligned path
                    ChunkMeta* time_cm = time_chunk_meta_cursor_.get();
                    std::vector<ChunkMeta*> value_cms;
                    value_cms.reserve(value_chunk_meta_cursors_.size());
                    for (auto& cur : value_chunk_meta_cursors_) {
                        value_cms.push_back(cur.get());
                    }
                    advance_to_next_chunk();
                    // Skip chunk by time filter using time chunk statistics.
                    if (filter != nullptr && time_cm->statistic_ != nullptr &&
                        !filter->satisfy(time_cm->statistic_)) {
                        continue;
                    }
                    if (should_skip_chunk_by_time(time_cm, min_time_hint)) {
                        continue;
                    }
                    chunk_reader_->reset();
                    auto* acr = static_cast<AlignedChunkReader*>(chunk_reader_);
                    if (RET_FAIL(acr->load_by_aligned_meta_multi(time_cm,
                                                                 value_cms))) {
                    }
                    break;
                } else if (!is_aligned_) {
                    ChunkMeta* cm = get_current_chunk_meta();
                    advance_to_next_chunk();
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
                    // Use time chunk statistics for time-based filtering.
                    ChunkMeta* filter_cm =
                        (time_cm->statistic_ != nullptr) ? time_cm : value_cm;
                    if (filter != nullptr && filter_cm->statistic_ != nullptr &&
                        !filter->satisfy(filter_cm->statistic_)) {
                        continue;
                    }
                    if (should_skip_chunk_by_time(filter_cm, min_time_hint)) {
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
        if (IS_SUCC(ret)) {
            if (alloc && ret_tsblock == nullptr) {
                ret_tsblock =
                    is_multi_value_ ? alloc_tsblock_multi() : alloc_tsblock();
            }
            ret = chunk_reader_->get_next_page(ret_tsblock, filter, *data_pa_,
                                               min_time_hint, row_offset_,
                                               row_limit_);
        }
        if (ret == common::E_NO_MORE_DATA && ret_tsblock != nullptr &&
            ret_tsblock->get_row_count() > 0) {
            return E_OK;
        }
        // When current chunk is exhausted (e.g. all pages skipped by offset)
        // but there are more chunks, load next chunk and retry.  Set the
        // force flag so the next iteration bypasses has_more_data() (which
        // can still report true on an aligned chunk that has actually
        // yielded all its rows).
        if (ret == common::E_NO_MORE_DATA && has_next_chunk()) {
            ret = E_OK;
            force_load_next_chunk = true;
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

    // Check if this is a multi-value aligned index. alloc_multi_ssi() creates
    // MultiAlignedTimeseriesIndex even when the query selects one value column,
    // so keep that path consistent with wider aligned reads.
    if (is_aligned_ && dynamic_cast<MultiAlignedTimeseriesIndex*>(
                           itimeseries_index_) != nullptr) {
        return init_chunk_reader_multi();
    }

    if (!is_aligned_) {
        void* buf =
            common::mem_alloc(sizeof(ChunkReader), common::MOD_CHUNK_READER);
        if (IS_NULL(buf)) return E_OOM;
        chunk_reader_ = new (buf) ChunkReader;
        chunk_meta_cursor_ = itimeseries_index_->get_chunk_meta_list()->begin();
        if (RET_FAIL(chunk_reader_->init(
                read_file_, itimeseries_index_->get_measurement_name(),
                itimeseries_index_->get_data_type(), time_filter_))) {
        }
    } else {
        void* buf = common::mem_alloc(sizeof(AlignedChunkReader),
                                      common::MOD_CHUNK_READER);
        if (IS_NULL(buf)) return E_OOM;
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

int TsFileSeriesScanIterator::init_chunk_reader_multi() {
    int ret = E_OK;
    is_multi_value_ = true;

    void* buf =
        common::mem_alloc(sizeof(AlignedChunkReader), common::MOD_CHUNK_READER);
    if (IS_NULL(buf)) {
        // The single-value path (init_chunk_reader) silently dereferenced
        // the null pointer on OOM; this path is new in the multi-value
        // reader work and would do the same via placement-new(nullptr) →
        // undefined behavior the moment any AlignedChunkReader field is
        // touched.  Surface E_OOM instead.
        is_multi_value_ = false;
        return E_OOM;
    }
    auto* acr = new (buf) AlignedChunkReader;
    chunk_reader_ = acr;

    uint32_t num_cols = itimeseries_index_->get_value_column_count();
#ifdef ENABLE_THREADS
    // Borrow the single process-wide worker pool (created in init_common()) for
    // multi-column decode.  Null when libtsfile_init() hasn't run; combined
    // with parallel_read_enabled_ this gates the parallel decode path — the
    // reader falls back to serial decode otherwise.
    if (num_cols > 1 && common::g_config_value_.parallel_read_enabled_ &&
        common::g_thread_pool_ != nullptr) {
        acr->set_decode_pool(common::g_thread_pool_);
    }
#endif

    // Per-column chunk lists must align 1:1 with the time chunk list:
    // load_by_aligned_meta_multi pairs them by index and the downstream
    // reader has no notion of a "missing" value chunk for a CGM.  If a
    // file evolved its schema and some column has fewer (or more) chunks
    // than the time column, naive index pairing would mate chunks from
    // different chunk groups, returning garbage and dereferencing past
    // end() once the shorter list ran out.  Refuse upfront with a clear
    // error rather than producing wrong data.
    uint32_t time_chunk_count =
        itimeseries_index_->get_time_chunk_meta_list()->size();
    for (uint32_t c = 0; c < num_cols; c++) {
        if (itimeseries_index_->get_value_chunk_meta_list(c)->size() !=
            time_chunk_count) {
            return E_NOT_SUPPORT;
        }
    }

    // Init time cursor
    time_chunk_meta_cursor_ =
        itimeseries_index_->get_time_chunk_meta_list()->begin();

    // Init all value cursors
    value_chunk_meta_cursors_.resize(num_cols);
    for (uint32_t c = 0; c < num_cols; c++) {
        value_chunk_meta_cursors_[c] =
            itimeseries_index_->get_value_chunk_meta_list(c)->begin();
    }

    // Init chunk reader
    if (RET_FAIL(
            acr->init(read_file_, itimeseries_index_->get_measurement_name(),
                      itimeseries_index_->get_data_type(), time_filter_))) {
        return ret;
    }

    // No chunks → nothing to load; iteration short-circuits via
    // has_next_chunk() returning false.
    if (time_chunk_count == 0) {
        return ret;
    }

    // Load first chunk set
    ChunkMeta* time_cm = time_chunk_meta_cursor_.get();
    std::vector<ChunkMeta*> value_cms;
    value_cms.reserve(num_cols);
    for (uint32_t c = 0; c < num_cols; c++) {
        value_cms.push_back(value_chunk_meta_cursors_[c].get());
    }

    if (RET_FAIL(acr->load_by_aligned_meta_multi(time_cm, value_cms))) {
        return ret;
    }

    // Advance cursors
    time_chunk_meta_cursor_++;
    for (auto& cur : value_chunk_meta_cursors_) cur++;

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

TsBlock* TsFileSeriesScanIterator::alloc_tsblock_multi() {
    auto* acr = static_cast<AlignedChunkReader*>(chunk_reader_);

    // Time column
    ColumnSchema time_cd("time", common::INT64, common::SNAPPY,
                         common::TS_2DIFF);
    tuple_desc_.push_back(time_cd);

    // Value columns
    uint32_t num_cols = acr->get_value_column_count();
    for (uint32_t c = 0; c < num_cols; c++) {
        ChunkHeader& ch = acr->get_value_chunk_header(c);
        ColumnSchema value_cd(ch.measurement_name_, ch.data_type_,
                              ch.compression_type_, ch.encoding_type_);
        tuple_desc_.push_back(value_cd);
    }

    tsblock_ = new TsBlock(&tuple_desc_);
    if (E_OK != tsblock_->init()) {
        delete tsblock_;
        tsblock_ = nullptr;
    }
    return tsblock_;
}

}  // end namespace storage
