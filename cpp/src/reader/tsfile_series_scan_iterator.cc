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

int TsFileSeriesScanIterator::get_next(TsBlock *&ret_tsblock, bool alloc,
                                       Filter *oneshoot_filter) {
    // TODO @filter
    int ret = E_OK;
    Filter *filter =
        (oneshoot_filter != nullptr) ? oneshoot_filter : time_filter_;
    if (alloc) {
        ret_tsblock = alloc_tsblock();
    }

    if (chunk_reader_->should_skip(filter)) {
        chunk_reader_->reset();
    }

    while (true) {
        if (!chunk_reader_->has_more_data()) {
            while (true) {
                if (!has_next_chunk()) {
                    return E_NO_MORE_DATA;
                }
                ChunkMeta *cm = nullptr;
                ChunkMeta *time_cm = nullptr;
                ChunkMeta *value_cm = nullptr;
                if (!is_aligned_) {
                    cm = get_current_chunk_meta();
                } else {
                    time_cm = time_chunk_meta_cursor_.get();
                    value_cm = value_chunk_meta_cursor_.get();
                    cm = time_cm;
                }
                advance_to_next_chunk();
                if (filter != nullptr && cm->statistic_ != nullptr && !filter->satisfy(cm->statistic_)) {
                    continue;
                }
                chunk_reader_->reset();
                if (!is_aligned_) {
                    if (RET_FAIL(chunk_reader_->load_by_meta(cm))) {
                        return ret;
                    }
                } else {
                    if (RET_FAIL(chunk_reader_->load_by_aligned_meta(time_cm, value_cm))) {
                        return ret;
                    }
                }
                break;
            }
        }

        ret = chunk_reader_->get_next_page(ret_tsblock, filter, *data_pa_);
        if (ret == E_NO_MORE_DATA) {
            continue;
        } else {
            return ret;
        }
    }
    return ret;
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
    is_aligned_ = itimeseries_index_->get_data_type() == common::VECTOR;
    if (!is_aligned_) {
        void *buf = common::mem_alloc(sizeof(ChunkReader), common::MOD_DEFAULT);
        chunk_reader_ = new (buf) ChunkReader;
        chunk_meta_cursor_ = itimeseries_index_->get_chunk_meta_list()->begin();
        ChunkMeta *cm = chunk_meta_cursor_.get();
        ASSERT(!chunk_reader_->has_more_data());
        if (RET_FAIL(chunk_reader_->init(
                read_file_, itimeseries_index_->get_measurement_name(),
                itimeseries_index_->get_data_type(), time_filter_))) {
        } else if (RET_FAIL(chunk_reader_->load_by_meta(cm))) {
        } else {
            chunk_meta_cursor_++;
        }
    } else {
        void *buf =
            common::mem_alloc(sizeof(AlignedChunkReader), common::MOD_DEFAULT);
        chunk_reader_ = new (buf) AlignedChunkReader;
        time_chunk_meta_cursor_ =
            itimeseries_index_->get_time_chunk_meta_list()->begin();
        value_chunk_meta_cursor_ =
            itimeseries_index_->get_value_chunk_meta_list()->begin();
        ChunkMeta *time_cm = time_chunk_meta_cursor_.get();
        ChunkMeta *value_cm = value_chunk_meta_cursor_.get();
        chunk_reader_->has_more_data();
        ASSERT(!chunk_reader_->has_more_data());
        if (RET_FAIL(chunk_reader_->init(
                read_file_, itimeseries_index_->get_measurement_name(),
                itimeseries_index_->get_data_type(), time_filter_))) {
        } else if (RET_FAIL(chunk_reader_->load_by_aligned_meta(time_cm,
                                                                value_cm))) {
        } else {
            time_chunk_meta_cursor_++;
            value_chunk_meta_cursor_++;
        }
    }

    return ret;
}

TsBlock *TsFileSeriesScanIterator::alloc_tsblock() {
    ChunkHeader &ch = chunk_reader_->get_chunk_header();

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