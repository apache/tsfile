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

#include "file/tsfile_io_reader.h"

#include <limits>

#include "common/allocator/alloc_base.h"
#include "file/read_file.h"
#include "reader/prepared_series.h"

using namespace common;

namespace storage {
int TsFileIOReader::init(const std::string& file_path) {
    int ret = E_OK;
    ReadFile* local_file = new ReadFile;
    read_file_ = local_file;
    read_file_created_ = true;
    if (RET_FAIL(local_file->open(file_path))) {
    }
    return ret;
}

int TsFileIOReader::init(RandomAccessFile* read_file) {
    if (IS_NULL(read_file)) {
        ASSERT(false);
        return E_INVALID_ARG;
    }
    read_file_created_ = false;
    read_file_ = read_file;
    return E_OK;
}

void TsFileIOReader::reset() {
    if (read_file_ != nullptr) {
        if (read_file_created_) {
            read_file_->close();
            delete read_file_;
        }
        read_file_ = nullptr;
        tsfile_meta_page_arena_.destroy();
        device_node_cache_.clear();
        device_node_cache_pa_.destroy();
        tsfile_meta_ready_ = false;
    }
}

int TsFileIOReader::alloc_ssi(std::shared_ptr<IDeviceID> device_id,
                              const std::string& measurement_name,
                              TsFileSeriesScanIterator*& ssi,
                              common::PageArena& pa, Filter* time_filter) {
    int ret = E_OK;
    if (RET_FAIL(load_tsfile_meta_if_necessary())) {
    } else if (!bloom_filter_contains(device_id->get_device_name(),
                                      measurement_name)) {
        return E_NO_MORE_DATA;
    } else {
        void* ssi_buf =
            mem_alloc(sizeof(TsFileSeriesScanIterator), MOD_TSFILE_READER);
        if (IS_NULL(ssi_buf)) return E_OOM;
        ssi = new (ssi_buf) TsFileSeriesScanIterator;
        ssi->init(device_id, measurement_name, read_file_, time_filter, pa);
        if (RET_FAIL(load_timeseries_index_for_ssi(device_id, measurement_name,
                                                   ssi))) {
        } else if (time_filter != nullptr &&
                   !filter_stasify(ssi->itimeseries_index_, time_filter)) {
            ret = E_NO_MORE_DATA;
        } else if (RET_FAIL(ssi->init_chunk_reader())) {
        }
        if (ret != E_OK) {
            ssi->destroy();
            mem_free(ssi);
            ssi = nullptr;
        }
    }
    return ret;
}

namespace {
int load_exact_timeseries_index(RandomAccessFile* read_file, uint64_t offset,
                                uint32_t length, PageArena& arena,
                                TimeseriesIndex*& index) {
    if (read_file == nullptr || length == 0 ||
        length > static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) ||
        offset > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        offset > static_cast<uint64_t>(read_file->file_size()) ||
        length > static_cast<uint64_t>(read_file->file_size()) - offset) {
        return E_TSFILE_CORRUPTED;
    }
    char* bytes = static_cast<char*>(arena.alloc(length));
    void* index_memory = arena.alloc(sizeof(TimeseriesIndex));
    if (bytes == nullptr || index_memory == nullptr) {
        return E_OOM;
    }
    int32_t read_length = 0;
    int ret = read_file->read(static_cast<int64_t>(offset), bytes,
                              static_cast<int32_t>(length), read_length);
    if (ret != E_OK || read_length != static_cast<int32_t>(length)) {
        return ret == E_OK ? E_TSFILE_CORRUPTED : ret;
    }
    ByteStream stream;
    stream.wrap_from(bytes, length);
    index = new (index_memory) TimeseriesIndex;
    ret = index->deserialize_from(stream, &arena);
    if (ret != E_OK || stream.read_pos() != length) {
        index = nullptr;
        return ret == E_OK ? E_TSFILE_CORRUPTED : ret;
    }
    index->set_metadata_range(static_cast<int64_t>(offset), length);
    return E_OK;
}
}  // namespace

int TsFileIOReader::prepare_series(const FileGeneration& generation,
                                   const PreparedLocator& locator,
                                   std::shared_ptr<PreparedSeries>& prepared) {
    return prepare_series(generation, locator, nullptr, prepared);
}

int TsFileIOReader::prepare_series(
    const FileGeneration& generation, const PreparedLocator& locator,
    const std::shared_ptr<PreparedSeries>& aligned_time_owner,
    std::shared_ptr<PreparedSeries>& prepared) {
    prepared.reset();
    uint64_t actual_size = 0;
    uint64_t actual_fingerprint = 0;
    if (read_file_ == nullptr || generation.file_size == 0 ||
        read_file_->generation(actual_size, actual_fingerprint) != E_OK ||
        actual_size != generation.file_size ||
        (generation.file_fingerprint != 0 &&
         actual_fingerprint != generation.file_fingerprint) ||
        locator.value_metadata_length == 0 || locator.layout > 1) {
        return E_INVALID_ARG;
    }
    std::shared_ptr<PreparedSeries> candidate =
        std::make_shared<PreparedSeries>(generation, locator);
    TimeseriesIndex* value_index = nullptr;
    int ret = load_exact_timeseries_index(
        read_file_, locator.value_metadata_offset,
        locator.value_metadata_length, candidate->arena(), value_index);
    if (ret != E_OK) {
        return ret;
    }
    if (locator.layout == 0) {
        candidate->set_index(value_index);
    } else {
        if (locator.time_metadata_length == 0) {
            return E_NOT_SUPPORT;
        }
        TimeseriesIndex* time_index = nullptr;
        if (aligned_time_owner != nullptr) {
            const FileGeneration& owner_generation =
                aligned_time_owner->generation();
            const PreparedLocator& owner_locator =
                aligned_time_owner->locator();
            auto* owner_index = dynamic_cast<AlignedTimeseriesIndex*>(
                aligned_time_owner->index());
            if (owner_generation.mapped_index_identity !=
                    generation.mapped_index_identity ||
                owner_generation.file_id != generation.file_id ||
                owner_generation.file_size != generation.file_size ||
                owner_generation.file_fingerprint !=
                    generation.file_fingerprint ||
                owner_locator.layout != 1 ||
                owner_locator.time_metadata_offset !=
                    locator.time_metadata_offset ||
                owner_locator.time_metadata_length !=
                    locator.time_metadata_length ||
                owner_index == nullptr ||
                owner_index->time_ts_idx_ == nullptr) {
                return E_INVALID_ARG;
            }
            time_index = owner_index->time_ts_idx_;
            candidate->set_aligned_time_owner(aligned_time_owner);
        } else {
            ret = load_exact_timeseries_index(
                read_file_, locator.time_metadata_offset,
                locator.time_metadata_length, candidate->arena(), time_index);
            if (ret != E_OK) {
                return ret;
            }
        }
        if (time_index->get_chunk_meta_list()->size() !=
            value_index->get_chunk_meta_list()->size()) {
            return E_NOT_SUPPORT;
        }
        void* aligned_memory =
            candidate->arena().alloc(sizeof(AlignedTimeseriesIndex));
        if (aligned_memory == nullptr) {
            return E_OOM;
        }
        AlignedTimeseriesIndex* aligned =
            new (aligned_memory) AlignedTimeseriesIndex;
        aligned->time_ts_idx_ = time_index;
        aligned->value_ts_idx_ = value_index;
        candidate->set_index(aligned);
    }
    prepared = candidate;
    return E_OK;
}

int TsFileIOReader::alloc_prepared_ssi(
    const std::shared_ptr<PreparedSeries>& prepared,
    TsFileSeriesScanIterator*& ssi, common::PageArena& pa,
    Filter* time_filter) {
    ssi = nullptr;
    if (prepared == nullptr || prepared->index() == nullptr) {
        return E_INVALID_ARG;
    }
    if (time_filter != nullptr &&
        !filter_stasify(prepared->index(), time_filter)) {
        return E_NO_MORE_DATA;
    }
    void* memory =
        mem_alloc(sizeof(TsFileSeriesScanIterator), MOD_TSFILE_READER);
    if (memory == nullptr) {
        return E_OOM;
    }
    ssi = new (memory) TsFileSeriesScanIterator;
    int ret = ssi->init_prepared(prepared, read_file_, time_filter, pa);
    if (ret == E_OK) {
        ret = ssi->init_chunk_reader();
    }
    if (ret != E_OK) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
    }
    return ret;
}

int TsFileIOReader::alloc_prepared_multi_ssi(
    const std::vector<std::shared_ptr<PreparedSeries>>& prepared,
    TsFileSeriesScanIterator*& ssi, common::PageArena& pa,
    Filter* time_filter) {
    ssi = nullptr;
    if (prepared.empty() || prepared.front() == nullptr ||
        prepared.front()->index() == nullptr) {
        return E_INVALID_ARG;
    }
    auto* first_aligned =
        dynamic_cast<AlignedTimeseriesIndex*>(prepared.front()->index());
    if (first_aligned == nullptr || first_aligned->time_ts_idx_ == nullptr) {
        return E_NOT_SUPPORT;
    }
    if (time_filter != nullptr &&
        !filter_stasify(first_aligned->time_ts_idx_, time_filter)) {
        return E_NO_MORE_DATA;
    }

    void* memory =
        mem_alloc(sizeof(TsFileSeriesScanIterator), MOD_TSFILE_READER);
    if (memory == nullptr) {
        return E_OOM;
    }
    ssi = new (memory) TsFileSeriesScanIterator;
    int ret = ssi->init_prepared_multi(prepared, read_file_, time_filter, pa);
    if (ret == E_OK) {
        ret = ssi->init_chunk_reader();
    }
    if (ret != E_OK) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
    }
    return ret;
}

int TsFileIOReader::alloc_multi_ssi(
    std::shared_ptr<IDeviceID> device_id,
    const std::vector<std::string>& measurement_names,
    TsFileSeriesScanIterator*& ssi, common::PageArena& pa,
    Filter* time_filter) {
    int ret = E_OK;
    if (RET_FAIL(load_tsfile_meta_if_necessary())) return ret;

    void* ssi_buf =
        mem_alloc(sizeof(TsFileSeriesScanIterator), MOD_TSFILE_READER);
    if (IS_NULL(ssi_buf)) return E_OOM;
    ssi = new (ssi_buf) TsFileSeriesScanIterator;
    ssi->init(device_id, measurement_names.empty() ? "" : measurement_names[0],
              read_file_, time_filter, pa);

    auto& ssi_pa = ssi->timeseries_index_pa_;

    // Use cached device measurement node (avoids repeated file I/O)
    CachedDeviceNode cached;
    if (RET_FAIL(get_cached_device_node(device_id, ssi_pa, cached))) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
        return ret;
    }
    auto top_node = cached.top_node;
    if (!cached.is_aligned) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
        return E_NOT_SUPPORT;
    }

    // Get time column metadata
    TimeseriesIndex* time_ts_idx = nullptr;
    if (RET_FAIL(get_time_column_metadata(top_node, time_ts_idx, ssi_pa))) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
        return ret;
    }

    // Create MultiAlignedTimeseriesIndex
    void* multi_buf = ssi_pa.alloc(sizeof(MultiAlignedTimeseriesIndex));
    if (IS_NULL(multi_buf)) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
        return E_OOM;
    }
    auto* multi_idx = new (multi_buf) MultiAlignedTimeseriesIndex;
    multi_idx->time_ts_idx_ = time_ts_idx;

    // Batch-load all measurement TimeseriesIndex entries in a single pass over
    // the index tree — cheaper than N separate binary searches + file reads.
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>
        all_leaves;
    if (RET_FAIL(get_all_leaf(top_node, all_leaves, ssi_pa))) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
        return ret;
    }
    std::vector<ITimeseriesIndex*> all_ts_idxs;
    if (RET_FAIL(
            do_load_all_timeseries_index(all_leaves, ssi_pa, all_ts_idxs))) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
        return ret;
    }
    std::unordered_map<std::string, TimeseriesIndex*> name_to_idx;
    for (ITimeseriesIndex* idx : all_ts_idxs) {
        auto* ts = static_cast<TimeseriesIndex*>(idx);
        name_to_idx[ts->get_measurement_name().to_std_string()] = ts;
    }
    for (const auto& meas_name : measurement_names) {
        auto it = name_to_idx.find(meas_name);
        if (it == name_to_idx.end()) {
            ssi->destroy();
            mem_free(ssi);
            ssi = nullptr;
            return E_NOT_EXIST;
        }
        multi_idx->value_ts_idxs_.push_back(it->second);
    }

    ssi->itimeseries_index_ = multi_idx;

    // Skip global statistic filter for multi — per-chunk filtering still works.

    if (RET_FAIL(ssi->init_chunk_reader())) {
        ssi->destroy();
        mem_free(ssi);
        ssi = nullptr;
    }
    return ret;
}

void TsFileIOReader::revert_ssi(TsFileSeriesScanIterator* ssi) {
    if (ssi != nullptr) {
        ssi->destroy();
        mem_free(ssi);
    }
}

int TsFileIOReader::get_device_timeseries_meta_without_chunk_meta(
    std::shared_ptr<IDeviceID> device_id,
    std::vector<ITimeseriesIndex*>& timeseries_indexs, PageArena& pa) {
    int ret = E_OK;
    load_tsfile_meta_if_necessary();
    std::shared_ptr<IMetaIndexEntry> meta_index_entry;
    int64_t end_offset;
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>
        meta_index_entry_list;
    if (RET_FAIL(load_device_index_entry(
            std::make_shared<DeviceIDComparable>(device_id), meta_index_entry,
            end_offset))) {
    } else if (RET_FAIL(load_all_measurement_index_entry(
                   meta_index_entry->get_offset(), end_offset, pa,
                   meta_index_entry_list))) {
    } else if (RET_FAIL(do_load_all_timeseries_index(meta_index_entry_list, pa,
                                                     timeseries_indexs))) {
    }
    return ret;
}

int TsFileIOReader::get_device_timeseries_meta_by_offset(
    int64_t start_offset, int64_t end_offset,
    std::vector<ITimeseriesIndex*>& timeseries_indexs, PageArena& pa) {
    int ret = E_OK;
    load_tsfile_meta_if_necessary();

    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>
        meta_index_entry_list;
    bool is_aligned = false;
    TimeseriesIndex* time_timeseries_index = nullptr;

    ASSERT(start_offset < end_offset);
    const int32_t read_size = end_offset - start_offset;
    int32_t ret_read_len = 0;
    char* data_buf = (char*)pa.alloc(read_size);
    void* m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
    if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
        return E_OOM;
    }
    auto* top_node_ptr = new (m_idx_node_buf) MetaIndexNode(&pa);
    auto top_node = std::shared_ptr<MetaIndexNode>(top_node_ptr,
                                                   MetaIndexNode::self_deleter);
    if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                  ret_read_len))) {
        return ret;
    }
    if (RET_FAIL(top_node->deserialize_from(data_buf, read_size))) {
        return ret;
    }

    is_aligned = is_aligned_device(top_node);
    if (is_aligned) {
        if (RET_FAIL(get_time_column_metadata(top_node, time_timeseries_index,
                                              pa))) {
            return ret;
        }
    }

    get_all_leaf(top_node, meta_index_entry_list, pa);

    if (RET_FAIL(do_load_all_timeseries_index(meta_index_entry_list, pa,
                                              timeseries_indexs))) {
        return ret;
    }

    if (is_aligned && time_timeseries_index != nullptr) {
        for (size_t i = 0; i < timeseries_indexs.size(); i++) {
            void* buf = pa.alloc(sizeof(AlignedTimeseriesIndex));
            if (IS_NULL(buf)) {
                return E_OOM;
            }
            auto* aligned_ts_idx = new (buf) AlignedTimeseriesIndex;
            aligned_ts_idx->time_ts_idx_ = time_timeseries_index;
            aligned_ts_idx->value_ts_idx_ =
                dynamic_cast<TimeseriesIndex*>(timeseries_indexs[i]);
            if (aligned_ts_idx->value_ts_idx_ == nullptr) {
                return E_TYPE_NOT_MATCH;
            }
            timeseries_indexs[i] = aligned_ts_idx;
        }
    }
    return ret;
}

bool TsFileIOReader::filter_stasify(ITimeseriesIndex* ts_index,
                                    Filter* time_filter) {
    ASSERT(ts_index->get_statistic() != nullptr);
    return time_filter->satisfy(ts_index->get_statistic());
}

bool TsFileIOReader::bloom_filter_contains(
    const std::string& device_name, const std::string& measurement_name) {
    BloomFilter* bf = tsfile_meta_.bloom_filter_;
    if (bf == nullptr || bf->is_empty()) {
        return true;  // no bloom filter — assume present
    }
    common::String dev_str, meas_str;
    dev_str.buf_ = const_cast<char*>(device_name.c_str());
    dev_str.len_ = static_cast<uint32_t>(device_name.size());
    meas_str.buf_ = const_cast<char*>(measurement_name.c_str());
    meas_str.len_ = static_cast<uint32_t>(measurement_name.size());
    return bf->contains(dev_str, meas_str);
}

int TsFileIOReader::load_tsfile_meta_if_necessary() {
    int ret = E_OK;
    if (!tsfile_meta_ready_) {
        if (RET_FAIL(load_tsfile_meta())) {
            // log_err("load_tsfile_meta error, ret=%d", ret);
            return ret;
        } else {
            tsfile_meta_ready_ = true;
        }
    }
    return ret;
}

int TsFileIOReader::load_tsfile_meta() {
    const int32_t TSFILE_READ_IO_SIZE = 1024;  // TODO make it configurable
    const int32_t TAIL_MAGIC_AND_META_SIZE_SIZE =
        10;                   // magic(6B) + meta_size(4B)
    ASSERT(file_size() > 0);  // > 13

    int ret = E_OK;
    uint32_t tsfile_meta_size = 0;
    int64_t read_offset = 0;
    int32_t ret_read_len = 0;

    // Step 1: reader the tsfile_meta_size
    // 1.1 prepare reader buffer
    const int64_t fsize = file_size();
    const int32_t alloc_size = static_cast<int32_t>(
        UTIL_MIN(static_cast<int64_t>(TSFILE_READ_IO_SIZE), fsize));
    char* read_buf = (char*)mem_alloc(alloc_size, MOD_TSFILE_READER);
    if (IS_NULL(read_buf)) {
        return E_OOM;
    }
    // 1.2 reader data from file
    read_offset = fsize - alloc_size;
    ret_read_len = 0;
    if (RET_FAIL(read_file_->read(read_offset, read_buf, alloc_size,
                                  ret_read_len))) {
    } else if (ret_read_len != alloc_size) {
        ret = E_FILE_READ_ERR;
        // log_err("do not reader enough data from tsfile, want-size=%d,
        // reader-size=%d, file=%s", alloc_size, ret_read_len,
        // get_file_path().c_str());
    }
    // 1.3 deserialize tsfile_meta_size
    if (IS_SUCC(ret)) {
        // deserialize tsfile_meta_size
        char* size_buf = read_buf + alloc_size - TAIL_MAGIC_AND_META_SIZE_SIZE;
        tsfile_meta_size = SerializationUtil::read_ui32(size_buf);
        ASSERT(tsfile_meta_size > 0 && tsfile_meta_size <= (1ll << 20));
    }

    // Step 2: reader TsFileMeta
    if (IS_SUCC(ret)) {
        // 2.1 prepare enough buffer (use the previous buffer if can).
        char* tsfile_meta_buf = nullptr;
        if (tsfile_meta_size + TAIL_MAGIC_AND_META_SIZE_SIZE >
            (uint32_t)alloc_size) {
            // prepare buffer to re-reader from start of tsfile_meta
            char* old_read_buf = read_buf;
            read_buf = (char*)mem_realloc(read_buf, tsfile_meta_size);
            if (IS_NULL(read_buf)) {
                read_buf = old_read_buf;
                ret = E_OOM;
            } else if (RET_FAIL(read_file_->read(
                           fsize - tsfile_meta_size -
                               TAIL_MAGIC_AND_META_SIZE_SIZE,
                           read_buf, tsfile_meta_size, ret_read_len))) {
            } else if (tsfile_meta_size != (uint32_t)ret_read_len) {
                ret = E_FILE_READ_ERR;
                // log_err("do not reader enough data from tsfile, want-size=%d,
                // reader-size=%d, file=%s", tsfile_meta_size, ret_read_len,
                // get_file_path().c_str());
            } else {
                tsfile_meta_buf = read_buf;
            }
        } else {
            // the previous buffer has contained the TsFileMeta data
            tsfile_meta_buf = read_buf + alloc_size - tsfile_meta_size -
                              TAIL_MAGIC_AND_META_SIZE_SIZE;
            // DEBUG_hex_dump_buf("tsfile_meta_buf=", tsfile_meta_buf,
            // tsfile_meta_size);
        }
        if (IS_SUCC(ret)) {
            ByteStream tsfile_meta_bs;
            tsfile_meta_bs.wrap_from(tsfile_meta_buf, tsfile_meta_size);
            if (RET_FAIL(tsfile_meta_.deserialize_from(tsfile_meta_bs))) {
            }
#if DEBUG_SE
            std::cout << "load tsfile_meta, ret=" << ret
                      << ", tsfile_meta_=" << tsfile_meta_ << std::endl;
#endif
        }
    }
    mem_free(read_buf);
    return ret;
}

std::string TsFileIOReader::device_node_cache_key(
    const std::shared_ptr<IDeviceID>& device_id) {
    // Length-prefixed, null-flagged encoding: for each segment emit either
    // "N;" (null) or "<len>:<bytes>;".  Distinct segment sequences always map
    // to distinct keys, so a real null tag never aliases the literal "null".
    std::string key;
    for (const std::string* seg : device_id->get_segments()) {
        if (seg == nullptr) {
            key += "N;";
        } else {
            key += std::to_string(seg->size());
            key += ':';
            key += *seg;
            key += ';';
        }
    }
    return key;
}

int TsFileIOReader::get_cached_device_node(std::shared_ptr<IDeviceID> device_id,
                                           common::PageArena& pa,
                                           CachedDeviceNode& out) {
    std::string dev_name = device_node_cache_key(device_id);

    {
        std::lock_guard<std::mutex> lk(device_node_cache_mu_);
        auto it = device_node_cache_.find(dev_name);
        if (it != device_node_cache_.end()) {
            out = it->second;
            return E_OK;
        }
    }

    // Read the device meta index outside the lock — load_device_index_entry()
    // and the file read can block on I/O, and we don't want to serialize all
    // concurrent first-time lookups behind one slow disk fetch.  Two callers
    // racing on the same missing device may both do the read; that's wasted
    // work but not corruption — the second insert is dropped below.
    int ret = E_OK;
    std::shared_ptr<IMetaIndexEntry> device_index_entry;
    int64_t device_ie_end_offset = 0;
    if (RET_FAIL(load_device_index_entry(
            std::make_shared<DeviceIDComparable>(device_id), device_index_entry,
            device_ie_end_offset))) {
        return ret;
    }

    int64_t start_offset = device_index_entry->get_offset(),
            end_offset = device_ie_end_offset;
    ASSERT(start_offset < end_offset);
    const int64_t read_size_i64 = end_offset - start_offset;
    // read_file_->read() takes int32_t; a meta index node larger than 2 GiB
    // is implausible but explicitly reject it instead of silently truncating
    // the read length and corrupting the parse.  Distinguish the two cases:
    // an inverted/empty range is corruption, an oversized one is an overflow.
    if (read_size_i64 <= 0) {
        return E_TSFILE_CORRUPTED;
    }
    if (read_size_i64 > INT32_MAX) {
        return E_OVERFLOW;
    }
    const int32_t read_size = static_cast<int32_t>(read_size_i64);
    int32_t ret_read_len = 0;

    // Read into a heap-owned buffer outside the lock.  The previous
    // implementation allocated data_buf inside device_node_cache_pa_ before
    // the read happened — every failed read or parse left that allocation
    // pinned forever in the shared arena, and repeated disk errors on the
    // same device let a long-lived reader grow it without bound.  Using a
    // unique_ptr here means the read buffer is released on every failure
    // path, and only the small MetaIndexNode allocations inside the lock
    // share the arena.
    std::unique_ptr<char[]> data_buf(new (std::nothrow) char[read_size]);
    if (data_buf == nullptr) {
        return E_OOM;
    }
    if (RET_FAIL(read_file_->read(start_offset, data_buf.get(), read_size,
                                  ret_read_len))) {
        return ret;
    }

    CachedDeviceNode cached;
    {
        // Allocations into device_node_cache_pa_ and the map insert must be
        // serialized — PageArena is not thread-safe, and unordered_map's
        // rehash invalidates concurrent lookups.
        std::lock_guard<std::mutex> lk(device_node_cache_mu_);
        // Re-check: another thread may have populated the entry while we
        // were doing I/O.
        auto it = device_node_cache_.find(dev_name);
        if (it != device_node_cache_.end()) {
            out = it->second;
            return E_OK;
        }

        void* m_idx_node_buf =
            device_node_cache_pa_.alloc(sizeof(MetaIndexNode));
        if (IS_NULL(m_idx_node_buf)) {
            return E_OOM;
        }
        auto* top_node_ptr =
            new (m_idx_node_buf) MetaIndexNode(&device_node_cache_pa_);
        auto top_node = std::shared_ptr<MetaIndexNode>(
            top_node_ptr, MetaIndexNode::self_deleter);
        if (RET_FAIL(top_node->deserialize_from(data_buf.get(), read_size))) {
            return ret;
        }
        cached.top_node = top_node;
        cached.is_aligned = is_aligned_device(top_node);
        device_node_cache_.emplace(std::move(dev_name), cached);
    }
    out = cached;
    return E_OK;
}

int TsFileIOReader::load_timeseries_index_for_ssi(
    std::shared_ptr<IDeviceID> device_id, const std::string& measurement_name,
    TsFileSeriesScanIterator*& ssi) {
    int ret = E_OK;
    auto& pa = ssi->timeseries_index_pa_;

    CachedDeviceNode cached;
    if (RET_FAIL(get_cached_device_node(device_id, pa, cached))) {
        return ret;
    }
    auto top_node = cached.top_node;
    bool is_aligned = cached.is_aligned;

    TimeseriesIndex* timeseries_index = nullptr;
    if (is_aligned) {
        if (RET_FAIL(
                get_time_column_metadata(top_node, timeseries_index, pa))) {
            return ret;
        }
    }

    std::shared_ptr<IMetaIndexEntry> measurement_index_entry;
    int64_t measurement_ie_end_offset = 0;
    if (RET_FAIL(load_measurement_index_entry(measurement_name, top_node,
                                              measurement_index_entry,
                                              measurement_ie_end_offset))) {
        return ret;
    } else if (RET_FAIL(do_load_timeseries_index(
                   measurement_name, measurement_index_entry->get_offset(),
                   measurement_ie_end_offset, ssi->timeseries_index_pa_,
                   ssi->itimeseries_index_, is_aligned))) {
        return ret;
    }
    if (is_aligned) {
        auto* aligned_timeseries_index =
            dynamic_cast<AlignedTimeseriesIndex*>(ssi->itimeseries_index_);
        if (aligned_timeseries_index) {
            aligned_timeseries_index->time_ts_idx_ = timeseries_index;
        }
    }

#if DEBUG_SE
    if (measurement_index_entry.name_.len_) {
        std::cout << "load timeseries index: "
                  << *((TimeseriesIndex*)ssi->itimeseries_index_) << std::endl;
    } else {
        std::cout << "load aligned timeseries index: "
                  << *((AlignedTimeseriesIndex*)ssi->itimeseries_index_)
                  << std::endl;
    }
#endif
    return ret;
}

int TsFileIOReader::load_device_index_entry(
    std::shared_ptr<IComparable> device_name,
    std::shared_ptr<IMetaIndexEntry>& device_index_entry, int64_t& end_offset) {
    int ret = E_OK;
    std::shared_ptr<DeviceIDComparable> device_id_comparable =
        std::dynamic_pointer_cast<DeviceIDComparable>(device_name);
    if (device_id_comparable == nullptr) {
        return E_INVALID_DATA_POINT;
    }
    std::string table_name = device_id_comparable->device_id_->get_table_name();
    auto it = tsfile_meta_.table_metadata_index_node_map_.find(table_name);
    if (it == tsfile_meta_.table_metadata_index_node_map_.end() ||
        it->second == nullptr) {
        return E_DEVICE_NOT_EXIST;
    }
    auto index_node = it->second;
    if (index_node->node_type_ == LEAF_DEVICE) {
        ret = index_node->binary_search_children(
            device_name, true, device_index_entry, end_offset);
    } else {
        ret = search_from_internal_node(device_name, true, index_node,
                                        device_index_entry, end_offset);
    }
    if (ret == E_NOT_EXIST) {
        ret = E_DEVICE_NOT_EXIST;
    }
#if DEBUG_SE
    std::cout << "load_device_index_entry, device_index_entry={"
              << device_index_entry << "}, end_offset=" << end_offset
              << std::endl;
#endif
    return ret;
}

int TsFileIOReader::load_measurement_index_entry(
    const std::string& measurement_name_str,
    std::shared_ptr<MetaIndexNode> top_node,
    std::shared_ptr<IMetaIndexEntry>& ret_measurement_index_entry,
    int64_t& ret_end_offset) {
    int ret = E_OK;
    // search from top_node in top-down way
    auto measurement_name =
        std::make_shared<StringComparable>(measurement_name_str);
    if (top_node->node_type_ == LEAF_MEASUREMENT) {
        ret = top_node->binary_search_children(
            measurement_name, /*exact*/ false, ret_measurement_index_entry,
            ret_end_offset);
    } else {
        ret = search_from_internal_node(measurement_name, false, top_node,
                                        ret_measurement_index_entry,
                                        ret_end_offset);
    }
    if (ret == E_NOT_EXIST) {
        ret = E_MEASUREMENT_NOT_EXIST;
    }
    return ret;
}

int TsFileIOReader::load_all_measurement_index_entry(
    int64_t start_offset, int64_t end_offset, common::PageArena& pa,
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>&
        ret_measurement_index_entry) {
#if DEBUG_SE
    std::cout << "load_measurement_index_entry: measurement_name_str= "
              << ", start_offset=" << start_offset
              << ", end_offset=" << end_offset << std::endl;
#endif
    ASSERT(start_offset < end_offset);
    int ret = E_OK;
    // 1. load top measuremnt_index_node
    const int32_t read_size = (int32_t)(end_offset - start_offset);
    int32_t ret_read_len = 0;
    char* data_buf = (char*)pa.alloc(read_size);
    void* m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
    if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
        return E_OOM;
    }
    auto* top_node_ptr = new (m_idx_node_buf) MetaIndexNode(&pa);
    auto top_node = std::shared_ptr<MetaIndexNode>(top_node_ptr,
                                                   MetaIndexNode::self_deleter);
    if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                  ret_read_len))) {
    } else if (RET_FAIL(top_node->deserialize_from(data_buf, read_size))) {
    }
#if DEBUG_SE
    std::cout
        << "load_measurement_index_entry deserialize MetaIndexNode, top_node="
        << *top_node << " at file pos " << start_offset << " to " << end_offset
        << std::endl;
#endif
    // 2. search from top_node in top-down way
    if (IS_SUCC(ret)) {
        get_all_leaf(top_node, ret_measurement_index_entry, pa);
    }
    if (ret == E_NOT_EXIST) {
        ret = E_MEASUREMENT_NOT_EXIST;
    }
    return ret;
}

int TsFileIOReader::read_device_meta_index(int64_t start_offset,
                                           int64_t end_offset,
                                           common::PageArena& pa,
                                           MetaIndexNode*& device_meta_index,
                                           bool leaf) {
    int ret = E_OK;
    ASSERT(start_offset < end_offset);
    const int32_t read_size = (int32_t)(end_offset - start_offset);
    int32_t ret_read_len = 0;
    char* data_buf = (char*)pa.alloc(read_size);
    void* m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
    if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
        return E_OOM;
    }
    device_meta_index = new (m_idx_node_buf) MetaIndexNode(&pa);
    if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                  ret_read_len))) {
    }
    if (!leaf) {
        ret = device_meta_index->device_deserialize_from(data_buf, read_size);
    } else {
        ret = device_meta_index->deserialize_from(data_buf, read_size);
    }
    return ret;
}

int TsFileIOReader::get_timeseries_indexes(
    std::shared_ptr<IDeviceID> device_id,
    const std::unordered_set<std::string>& measurement_names,
    std::vector<ITimeseriesIndex*>& timeseries_indexs, common::PageArena& pa) {
    int ret = E_OK;
    std::shared_ptr<IMetaIndexEntry> device_index_entry;
    int64_t device_ie_end_offset = 0;
    std::shared_ptr<IMetaIndexEntry> measurement_index_entry;
    int64_t measurement_ie_end_offset = 0;
    if (RET_FAIL(load_device_index_entry(
            std::make_shared<DeviceIDComparable>(device_id), device_index_entry,
            device_ie_end_offset))) {
        return ret;
    }

    int64_t start_offset = device_index_entry->get_offset(),
            end_offset = device_ie_end_offset;
    ASSERT(start_offset < end_offset);
    const int32_t read_size = end_offset - start_offset;
    int32_t ret_read_len = 0;
    char* data_buf = (char*)pa.alloc(read_size);
    void* m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
    if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
        return E_OOM;
    }
    auto* top_node_ptr = new (m_idx_node_buf) MetaIndexNode(&pa);
    auto top_node = std::shared_ptr<MetaIndexNode>(top_node_ptr,
                                                   MetaIndexNode::self_deleter);

    if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                  ret_read_len))) {
        return ret;
    } else if (RET_FAIL(top_node->deserialize_from(data_buf, read_size))) {
        return ret;
    }

    bool is_aligned = is_aligned_device(top_node);
    TimeseriesIndex* timeseries_index = nullptr;
    if (is_aligned) {
        get_time_column_metadata(top_node, timeseries_index, pa);
    }

    int64_t idx = 0;
    for (const auto& measurement_name : measurement_names) {
        timeseries_indexs[idx] = nullptr;
        ret = load_measurement_index_entry(measurement_name, top_node,
                                           measurement_index_entry,
                                           measurement_ie_end_offset);
        if (ret == E_MEASUREMENT_NOT_EXIST || ret == E_NOT_EXIST) {
            ret = E_OK;
            idx++;
            continue;
        }
        if (RET_FAIL(ret)) {
            return ret;
        }

        ret = do_load_timeseries_index(
            measurement_name, measurement_index_entry->get_offset(),
            measurement_ie_end_offset, pa, timeseries_indexs[idx], is_aligned);
        if (ret == E_NOT_EXIST) {
            ret = E_OK;
            idx++;
            continue;
        }
        if (RET_FAIL(ret)) {
            return ret;
        }
        if (is_aligned) {
            AlignedTimeseriesIndex* aligned_timeseries_index =
                dynamic_cast<AlignedTimeseriesIndex*>(timeseries_indexs[idx]);
            if (aligned_timeseries_index) {
                aligned_timeseries_index->time_ts_idx_ = timeseries_index;
            }
        }

        idx++;
    }
    return ret;
}

/*
 * @target_name device_name or measurement_name
 * @index_node  leaf device node or leaf measurement node
 */
int TsFileIOReader::search_from_leaf_node(
    std::shared_ptr<IComparable> target_name,
    std::shared_ptr<MetaIndexNode> index_node,
    std::shared_ptr<IMetaIndexEntry>& ret_index_entry,
    int64_t& ret_end_offset) {
    int ret = E_OK;
    ret = index_node->binary_search_children(target_name, true, ret_index_entry,
                                             ret_end_offset);
    return ret;
}

int TsFileIOReader::search_from_internal_node(
    std::shared_ptr<IComparable> target_name, bool is_device,
    std::shared_ptr<MetaIndexNode> index_node,
    std::shared_ptr<IMetaIndexEntry>& ret_index_entry,
    int64_t& ret_end_offset) {
    int ret = E_OK;
    std::shared_ptr<IMetaIndexEntry> index_entry;
    int64_t end_offset = 0;

    ASSERT(index_node->node_type_ == INTERNAL_MEASUREMENT ||
           index_node->node_type_ == INTERNAL_DEVICE);
    if (RET_FAIL(index_node->binary_search_children(
            target_name, /*exact=*/false, index_entry, end_offset))) {
        return ret;
    }

    while (IS_SUCC(ret)) {
        // reader next level index node
        const int read_size = end_offset - index_entry->get_offset();
#if DEBUG_SE
        std::cout << "search_from_internal_node, end_offset=" << end_offset
                  << ", index_entry.offset_=" << index_entry.get_offset()
                  << std::endl;
#endif
        ASSERT(read_size > 0 && read_size < (1 << 30));
        PageArena cur_level_index_node_pa;
        void* buf = cur_level_index_node_pa.alloc(sizeof(MetaIndexNode));
        char* data_buf = (char*)cur_level_index_node_pa.alloc(read_size);
        if (IS_NULL(buf) || IS_NULL(data_buf)) {
            return E_OOM;
        }
        MetaIndexNode* cur_level_index_node =
            new (buf) MetaIndexNode(&cur_level_index_node_pa);
        int32_t ret_read_len = 0;
        if (RET_FAIL(read_file_->read(index_entry->get_offset(), data_buf,
                                      read_size, ret_read_len))) {
        } else if (read_size != ret_read_len) {
            return E_TSFILE_CORRUPTED;
        }
        if (!is_device) {
            ret = cur_level_index_node->deserialize_from(data_buf, read_size);
        } else {
            ret = cur_level_index_node->device_deserialize_from(data_buf,
                                                                read_size);
        }
        if (ret != E_OK) {
            return ret;
        }
        if (cur_level_index_node->node_type_ == LEAF_DEVICE) {
            ret = cur_level_index_node->binary_search_children(
                target_name, /*exact=*/true, ret_index_entry, ret_end_offset);
            cur_level_index_node->destroy();
            return ret;  //// FIXME
        } else if (cur_level_index_node->node_type_ == LEAF_MEASUREMENT) {
            ret = cur_level_index_node->binary_search_children(
                target_name, /*exact=*/false, ret_index_entry, ret_end_offset);
            cur_level_index_node->destroy();
            return ret;  //// FIXME
        } else {
            ret = cur_level_index_node->binary_search_children(
                target_name, /*exact=*/false, index_entry, end_offset);
            cur_level_index_node->destroy();
        }
    }
    return ret;
}

bool TsFileIOReader::is_aligned_device(
    std::shared_ptr<MetaIndexNode> measurement_node) {
    if (measurement_node->children_.empty()) {
        return false;
    }
    auto entry = measurement_node->children_[0];
    return entry->get_name().is_null() ||
           entry->get_name().to_std_string() == "";
}

int TsFileIOReader::get_time_column_metadata(
    std::shared_ptr<MetaIndexNode> measurement_node,
    TimeseriesIndex*& ret_timeseries_index, PageArena& pa) {
    int ret = E_OK;
    if (!is_aligned_device(measurement_node)) {
        return ret;
    }
    char* ti_buf = nullptr;
    int64_t start_idx = 0, end_idx = 0;
    int ret_read_len = 0;
    if (measurement_node->node_type_ == LEAF_MEASUREMENT) {
        ByteStream buffer;
        if (measurement_node->children_.size() > 1) {
            start_idx = measurement_node->children_[0]->get_offset();
            end_idx = measurement_node->children_[1]->get_offset();
            ti_buf = pa.alloc(end_idx - start_idx);
            if (RET_FAIL(read_file_->read(start_idx, ti_buf,
                                          end_idx - start_idx, ret_read_len))) {
                return ret;
            }
        } else {
            start_idx = measurement_node->children_[0]->get_offset();
            end_idx = measurement_node->end_offset_;
            ti_buf = pa.alloc(end_idx - start_idx);
            if (RET_FAIL(read_file_->read(start_idx, ti_buf,
                                          end_idx - start_idx, ret_read_len))) {
                return ret;
            }
        }
        buffer.wrap_from(ti_buf, end_idx - start_idx);
        void* buf = pa.alloc(sizeof(TimeseriesIndex));
        if (IS_NULL(buf)) {
            return E_OOM;
        }
        ret_timeseries_index = new (buf) TimeseriesIndex;
        if (RET_FAIL(ret_timeseries_index->deserialize_from(buffer, &pa))) {
            return ret;
        }
        if (buffer.read_pos() > UINT32_MAX) {
            return E_OVERFLOW;
        }
        ret_timeseries_index->set_metadata_range(
            start_idx, static_cast<uint32_t>(buffer.read_pos()));
    } else if (measurement_node->node_type_ == INTERNAL_MEASUREMENT) {
        start_idx = measurement_node->children_[0]->get_offset();
        end_idx = measurement_node->children_[1]->get_offset();
        ti_buf = pa.alloc(end_idx - start_idx);
        if (RET_FAIL(read_file_->read(start_idx, ti_buf, end_idx - start_idx,
                                      ret_read_len))) {
            return ret;
        }
        std::shared_ptr<MetaIndexNode> meta_index_node =
            std::make_shared<MetaIndexNode>(&pa);
        meta_index_node->deserialize_from(ti_buf, end_idx - start_idx);
        return get_time_column_metadata(meta_index_node, ret_timeseries_index,
                                        pa);
    }
    return ret;
}

int TsFileIOReader::do_load_timeseries_index(
    const std::string& measurement_name_str, int64_t start_offset,
    int64_t end_offset, PageArena& in_timeseries_index_pa,
    ITimeseriesIndex*& ret_timeseries_index, bool is_aligned) {
    ASSERT(end_offset > start_offset);
    int ret = E_OK;
    int32_t read_size = (int32_t)(end_offset - start_offset);
    int32_t ret_read_len = 0;
    char* ti_buf = (char*)mem_alloc(read_size, MOD_TSFILE_READER);
    if (IS_NULL(ti_buf)) {
        return E_OOM;
    }
    if (RET_FAIL(
            read_file_->read(start_offset, ti_buf, read_size, ret_read_len))) {
    } else {
        ByteStream bs;
        bs.wrap_from(ti_buf, read_size);
        const String target_measurement_name(
            (char*)measurement_name_str.c_str(),
            strlen(measurement_name_str.c_str()));
        bool found = false;
#if DEBUG_SE
        std::cout << "do_load_timeseries_index, reader file at " << start_offset
                  << " to " << end_offset << std::endl;
#endif
        while (IS_SUCC(ret)) {
            TimeseriesIndex cur_timeseries_index;
            PageArena cur_timeseries_index_pa;
            cur_timeseries_index_pa.init(512, MOD_TSFILE_READER);  // TODO 512
            const uint64_t relative_start = bs.read_pos();
            if (RET_FAIL(cur_timeseries_index.deserialize_from(
                    bs, &cur_timeseries_index_pa))) {
            } else if (bs.read_pos() < relative_start ||
                       bs.read_pos() - relative_start > UINT32_MAX) {
                ret = E_OVERFLOW;
            } else {
                cur_timeseries_index.set_metadata_range(
                    start_offset + relative_start,
                    static_cast<uint32_t>(bs.read_pos() - relative_start));
            }
            if (RET_FAIL(ret)) {
            } else if (is_aligned &&
                       cur_timeseries_index.get_measurement_name().equal_to(
                           target_measurement_name)) {
                void* buf = in_timeseries_index_pa.alloc(
                    sizeof(AlignedTimeseriesIndex));
                if (IS_NULL(buf)) {
                    return E_OOM;
                }
                AlignedTimeseriesIndex* aligned_ts_idx =
                    new (buf) AlignedTimeseriesIndex;
                buf = in_timeseries_index_pa.alloc(sizeof(TimeseriesIndex));
                if (IS_NULL(buf)) {
                    return E_OOM;
                }
                aligned_ts_idx->value_ts_idx_ = new (buf) TimeseriesIndex;
                aligned_ts_idx->value_ts_idx_->clone_from(
                    cur_timeseries_index, &in_timeseries_index_pa);
                ret_timeseries_index = aligned_ts_idx;
                found = true;
                break;
            } else if (!is_aligned &&
                       cur_timeseries_index.get_measurement_name().equal_to(
                           target_measurement_name)) {
                void* buf =
                    in_timeseries_index_pa.alloc(sizeof(TimeseriesIndex));
                auto ts_idx = new (buf) TimeseriesIndex;
                ts_idx->clone_from(cur_timeseries_index,
                                   &in_timeseries_index_pa);
                ret_timeseries_index = ts_idx;
                found = true;
                break;
            }
        }  // end while
        if (!found) {
            ret = E_NOT_EXIST;
        }
    }
    mem_free(ti_buf);
    return ret;
}

int TsFileIOReader::do_load_all_timeseries_index(
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>&
        index_node_entry_list,
    common::PageArena& in_timeseries_index_pa,
    std::vector<ITimeseriesIndex*>& ts_indexs) {
    int ret = E_OK;
    for (const auto& index_node_entry : index_node_entry_list) {
        int64_t start_offset = index_node_entry.first->get_offset(),
                end_offset = index_node_entry.second;
        int32_t read_size = (int32_t)(end_offset - start_offset);
        int32_t ret_read_len = 0;
        char* ti_buf = in_timeseries_index_pa.alloc(read_size);
        if (IS_NULL(ti_buf)) {
            return E_OOM;
        }
        if (RET_FAIL(read_file_->read(start_offset, ti_buf, read_size,
                                      ret_read_len))) {
            return ret;
        }
        ByteStream bs;
        bs.wrap_from(ti_buf, read_size);
        while (bs.has_remaining()) {
            const uint64_t relative_start = bs.read_pos();
            void* buf = in_timeseries_index_pa.alloc(sizeof(TimeseriesIndex));
            auto ts_idx = new (buf) TimeseriesIndex;
            if (RET_FAIL(
                    ts_idx->deserialize_from(bs, &in_timeseries_index_pa))) {
                return ret;
            }
            if (bs.read_pos() < relative_start ||
                bs.read_pos() - relative_start > UINT32_MAX) {
                return E_OVERFLOW;
            }
            ts_idx->set_metadata_range(
                start_offset + relative_start,
                static_cast<uint32_t>(bs.read_pos() - relative_start));
            if (ts_idx->get_measurement_name().len_ == 0) continue;
            ts_indexs.push_back(ts_idx);
        }
    }
    return ret;
}

int TsFileIOReader::get_all_leaf(
    std::shared_ptr<MetaIndexNode> index_node,
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>&
        index_node_entry_list,
    common::PageArena& pa) {
    int ret = E_OK;
    if (index_node->node_type_ == LEAF_MEASUREMENT ||
        index_node->node_type_ == LEAF_DEVICE) {
        for (size_t i = 0; i < index_node->children_.size(); i++) {
            if (i + 1 < index_node->children_.size()) {
                index_node_entry_list.push_back(
                    std::make_pair(index_node->children_[i],
                                   index_node->children_[i + 1]->get_offset()));
            } else {
                index_node_entry_list.push_back(std::make_pair(
                    index_node->children_[i], index_node->end_offset_));
            }
        }
    } else {
        // read next level index node
        for (size_t i = 0; i < index_node->children_.size(); i++) {
            int64_t end_offset = index_node->end_offset_;
            if (i + 1 < index_node->children_.size()) {
                end_offset = index_node->children_[i + 1]->get_offset();
            }
            const int read_size =
                end_offset - index_node->children_[i]->get_offset();
#if DEBUG_SE
            std::cout << "search_from_internal_node, end_offset=" << end_offset
                      << ", index_entry.offset_="
                      << index_node->children_[i]->get_offset() << std::endl;
#endif
            ASSERT(read_size > 0 && read_size < (1 << 30));
            // Allocate the intermediate node and its children from @pa (which
            // outlives index_node_entry_list), not a function-local PageArena:
            // the leaf entries collected below are shared_ptrs into this
            // node's arena and must stay valid after this call returns. A
            // local arena freed the entries on scope exit -> use-after-free in
            // do_load_all_timeseries_index().
            void* buf = pa.alloc(sizeof(MetaIndexNode));
            char* data_buf = (char*)pa.alloc(read_size);
            if (IS_NULL(buf) || IS_NULL(data_buf)) {
                return E_OOM;
            }
            auto* cur_level_index_node_ptr = new (buf) MetaIndexNode(&pa);
            auto cur_level_index_node = std::shared_ptr<MetaIndexNode>(
                cur_level_index_node_ptr, MetaIndexNode::self_deleter);

            int32_t ret_read_len = 0;
            if (RET_FAIL(
                    read_file_->read(index_node->children_[i]->get_offset(),
                                     data_buf, read_size, ret_read_len))) {
            } else if (read_size != ret_read_len) {
                ret = E_TSFILE_CORRUPTED;
            } else if (RET_FAIL(cur_level_index_node->deserialize_from(
                           data_buf, read_size))) {
            } else {
                ret = get_all_leaf(cur_level_index_node, index_node_entry_list,
                                   pa);
            }
        }
    }
    return ret;
}
#if 0
int TsFileIOReader::get_next(const std::string &device_path,
                             const std::string &measurement_name,
                             TsBlock *ret_tsblock,
                             TimeRange &ret_time_range)
{
  int ret = E_OK;
  if (RET_FAIL(load_timeseries_index_if_necessary(device_path, measurement_name))) {
    return ret;
  }
  return get_next_page(ret_tsblock);
}

int TsFileIOReader::get_next_page(TsBlock *ret_tsblock)
{
  int ret = E_OK;

  if (!chunk_reader_.has_more_data()) {
    // has finished reading current chunk
    if (has_next_chunk()) {
      cursor_to_next_chunk();
      ChunkMeta *next_chunk_meta = chunk_meta_cursor_.get();
      if (RET_FAIL(init_next_chunk_reader(next_chunk_meta))) {
      }
    } else {
      // has finished reading all chunks of this tsfile
      ret = E_NO_MORE_DATA;
    }
  } // end if (!chunk_reader_.has_more_data())

  if (IS_SUCC(ret)) {
    ret = chunk_reader_.get_next_page(ret_tsblock);
  }
  return ret;
}

int TsFileIOReader::init_first_chunk_reader(ChunkMeta *cm,
                                            RandomAccessFile *read_file,
                                            const ColumnDesc &col_desc)
{
  ASSERT(!chunk_reader_.has_more_data());
  int ret = E_OK;
  if (RET_FAIL(chunk_reader_.init(read_file,
                                  timeseries_index_.get_measurement_name(),
                                  col_desc.type_,
                                  col_desc.encoding_))) {
  } else if (RET_FAIL(chunk_reader_.load_by_meta(cm))) {
  }
  return ret;
}

int TsFileIOReader::init_next_chunk_reader(ChunkMeta *cm)
{
  ASSERT(!chunk_reader_.has_more_data());
  chunk_reader_.reset();
  return chunk_reader_.load_by_meta(cm);
}

int TsFileIOReader::load_timeseries_index_if_necessary(const std::string &device_path,
                                                       const std::string &measurement_name)
{
  int ret = E_OK;
  if (col_desc.ts_id_ != timeseries_index_.get_ts_id()) {
    if (RET_FAIL(load_timeseries_index(col_desc))) {
      //log_err("load timeseries_index error, ret=%d", ret);
    } else {
      chunk_meta_cursor_ = timeseries_index_.get_chunk_meta_list()->begin();
      ChunkMeta *next_chunk_meta = chunk_meta_cursor_.get();
      if (RET_FAIL(init_first_chunk_reader(next_chunk_meta, read_file_, col_desc))) {
      } else {
        cursor_to_next_chunk();
      }
    }
  } else {
    // timeseries_index_ is ready
  }
  return ret;
}

// TODO add a result cache for load_timeseries_index
int TsFileIOReader::load_timeseries_index(const ColumnDesc &col_desc)
{
  int ret = E_OK;

  if (RET_FAIL(load_tsfile_meta_if_necessary())) {
    return ret;
  }

  MetaIndexEntry device_index_entry;
  int64_t device_ie_end_offset = 0;
  MetaIndexEntry measurement_index_entry;
  int64_t measurement_ie_end_offset = 0;

  if (RET_FAIL(load_device_index_entry(col_desc, device_index_entry, device_ie_end_offset))) {
  } else if (RET_FAIL(load_measurement_index_entry(col_desc, device_index_entry.offset_,
                                                   device_ie_end_offset, measurement_index_entry,
                                                   measurement_ie_end_offset))) {
  } else if (RET_FAIL(do_load_timeseries_index(col_desc, measurement_index_entry.offset_,
                                               measurement_ie_end_offset))) {
  } else {
#if STORAGE_ENGIEN_DEBUG
    std::cout << "load timeseries index: " << timeseries_index_ << std::endl;
#endif
  }
  return ret;
}
#endif
}  // end namespace storage
