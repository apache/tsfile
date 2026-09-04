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

#ifndef FILE_TSFILE_IO_REAER_H
#define FILE_TSFILE_IO_REAER_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "common/tsblock/tsblock.h"
#include "file/random_access_file.h"
#include "reader/chunk_reader.h"
#include "reader/filter/filter.h"
#include "reader/tsfile_series_scan_iterator.h"
#include "utils/db_utils.h"
#include "utils/storage_utils.h"
namespace storage {
class TsFileSeriesScanIterator;
class PreparedSeries;
struct FileGeneration;
struct PreparedLocator;

/*
 * TODO:
 * TsFileIOReader correspond to one tsfile.
 * It may be shared by many query.
 */
class TsFileIOReader {
   public:
    TsFileIOReader()
        : read_file_(nullptr),
          tsfile_meta_page_arena_(),
          tsfile_meta_(&tsfile_meta_page_arena_),
          tsfile_meta_ready_(false),
          read_file_created_(false) {
        tsfile_meta_page_arena_.init(512, common::MOD_TSFILE_READER);
        device_node_cache_pa_.init(512, common::MOD_TSFILE_READER);
    }

    // Free only the local source we own (created by init(const std::string&)).
    // Without an explicit destructor that raw pointer leaks whenever a
    // TsFileIOReader value goes out of scope without an explicit reset() (e.g.
    // a stack instance in a test).  We deliberately do NOT call reset() here:
    // reset() also runs tsfile_meta_page_arena_.destroy(), which would free the
    // arena that tsfile_meta_ lives in *before* the implicit ~TsFileMeta member
    // destructor runs, leaving its arena-allocated MetaIndexNode / shared_ptr
    // graph dangling (use-after-free / crash).  The arenas and TsFileMeta clean
    // themselves up correctly via member destruction order (tsfile_meta_ is
    // destroyed before its backing arena).  An owner that already called
    // reset() leaves read_file_ == nullptr, so this never double-frees.
    ~TsFileIOReader() {
        if (read_file_created_ && read_file_ != nullptr) {
            read_file_->close();
            delete read_file_;
            read_file_ = nullptr;
        }
    }

    int init(const std::string& file_path);

    int init(RandomAccessFile* read_file);

    void reset();

    int alloc_ssi(std::shared_ptr<IDeviceID> device_id,
                  const std::string& measurement_name,
                  TsFileSeriesScanIterator*& ssi, common::PageArena& pa,
                  Filter* time_filter = nullptr);

    int alloc_multi_ssi(std::shared_ptr<IDeviceID> device_id,
                        const std::vector<std::string>& measurement_names,
                        TsFileSeriesScanIterator*& ssi, common::PageArena& pa,
                        Filter* time_filter = nullptr);

    int prepare_series(const FileGeneration& generation,
                       const PreparedLocator& locator,
                       std::shared_ptr<PreparedSeries>& prepared);
    int prepare_series(
        const FileGeneration& generation, const PreparedLocator& locator,
        const std::shared_ptr<PreparedSeries>& aligned_time_owner,
        std::shared_ptr<PreparedSeries>& prepared);

    int alloc_prepared_ssi(const std::shared_ptr<PreparedSeries>& prepared,
                           TsFileSeriesScanIterator*& ssi,
                           common::PageArena& pa,
                           Filter* time_filter = nullptr);

    int alloc_prepared_multi_ssi(
        const std::vector<std::shared_ptr<PreparedSeries>>& prepared,
        TsFileSeriesScanIterator*& ssi, common::PageArena& pa,
        Filter* time_filter = nullptr);

    void revert_ssi(TsFileSeriesScanIterator* ssi);

    std::string get_file_path() const { return read_file_->file_path(); }

    int get_tsfile_meta(TsFileMeta*& tsfile_meta) {
        const int ret = load_tsfile_meta_if_necessary();
        tsfile_meta = ret == common::E_OK ? &tsfile_meta_ : nullptr;
        return ret;
    }

    TsFileMeta* get_tsfile_meta() {
        load_tsfile_meta_if_necessary();
        return &tsfile_meta_;
    }

    int get_device_timeseries_meta_without_chunk_meta(
        std::shared_ptr<IDeviceID> device_id,
        std::vector<ITimeseriesIndex*>& timeseries_indexs,
        common::PageArena& pa);

    int get_chunk_metadata_list(IDeviceID device_id, std::string measurement,
                                std::vector<ChunkMeta*>& chunk_meta_list);
    int read_device_meta_index(int64_t start_offset, int64_t end_offset,
                               common::PageArena& pa,
                               MetaIndexNode*& device_meta_index, bool leaf);
    int get_timeseries_indexes(
        std::shared_ptr<IDeviceID> device_id,
        const std::unordered_set<std::string>& measurement_names,
        std::vector<ITimeseriesIndex*>& timeseries_indexs,
        common::PageArena& pa);

    int get_device_timeseries_meta_by_offset(
        int64_t start_offset, int64_t end_offset,
        std::vector<ITimeseriesIndex*>& timeseries_indexs,
        common::PageArena& pa);

    int load_device_index_entry(
        std::shared_ptr<IComparable> target_name,
        std::shared_ptr<IMetaIndexEntry>& device_index_entry,
        int64_t& end_offset);

   private:
    FORCE_INLINE int64_t file_size() const { return read_file_->file_size(); }

    int load_tsfile_meta();

    int load_tsfile_meta_if_necessary();

    int load_measurement_index_entry(
        const std::string& measurement_name,
        std::shared_ptr<MetaIndexNode> top_node,
        std::shared_ptr<IMetaIndexEntry>& ret_measurement_index_entry,
        int64_t& ret_end_offset);

    int load_all_measurement_index_entry(
        int64_t start_offset, int64_t end_offset, common::PageArena& pa,
        std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>&
            ret_measurement_index_entry);

    bool is_aligned_device(std::shared_ptr<MetaIndexNode> measurement_node);

    int get_time_column_metadata(
        std::shared_ptr<MetaIndexNode> measurement_node,
        TimeseriesIndex*& ret_timeseries_index, common::PageArena& pa);

    int do_load_timeseries_index(const std::string& measurement_name_str,
                                 int64_t start_offset, int64_t end_offset,
                                 common::PageArena& pa,
                                 ITimeseriesIndex*& ts_index,
                                 bool is_aligned = false);

    int do_load_all_timeseries_index(
        std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>&
            index_node_entry_list,
        common::PageArena& in_timeseries_index_pa,
        std::vector<ITimeseriesIndex*>& ts_indexs);

    int load_timeseries_index_for_ssi(std::shared_ptr<IDeviceID> device_id,
                                      const std::string& measurement_name,
                                      TsFileSeriesScanIterator*& ssi);

    int search_from_leaf_node(std::shared_ptr<IComparable> target_name,
                              std::shared_ptr<MetaIndexNode> index_node,
                              std::shared_ptr<IMetaIndexEntry>& ret_index_entry,
                              int64_t& ret_end_offset);

    int search_from_internal_node(
        std::shared_ptr<IComparable> target_name, bool is_device,
        std::shared_ptr<MetaIndexNode> index_node,
        std::shared_ptr<IMetaIndexEntry>& ret_index_entry,
        int64_t& ret_end_offset);

    bool filter_stasify(ITimeseriesIndex* ts_index, Filter* time_filter);

    bool bloom_filter_contains(const std::string& device_name,
                               const std::string& measurement_name);

    // Collect every leaf index entry under index_node. Intermediate index
    // nodes read while descending are allocated from @pa, NOT a local arena:
    // the collected entries are shared_ptrs whose backing memory lives in that
    // arena (self_deleter only runs the dtor, it does not free), so @pa must
    // outlive index_node_entry_list. Callers pass the same arena they later
    // hand to do_load_all_timeseries_index().
    int get_all_leaf(
        std::shared_ptr<MetaIndexNode> index_node,
        std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t>>&
            index_node_entry_list,
        common::PageArena& pa);

    struct CachedDeviceNode {
        std::shared_ptr<MetaIndexNode> top_node;
        bool is_aligned;
    };

    // Returns E_OK on hit (out is filled), or an error code on miss / load
    // failure (E_DEVICE_NOT_EXIST when the device is absent, the propagated
    // error otherwise).  Copying into out keeps the caller safe from rehash /
    // concurrent eviction of the cache map.
    int get_cached_device_node(std::shared_ptr<IDeviceID> device_id,
                               common::PageArena& pa, CachedDeviceNode& out);

   private:
    // Build a collision-free key for device_node_cache_.  get_device_name()
    // renders a null tag segment as the literal "null", so a device with a
    // real null tag and one whose tag value is the string "null" produce the
    // same name and would alias in the cache — the second device would read
    // the first device's chunks.  Encode each segment length-prefixed and
    // flag null segments explicitly so the two can never collide.
    static std::string device_node_cache_key(
        const std::shared_ptr<IDeviceID>& device_id);

    RandomAccessFile* read_file_;
    common::PageArena tsfile_meta_page_arena_;
    TsFileMeta tsfile_meta_;
    bool tsfile_meta_ready_;
    bool read_file_created_;
    // Cache: device_name → deserialized measurement MetaIndexNode.
    // Guarded by device_node_cache_mu_ — multiple SSIs and Result Sets can
    // hit the cache concurrently on the same reader, and an unsynchronized
    // unordered_map insert would race with a parallel lookup (rehash,
    // bucket-list rewrite) and with the underlying PageArena allocation.
    common::PageArena device_node_cache_pa_;
    std::unordered_map<std::string, CachedDeviceNode> device_node_cache_;
    mutable std::mutex device_node_cache_mu_;
};

}  // end namespace storage
#endif  // FILE_TSFILE_IO_REAER_H
