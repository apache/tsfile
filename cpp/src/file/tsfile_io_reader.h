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

#include <unordered_set>

#include "common/tsblock/tsblock.h"
#include "file/read_file.h"
#include "reader/chunk_reader.h"
#include "reader/filter/filter.h"
#include "reader/tsfile_series_scan_iterator.h"
#include "utils/db_utils.h"
#include "utils/storage_utils.h"
namespace storage {
class TsFileSeriesScanIterator;

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
        tsfile_meta_page_arena_.init(512, common::MOD_DEFAULT);
    }

    int init(const std::string &file_path);

    int init(ReadFile *read_file);

    void reset();

    int alloc_ssi(std::shared_ptr<IDeviceID> device_id,
                  const std::string &measurement_name,
                  TsFileSeriesScanIterator *&ssi, common::PageArena &pa,
                  Filter *time_filter = nullptr);

    void revert_ssi(TsFileSeriesScanIterator *ssi);

    std::string get_file_path() const { return read_file_->file_path(); }

    TsFileMeta *get_tsfile_meta() {
        load_tsfile_meta_if_necessary();
        return &tsfile_meta_;
    }

    int get_device_timeseries_meta_without_chunk_meta(
        std::shared_ptr<IDeviceID> device_id,
        std::vector<ITimeseriesIndex *> &timeseries_indexs,
        common::PageArena &pa);

    int get_chunk_metadata_list(IDeviceID device_id, std::string measurement,
                                std::vector<ChunkMeta *> &chunk_meta_list);
    int read_device_meta_index(int32_t start_offset, int32_t end_offset,
                               common::PageArena &pa,
                               MetaIndexNode *&device_meta_index, bool leaf);
    int get_timeseries_indexes(
        std::shared_ptr<IDeviceID> device_id,
        const std::unordered_set<std::string> &measurement_names,
        std::vector<ITimeseriesIndex *> &timeseries_indexs,
        common::PageArena &pa);

   private:
    FORCE_INLINE int32_t file_size() const { return read_file_->file_size(); }

    int load_tsfile_meta();

    int load_tsfile_meta_if_necessary();

    int load_device_index_entry(
        std::shared_ptr<IComparable> target_name,
        std::shared_ptr<IMetaIndexEntry> &device_index_entry,
        int64_t &end_offset);

    int load_measurement_index_entry(
        const std::string &measurement_name,
        std::shared_ptr<MetaIndexNode> top_node,
        std::shared_ptr<IMetaIndexEntry> &ret_measurement_index_entry,
        int64_t &ret_end_offset);

    int load_all_measurement_index_entry(
        int64_t start_offset, int64_t end_offset, common::PageArena &pa,
        std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> >
            &ret_measurement_index_entry);

    bool is_aligned_device(std::shared_ptr<MetaIndexNode> measurement_node);

    int get_time_column_metadata(
        std::shared_ptr<MetaIndexNode> measurement_node,
        TimeseriesIndex *&ret_timeseries_index, common::PageArena &pa);

    int do_load_timeseries_index(const std::string &measurement_name_str,
                                 int64_t start_offset, int64_t end_offset,
                                 common::PageArena &pa,
                                 ITimeseriesIndex *&ts_index,
                                 bool is_aligned = false);

    int do_load_all_timeseries_index(
        std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> >
            &index_node_entry_list,
        common::PageArena &in_timeseries_index_pa,
        std::vector<ITimeseriesIndex *> &ts_indexs);

    int load_timeseries_index_for_ssi(std::shared_ptr<IDeviceID> device_id,
                                      const std::string &measurement_name,
                                      TsFileSeriesScanIterator *&ssi);

    int search_from_leaf_node(std::shared_ptr<IComparable> target_name,
                              std::shared_ptr<MetaIndexNode> index_node,
                              std::shared_ptr<IMetaIndexEntry> &ret_index_entry,
                              int64_t &ret_end_offset);

    int search_from_internal_node(
        std::shared_ptr<IComparable> target_name, bool is_device,
        std::shared_ptr<MetaIndexNode> index_node,
        std::shared_ptr<IMetaIndexEntry> &ret_index_entry,
        int64_t &ret_end_offset);

    bool filter_stasify(ITimeseriesIndex *ts_index, Filter *time_filter);

    int get_all_leaf(
        std::shared_ptr<MetaIndexNode> index_node,
        std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> >
            &index_node_entry_list);

   private:
    ReadFile *read_file_;
    common::PageArena tsfile_meta_page_arena_;
    TsFileMeta tsfile_meta_;
    bool tsfile_meta_ready_;
    bool read_file_created_;
};

}  // end namespace storage
#endif  // FILE_TSFILE_IO_REAER_H
