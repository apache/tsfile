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

#ifndef READER_META_DATA_QUERIER_H
#define READER_META_DATA_QUERIER_H

#include "common/cache/lru_cache.h"
#include "common/device_id.h"
#include "device_meta_iterator.h"
#include "file/tsfile_io_reader.h"
#include "imeta_data_querier.h"

namespace storage {

class MetadataQuerier : public IMetadataQuerier {
   public:
    static constexpr int CACHED_ENTRY_NUMBER = 1000;

    enum class LocateStatus { BEFORE, IN, AFTER };

    explicit MetadataQuerier(TsFileIOReader* tsfile_io_reader);
    ~MetadataQuerier() override;
    std::vector<std::shared_ptr<ChunkMeta>> get_chunk_metadata_list(
        const Path& path) const override;

    std::vector<std::vector<std::shared_ptr<ChunkMeta>>>
    get_chunk_metadata_lists(
        std::shared_ptr<IDeviceID> device_id,
        const std::unordered_set<std::string>& field_names,
        const MetaIndexNode* field_node = nullptr) const override;

    std::map<Path, std::vector<std::shared_ptr<ChunkMeta>>>
    get_chunk_metadata_map(const std::vector<Path>& paths) const override;

    int get_whole_file_metadata(TsFileMeta* tsfile_meta) const override;

    void load_chunk_metadatas(const std::vector<Path>& paths) override;

    common::TSDataType get_data_type(const Path& path) const override;

    std::vector<TimeRange> convert_space_to_time_partition(
        const std::vector<Path>& paths, int64_t spacePartitionStartPos,
        int64_t spacePartitionEndPos) const override;

    std::unique_ptr<DeviceMetaIterator> device_iterator(
        MetaIndexNode* root, const Filter* id_filter) override;

    void clear() override;

   private:
    TsFileIOReader* io_reader_;
    TsFileMeta* file_metadata_;
    std::unique_ptr<
        common::Cache<std::string, /*Todo std::pair<IDeviceID, std::string>*/
                      std::vector<std::shared_ptr<ChunkMeta>>, std::mutex>>
        device_chunk_meta_cache_;

    int load_chunk_meta(const std::pair<IDeviceID, std::string>& key,
                        std::vector<ChunkMeta*>& chunk_meta_list);

    static LocateStatus check_locate_status(
        const std::shared_ptr<ChunkMeta>& chunk_meta, long start, long end);
};

}  // end namespace storage
#endif  // READER_META_DATA_QUERIER_H