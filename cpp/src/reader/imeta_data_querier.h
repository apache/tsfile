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

#ifndef READER_IMETA_DATA_QUERIER_H
#define READER_IMETA_DATA_QUERIER_H

#include "common/allocator/my_string.h"
#include "common/device_id.h"
#include "common/path.h"
#include "reader/device_meta_iterator.h"

namespace storage {

class IMetadataQuerier {
   public:
    virtual ~IMetadataQuerier() = default;

    virtual std::vector<std::shared_ptr<ChunkMeta>> get_chunk_metadata_list(
        const Path& path) const = 0;

    virtual std::vector<std::vector<std::shared_ptr<ChunkMeta>>>
    get_chunk_metadata_lists(
        std::shared_ptr<IDeviceID> device_id,
        const std::unordered_set<std::string>& field_names,
        const MetaIndexNode* field_node = nullptr) const = 0;

    virtual std::map<Path, std::vector<std::shared_ptr<ChunkMeta>>>
    get_chunk_metadata_map(const std::vector<Path>& paths) const = 0;

    virtual int get_whole_file_metadata(TsFileMeta* tsfile_meta) const = 0;

    virtual void load_chunk_metadatas(const std::vector<Path>& paths) = 0;

    virtual common::TSDataType get_data_type(const Path& path) const = 0;

    virtual std::vector<TimeRange> convert_space_to_time_partition(
        const std::vector<Path>& paths, int64_t space_partition_start_pos,
        int64_t space_partition_end_pos) const = 0;

    virtual void clear() = 0;

    virtual std::unique_ptr<DeviceMetaIterator> device_iterator(
        MetaIndexNode* root, const Filter* id_filter) = 0;
};

}  // end namespace storage
#endif  // READER_IMETA_DATA_QUERIER_H
