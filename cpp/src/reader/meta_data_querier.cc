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

#include "reader/meta_data_querier.h"

#include "device_meta_iterator.h"

namespace storage {

MetadataQuerier::MetadataQuerier(TsFileIOReader* tsfile_io_reader)
    : io_reader_(tsfile_io_reader) {
    file_metadata_ = io_reader_->get_tsfile_meta();
    device_chunk_meta_cache_ = std::unique_ptr<
        common::Cache<std::string /*ToDO: Device ID*/,
                      std::vector<std::shared_ptr<ChunkMeta>>, std::mutex>>(
        new common::Cache<std::string, std::vector<std::shared_ptr<ChunkMeta>>,
                          std::mutex>(CACHED_ENTRY_NUMBER,
                                      CACHED_ENTRY_NUMBER / 10));
}

MetadataQuerier::~MetadataQuerier() {}


std::vector<std::shared_ptr<ChunkMeta>> MetadataQuerier::get_chunk_metadata_list(
    const Path& path) const {
    // std::vector<std::shared_ptr<ChunkMeta>> chunk_meta_list;
    // if (device_chunk_meta_cache_->tryGet(path.device_, chunk_meta_list)) {
    //     return chunk_meta_list;
    // } else {
    //     io_reader_->get_chunk_metadata_list(path.device_, path.measurement_, chunk_meta_list);
    // }
    // return io_reader_->get_chunk_metadata_list(path);
    ASSERT(false);
}

std::vector<std::vector<std::shared_ptr<ChunkMeta>>> MetadataQuerier::get_chunk_metadata_lists(
    std::shared_ptr<IDeviceID> device_id, const std::unordered_set<std::string>& field_names,
    const MetaIndexNode* field_node) const {
    // return io_reader_->get_chunk_metadata_lists(device_id, field_names, field_node);
    ASSERT(false);
}

std::map<Path, std::vector<std::shared_ptr<ChunkMeta>>> MetadataQuerier::get_chunk_metadata_map(const std::vector<Path>& paths) const {
    // return io_reader_->get_chunk_metadata_map(paths);
    ASSERT(false);
}

int MetadataQuerier::get_whole_file_metadata(TsFileMeta* tsfile_meta) const {
    tsfile_meta = io_reader_->get_tsfile_meta();
    return common::E_OK;
}

void MetadataQuerier::load_chunk_metadatas(const std::vector<Path>& paths) {
    // io_reader_->load_chunk_metadatas(paths);
    ASSERT(false);
}

common::TSDataType MetadataQuerier::get_data_type(const Path& path) const {
    ASSERT(false);
}

std::vector<TimeRange> MetadataQuerier::convert_space_to_time_partition(
    const std::vector<Path>& paths, int64_t spacePartitionStartPos,
    int64_t spacePartitionEndPos) const {
    ASSERT(false);
}

void MetadataQuerier::clear() {
}

std::unique_ptr<DeviceMetaIterator> MetadataQuerier::device_iterator(
    MetaIndexNode* root, const Filter* id_filter) {
    return std::unique_ptr<DeviceMetaIterator>(
        new DeviceMetaIterator(io_reader_, root, id_filter));
}

int MetadataQuerier::load_chunk_meta(const std::pair<IDeviceID, std::string>& key,
                        std::vector<ChunkMeta*>& chunk_meta_list) {
    // return io_reader_->load_chunk_meta(key, chunk_meta_list);
    ASSERT(false);
}

}  // end namespace storage
