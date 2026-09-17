/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#ifndef TSFILE_CLI_SKETCH_LAYOUT_H
#define TSFILE_CLI_SKETCH_LAYOUT_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/allocator/page_arena.h"
#include "common/constant/tsfile_constant.h"
#include "common/schema.h"
#include "common/tsfile_common.h"
#include "file/read_file.h"

namespace tsfile_cli {

constexpr int kSketchFileHeaderSize = storage::MAGIC_STRING_TSFILE_LEN + 1;
constexpr int kSketchFileTailSize = 4 + storage::MAGIC_STRING_TSFILE_LEN;

struct ChunkMetadataSketch {
    int64_t offset = 0;
    uint32_t size = 0;
};

struct PageSketch {
    int page_id = 0;
    int64_t header_offset = 0;
    int64_t data_offset = 0;
    uint32_t header_size = 0;
    uint32_t uncompressed_size = 0;
    uint32_t compressed_size = 0;
    std::string statistics;
    bool has_statistics = false;
};

struct ChunkSketch {
    int64_t offset = 0;
    storage::ChunkHeader header;
    uint32_t header_size = 0;
    std::string path;
    std::string statistics;
    std::vector<PageSketch> pages;
};

struct ChunkGroupSketch {
    int64_t offset = 0;
    std::string device;
    uint32_t device_id_size = 0;
    std::vector<ChunkSketch> chunks;
};

struct OperationIndexRangeSketch {
    int64_t offset = 0;
    int64_t min_plan_index = 0;
    int64_t max_plan_index = 0;
};

struct TimeseriesSketch {
    int64_t offset = 0;
    int64_t end_offset = 0;
    uint32_t size_without_chunk_metadata = 0;
    std::string device;
    std::string measurement;
    std::string path;
    std::string data_type;
    std::string statistics;
    uint32_t chunk_count = 0;
    std::vector<ChunkMetadataSketch> chunks;
};

struct MetadataNodeSketch {
    int64_t offset = 0;
    int64_t end_offset = 0;
    std::string table;
    std::string device;
    bool device_entries = false;
    std::shared_ptr<storage::MetaIndexNode> node;
};

struct FooterRootSketch {
    std::string table;
    int64_t table_name_offset = 0;
    uint32_t table_name_size = 0;
    int64_t node_offset = 0;
    uint32_t node_size = 0;
    std::shared_ptr<storage::MetaIndexNode> node;
};

struct FooterSchemaSketch {
    std::string table;
    int64_t table_name_offset = 0;
    uint32_t table_name_size = 0;
    int64_t schema_offset = 0;
    uint32_t schema_size = 0;
    std::shared_ptr<storage::TableSchema> schema;
};

struct FooterPropertySketch {
    std::string key;
    std::string value;
    int64_t offset = 0;
};

struct FooterSketch {
    int64_t table_index_count_offset = 0;
    uint32_t table_index_count = 0;
    std::vector<FooterRootSketch> roots;
    int64_t table_schema_count_offset = 0;
    uint32_t table_schema_count = 0;
    std::vector<FooterSchemaSketch> schemas;
    int64_t meta_offset_offset = 0;
    int64_t meta_offset_value = 0;
    int64_t bloom_filter_size_offset = 0;
    int64_t bloom_filter_offset = 0;
    uint32_t bloom_filter_data_size = 0;
    uint32_t bloom_filter_size = 0;
    uint32_t bloom_filter_hash_count = 0;
    uint32_t bloom_filter_serialized_size = 0;
    int64_t properties_offset = 0;
    int32_t properties_count = 0;
    std::vector<FooterPropertySketch> properties;
};

class SketchLayout {
   public:
    SketchLayout();
    ~SketchLayout();

    int load_file(const std::string& path);
    int collect_metadata_layout();
    int scan_data_area();
    void close();

    void add_blocker(const std::string& blocker);
    std::string error_text(int ret) const;

    std::string path_;
    int64_t file_size_ = 0;
    std::string head_magic_;
    std::string tail_magic_;
    char version_ = 0;
    uint32_t file_metadata_size_ = 0;
    int64_t file_metadata_pos_ = 0;
    int64_t separator_offset_ = -1;
    unsigned char separator_marker_ = 0;
    bool metadata_loaded_ = false;
    FooterSketch footer_;
    std::vector<ChunkGroupSketch> groups_;
    std::vector<OperationIndexRangeSketch> operation_ranges_;
    std::vector<MetadataNodeSketch> metadata_nodes_;
    std::vector<std::string> blockers_;
    std::map<int64_t, TimeseriesSketch> timeseries_by_offset_;
    std::map<int64_t, std::shared_ptr<storage::MetaIndexNode>> node_by_offset_;

   private:
    int read_block(int64_t offset, int64_t len, std::vector<char>* out);
    int read_marker(int64_t offset, unsigned char* marker);
    int trace_footer_layout();
    void add_metadata_node(const std::string& table, const std::string& device,
                           int64_t offset, int64_t end_offset,
                           bool device_entries,
                           std::shared_ptr<storage::MetaIndexNode> node);
    int traverse_index_node(const std::string& table,
                            const std::shared_ptr<storage::MetaIndexNode>& node,
                            bool device_entries,
                            std::shared_ptr<storage::IDeviceID> device_id);
    int read_index_node(int64_t offset, int64_t end_offset, bool device_entries,
                        std::shared_ptr<storage::MetaIndexNode>* out);
    int collect_timeseries_index(
        const std::string& table,
        const std::shared_ptr<storage::MetaIndexNode>& node,
        const std::shared_ptr<storage::IDeviceID>& device_id);
    void add_timeseries_record(
        int64_t offset, int64_t end_offset,
        const std::shared_ptr<storage::IDeviceID>& device_id,
        storage::TimeseriesIndex* ts_index);
    uint32_t serialized_chunk_metadata_size(storage::ChunkMeta* chunk_meta,
                                            bool include_statistics);
    int parse_chunk_group_header(int64_t marker_offset,
                                 ChunkGroupSketch* group);
    int parse_operation_index_range(int64_t marker_offset,
                                    OperationIndexRangeSketch* op);
    int parse_chunk(int64_t chunk_offset, const std::string& device,
                    ChunkSketch* chunk);
    int parse_chunk_header(int64_t chunk_offset, ChunkSketch* chunk);
    int parse_pages(int64_t data_offset, const std::vector<char>& data,
                    ChunkSketch* chunk);

    common::PageArena pa_;
    storage::TsFileMeta tsfile_meta_;
    storage::ReadFile file_;
    std::vector<char> metadata_buf_;
    std::map<int64_t, std::string> chunk_path_by_offset_;
    std::map<int64_t, std::string> chunk_stat_by_offset_;
    std::map<std::string, int64_t> root_offsets_by_table_;
    std::map<std::string, int64_t> root_ends_by_table_;
};

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_SKETCH_LAYOUT_H
