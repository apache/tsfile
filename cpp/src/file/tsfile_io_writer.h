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

#ifndef FILE_TSFILE_IO_WRITER_H
#define FILE_TSFILE_IO_WRITER_H

#include <map>
#include <vector>

#include "common/allocator/page_arena.h"
#include "common/container/list.h"
#include "common/global.h"
#include "common/schema.h"
#include "common/tsfile_common.h"
#include "reader/bloom_filter.h"
#include "write_file.h"

namespace storage {

struct FileIndexWritingMemManager {
    common::PageArena pa_;
    std::vector<std::shared_ptr<MetaIndexNode>> all_index_nodes_;

    FileIndexWritingMemManager() {
        pa_.init(512, common::MOD_WRITER_INDEX_NODE);
    }
    ~FileIndexWritingMemManager() {
        for (size_t i = 0; i < all_index_nodes_.size(); i++) {
            all_index_nodes_[i]->children_.clear();
        }
        all_index_nodes_.clear();
    }
};

class TsFileIOWriter {
   public:
    typedef std::map<std::shared_ptr<IDeviceID>, std::shared_ptr<MetaIndexNode>,
                     IDeviceIDComparator>
        DeviceNodeMap;
    typedef DeviceNodeMap::iterator DeviceNodeMapIterator;

   public:
    static const uint32_t WRITE_STREAM_PAGE_SIZE = 512;  // FIXME
   public:
    TsFileIOWriter()
        : meta_allocator_(),
          write_stream_(WRITE_STREAM_PAGE_SIZE, common::MOD_TSFILE_WRITE_STREAM,
                        /*atomic*/ true),
          write_stream_consumer_(write_stream_),
          cur_chunk_meta_(nullptr),
          cur_chunk_group_meta_(nullptr),
          chunk_meta_count_(0),
          chunk_group_meta_list_(&meta_allocator_),
          use_prev_alloc_cgm_(false),
          cur_device_name_(),
          file_(nullptr),
          ts_time_index_vector_(),
          write_file_created_(false),
          generate_table_schema_(false),
          schema_(std::make_shared<Schema>()) {
        if (common::g_config_value_.encrypt_flag_) {
            // TODO: support encrypt
            encrypt_level_ = "2";
            encrypt_type_ = "";
            encrypt_key_ = "";
        } else {
            encrypt_level_ = "0";
            encrypt_type_ = "org.apache.tsfile.encrypt.UNENCRYPTED";
            encrypt_key_ = "";
        }
    }
    ~TsFileIOWriter() { destroy(); }

#ifndef LIBTSFILE_SDK
    int init();
    FORCE_INLINE common::FileID get_file_id() { return file_->get_file_id(); }
#endif
    int init(WriteFile *write_file);
    void destroy();

    void set_generate_table_schema(bool generate_table_schema);
    int start_file();
    int start_flush_chunk_group(std::shared_ptr<IDeviceID> device_id,
                                bool is_aligned = false);
    int start_flush_chunk(common::ByteStream &chunk_data,
                          common::ColumnSchema &col_schema,
                          int32_t num_of_pages);
    int start_flush_chunk(common::ByteStream &chunk_data,
                          std::string &measurement_name,
                          common::TSDataType data_type,
                          common::TSEncoding encoding,
                          common::CompressionType compression,
                          int32_t num_of_pages);
    int flush_chunk(common::ByteStream &chunk_data);
    int end_flush_chunk(Statistic *chunk_statistic);
    int end_flush_chunk_group(bool is_aligned = false);
    int end_file();

    FORCE_INLINE std::vector<TimeseriesTimeIndexEntry> &
    get_ts_time_index_vector() {
        return ts_time_index_vector_;
    }
    FORCE_INLINE std::string get_file_path() { return file_->get_file_path(); }
    FORCE_INLINE std::shared_ptr<Schema> get_schema() { return schema_; }

   private:
    int write_log_index_range();
    int write_file_index();
    FORCE_INLINE int sync_file() { return file_->sync(); }
    FORCE_INLINE int close_file() { return file_->close(); }
    int flush_stream_to_file();
    int write_chunk_data(common::ByteStream &chunk_data);
    FORCE_INLINE int64_t cur_file_position() const {
        return write_stream_.total_size();
    }
    FORCE_INLINE int write_buf(const char *buf, uint32_t len) {
        return write_stream_.write_buf(buf, len);
    }
    FORCE_INLINE int write_byte(const char byte) {
        return common::SerializationUtil::write_char(byte, write_stream_);
    }
    FORCE_INLINE int write_string(const std::string &str) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_var_int(str.size(),
                                                              write_stream_))) {
        } else if (RET_FAIL(write_stream_.write_buf(str.c_str(), str.size()))) {
        }
        return ret;
    }
    int write_file_footer();
    int build_device_level(DeviceNodeMap &device_map,
                           std::shared_ptr<MetaIndexNode> &ret_root,
                           FileIndexWritingMemManager &wmm);
    int alloc_and_init_meta_index_entry(
        FileIndexWritingMemManager &wmm,
        std::shared_ptr<IMetaIndexEntry> &ret_entry, common::String &name);
    int alloc_and_init_meta_index_entry(
        FileIndexWritingMemManager &wmm,
        std::shared_ptr<IMetaIndexEntry> &ret_entry,
        const std::shared_ptr<IDeviceID> &device_id);
    int alloc_and_init_meta_index_node(FileIndexWritingMemManager &wmm,
                                       std::shared_ptr<MetaIndexNode> &ret_node,
                                       MetaIndexNodeType node_type);
    int add_cur_index_node_to_queue(
        std::shared_ptr<MetaIndexNode> node,
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *queue) const;
    int alloc_meta_index_node_queue(
        FileIndexWritingMemManager &wmm,
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *&queue);
    int add_device_node(DeviceNodeMap &device_map,
                        std::shared_ptr<IDeviceID> device_id,
                        common::SimpleList<std::shared_ptr<MetaIndexNode>>
                            *measurement_index_node_queue,
                        FileIndexWritingMemManager &wmm);
    void destroy_node_list(
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *list);
    int clone_node_list(
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *src,
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *dest);
    int generate_root(
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *node_queue,
        std::shared_ptr<MetaIndexNode> &root_node, MetaIndexNodeType node_type,
        FileIndexWritingMemManager &wmm);
    FORCE_INLINE void swap_list(
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *&l1,
        common::SimpleList<std::shared_ptr<MetaIndexNode>> *&l2) {
        auto tmp = l1;
        l1 = l2;
        l2 = tmp;
    }

    std::shared_ptr<MetaIndexNode> check_and_build_level_index(
        DeviceNodeMap &device_metadata_index_map);

    int write_separator_marker(int64_t &meta_offset);

    // for bloom filter
    int init_bloom_filter(BloomFilter &filter);
    int32_t get_path_count(common::SimpleList<ChunkGroupMeta *> &cgm_list);

    // for open file
    void add_ts_time_index_entry(TimeseriesIndex &ts_index);

   private:
    common::PageArena meta_allocator_;
    common::ByteStream write_stream_;
    common::ByteStream::Consumer write_stream_consumer_;
    ChunkMeta *cur_chunk_meta_;
    ChunkGroupMeta *cur_chunk_group_meta_;
    int32_t chunk_meta_count_;  // for debug
    common::SimpleList<ChunkGroupMeta *> chunk_group_meta_list_;
    bool use_prev_alloc_cgm_;  // chunk group meta
    std::shared_ptr<IDeviceID> cur_device_name_;
    WriteFile *file_;
    std::vector<TimeseriesTimeIndexEntry> ts_time_index_vector_;
    bool write_file_created_;
    bool generate_table_schema_;
    std::shared_ptr<Schema> schema_;
    std::string encrypt_level_;
    std::string encrypt_type_;
    std::string encrypt_key_;
    bool is_aligned_;
};

}  // end namespace storage
#endif  // FILE_TSFILE_IO_WRITER_H
