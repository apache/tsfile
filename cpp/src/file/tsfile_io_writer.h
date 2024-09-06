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
#include "common/tsfile_common.h"
#include "reader/bloom_filter.h"
#include "write_file.h"

namespace storage {

struct FileIndexWritingMemManager {
    common::PageArena pa_;
    std::vector<MetaIndexNode *> all_index_nodes_;

    FileIndexWritingMemManager() {
        pa_.init(512, common::MOD_WRITER_INDEX_NODE);
    }
    ~FileIndexWritingMemManager() {
        for (size_t i = 0; i < all_index_nodes_.size(); i++) {
            all_index_nodes_[i]->children_.~vector();
        }
        all_index_nodes_.clear();
    }
};

class TsFileIOWriter {
   public:
    typedef std::map<common::String, MetaIndexNode *, common::StringLessThan>
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
          write_file_created_(false) {}
    ~TsFileIOWriter() { destroy(); }

#ifndef LIBTSFILE_SDK
    int init();
    FORCE_INLINE common::FileID get_file_id() { return file_->get_file_id(); }
#endif
    int init(WriteFile *write_file);
    void destroy();

    int start_file();
    int start_flush_chunk_group(const std::string &device_name,
                                bool is_aligned = false);
    int start_flush_chunk(common::ByteStream &chunk_data,
                          common::ColumnDesc &col_desc, int32_t num_of_pages);
    int start_flush_chunk(common::ByteStream &chunk_data,
                          std::string &measurement_name,
                          common::TSDataType data_type,
                          common::TSEncoding encoding,
                          common::CompressionType compression,
                          int32_t num_of_pages) {
        common::TsID dummy_ts_id;
        return start_flush_chunk(chunk_data, measurement_name, data_type,
                                 encoding, compression, num_of_pages,
                                 dummy_ts_id);
    }
    int flush_chunk(common::ByteStream &chunk_data);
    int end_flush_chunk(Statistic *chunk_statistic);
    int end_flush_chunk_group(bool is_aligned = false);
    int end_file();

    FORCE_INLINE std::vector<TimeseriesTimeIndexEntry>
        &get_ts_time_index_vector() {
        return ts_time_index_vector_;
    }
    FORCE_INLINE std::string get_file_path() { return file_->get_file_path(); }

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
    int build_device_level(DeviceNodeMap &device_map, MetaIndexNode *&ret_root,
                           FileIndexWritingMemManager &wmm);
    int alloc_and_init_meta_index_entry(FileIndexWritingMemManager &wmm,
                                        MetaIndexEntry *&ret_entry,
                                        common::String &name);
    int alloc_and_init_meta_index_node(FileIndexWritingMemManager &wmm,
                                       MetaIndexNode *&ret_node,
                                       const MetaIndexNodeType node_type);
    int add_cur_index_node_to_queue(MetaIndexNode *node,
                                    common::SimpleList<MetaIndexNode *> *queue);
    int alloc_meta_index_node_queue(
        FileIndexWritingMemManager &wmm,
        common::SimpleList<MetaIndexNode *> *&queue);
    int add_device_node(
        DeviceNodeMap &device_map, common::String device_name,
        common::SimpleList<MetaIndexNode *> *measurement_index_node_queue,
        FileIndexWritingMemManager &wmm);
    int clone_node_list(common::SimpleList<MetaIndexNode *> *src,
                        common::SimpleList<MetaIndexNode *> *dest);
    int generate_root(common::SimpleList<MetaIndexNode *> *node_queue,
                      MetaIndexNode *&root_node, MetaIndexNodeType node_type,
                      FileIndexWritingMemManager &wmm);
    FORCE_INLINE void swap_list(common::SimpleList<MetaIndexNode *> *&l1,
                                common::SimpleList<MetaIndexNode *> *&l2) {
        common::SimpleList<MetaIndexNode *> *tmp = l1;
        l1 = l2;
        l2 = tmp;
    }

    int write_separator_marker(int64_t &meta_offset);

    // for bloom filter
    int init_bloom_filter(BloomFilter &filter);
    int32_t get_path_count(common::SimpleList<ChunkGroupMeta *> &cgm_list);

    // for open file
    void add_ts_time_index_entry(TimeseriesIndex &ts_index);

    int start_flush_chunk(common::ByteStream &chunk_data,
                          std::string &measurement_name,
                          common::TSDataType data_type,
                          common::TSEncoding encoding,
                          common::CompressionType compression,
                          int32_t num_of_pages, common::TsID ts_id);

   private:
    common::PageArena meta_allocator_;
    common::ByteStream write_stream_;
    common::ByteStream::Consumer write_stream_consumer_;
    ChunkMeta *cur_chunk_meta_;
    ChunkGroupMeta *cur_chunk_group_meta_;
    int32_t chunk_meta_count_;  // for debug
    common::SimpleList<ChunkGroupMeta *> chunk_group_meta_list_;
    bool use_prev_alloc_cgm_;  // chunk group meta
    std::string cur_device_name_;
    WriteFile *file_;
    std::vector<TimeseriesTimeIndexEntry> ts_time_index_vector_;
    bool write_file_created_;
};

}  // end namespace storage
#endif  // FILE_TSFILE_IO_WRITER_H
