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

#include "tsfile_io_writer.h"

#include <fcntl.h>

#include <memory>

#include "common/device_id.h"
#include "common/global.h"
#include "common/logger/elog.h"
#include "writer/chunk_writer.h"

using namespace common;

namespace storage {

#if 0
#define OFFSET_DEBUG(msg)                \
    std::cout << "OFFSET_DEBUG: " << msg \
              << ". offset=" << write_stream_.total_size() << std::endl
#else
#define OFFSET_DEBUG(msg) void(msg)
#endif

#ifndef LIBTSFILE_SDK
int TsFileIOWriter::init() {
    int ret = E_OK;
    const uint32_t page_size = 1024;
    meta_allocator_.init(page_size, MOD_TSFILE_WRITER_META);
    chunk_meta_count_ = 0;
    file_ = new WriteFile;
    write_file_created_ = true;

    FileID file_id;
    file_id.seq_ = get_cur_timestamp();
    file_id.version_ = 0;
    file_id.merge_ = 0;
    if (RET_FAIL(file_->create(file_id, O_RDWR | O_CREAT, 0644))) {
        // log_err("file open error, ret=%d", ret);
    }
    return ret;
}
#endif

int TsFileIOWriter::init(WriteFile *write_file) {
    int ret = E_OK;
    const uint32_t page_size = 1024;
    meta_allocator_.init(page_size, MOD_TSFILE_WRITER_META);
    chunk_meta_count_ = 0;
    file_ = write_file;
    return ret;
}

void TsFileIOWriter::destroy() {
    for (auto iter = chunk_group_meta_list_.begin();
         iter != chunk_group_meta_list_.end(); iter++) {
        if (iter.get() && iter.get()->device_id_) {
            iter.get()->device_id_.reset();
        }
        if (iter.get()) {
            for (auto chunk_meta = iter.get()->chunk_meta_list_.begin();
                 chunk_meta != iter.get()->chunk_meta_list_.end();
                 chunk_meta++) {
                if (chunk_meta.get()) {
                    chunk_meta.get()->statistic_->destroy();
                }
            }
        }
    }

    meta_allocator_.destroy();
    write_stream_.destroy();
    if (write_file_created_ && file_ != nullptr) {
        delete file_;
        file_ = nullptr;
    }
}

int TsFileIOWriter::start_file() {
    int ret = E_OK;
    if (RET_FAIL(write_buf(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN))) {
    } else if (RET_FAIL(write_byte(VERSION_NUM_BYTE))) {
    } else if (RET_FAIL(flush_stream_to_file())) {
    }
    return ret;
}

int TsFileIOWriter::start_flush_chunk_group(
    std::shared_ptr<IDeviceID> device_name, bool is_aligned) {
    int ret = write_byte(CHUNK_GROUP_HEADER_MARKER);
    if (ret != common::E_OK) {
        return ret;
    }
    ret = device_name->serialize(write_stream_);
    if (ret != common::E_OK) {
        return ret;
    }
    cur_device_name_ = device_name;
    ASSERT(cur_chunk_group_meta_ == nullptr);
    use_prev_alloc_cgm_ = false;
    for (auto iter = chunk_group_meta_list_.begin();
         iter != chunk_group_meta_list_.end(); iter++) {
        if (*iter.get()->device_id_ == *cur_device_name_) {
            use_prev_alloc_cgm_ = true;
            cur_chunk_group_meta_ = iter.get();
            break;
        }
    }
    if (!use_prev_alloc_cgm_) {
        void *buf = meta_allocator_.alloc(sizeof(*cur_chunk_group_meta_));
        if (IS_NULL(buf)) {
            ret = E_OOM;
        } else {
            cur_chunk_group_meta_ = new (buf) ChunkGroupMeta(&meta_allocator_);
            cur_chunk_group_meta_->init(device_name);
        }
    }
    return ret;
}

int TsFileIOWriter::start_flush_chunk(ByteStream &chunk_data,
                                      ColumnDesc &col_desc,
                                      int32_t num_of_pages) {
    std::string measurement_name = col_desc.get_measurement_name_str();
    return start_flush_chunk(chunk_data, measurement_name, col_desc.type_,
                             col_desc.encoding_, col_desc.compression_,
                             num_of_pages, col_desc.ts_id_);
}

int TsFileIOWriter::start_flush_chunk(common::ByteStream &chunk_data,
                                      std::string &measurement_name,
                                      common::TSDataType data_type,
                                      common::TSEncoding encoding,
                                      common::CompressionType compression,
                                      int32_t num_of_pages,
                                      common::TsID ts_id) {
    int ret = E_OK;

    // Step 1. record chunk meta
    const int mask = 0;  // for common chunk
    ASSERT(cur_chunk_meta_ == nullptr);
    void *buf1 = meta_allocator_.alloc(sizeof(*cur_chunk_meta_));
    void *buf2 = meta_allocator_.alloc(get_typed_statistic_sizeof(data_type));
    if (IS_NULL(buf1) || IS_NULL(buf2)) {
        ret = E_OOM;
    } else {
        cur_chunk_meta_ = new (buf1) ChunkMeta();
        Statistic *chunk_statistic_copy =
            placement_new_statistic(data_type, buf2);
        String mname((char *)measurement_name.c_str(),
                     strlen(measurement_name.c_str()));
        ret = cur_chunk_meta_->init(mname, data_type, cur_file_position(),
                                    chunk_statistic_copy, ts_id, mask, encoding,
                                    compression, meta_allocator_);
    }

    // Step 2. serialize chunk header to write_stream_
    if (IS_SUCC(ret)) {
        ChunkHeader chunk_header;
        chunk_header.measurement_name_ = measurement_name,
        chunk_header.data_size_ = chunk_data.total_size();
        chunk_header.data_type_ = data_type;
        chunk_header.compression_type_ = compression;
        chunk_header.encoding_type_ = encoding;
        chunk_header.num_of_pages_ = num_of_pages;
        chunk_header.chunk_type_ =
            (num_of_pages <= 1 ? ONLY_ONE_PAGE_CHUNK_HEADER_MARKER
                               : CHUNK_HEADER_MARKER);
        OFFSET_DEBUG("start flush chunk header");
        ret = chunk_header.serialize_to(write_stream_);
        OFFSET_DEBUG("after flush chunk header");
#if DEBUG_SE
        std::cout << "TsFileIOWriter writer ChunkHeader: " << chunk_header
                  << std::endl;
#endif
    }
    return ret;
}

int TsFileIOWriter::flush_chunk(ByteStream &chunk_data) {
    int ret = E_OK;
    if (RET_FAIL(write_chunk_data(chunk_data))) {
        // log_err("writer chunk data error, ret=%d", ret);
    } else if (RET_FAIL(flush_stream_to_file())) {
        // log_err("flush stream error, ret=%d", ret);
    }
    return ret;
}

int TsFileIOWriter::write_chunk_data(ByteStream &chunk_data) {
    OFFSET_DEBUG("before writer chunk data");
    // may print chunk_data here for debug.
#if DEBUG_SE
    DEBUG_print_byte_stream("WriteChunkData chunk_data=", chunk_data);
    std::cout << "WriteChunkData: write_stream_.total_size="
              << write_stream_.total_size()
              << ", chunk_data.total_size=" << chunk_data.total_size()
              << std::endl;
#endif
    int ret = merge_byte_stream(write_stream_, chunk_data, true);
    OFFSET_DEBUG("before writer chunk data");
    return ret;
}

int TsFileIOWriter::end_flush_chunk(Statistic *chunk_statistic) {
    int ret = E_OK;
    chunk_meta_count_++;
    cur_chunk_meta_->clone_statistic_from(chunk_statistic);
    if (RET_FAIL(cur_chunk_group_meta_->push(cur_chunk_meta_))) {
    } else {
        cur_chunk_meta_ = nullptr;
    }
    return ret;
}

int TsFileIOWriter::end_flush_chunk_group(bool is_aligned) {
    if (generate_table_schema_) {
        schema_->update_table_schema(cur_chunk_group_meta_);
    }

    if (use_prev_alloc_cgm_) {
        cur_chunk_group_meta_ = nullptr;
        return common::E_OK;
    }
    int ret = chunk_group_meta_list_.push_back(cur_chunk_group_meta_);
    cur_chunk_group_meta_ = nullptr;
    return ret;
}

int TsFileIOWriter::end_file() {
    int ret = E_OK;
    OFFSET_DEBUG("before end file");
    if (RET_FAIL(write_log_index_range())) {
        std::cout << "writer range index error, ret =" << ret << std::endl;
    } else if (RET_FAIL(write_file_index())) {
        std::cout << "writer file index error, ret = " << ret << std::endl;
    } else if (RET_FAIL(write_file_footer())) {
        std::cout << "writer file footer error, ret = " << ret << std::endl;
    } else if (RET_FAIL(sync_file())) {
        std::cout << "sync file error, ret = " << ret << std::endl;
    } else if (RET_FAIL(close_file())) {
        std::cout << "close file error, ret = " << ret << std::endl;
    }
    return ret;
}

int TsFileIOWriter::write_log_index_range() {
    int ret = E_OK;
    // Currently, Java IoTDB leaves it as default value.
    const int64_t min_plan_index = 0;  // 0x7fffffffffffffff;
    const int64_t max_plan_index = 0;  // 0x8000000000000000;
    //  OFFSET_DEBUG("before writer log index");
    if (RET_FAIL(write_byte(OPERATION_INDEX_RANGE))) {
        std::cout << "writer byte error " << ret << std::endl;
    } else if (RET_FAIL(SerializationUtil::write_i64(min_plan_index,
                                                     write_stream_))) {
        std::cout << "min index error " << ret << std::endl;
    } else if (RET_FAIL(SerializationUtil::write_i64(max_plan_index,
                                                     write_stream_))) {
        std::cout << "max index error " << ret << std::endl;
    }
    return ret;
}

#if DEBUG_SE
void debug_print_chunk_group_meta(ChunkGroupMeta *cgm) {
    std::cout << "ChunkGroupMeta = {device_id_="
              << cgm->device_id_ << ", chunk_meta_list_={";
    SimpleList<ChunkMeta *>::Iterator cm_it = cgm->chunk_meta_list_.begin();
    for (; cm_it != cgm->chunk_meta_list_.end(); cm_it++) {
        ChunkMeta *cm = cm_it.get();
        std::cout << "{measurement=" << cm->measurement_name_
                  << ", offset_of_chunk_header=" << cm->offset_of_chunk_header_
                  << ", statistic=" << cm->statistic_->to_string() << "}}}";
    }
    std::cout << std::endl;
}

void debug_print_chunk_group_meta_list(
    common::SimpleList<ChunkGroupMeta *> &cgm_list) {
    SimpleList<ChunkGroupMeta *>::Iterator cgm_it = cgm_list.begin();
    for (; cgm_it != cgm_list.end(); cgm_it++) {
        ChunkGroupMeta *cgm = cgm_it.get();
        debug_print_chunk_group_meta(cgm);
    }
}
#endif

// TODO better memory management.
int TsFileIOWriter::write_file_index() {
#if DEBUG_SE
    debug_print_chunk_group_meta_list(chunk_group_meta_list_);
#endif

    int ret = E_OK;
    int64_t meta_offset = 0;
    BloomFilter filter;

    // TODO: better memory manage for this while-loop, cur_index_node_queue
    FileIndexWritingMemManager writing_mm;
    std::shared_ptr<IDeviceID> device_id;
    String measurement_name;
    TimeseriesIndex ts_index;
    std::shared_ptr<IDeviceID> prev_device_id;
    int entry_count_in_cur_device = 0;
    std::shared_ptr<IMetaIndexEntry> meta_index_entry = nullptr;
    std::shared_ptr<MetaIndexNode> cur_index_node = nullptr;
    SimpleList<std::shared_ptr<MetaIndexNode>> *cur_index_node_queue = nullptr;
    DeviceNodeMap device_map;

    TSMIterator tsm_iter(chunk_group_meta_list_);

    if (RET_FAIL(write_separator_marker(meta_offset))) {
        return ret;
    } else if (RET_FAIL(init_bloom_filter(filter))) {
        return ret;
    }

    if (RET_FAIL(tsm_iter.init())) {
        return ret;
    }

    while (IS_SUCC(ret) && tsm_iter.has_next()) {
        ts_index.reset();  // TODO reuse
        if (RET_FAIL(
                tsm_iter.get_next(device_id, measurement_name, ts_index))) {
            break;
        }
#if DEBUG_SE
        std::cout << "tsm_iter get next = {device_name=" << device_id
                  << ", measurement_name=" << measurement_name
                  << ", ts_index=" << ts_index << std::endl;
#endif

        // prepare if it is an entry of a new device
        if (prev_device_id == nullptr || prev_device_id != device_id) {
            if (prev_device_id != nullptr) {
                if (RET_FAIL(add_cur_index_node_to_queue(
                        cur_index_node, cur_index_node_queue))) {
                } else if (RET_FAIL(add_device_node(device_map, prev_device_id,
                                                    cur_index_node_queue,
                                                    writing_mm))) {
                }
            }
            if (IS_SUCC(ret)) {
                destroy_node_list(cur_index_node_queue);
                if (RET_FAIL(alloc_meta_index_node_queue(
                        writing_mm, cur_index_node_queue))) {
                } else if (RET_FAIL(alloc_and_init_meta_index_node(
                               writing_mm, cur_index_node, LEAF_MEASUREMENT))) {
                }
            }
            prev_device_id = device_id;
            entry_count_in_cur_device = 0;
        }

        // pick an entry as index entry every max_degree_of_index_node entries.
        if (IS_SUCC(ret) && entry_count_in_cur_device %
                                    g_config_value_.max_degree_of_index_node_ ==
                                0) {
            if (cur_index_node->is_full()) {
                if (RET_FAIL(add_cur_index_node_to_queue(
                        cur_index_node, cur_index_node_queue))) {
                } else if (RET_FAIL(alloc_and_init_meta_index_node(
                               writing_mm, cur_index_node, LEAF_MEASUREMENT))) {
                }
            }

            if (IS_SUCC(ret)) {
                if (RET_FAIL(alloc_and_init_meta_index_entry(
                        writing_mm, meta_index_entry, measurement_name))) {
                } else if (RET_FAIL(
                               cur_index_node->push_entry(meta_index_entry))) {
                }
            }
        }

        if (IS_SUCC(ret)) {
            OFFSET_DEBUG("before ts_index written");
            if (ts_index.get_data_type() != common::VECTOR) {
                common::String tmp_device_name;
                tmp_device_name.dup_from(device_id->get_device_name(),
                                         meta_allocator_);
                ret = filter.add_path_entry(tmp_device_name, measurement_name);
            }

            if (RET_FAIL(ts_index.serialize_to(write_stream_))) {
            } else {
#if DEBUG_SE
                std::cout << "ts_index.serialize. ts_index=" << ts_index
                          << " file_pos=" << cur_file_position() << std::endl;
#endif
                entry_count_in_cur_device++;
                // add_ts_time_index_entry(ts_index);
            }
        }
    }  // end while
    if (ret == E_NO_MORE_DATA) {
        ret = E_OK;
    }
    ASSERT(ret == E_OK);
    if (IS_SUCC(ret) && cur_index_node != nullptr &&
        cur_index_node_queue != nullptr) {  // iter finish
        ASSERT(cur_index_node != nullptr);
        ASSERT(cur_index_node_queue != nullptr);
        if (RET_FAIL(add_cur_index_node_to_queue(cur_index_node,
                                                 cur_index_node_queue))) {
        } else if (RET_FAIL(add_device_node(device_map, prev_device_id,
                                            cur_index_node_queue,
                                            writing_mm))) {
        }
    }

    if (IS_SUCC(ret)) {
        TsFileMeta tsfile_meta;
        tsfile_meta.meta_offset_ = meta_offset;
        tsfile_meta.bloom_filter_ = &filter;
        // split device by table
        std::map<std::string, DeviceNodeMap> table_device_nodes_map;
        for (const auto &entry : device_map) {
            std::string table_name = entry.first->get_table_name();
            auto &table_map = table_device_nodes_map[table_name];
            if (table_map.empty() ||
                table_map.find(entry.first) == table_map.end()) {
                table_map[entry.first] = entry.second;
            }
        }
        std::map<std::string, std::shared_ptr<MetaIndexNode>> table_nodes_map;
        for (auto &entry : table_device_nodes_map) {
            auto meta_index_node = std::make_shared<MetaIndexNode>(&meta_allocator_);
            build_device_level(entry.second, meta_index_node, writing_mm);
            table_nodes_map[entry.first] = meta_index_node;
        }
        tsfile_meta.table_metadata_index_node_map_ = table_nodes_map;
        tsfile_meta.table_schemas_ = schema_->table_schema_map_;
        tsfile_meta.tsfile_properties_.insert(
            std::make_pair("encryptLevel", encrypt_level_));
        tsfile_meta.tsfile_properties_.insert(
            std::make_pair("encryptType", encrypt_type_));
        tsfile_meta.tsfile_properties_.insert(
            std::make_pair("encryptKey", encrypt_key_));
#if DEBUG_SE
        auto tsfile_meta_offset = write_stream_.total_size();
#endif
        auto total_write_size = tsfile_meta.serialize_to(write_stream_);
        if (RET_FAIL(common::SerializationUtil::write_i32(total_write_size,
                                                          write_stream_))) {
            return ret;
        }
        tsfile_meta.bloom_filter_ = nullptr;
#if DEBUG_SE
        std::cout << "writer tsfile_meta: " << tsfile_meta
                  << ", tsfile_meta_offset=" << tsfile_meta_offset
                  << ", size=" << total_write_size << std::endl;
        DEBUG_print_byte_stream("byte_stream", write_stream_);
#endif
    }
    destroy_node_list(cur_index_node_queue);
    return ret;
}

int TsFileIOWriter::build_device_level(DeviceNodeMap &device_map,
                                       std::shared_ptr<MetaIndexNode> &ret_root,
                                       FileIndexWritingMemManager &wmm) {
    int ret = E_OK;

    SimpleList<std::shared_ptr<MetaIndexNode>> node_queue(1024,
                                           MOD_TSFILE_WRITER_META);  // FIXME
    DeviceNodeMapIterator device_map_iter;

    std::shared_ptr<MetaIndexNode> cur_index_node = nullptr;
    if (RET_FAIL(
            alloc_and_init_meta_index_node(wmm, cur_index_node, LEAF_DEVICE))) {
        return ret;
    }

    for (device_map_iter = device_map.begin();
         device_map_iter != device_map.end() && IS_SUCC(ret);
         device_map_iter++) {
        auto device_id = device_map_iter->first;
        std::shared_ptr<IMetaIndexEntry> entry = nullptr;
        if (cur_index_node->is_full()) {
            cur_index_node->end_offset_ = cur_file_position();
#if DEBUG_SE
            std::cout << "TsFileIOWriter::build_device_level, cur_index_node="
                      << *cur_index_node << std::endl;
#endif
            if (RET_FAIL(node_queue.push_back(cur_index_node))) {
            } else if (RET_FAIL(alloc_and_init_meta_index_node(
                           wmm, cur_index_node, LEAF_DEVICE))) {
            }
        }
        if (RET_FAIL(
                alloc_and_init_meta_index_entry(wmm, entry, device_id))) {
        } else if (RET_FAIL(
                       device_map_iter->second->serialize_to(write_stream_))) {
        } else if (RET_FAIL(cur_index_node->push_entry(entry))) {
        }
    }  // end for
    if (IS_SUCC(ret)) {
        if (!cur_index_node->is_empty()) {
            cur_index_node->end_offset_ = cur_file_position();
            ret = node_queue.push_back(cur_index_node);
        }
    }

    if (IS_SUCC(ret)) {
        if (node_queue.size() > 0) {
            if (RET_FAIL(generate_root(&node_queue, ret_root, INTERNAL_DEVICE,
                                       wmm))) {
            }
        } else {
            ret_root = cur_index_node;
            ret_root->end_offset_ = cur_file_position();
            ret_root->node_type_ = LEAF_DEVICE;
        }
    }
    destroy_node_list(&node_queue);
    return ret;
}

int TsFileIOWriter::write_separator_marker(int64_t &meta_offset) {
    meta_offset = cur_file_position();
    return write_byte(SEPARATOR_MARKER);
}

int TsFileIOWriter::init_bloom_filter(BloomFilter &filter) {
    int ret = E_OK;
    int32_t path_count = get_path_count(chunk_group_meta_list_);
    if (RET_FAIL(filter.init(
            g_config_value_.tsfile_index_bloom_filter_error_percent_,
            path_count))) {
    }
    return ret;
}

int32_t TsFileIOWriter::get_path_count(
    common::SimpleList<ChunkGroupMeta *> &cgm_list) {
    int32_t path_count = 0;
    String prev_measurement;

    SimpleList<ChunkGroupMeta *>::Iterator cgm_it = cgm_list.begin();
    for (; cgm_it != cgm_list.end(); cgm_it++) {
        ChunkGroupMeta *cgm = cgm_it.get();
        SimpleList<ChunkMeta *>::Iterator cm_it = cgm->chunk_meta_list_.begin();
        for (; cm_it != cgm->chunk_meta_list_.end(); cm_it++) {
            ChunkMeta *cm = cm_it.get();
            // TODO currently, we do not have multi chunks of same timeseries in
            // tsfile.
            if (!cm->measurement_name_.equal_to(prev_measurement)) {
                path_count++;
                prev_measurement = cm->measurement_name_;
            }
        }
    }
    return path_count;
}

int TsFileIOWriter::write_file_footer() {
    int ret = E_OK;
    if (RET_FAIL(write_buf(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN))) {
    } else if (RET_FAIL(flush_stream_to_file())) {
    }
    return ret;
}

int TsFileIOWriter::alloc_and_init_meta_index_entry(
    FileIndexWritingMemManager &wmm,
    std::shared_ptr<IMetaIndexEntry> &ret_entry,
    const std::shared_ptr<IDeviceID> &device_id) {
    void *buf = wmm.pa_.alloc(sizeof(DeviceMetaIndexEntry));
    if (IS_NULL(buf)) {
        return E_OOM;
    }
    auto entry_ptr = static_cast<DeviceMetaIndexEntry *>(buf);
    new (entry_ptr) DeviceMetaIndexEntry(device_id, cur_file_position());
    ret_entry =
        std::shared_ptr<IMetaIndexEntry>(entry_ptr, IMetaIndexEntry::self_destructor);
#if DEBUG_SE
    std::cout << "alloc_and_init_meta_index_entry, MetaIndexEntry="
              << *ret_entry << std::endl;
#endif
    return E_OK;
}

int TsFileIOWriter::alloc_and_init_meta_index_entry(
    FileIndexWritingMemManager &wmm,
    std::shared_ptr<IMetaIndexEntry> &ret_entry, common::String &name) {
    void *buf = wmm.pa_.alloc(sizeof(MeasurementMetaIndexEntry));
    if (IS_NULL(buf)) {
        return E_OOM;
    }
    auto entry_ptr = static_cast<MeasurementMetaIndexEntry *>(buf);
    new (entry_ptr)
        MeasurementMetaIndexEntry(name, cur_file_position(), wmm.pa_);
    ret_entry =
        std::shared_ptr<IMetaIndexEntry>(entry_ptr, IMetaIndexEntry::self_destructor);
#if DEBUG_SE
    std::cout << "alloc_and_init_meta_index_entry, MetaIndexEntry="
              << *ret_entry << std::endl;
#endif
    return E_OK;
}

int TsFileIOWriter::alloc_and_init_meta_index_node(
    FileIndexWritingMemManager &wmm, std::shared_ptr<MetaIndexNode> &ret_node,
    MetaIndexNodeType node_type) {
//    void *buf = wmm.pa_.alloc(sizeof(MetaIndexNode));
//    if (IS_NULL(buf)) {
//        return E_OOM;
//    }
//    auto *node_ptr = new (buf) MetaIndexNode(&wmm.pa_);
//    node_ptr->node_type_ = node_type;
//    ret_node = std::shared_ptr<MetaIndexNode>(node_ptr, [](MetaIndexNode *ptr) {
//        if (ptr) {
//            ptr->~MetaIndexNode();
//        }
//    });
    ret_node = std::make_shared<MetaIndexNode>(&wmm.pa_);
    ret_node->node_type_ = node_type;
    wmm.all_index_nodes_.push_back(ret_node);
    return E_OK;
}

int TsFileIOWriter::add_cur_index_node_to_queue(
    std::shared_ptr<MetaIndexNode> node,
    SimpleList<std::shared_ptr<MetaIndexNode>> *queue) const {
    node->end_offset_ = cur_file_position();
#if DEBUG_SE
    std::cout << "add_cur_index_node_to_queue, node=" << *node << ", set offset=" << cur_file_position() << std::endl;
#endif
    return queue->push_back(node);
}

int TsFileIOWriter::alloc_meta_index_node_queue(
    FileIndexWritingMemManager &wmm,
    SimpleList<std::shared_ptr<MetaIndexNode>> *&queue) {
    void *buf = wmm.pa_.alloc(sizeof(*queue));
    if (IS_NULL(buf)) {
        return E_OOM;
    }
    queue = new (buf) SimpleList<std::shared_ptr<MetaIndexNode>>(&wmm.pa_);
    return E_OK;
}

int TsFileIOWriter::add_device_node(
    DeviceNodeMap &device_map, std::shared_ptr<IDeviceID> device_id,
    common::SimpleList<std::shared_ptr<MetaIndexNode>>
        *measurement_index_node_queue,
    FileIndexWritingMemManager &wmm) {
    ASSERT(measurement_index_node_queue->size() > 0);
    int ret = E_OK;
    auto find_iter = device_map.find(device_id);
    if (find_iter != device_map.end()) {
        return E_ALREADY_EXIST;
    }

    std::shared_ptr<MetaIndexNode> root = nullptr;
    if (RET_FAIL(generate_root(measurement_index_node_queue, root,
                               INTERNAL_MEASUREMENT, wmm))) {
    } else {
        std::pair<DeviceNodeMapIterator, bool> ins_res =
            device_map.insert(std::make_pair(device_id, root));
        if (!ins_res.second) {
            ASSERT(false);
        }
    }
    return ret;
}

void TsFileIOWriter::set_generate_table_schema(bool generate_table_schema) {
    generate_table_schema_ = generate_table_schema;
}

int TsFileIOWriter::generate_root(
    SimpleList<std::shared_ptr<MetaIndexNode>> *node_queue,
    std::shared_ptr<MetaIndexNode> &root_node, MetaIndexNodeType node_type,
    FileIndexWritingMemManager &wmm) {
    int ret = E_OK;

    ASSERT(node_queue->size() > 0);
    if (node_queue->size() == 1) {
        root_node = node_queue->front();
        return ret;
    }

    const uint32_t LIST_PAGE_SIZE = 256;
    const AllocModID mid = MOD_TSFILE_WRITER_META;
    SimpleList<std::shared_ptr<MetaIndexNode>> list_x(LIST_PAGE_SIZE, mid);
    SimpleList<std::shared_ptr<MetaIndexNode>> list_y(LIST_PAGE_SIZE, mid);

    if (RET_FAIL(clone_node_list(node_queue, &list_x))) {
        return ret;
    }

    common::SimpleList<std::shared_ptr<MetaIndexNode>> *from = &list_x;
    common::SimpleList<std::shared_ptr<MetaIndexNode>> *to = &list_y;

    std::shared_ptr<MetaIndexNode> cur_index_node = nullptr;
    if (RET_FAIL(
            alloc_and_init_meta_index_node(wmm, cur_index_node, node_type))) {
    }
    while (IS_SUCC(ret)) {
        to->clear();
        SimpleList<std::shared_ptr<MetaIndexNode>>::Iterator from_iter;
        for (from_iter = from->begin();
             IS_SUCC(ret) && from_iter != from->end(); from_iter++) {
            auto iter_node = from_iter.get();
            std::shared_ptr<IMetaIndexEntry> entry = nullptr;
            auto first_child = iter_node->peek();
            if (const auto derived_entry =
                    std::dynamic_pointer_cast<DeviceMetaIndexEntry>(
                        first_child)) {
                ret = alloc_and_init_meta_index_entry(
                    wmm, entry, derived_entry->device_id_);
            } else if (const auto measurement_entry =
                           std::dynamic_pointer_cast<MeasurementMetaIndexEntry>(
                               first_child)) {
                ret = alloc_and_init_meta_index_entry(wmm, entry,
                                                      measurement_entry->name_);
            } else {
                ret = E_INVALID_DATA_POINT;
            }
            if (IS_FAIL(ret)) {
                continue;
            }

            if (cur_index_node->is_full()) {
                cur_index_node->end_offset_ = cur_file_position();
                if (RET_FAIL(to->push_back(cur_index_node))) {
                } else {
#if DEBUG_SE
                    std::cout
                        << "generate root, alloc_and_init_meta_index_node. "
                           "cur_index_node="
                        << *cur_index_node << std::endl;
#endif
                    if (RET_FAIL(alloc_and_init_meta_index_node(
                            wmm, cur_index_node, node_type))) {
                    }
                }
            }
            if (IS_SUCC(ret)) {
                if (RET_FAIL(cur_index_node->push_entry(entry))) {
                } else {
                    OFFSET_DEBUG("before writer index_node in generate_root");
                    ret = iter_node->serialize_to(write_stream_);
                    OFFSET_DEBUG("after writer index_node in generate_root");
                }
            }
        }  // end for
        if (IS_SUCC(ret)) {
            if (!cur_index_node->is_empty()) {
                cur_index_node->end_offset_ = cur_file_position();
                if (RET_FAIL(to->push_back(cur_index_node))) {
                }
#if DEBUG_SE
                std::cout << "genereate root 2, "
                             "alloc_and_init_meta_index_node. cur_index_node="
                          << *cur_index_node << std::endl;
#endif
                if (RET_FAIL(alloc_and_init_meta_index_node(wmm, cur_index_node,
                                                            node_type))) {
                }
            }
        }
        if (IS_SUCC(ret)) {
            ASSERT(from->size() > to->size());
            if (to->size() == 1) {
                root_node = to->front();
                break;
            } else {
                swap_list(from, to);
            }
        }
    }  // end while
    destroy_node_list(&list_x);
    destroy_node_list(&list_y);
    return ret;
}

void TsFileIOWriter::destroy_node_list(common::SimpleList<std::shared_ptr<MetaIndexNode>> *list) {
    if (list) {
        for (auto iter = list->begin(); iter != list->end(); iter++) {
            if (iter.get()) {
                iter.get().reset();
            }
        }
    }

}

int TsFileIOWriter::clone_node_list(
    SimpleList<std::shared_ptr<MetaIndexNode>> *src,
    SimpleList<std::shared_ptr<MetaIndexNode>> *dest) {
    int ret = E_OK;
    SimpleList<std::shared_ptr<MetaIndexNode>>::Iterator it;
    for (it = src->begin(); IS_SUCC(ret) && it != src->end(); it++) {
        ret = dest->push_back(it.get());
    }
    return ret;
}

// #if DEBUG_SE
// void DEBUG_print_byte_stream_buf(const char *tag,
//                                  const char *buf,
//                                  const uint32_t len,
//                                  bool reset_print)
// {
//   static int print_count = 0;
//   if (reset_print) {
//     print_count = 0;
//   }
//
//   for (uint32_t i = 0; i < len; i++) {
//     if (print_count++ % 16 == 0) {
//       printf("\n%s: BUF=", tag);
//     }
//     printf("%02x ", (uint8_t)buf[i]);
//   }
//   printf("\n\n");
// }
// #endif

/*
 * TODO:
 * when finish flushing stream to file, reclaim memory used by stream
 */
int TsFileIOWriter::flush_stream_to_file() {
    int ret = E_OK;
    while (true) {
        ByteStream::Buffer b =
            write_stream_consumer_.get_next_buf(write_stream_);
        if (b.buf_ == nullptr) {
            break;
        } else {
            if (RET_FAIL(file_->write(b.buf_, b.len_))) {
                break;
            }
        }
    }

    write_stream_.purge_prev_pages();

    return ret;
}

void TsFileIOWriter::add_ts_time_index_entry(TimeseriesIndex &ts_index) {
    TimeseriesTimeIndexEntry time_index_entry;
    time_index_entry.ts_id_ = ts_index.get_ts_id();
    time_index_entry.time_range_.start_time_ =
        ts_index.get_statistic()->start_time_;
    time_index_entry.time_range_.end_time_ =
        ts_index.get_statistic()->end_time_;
    ts_time_index_vector_.push_back(time_index_entry);
}

}  // namespace storage
