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

#include "commands/sketch_layout.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <sstream>

#include "common/allocator/byte_stream.h"
#include "common/device_id.h"
#include "format/output_format.h"
#include "utils/errno_define.h"

namespace tsfile_cli {
namespace {

constexpr int32_t kMinChunkHeaderSize = 7;
constexpr int32_t kMaxHeaderProbeBytes = 1 << 20;
constexpr int32_t kDeviceHeaderProbeBytes = 64 << 10;
const char* const kMetadataMissing = "<metadata-missing>";

std::string to_string_i64(int64_t v) {
    std::ostringstream ss;
    ss << v;
    return ss.str();
}

std::string java_bool_string(bool value) { return value ? "true" : "false"; }

std::string java_floating_string(double value) {
    std::ostringstream ss;
    ss << std::setprecision(15) << value;
    std::string result = ss.str();
    if (result.find('.') == std::string::npos &&
        result.find('E') == std::string::npos &&
        result.find('e') == std::string::npos) {
        result += ".0";
    }
    return result;
}

std::string java_stat_base(storage::Statistic* stat) {
    std::ostringstream ss;
    ss << "startTime: " << stat->start_time_ << " endTime: " << stat->end_time_
       << " count: " << stat->count_;
    return ss.str();
}

template <typename T>
std::string java_numeric_stat_string(storage::Statistic* stat, T min_value,
                                     T max_value, T first_value, T last_value,
                                     const std::string& sum_value) {
    std::ostringstream ss;
    ss << java_stat_base(stat) << " [minValue:" << min_value
       << ",maxValue:" << max_value << ",firstValue:" << first_value
       << ",lastValue:" << last_value << ",sumValue:" << sum_value << "]";
    return ss.str();
}

std::string statistic_string(storage::Statistic* stat) {
    if (stat == nullptr) {
        return std::string();
    }
    switch (stat->get_type()) {
        case common::VECTOR:
            return java_stat_base(stat);
        case common::INT32:
        case common::DATE: {
            storage::Int32Statistic* typed =
                dynamic_cast<storage::Int32Statistic*>(stat);
            return typed == nullptr
                       ? stat->to_string()
                       : java_numeric_stat_string(
                             stat, typed->min_value_, typed->max_value_,
                             typed->first_value_, typed->last_value_,
                             to_string_i64(typed->sum_value_));
        }
        case common::INT64:
        case common::TIMESTAMP: {
            storage::Int64Statistic* typed =
                dynamic_cast<storage::Int64Statistic*>(stat);
            return typed == nullptr
                       ? stat->to_string()
                       : java_numeric_stat_string(
                             stat, typed->min_value_, typed->max_value_,
                             typed->first_value_, typed->last_value_,
                             java_floating_string(typed->sum_value_));
        }
        case common::FLOAT: {
            storage::FloatStatistic* typed =
                dynamic_cast<storage::FloatStatistic*>(stat);
            return typed == nullptr
                       ? stat->to_string()
                       : java_numeric_stat_string(
                             stat, java_floating_string(typed->min_value_),
                             java_floating_string(typed->max_value_),
                             java_floating_string(typed->first_value_),
                             java_floating_string(typed->last_value_),
                             java_floating_string(typed->sum_value_));
        }
        case common::DOUBLE: {
            storage::DoubleStatistic* typed =
                dynamic_cast<storage::DoubleStatistic*>(stat);
            return typed == nullptr
                       ? stat->to_string()
                       : java_numeric_stat_string(
                             stat, java_floating_string(typed->min_value_),
                             java_floating_string(typed->max_value_),
                             java_floating_string(typed->first_value_),
                             java_floating_string(typed->last_value_),
                             java_floating_string(typed->sum_value_));
        }
        case common::BOOLEAN: {
            storage::BooleanStatistic* typed =
                dynamic_cast<storage::BooleanStatistic*>(stat);
            if (typed == nullptr) {
                return stat->to_string();
            }
            std::ostringstream ss;
            ss << java_stat_base(stat)
               << " [firstValue=" << java_bool_string(typed->first_value_)
               << ", lastValue=" << java_bool_string(typed->last_value_)
               << ", sumValue=" << typed->sum_value_ << "]";
            return ss.str();
        }
        case common::TEXT: {
            storage::TextStatistic* typed =
                dynamic_cast<storage::TextStatistic*>(stat);
            if (typed == nullptr) {
                return stat->to_string();
            }
            std::ostringstream ss;
            ss << java_stat_base(stat)
               << " [firstValue:" << typed->first_value_.to_std_string()
               << ",lastValue:" << typed->last_value_.to_std_string() << "]";
            return ss.str();
        }
        case common::STRING: {
            storage::StringStatistic* typed =
                dynamic_cast<storage::StringStatistic*>(stat);
            if (typed == nullptr) {
                return stat->to_string();
            }
            std::ostringstream ss;
            ss << java_stat_base(stat)
               << " [firstValue:" << typed->first_value_.to_std_string()
               << ", lastValue:" << typed->last_value_.to_std_string()
               << ", minValue:" << typed->min_value_.to_std_string()
               << ", maxValue:" << typed->max_value_.to_std_string() << "]";
            return ss.str();
        }
        case common::BLOB:
            return "BlobStatistics{}";
        default:
            return stat->to_string();
    }
}

bool is_chunk_marker(unsigned char marker) {
    const unsigned char marker_type = marker & 0x3F;
    return marker == static_cast<unsigned char>(storage::CHUNK_HEADER_MARKER) ||
           marker == static_cast<unsigned char>(
                         storage::ONLY_ONE_PAGE_CHUNK_HEADER_MARKER) ||
           marker_type ==
               static_cast<unsigned char>(storage::CHUNK_HEADER_MARKER) ||
           marker_type == static_cast<unsigned char>(
                              storage::ONLY_ONE_PAGE_CHUNK_HEADER_MARKER);
}

bool is_one_page_chunk(char chunk_type) {
    return (static_cast<unsigned char>(chunk_type) & 0x3F) ==
           static_cast<unsigned char>(
               storage::ONLY_ONE_PAGE_CHUNK_HEADER_MARKER);
}

int64_t read_i64_from_big_endian(char* buffer) {
    return static_cast<int64_t>(common::SerializationUtil::read_ui64(buffer));
}

uint32_t java_int_string_size(const std::string& s) {
    return static_cast<uint32_t>(4 + s.size());
}

uint32_t java_var_int_size(uint32_t value) {
    uint32_t u_value = value << 1;
    uint32_t position = 1;
    while ((u_value & 0xFFFFFF80) != 0) {
        u_value >>= 7;
        position++;
    }
    return position;
}

uint32_t unsigned_var_int_size(uint32_t value) {
    uint32_t position = 1;
    while ((value & 0xFFFFFF80) != 0) {
        value >>= 7;
        position++;
    }
    return position;
}

}  // namespace

SketchLayout::SketchLayout()
    : pa_(common::MOD_TSFILE_READER), tsfile_meta_(&pa_) {
    pa_.init(4096, common::MOD_TSFILE_READER);
}

SketchLayout::~SketchLayout() {
    node_by_offset_.clear();
    metadata_nodes_.clear();
    footer_.roots.clear();
}

int SketchLayout::load_file(const std::string& path) {
    path_ = path;
    int ret = file_.open(path_);
    if (ret != common::E_OK) {
        return ret;
    }
    file_size_ = file_.file_size();
    if (file_size_ < kSketchFileHeaderSize + kSketchFileTailSize) {
        return common::E_TSFILE_CORRUPTED;
    }

    std::vector<char> head;
    ret = read_block(0, kSketchFileHeaderSize, &head);
    if (ret != common::E_OK) {
        return ret;
    }
    head_magic_.assign(head.data(), storage::MAGIC_STRING_TSFILE_LEN);
    version_ = head[storage::MAGIC_STRING_TSFILE_LEN];

    std::vector<char> tail;
    ret = read_block(file_size_ - kSketchFileTailSize, kSketchFileTailSize,
                     &tail);
    if (ret != common::E_OK) {
        return ret;
    }
    file_metadata_size_ = common::SerializationUtil::read_ui32(tail.data());
    tail_magic_.assign(tail.data() + 4, storage::MAGIC_STRING_TSFILE_LEN);
    file_metadata_pos_ = file_size_ -
                         static_cast<int64_t>(file_metadata_size_) -
                         kSketchFileTailSize;
    if (file_metadata_size_ == 0 ||
        file_metadata_pos_ < kSketchFileHeaderSize ||
        file_metadata_pos_ >= file_size_) {
        return common::E_TSFILE_CORRUPTED;
    }
    if (file_metadata_size_ >
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        add_blocker(
            "TsFileMetadata is larger than the C++ sketch reader "
            "can buffer; metadata output is truncated.");
        return common::E_OK;
    }

    ret = read_block(file_metadata_pos_, file_metadata_size_, &metadata_buf_);
    if (ret != common::E_OK) {
        return ret;
    }
    common::ByteStream metadata_stream;
    metadata_stream.wrap_from(metadata_buf_.data(),
                              static_cast<int32_t>(metadata_buf_.size()));
    ret = tsfile_meta_.deserialize_from(metadata_stream);
    if (ret != common::E_OK) {
        return ret;
    }
    if (metadata_stream.read_pos() > metadata_buf_.size()) {
        return common::E_TSFILE_CORRUPTED;
    }
    ret = trace_footer_layout();
    if (ret == common::E_OK) {
        metadata_loaded_ = true;
    }
    return ret;
}

int SketchLayout::collect_metadata_layout() {
    for (auto& root_entry : tsfile_meta_.table_metadata_index_node_map_) {
        const std::string& table = root_entry.first;
        int64_t offset = -1;
        int64_t end_offset = -1;
        auto off = root_offsets_by_table_.find(table);
        if (off != root_offsets_by_table_.end()) {
            offset = off->second;
        }
        auto end = root_ends_by_table_.find(table);
        if (end != root_ends_by_table_.end()) {
            end_offset = end->second;
        }
        add_metadata_node(table, "", offset, end_offset, true,
                          root_entry.second);
        int ret = traverse_index_node(table, root_entry.second, true,
                                      std::shared_ptr<storage::IDeviceID>());
        if (ret != common::E_OK) {
            return ret;
        }
    }
    return common::E_OK;
}

int SketchLayout::scan_data_area() {
    int64_t pos = kSketchFileHeaderSize;
    ChunkGroupSketch* current_group = nullptr;
    while (pos < file_metadata_pos_) {
        unsigned char marker = 0;
        int ret = read_marker(pos, &marker);
        if (ret != common::E_OK) {
            return ret;
        }
        if (marker == static_cast<unsigned char>(storage::SEPARATOR_MARKER)) {
            separator_offset_ = pos;
            separator_marker_ = marker;
            break;
        }
        if (marker ==
            static_cast<unsigned char>(storage::CHUNK_GROUP_HEADER_MARKER)) {
            ChunkGroupSketch group;
            group.offset = pos;
            ret = parse_chunk_group_header(pos, &group);
            if (ret != common::E_OK) {
                return ret;
            }
            groups_.push_back(group);
            current_group = &groups_.back();
            pos += 1 + group.device_id_size;
            continue;
        }
        if (marker ==
            static_cast<unsigned char>(storage::OPERATION_INDEX_RANGE)) {
            OperationIndexRangeSketch op;
            op.offset = pos;
            ret = parse_operation_index_range(pos, &op);
            if (ret != common::E_OK) {
                return ret;
            }
            operation_ranges_.push_back(op);
            pos += 1 + 16;
            continue;
        }
        if (is_chunk_marker(marker)) {
            if (current_group == nullptr) {
                return common::E_TSFILE_CORRUPTED;
            }
            ChunkSketch chunk;
            ret = parse_chunk(pos, current_group->device, &chunk);
            if (ret != common::E_OK) {
                return ret;
            }
            pos += chunk.header_size + chunk.header.data_size_;
            current_group->chunks.push_back(chunk);
            continue;
        }
        return common::E_TSFILE_CORRUPTED;
    }
    if (separator_offset_ < 0) {
        return common::E_TSFILE_CORRUPTED;
    }
    return common::E_OK;
}

void SketchLayout::close() { file_.close(); }

void SketchLayout::add_blocker(const std::string& blocker) {
    if (std::find(blockers_.begin(), blockers_.end(), blocker) ==
        blockers_.end()) {
        blockers_.push_back(blocker);
    }
}

std::string SketchLayout::error_text(int ret) const {
    return std::string(error_code_message(ret)) + " (code " +
           to_string_i64(ret) + ")";
}

int SketchLayout::read_block(int64_t offset, int64_t len,
                             std::vector<char>* out) {
    if (len < 0) {
        return common::E_OUT_OF_RANGE;
    }
    if (len > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
        add_blocker("read range [" + to_string_i64(offset) + ", " +
                    to_string_i64(offset + len) +
                    ") exceeds the C++ sketch reader buffer limit; output "
                    "is truncated.");
        return common::E_OUT_OF_RANGE;
    }
    out->assign(static_cast<size_t>(len), '\0');
    if (len == 0) {
        return common::E_OK;
    }
    int32_t read_len = 0;
    int ret =
        file_.read(offset, out->data(), static_cast<int32_t>(len), read_len);
    if (ret != common::E_OK) {
        return ret;
    }
    return read_len == static_cast<int32_t>(len) ? common::E_OK
                                                 : common::E_TSFILE_CORRUPTED;
}

int SketchLayout::read_marker(int64_t offset, unsigned char* marker) {
    char ch = 0;
    int32_t read_len = 0;
    int ret = file_.read(offset, &ch, 1, read_len);
    if (ret != common::E_OK) {
        return ret;
    }
    if (read_len != 1) {
        return common::E_TSFILE_CORRUPTED;
    }
    *marker = static_cast<unsigned char>(ch);
    return common::E_OK;
}

int SketchLayout::trace_footer_layout() {
    common::ByteStream bs;
    bs.wrap_from(metadata_buf_.data(),
                 static_cast<int32_t>(metadata_buf_.size()));

    int ret = common::E_OK;
    footer_.table_index_count_offset = file_metadata_pos_ + bs.read_pos();
    if (RET_FAIL(common::SerializationUtil::read_var_uint(
            footer_.table_index_count, bs))) {
        return ret;
    }

    int64_t java_pos = footer_.table_index_count_offset + 4;
    common::PageArena trace_pa(common::MOD_TSFILE_READER);
    trace_pa.init(4096, common::MOD_TSFILE_READER);
    for (uint32_t i = 0; i < footer_.table_index_count; ++i) {
        FooterRootSketch root;
        if (RET_FAIL(common::SerializationUtil::read_var_str(root.table, bs))) {
            return ret;
        }
        root.table_name_offset = java_pos;
        root.table_name_size = java_int_string_size(root.table);
        java_pos += root.table_name_size;

        const int64_t actual_node_offset = file_metadata_pos_ + bs.read_pos();
        root.node_offset = java_pos;
        storage::MetaIndexNode trace_node(&trace_pa);
        if (RET_FAIL(trace_node.device_deserialize_from(bs))) {
            return ret;
        }
        root.node_size = static_cast<uint32_t>(
            file_metadata_pos_ + bs.read_pos() - actual_node_offset);
        java_pos += root.node_size;
        auto node_it =
            tsfile_meta_.table_metadata_index_node_map_.find(root.table);
        if (node_it != tsfile_meta_.table_metadata_index_node_map_.end()) {
            root.node = node_it->second;
        }
        root_offsets_by_table_[root.table] = root.node_offset;
        root_ends_by_table_[root.table] = root.node_offset + root.node_size;
        footer_.roots.push_back(root);
    }

    footer_.table_schema_count_offset = java_pos;
    if (RET_FAIL(common::SerializationUtil::read_var_uint(
            footer_.table_schema_count, bs))) {
        return ret;
    }
    java_pos += 4;
    for (uint32_t i = 0; i < footer_.table_schema_count; ++i) {
        FooterSchemaSketch schema;
        if (RET_FAIL(
                common::SerializationUtil::read_var_str(schema.table, bs))) {
            return ret;
        }
        schema.table_name_offset = java_pos;
        schema.table_name_size = java_int_string_size(schema.table);
        const uint32_t actual_schema_start = bs.read_pos();
        schema.schema_offset = java_pos;
        storage::TableSchema trace_schema;
        if (RET_FAIL(trace_schema.deserialize(bs))) {
            return ret;
        }
        schema.schema_size = bs.read_pos() - actual_schema_start;
        java_pos += schema.table_name_size + schema.schema_size;
        auto schema_it = tsfile_meta_.table_schemas_.find(schema.table);
        if (schema_it != tsfile_meta_.table_schemas_.end()) {
            schema.schema = schema_it->second;
        }
        footer_.schemas.push_back(schema);
    }

    footer_.meta_offset_offset = java_pos;
    if (RET_FAIL(common::SerializationUtil::read_i64(footer_.meta_offset_value,
                                                     bs))) {
        return ret;
    }
    java_pos += 8;

    footer_.bloom_filter_size_offset = java_pos;
    if (RET_FAIL(common::SerializationUtil::read_var_uint(
            footer_.bloom_filter_data_size, bs))) {
        return ret;
    }
    java_pos += 4;
    footer_.bloom_filter_offset = java_pos;
    if (bs.remaining_size() < footer_.bloom_filter_data_size) {
        return common::E_TSFILE_CORRUPTED;
    }
    bs.wrapped_buf_advance_read_pos(footer_.bloom_filter_data_size);
    java_pos += footer_.bloom_filter_data_size;
    if (RET_FAIL(common::SerializationUtil::read_var_uint(
            footer_.bloom_filter_size, bs))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::read_var_uint(
            footer_.bloom_filter_hash_count, bs))) {
        return ret;
    }
    footer_.bloom_filter_serialized_size = footer_.bloom_filter_data_size + 4;

    footer_.properties_offset = file_metadata_pos_ + bs.read_pos();
    if (RET_FAIL(common::SerializationUtil::read_var_int(
            footer_.properties_count, bs))) {
        return ret;
    }
    for (int32_t i = 0; i < footer_.properties_count; ++i) {
        FooterPropertySketch prop;
        prop.offset = file_metadata_pos_ + bs.read_pos();
        std::string* value = nullptr;
        if (RET_FAIL(common::SerializationUtil::read_var_str(prop.key, bs))) {
            return ret;
        }
        if (RET_FAIL(common::SerializationUtil::read_var_char_ptr(value, bs))) {
            delete value;
            return ret;
        }
        if (value != nullptr) {
            prop.value = *value;
            delete value;
        }
        footer_.properties.push_back(prop);
    }

    if (bs.read_pos() != metadata_buf_.size()) {
        return common::E_TSFILE_CORRUPTED;
    }
    return common::E_OK;
}

void SketchLayout::add_metadata_node(
    const std::string& table, const std::string& device, int64_t offset,
    int64_t end_offset, bool device_entries,
    std::shared_ptr<storage::MetaIndexNode> node) {
    MetadataNodeSketch rec;
    rec.table = table;
    rec.device = device;
    rec.offset = offset;
    rec.end_offset = end_offset;
    rec.device_entries = device_entries;
    rec.node = node;
    metadata_nodes_.push_back(rec);
    if (offset >= 0) {
        node_by_offset_[offset] = node;
    }
}

int SketchLayout::traverse_index_node(
    const std::string& table,
    const std::shared_ptr<storage::MetaIndexNode>& node, bool device_entries,
    std::shared_ptr<storage::IDeviceID> device_id) {
    if (node == nullptr) {
        return common::E_OK;
    }
    if (node->node_type_ == storage::LEAF_MEASUREMENT) {
        return collect_timeseries_index(table, node, device_id);
    }

    for (size_t i = 0; i < node->children_.size(); ++i) {
        const std::shared_ptr<storage::IMetaIndexEntry>& child =
            node->children_[i];
        int64_t child_end = node->end_offset_;
        if (i + 1 < node->children_.size()) {
            child_end = node->children_[i + 1]->get_offset();
        }
        std::shared_ptr<storage::IDeviceID> next_device = device_id;
        bool child_device_entries = false;
        if (node->node_type_ == storage::LEAF_DEVICE) {
            next_device = child->get_device_id();
            child_device_entries = false;
        } else if (node->node_type_ == storage::INTERNAL_DEVICE) {
            child_device_entries = true;
        } else if (node->node_type_ == storage::INTERNAL_MEASUREMENT) {
            child_device_entries = false;
        } else {
            return common::E_TSFILE_CORRUPTED;
        }

        std::shared_ptr<storage::MetaIndexNode> child_node;
        int ret = read_index_node(child->get_offset(), child_end,
                                  child_device_entries, &child_node);
        if (ret != common::E_OK) {
            return ret;
        }
        std::string device_name =
            next_device == nullptr ? "" : next_device->get_device_name();
        add_metadata_node(table, device_name, child->get_offset(), child_end,
                          child_device_entries, child_node);
        ret = traverse_index_node(table, child_node, child_device_entries,
                                  next_device);
        if (ret != common::E_OK) {
            return ret;
        }
    }
    return common::E_OK;
}

int SketchLayout::read_index_node(
    int64_t offset, int64_t end_offset, bool device_entries,
    std::shared_ptr<storage::MetaIndexNode>* out) {
    if (end_offset <= offset) {
        return common::E_TSFILE_CORRUPTED;
    }
    std::vector<char> buf;
    int ret = read_block(offset, end_offset - offset, &buf);
    if (ret != common::E_OK) {
        return ret;
    }
    void* node_buf = pa_.alloc(sizeof(storage::MetaIndexNode));
    if (node_buf == nullptr) {
        return common::E_OOM;
    }
    storage::MetaIndexNode* node = new (node_buf) storage::MetaIndexNode(&pa_);
    std::shared_ptr<storage::MetaIndexNode> holder(
        node, storage::MetaIndexNode::self_deleter);
    common::ByteStream bs;
    bs.wrap_from(buf.data(), static_cast<int32_t>(buf.size()));
    ret = device_entries ? node->device_deserialize_from(bs)
                         : node->deserialize_from(bs);
    if (ret != common::E_OK) {
        return ret;
    }
    *out = holder;
    return common::E_OK;
}

int SketchLayout::collect_timeseries_index(
    const std::string& table,
    const std::shared_ptr<storage::MetaIndexNode>& node,
    const std::shared_ptr<storage::IDeviceID>& device_id) {
    (void)table;
    for (size_t i = 0; i < node->children_.size(); ++i) {
        const std::shared_ptr<storage::IMetaIndexEntry>& child =
            node->children_[i];
        int64_t start = child->get_offset();
        int64_t end = node->end_offset_;
        if (i + 1 < node->children_.size()) {
            end = node->children_[i + 1]->get_offset();
        }
        if (end <= start) {
            return common::E_TSFILE_CORRUPTED;
        }
        std::vector<char> buf;
        int ret = read_block(start, end - start, &buf);
        if (ret != common::E_OK) {
            return ret;
        }
        common::ByteStream bs;
        bs.wrap_from(buf.data(), static_cast<int32_t>(buf.size()));
        while (bs.has_remaining()) {
            uint32_t before = bs.read_pos();
            void* ts_buf = pa_.alloc(sizeof(storage::TimeseriesIndex));
            if (ts_buf == nullptr) {
                return common::E_OOM;
            }
            storage::TimeseriesIndex* ts_index =
                new (ts_buf) storage::TimeseriesIndex();
            ret = ts_index->deserialize_from(bs, &pa_);
            if (ret != common::E_OK) {
                return ret;
            }
            if (bs.read_pos() <= before) {
                return common::E_TSFILE_CORRUPTED;
            }
            add_timeseries_record(start + before, start + bs.read_pos(),
                                  device_id, ts_index);
        }
    }
    return common::E_OK;
}

void SketchLayout::add_timeseries_record(
    int64_t offset, int64_t end_offset,
    const std::shared_ptr<storage::IDeviceID>& device_id,
    storage::TimeseriesIndex* ts_index) {
    std::string measurement = ts_index->get_measurement_name().to_std_string();
    std::string device =
        device_id == nullptr ? std::string() : device_id->get_device_name();
    std::string path =
        device.empty() ? measurement : device + "." + measurement;

    TimeseriesSketch rec;
    rec.offset = offset;
    rec.end_offset = end_offset;
    rec.device = device;
    rec.measurement = measurement;
    rec.path = path;
    rec.data_type = tsdatatype_name(ts_index->get_data_type());
    rec.statistics = statistic_string(ts_index->get_statistic());
    rec.size_without_chunk_metadata =
        static_cast<uint32_t>(end_offset - offset);
    common::SimpleList<storage::ChunkMeta*>* chunks =
        ts_index->get_chunk_meta_list();
    rec.chunk_count = chunks == nullptr ? 0 : chunks->size();

    if (chunks == nullptr) {
        timeseries_by_offset_[offset] = rec;
        return;
    }
    uint32_t chunk_index = 0;
    for (common::SimpleList<storage::ChunkMeta*>::Iterator it = chunks->begin();
         it != chunks->end(); it++, chunk_index++) {
        storage::ChunkMeta* chunk_meta = it.get();
        if (chunk_meta == nullptr) {
            continue;
        }
        ChunkMetadataSketch chunk_rec;
        chunk_rec.offset = chunk_meta->offset_of_chunk_header_;
        chunk_rec.size =
            serialized_chunk_metadata_size(chunk_meta, chunk_index != 0);
        if (rec.size_without_chunk_metadata >= chunk_rec.size) {
            rec.size_without_chunk_metadata -= chunk_rec.size;
        }
        rec.chunks.push_back(chunk_rec);
        std::string stat = statistic_string(chunk_meta->statistic_);
        if (stat.empty() && rec.chunk_count == 1) {
            stat = rec.statistics;
        }
        chunk_path_by_offset_[chunk_meta->offset_of_chunk_header_] = path;
        chunk_stat_by_offset_[chunk_meta->offset_of_chunk_header_] = stat;
    }
    timeseries_by_offset_[offset] = rec;
}

uint32_t SketchLayout::serialized_chunk_metadata_size(
    storage::ChunkMeta* chunk_meta, bool include_statistics) {
    if (include_statistics && chunk_meta->statistic_ == nullptr) {
        include_statistics = false;
    }
    common::ByteStream bs(128, common::MOD_TSFILE_READER);
    if (chunk_meta->serialize_to(bs, include_statistics) != common::E_OK) {
        return 0;
    }
    return bs.total_size();
}

int SketchLayout::parse_chunk_group_header(int64_t marker_offset,
                                           ChunkGroupSketch* group) {
    const int64_t remaining = file_metadata_pos_ - marker_offset - 1;
    const int32_t read_size = static_cast<int32_t>(
        std::min<int64_t>(remaining, kDeviceHeaderProbeBytes));
    if (read_size <= 0) {
        return common::E_TSFILE_CORRUPTED;
    }
    std::vector<char> buf;
    int ret = read_block(marker_offset + 1, read_size, &buf);
    if (ret != common::E_OK) {
        return ret;
    }
    common::ByteStream bs;
    bs.wrap_from(buf.data(), read_size);
    std::shared_ptr<storage::IDeviceID> device_id =
        std::make_shared<storage::StringArrayDeviceID>();
    ret = device_id->deserialize(bs);
    if (ret != common::E_OK) {
        return ret;
    }
    group->device = device_id->get_device_name();
    group->device_id_size = bs.read_pos();
    if (group->device_id_size == 0) {
        return common::E_TSFILE_CORRUPTED;
    }
    return common::E_OK;
}

int SketchLayout::parse_operation_index_range(int64_t marker_offset,
                                              OperationIndexRangeSketch* op) {
    std::vector<char> buf;
    int ret = read_block(marker_offset + 1, 16, &buf);
    if (ret != common::E_OK) {
        return ret;
    }
    op->min_plan_index = read_i64_from_big_endian(buf.data());
    op->max_plan_index = read_i64_from_big_endian(buf.data() + 8);
    return common::E_OK;
}

int SketchLayout::parse_chunk(int64_t chunk_offset, const std::string& device,
                              ChunkSketch* chunk) {
    (void)device;
    int ret = parse_chunk_header(chunk_offset, chunk);
    if (ret != common::E_OK) {
        return ret;
    }
    const int64_t data_offset = chunk_offset + chunk->header_size;
    const int64_t total_end = data_offset + chunk->header.data_size_;
    if (total_end > file_metadata_pos_) {
        return common::E_TSFILE_CORRUPTED;
    }
    std::vector<char> data;
    if (chunk->header.data_size_ >
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        add_blocker("chunk data at offset " + to_string_i64(data_offset) +
                    " exceeds the C++ sketch reader buffer limit; page "
                    "output is truncated.");
    } else {
        ret = read_block(data_offset, chunk->header.data_size_, &data);
        if (ret != common::E_OK) {
            return ret;
        }
        ret = parse_pages(data_offset, data, chunk);
        if (ret != common::E_OK) {
            return ret;
        }
    }

    std::map<int64_t, std::string>::const_iterator path_it =
        chunk_path_by_offset_.find(chunk_offset);
    if (path_it != chunk_path_by_offset_.end()) {
        chunk->path = path_it->second;
    } else {
        chunk->path = kMetadataMissing;
        add_blocker("chunk metadata missing for chunk header offset " +
                    to_string_i64(chunk_offset));
    }
    std::map<int64_t, std::string>::const_iterator stat_it =
        chunk_stat_by_offset_.find(chunk_offset);
    if (stat_it != chunk_stat_by_offset_.end()) {
        chunk->statistics = stat_it->second;
    }
    return common::E_OK;
}

int SketchLayout::parse_chunk_header(int64_t chunk_offset, ChunkSketch* chunk) {
    const int64_t remaining = file_metadata_pos_ - chunk_offset;
    const int32_t max_probe = static_cast<int32_t>(
        std::min<int64_t>(remaining, kMaxHeaderProbeBytes));
    if (max_probe < kMinChunkHeaderSize) {
        return common::E_TSFILE_CORRUPTED;
    }
    int32_t probe = std::min<int32_t>(4096, max_probe);
    probe = std::max<int32_t>(probe, kMinChunkHeaderSize);
    while (probe <= max_probe) {
        std::vector<char> buf;
        int ret = read_block(chunk_offset, probe, &buf);
        if (ret != common::E_OK) {
            return ret;
        }
        common::ByteStream bs;
        bs.wrap_from(buf.data(), probe);
        storage::ChunkHeader header;
        ret = header.deserialize_from(bs);
        if (ret == common::E_OK) {
            chunk->offset = chunk_offset;
            chunk->header = header;
            chunk->header_size = bs.read_pos();
            return common::E_OK;
        }
        if (probe == max_probe) {
            return common::E_TSFILE_CORRUPTED;
        }
        probe = std::min<int32_t>(max_probe, probe * 2);
    }
    return common::E_TSFILE_CORRUPTED;
}

int SketchLayout::parse_pages(int64_t data_offset,
                              const std::vector<char>& data,
                              ChunkSketch* chunk) {
    common::ByteStream bs;
    bs.wrap_from(data.data(), static_cast<int32_t>(data.size()));
    const bool one_page = is_one_page_chunk(chunk->header.chunk_type_);
    int page_id = 0;
    while (bs.has_remaining()) {
        const uint32_t header_start = bs.read_pos();
        storage::PageHeader page_header;
        int ret = page_header.deserialize_from(bs, !one_page,
                                               chunk->header.data_type_);
        if (ret != common::E_OK) {
            return ret;
        }
        const uint32_t header_end = bs.read_pos();
        if (bs.remaining_size() < page_header.compressed_size_) {
            return common::E_TSFILE_CORRUPTED;
        }
        PageSketch page;
        page.page_id = page_id++;
        page.header_offset = data_offset + header_start;
        page.data_offset = data_offset + header_end;
        page.uncompressed_size = page_header.uncompressed_size_;
        page.compressed_size = page_header.compressed_size_;
        const uint32_t actual_header_size = header_end - header_start;
        const uint32_t actual_size_prefix =
            unsigned_var_int_size(page.uncompressed_size) +
            unsigned_var_int_size(page.compressed_size);
        const uint32_t java_size_prefix =
            java_var_int_size(page.uncompressed_size) +
            java_var_int_size(page.compressed_size);
        page.header_size =
            actual_header_size - actual_size_prefix + java_size_prefix;
        page.statistics = statistic_string(page_header.statistic_);
        page.has_statistics = page_header.statistic_ != nullptr;
        chunk->pages.push_back(page);
        bs.wrapped_buf_advance_read_pos(page_header.compressed_size_);
        if (one_page) {
            break;
        }
    }
    chunk->header.num_of_pages_ = static_cast<int32_t>(chunk->pages.size());
    if (bs.has_remaining()) {
        return common::E_TSFILE_CORRUPTED;
    }
    return common::E_OK;
}

}  // namespace tsfile_cli
