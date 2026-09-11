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

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "commands/sketch_layout.h"
#include "common/allocator/byte_stream.h"
#include "format/atomic_output.h"
#include "format/output_format.h"
#include "utils/errno_define.h"

namespace tsfile_cli {
namespace {

std::string to_string_i64(int64_t v) {
    std::ostringstream ss;
    ss << v;
    return ss.str();
}

std::string to_string_u32(uint32_t v) {
    std::ostringstream ss;
    ss << v;
    return ss.str();
}

std::string metadata_node_type_name(storage::MetaIndexNodeType type) {
    switch (type) {
        case storage::INTERNAL_DEVICE:
            return "INTERNAL_DEVICE";
        case storage::LEAF_DEVICE:
            return "LEAF_DEVICE";
        case storage::INTERNAL_MEASUREMENT:
            return "INTERNAL_MEASUREMENT";
        case storage::LEAF_MEASUREMENT:
            return "LEAF_MEASUREMENT";
        default:
            return "INVALID_META_NODE_TYPE";
    }
}

std::string column_category_name(common::ColumnCategory category) {
    switch (category) {
        case common::ColumnCategory::TAG:
            return "TAG";
        case common::ColumnCategory::FIELD:
            return "FIELD";
        case common::ColumnCategory::ATTRIBUTE:
            return "ATTRIBUTE";
        case common::ColumnCategory::TIME:
            return "TIME";
        default:
            return "UNKNOWN";
    }
}

std::string entry_key(const std::shared_ptr<storage::IMetaIndexEntry>& entry) {
    if (entry == nullptr) {
        return "";
    }
    if (entry->is_device_level() && entry->get_device_id() != nullptr) {
        return entry->get_device_id()->get_device_name();
    }
    return entry->get_name().to_std_string();
}

uint32_t java_uvar_int_size(uint32_t value) {
    uint32_t position = 1;
    while ((value & 0xFFFFFF80) != 0) {
        value >>= 7;
        position++;
    }
    return position;
}

uint32_t entry_serialized_size(
    const std::shared_ptr<storage::IMetaIndexEntry>& entry) {
    if (entry == nullptr) {
        return 0;
    }
    common::ByteStream bs(128, common::MOD_TSFILE_READER);
    if (entry->serialize_to(bs) != common::E_OK) {
        return 0;
    }
    return bs.total_size();
}

int signed_marker(char marker) { return static_cast<int>(marker); }

bool is_one_page_chunk(char chunk_type) {
    return (static_cast<unsigned char>(chunk_type) & 0x3F) ==
           static_cast<unsigned char>(
               storage::ONLY_ONE_PAGE_CHUNK_HEADER_MARKER);
}

int write_atomic_text(const std::string& path, const std::string& content,
                      const std::string& source, bool force,
                      std::ostream& err) {
    std::string tmp;
    int code = prepare_atomic_output(path, source, force, tmp, err);
    if (code != kExitOk) {
        return code;
    }
    {
        std::ofstream output(tmp.c_str(),
                             std::ios::binary | std::ios::trunc);
        if (!output.is_open()) {
            err << "Error: cannot create output target '" << path << "'\n";
            remove_atomic_temp(tmp, err);
            return kExitRuntime;
        }
        output << content;
        output.flush();
        if (!output.good()) {
            err << "Error: failed to write output target '" << path << "'\n";
            output.close();
            remove_atomic_temp(tmp, err);
            return kExitRuntime;
        }
        output.close();
        if (output.fail()) {
            err << "Error: failed to close output target '" << path << "'\n";
            remove_atomic_temp(tmp, err);
            return kExitRuntime;
        }
    }
    code = commit_atomic_output(tmp, path, force, err);
    if (code != kExitOk && !remove_atomic_temp(tmp, err)) {
        code = kExitRuntime;
    }
    return code;
}

class SketchPrinter {
   public:
    int run(const ParsedArgs& args, std::ostream& out, std::ostream& err) {
        int ret = layout_.load_file(args.file);
        if (ret != common::E_OK) {
            print_java_load_error(err);
            layout_.close();
            return kExitFile;
        }
        int metadata_ret = common::E_OK;
        if (layout_.metadata_loaded_) {
            metadata_ret = layout_.collect_metadata_layout();
            if (metadata_ret != common::E_OK) {
                layout_.add_blocker("metadata layout parsing stopped: " +
                                    layout_.error_text(metadata_ret));
            }
        }
        int scan_ret = layout_.scan_data_area();
        if (scan_ret != common::E_OK) {
            layout_.add_blocker("data area scan stopped: " +
                                layout_.error_text(scan_ret));
        }
        print(out);
        layout_.close();
        if (metadata_ret != common::E_OK || scan_ret != common::E_OK ||
            !layout_.blockers_.empty()) {
            print_java_load_error(err);
            return kExitFile;
        }
        return kExitOk;
    }

   private:
    void print_java_load_error(std::ostream& err) {
        err << "Cannot load file " << layout_.path_
            << " because the file has crashed.\n";
    }

    void print(std::ostream& out) {
        out << "-------------------------------- TsFile Sketch "
               "--------------------------------\n";
        out << "file path: " << layout_.path_ << "\n";
        out << "file length: " << layout_.file_size_ << "\n";
        print_file_info(out);
        print_chunks(out);
        print_separator(out);
        print_metadata_and_timeseries(out);
        print_tsfile_metadata(out);
        print_line(out, layout_.file_size_, "END of TsFile");
        print_index_tree(out);
        print_blockers(out);
        out << "---------------------------------- TsFile Sketch End "
               "----------------------------------\n";
    }

    void print_line(std::ostream& out, int64_t pos,
                    const std::string& content) const {
        out << std::setw(20) << pos << "|\t" << content << "\n";
    }

    void print_split(std::ostream& out, const std::string& content) const {
        out << "||||||||||||||||||||| " << content << "\n";
    }

    void print_file_info(std::ostream& out) {
        out << "\n";
        out << std::setw(20) << "POSITION"
            << "|\tCONTENT\n";
        out << std::setw(20) << "--------"
            << " \t-------\n";
        print_line(out, 0, "[magic head] " + layout_.head_magic_);
        print_line(
            out, storage::MAGIC_STRING_TSFILE_LEN,
            "[version number] " +
                to_string_i64(static_cast<unsigned char>(layout_.version_)));
    }

    void print_chunks(std::ostream& out) {
        for (const ChunkGroupSketch& group : layout_.groups_) {
            print_split(
                out,
                "[ChunkGroup] of " + group.device + ", num of Chunks:" +
                    to_string_u32(static_cast<uint32_t>(group.chunks.size())));
            print_line(out, group.offset, "[ChunkGroup Header]");
            print_line(
                out, group.offset,
                "\t[marker] " + to_string_i64(static_cast<unsigned char>(
                                    storage::CHUNK_GROUP_HEADER_MARKER)));
            print_line(out, group.offset + 1,
                       "\t[deviceID] " + group.device +
                           " size=" + to_string_u32(group.device_id_size));
            for (const ChunkSketch& chunk : group.chunks) {
                print_chunk(out, chunk);
            }
            print_split(out, "[ChunkGroup] of " + group.device + " ends");
        }
    }

    void print_chunk(std::ostream& out, const ChunkSketch& chunk) {
        std::string line = "[Chunk] of " + chunk.path;
        if (!chunk.statistics.empty()) {
            line += ", " + chunk.statistics;
        }
        print_line(out, chunk.offset, line);

        std::ostringstream header;
        header << "\t[Chunk Header] marker="
               << signed_marker(chunk.header.chunk_type_)
               << ", measurementID=" << chunk.header.measurement_name_
               << ", dataSize=" << chunk.header.data_size_
               << ", dataType=" << tsdatatype_name(chunk.header.data_type_)
               << ", compressionType="
               << compression_name(chunk.header.compression_type_)
               << ", encodingType="
               << tsencoding_name(chunk.header.encoding_type_)
               << ", size=" << chunk.header_size;
        print_line(out, chunk.offset, header.str());

        int64_t page_pos = chunk.offset + chunk.header_size;
        const bool one_page = is_one_page_chunk(chunk.header.chunk_type_);
        for (const PageSketch& page : chunk.pages) {
            std::ostringstream page_header;
            if (one_page) {
                page_header << "\t\t[Page Header] "
                            << " HeaderSize:" << page.header_size
                            << ", UncompressedSize:" << page.uncompressed_size
                            << ", CompressedSize:" << page.compressed_size;
                print_line(out, page_pos, page_header.str());
                page_pos += page.header_size;
                print_line(out, page_pos,
                           "\t\t[Page Data]  Size:" +
                               to_string_u32(page.compressed_size));
            } else {
                const int java_page_id = page.page_id + 1;
                page_header << "\t\t[PageHeader-" << java_page_id << "] "
                            << " HeaderSize:" << page.header_size
                            << ", UncompressedSize:" << page.uncompressed_size
                            << ", CompressedSize:" << page.compressed_size;
                if (page.has_statistics && !page.statistics.empty()) {
                    page_header << ", " << page.statistics;
                }
                print_line(out, page_pos, page_header.str());
                page_pos += page.header_size;
                std::ostringstream page_data;
                page_data << "\t\t[Page-" << java_page_id
                          << "] , CompressedSize:" << page.compressed_size;
                if (page.has_statistics && !page.statistics.empty()) {
                    page_data << ", " << page.statistics;
                }
                print_line(out, page_pos, page_data.str());
            }
            page_pos += page.compressed_size;
        }
    }

    void print_separator(std::ostream& out) {
        print_line(out, layout_.separator_offset_,
                   "[marker] " + to_string_i64(layout_.separator_marker_));
    }

    struct LayoutItem {
        int64_t offset = 0;
        int kind = 0;  // 0 node, 1 timeseries
        size_t index = 0;
    };

    void print_metadata_and_timeseries(std::ostream& out) {
        std::vector<LayoutItem> items;
        for (size_t i = 0; i < layout_.metadata_nodes_.size(); ++i) {
            if (layout_.metadata_nodes_[i].offset >= 0 &&
                layout_.metadata_nodes_[i].offset <
                    layout_.file_metadata_pos_) {
                LayoutItem item;
                item.offset = layout_.metadata_nodes_[i].offset;
                item.kind = 0;
                item.index = i;
                items.push_back(item);
            }
        }
        for (std::map<int64_t, TimeseriesSketch>::const_iterator it =
                 layout_.timeseries_by_offset_.begin();
             it != layout_.timeseries_by_offset_.end(); ++it) {
            LayoutItem item;
            item.offset = it->first;
            item.kind = 1;
            item.index = 0;
            items.push_back(item);
        }
        std::sort(items.begin(), items.end(),
                  [](const LayoutItem& a, const LayoutItem& b) {
                      if (a.offset != b.offset) {
                          return a.offset < b.offset;
                      }
                      return a.kind < b.kind;
                  });
        for (const LayoutItem& item : items) {
            if (item.kind == 0) {
                print_metadata_node(out,
                                    layout_.metadata_nodes_[item.index].offset,
                                    *layout_.metadata_nodes_[item.index].node);
            } else {
                std::map<int64_t, TimeseriesSketch>::const_iterator it =
                    layout_.timeseries_by_offset_.find(item.offset);
                if (it != layout_.timeseries_by_offset_.end()) {
                    print_timeseries(out, it->second);
                }
            }
        }
    }

    void print_timeseries(std::ostream& out, const TimeseriesSketch& ts) {
        std::ostringstream ss;
        ss << "[TimeseriesMetadata] of " << ts.path
           << ", tsDataType:" << ts.data_type
           << ", sizeWithoutChunkMetadata:" << ts.size_without_chunk_metadata;
        if (!ts.statistics.empty()) {
            ss << ", " << ts.statistics;
        }
        print_line(out, ts.offset, ss.str());
        int64_t chunk_pos = ts.offset + ts.size_without_chunk_metadata;
        for (size_t i = 0; i < ts.chunks.size(); ++i) {
            std::ostringstream chunk_line;
            chunk_line << "\t[ChunkMetadata] offset=" << ts.chunks[i].offset
                       << ", size=" << ts.chunks[i].size;
            print_line(out, chunk_pos, chunk_line.str());
            chunk_pos += ts.chunks[i].size;
        }
    }

    int64_t print_metadata_node(std::ostream& out, int64_t offset,
                                const storage::MetaIndexNode& node) {
        print_line(out, offset, "[MetadataIndexNode]");
        print_line(out, offset,
                   "\t childrenCnt=" + to_string_u32(static_cast<uint32_t>(
                                           node.children_.size())));
        int64_t pos =
            offset +
            java_uvar_int_size(static_cast<uint32_t>(node.children_.size()));
        for (const std::shared_ptr<storage::IMetaIndexEntry>& child :
             node.children_) {
            std::ostringstream child_line;
            child_line << "\t<" << entry_key(child) << ", "
                       << child->get_offset() << ">";
            print_line(out, pos, child_line.str());
            pos += entry_serialized_size(child);
        }
        print_line(out, pos, "\tendOffset=" + to_string_i64(node.end_offset_));
        pos += 8;
        print_line(out, pos,
                   "\tnodeType=" + metadata_node_type_name(node.node_type_));
        pos += 1;
        return pos;
    }

    void print_tsfile_metadata(std::ostream& out) {
        print_split(out, "[TsFileMetadata] begins");
        if (!layout_.metadata_loaded_) {
            print_line(out, layout_.file_metadata_pos_,
                       "[TsFileMetadata] <metadata-truncated>");
            print_line(out, layout_.file_size_ - kSketchFileTailSize,
                       "[TsFileMetadataSize] " +
                           to_string_u32(layout_.file_metadata_size_));
            print_line(out,
                       layout_.file_size_ - storage::MAGIC_STRING_TSFILE_LEN,
                       "[magic tail] " + layout_.tail_magic_);
            return;
        }
        print_line(out, layout_.footer_.table_index_count_offset,
                   "TableIndexRootCnt=" +
                       to_string_u32(layout_.footer_.table_index_count));
        for (const FooterRootSketch& root : layout_.footer_.roots) {
            print_line(out, root.table_name_offset,
                       "[Table Name] " + root.table +
                           ", size=" + to_string_u32(root.table_name_size));
            if (root.node != nullptr) {
                print_metadata_node(out, root.node_offset, *root.node);
            }
        }

        print_line(out, layout_.footer_.table_schema_count_offset,
                   "TableSchemaCnt=" +
                       to_string_u32(layout_.footer_.table_schema_count));
        for (const FooterSchemaSketch& schema : layout_.footer_.schemas) {
            if (schema.schema != nullptr) {
                print_line(
                    out, schema.schema_offset,
                    "[TableSchema] " +
                        table_schema_string(schema.table, *schema.schema) +
                        ", size=" + to_string_u32(schema.schema_size));
            }
        }

        print_line(out, layout_.footer_.meta_offset_offset,
                   "[Meta Offset] " +
                       to_string_i64(layout_.footer_.meta_offset_value));
        if (layout_.footer_.bloom_filter_data_size > 0) {
            print_line(
                out, layout_.footer_.bloom_filter_size_offset,
                "[Bloom Filter Size] bit vector byte array length=" +
                    to_string_u32(layout_.footer_.bloom_filter_data_size) +
                    to_string_u32(layout_.footer_.bloom_filter_hash_count));
            std::ostringstream bloom;
            bloom << "[Bloom Filter] , filterCapacity="
                  << layout_.footer_.bloom_filter_size
                  << ", hashFunctionSize="
                  << layout_.footer_.bloom_filter_hash_count;
            print_line(out, layout_.footer_.bloom_filter_offset, bloom.str());
        }

        print_split(out, "[TsFileMetadata] ends");
        print_line(out, layout_.file_size_ - kSketchFileTailSize,
                   "[TsFileMetadataSize] " +
                       to_string_u32(layout_.file_metadata_size_));
        print_line(out, layout_.file_size_ - storage::MAGIC_STRING_TSFILE_LEN,
                   "[magic tail] " + layout_.tail_magic_);
    }

    std::string measurement_schema_string(
        const std::shared_ptr<storage::MeasurementSchema>& measurement) {
        if (measurement == nullptr) {
            return "[]";
        }
        std::ostringstream ss;
        ss << "[" << measurement->measurement_name_ << ","
           << tsdatatype_name(measurement->data_type_) << ","
           << tsencoding_name(measurement->encoding_) << ",";
        if (!measurement->props_.empty()) {
            ss << "{";
            bool first = true;
            for (const std::pair<const std::string, std::string>& prop :
                 measurement->props_) {
                if (!first) {
                    ss << ", ";
                }
                first = false;
                ss << prop.first << "=" << prop.second;
            }
            ss << "}";
        }
        ss << "," << compression_name(measurement->compression_type_) << "]";
        return ss.str();
    }

    std::string table_schema_string(const std::string& table,
                                    storage::TableSchema& schema) {
        std::ostringstream ss;
        ss << "TableSchema{tableName='" << table << "', columnSchemas=[";
        std::vector<std::shared_ptr<storage::MeasurementSchema>> measurements =
            schema.get_measurement_schemas();
        std::vector<common::ColumnCategory> categories =
            schema.get_column_categories();
        for (size_t i = 0; i < measurements.size(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            ss << measurement_schema_string(measurements[i]);
        }
        ss << "], columnTypes=[";
        for (size_t i = 0; i < categories.size(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            ss << column_category_name(categories[i]);
        }
        ss << "]}";
        return ss.str();
    }

    void print_index_tree(std::ostream& out) {
        out << "---------------------------- IndexOfTimerseriesIndex Tree "
               "-----------------------------\n";
        for (const FooterRootSketch& root : layout_.footer_.roots) {
            if (root.node == nullptr) {
                continue;
            }
            out << root.table << "\n";
            print_tree_node(out, root.node, 0);
        }
    }

    void print_blockers(std::ostream& out) {
        if (layout_.blockers_.empty()) {
            return;
        }
        print_split(out, "[Blockers]");
        out << "Blockers\n";
        for (const std::string& blocker : layout_.blockers_) {
            print_line(out, -1, "[BLOCKER] " + blocker);
        }
    }

    void print_tree_node(std::ostream& out,
                         const std::shared_ptr<storage::MetaIndexNode>& node,
                         int depth) {
        if (node == nullptr) {
            return;
        }
        out << tree_indent(depth)
            << "[MetadataIndex:" << metadata_node_type_name(node->node_type_)
            << "]\n";
        for (const std::shared_ptr<storage::IMetaIndexEntry>& child :
             node->children_) {
            out << tree_indent(depth) << "└──────[" << entry_key(child) << ","
                << child->get_offset() << "]\n";
            std::map<int64_t, std::shared_ptr<storage::MetaIndexNode>>::iterator
                child_node = layout_.node_by_offset_.find(child->get_offset());
            if (node->node_type_ != storage::LEAF_MEASUREMENT &&
                child_node != layout_.node_by_offset_.end()) {
                print_tree_node(out, child_node->second, depth + 1);
            }
        }
    }

    std::string tree_indent(int depth) const {
        std::string result = "\t";
        for (int i = 0; i < depth; ++i) {
            result += "\t\t";
        }
        return result;
    }

    SketchLayout layout_;
};

}  // namespace

int cmd_sketch(const ParsedArgs& args, std::ostream& out, std::ostream& err) {
    SketchPrinter printer;
    std::ostringstream content;
    int code = printer.run(args, content, err);
    if (code != kExitOk) {
        if (args.output.empty()) {
            out << content.str();
            out.flush();
            return out.good() ? code : kExitRuntime;
        }
        return code;
    }
    if (!args.output.empty()) {
        return write_atomic_text(args.output, content.str(), args.file,
                                 args.force, err);
    }
    out << content.str();
    out.flush();
    return out.good() ? kExitOk : kExitRuntime;
}

}  // namespace tsfile_cli
