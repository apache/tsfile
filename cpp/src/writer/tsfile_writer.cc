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

#include "tsfile_writer.h"

#include <chrono>
#include <iomanip>

#include "chunk_writer.h"
#include "common/config/config.h"
#include "common/global.h"
#ifdef ENABLE_THREADS
#include "common/thread_pool.h"
#endif
#include "file/restorable_tsfile_io_writer.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "utils/errno_define.h"

using namespace common;

namespace storage {

namespace libtsfile {
bool g_s_is_inited = false;
}

int libtsfile_init() {
    if (libtsfile::g_s_is_inited) {
        return E_OK;
    }
    ModStat::get_instance().init();

    init_common();

    libtsfile::g_s_is_inited = true;
    return E_OK;
}

void libtsfile_destroy() {
    ModStat::get_instance().destroy();
#ifdef ENABLE_THREADS
    delete common::g_thread_pool_;
    common::g_thread_pool_ = nullptr;
#endif
    libtsfile::g_s_is_inited = false;
}

int set_page_max_point_count(uint32_t page_max_ponint_count) {
    return config_set_page_max_point_count(page_max_ponint_count);
}
int set_max_degree_of_index_node(uint32_t max_degree_of_index_node) {
    return config_set_max_degree_of_index_node(max_degree_of_index_node);
}

TsFileWriter::TsFileWriter()
    : write_file_(nullptr),
      io_writer_(nullptr),
      schemas_(),
      start_file_done_(false),
      record_count_since_last_flush_(0),
      record_count_for_next_mem_check_(
          g_config_value_.record_count_for_next_mem_check_),
      write_file_created_(false),
      io_writer_owned_(true) {}

TsFileWriter::~TsFileWriter() { destroy(); }

void TsFileWriter::destroy() {
    if (write_file_created_ && write_file_ != nullptr) {
        delete write_file_;
        write_file_ = nullptr;
    }
    if (io_writer_owned_ && io_writer_) {
        delete io_writer_;
    }
    io_writer_ = nullptr;
    DeviceSchemasMapIter dev_iter;
    // cppcheck-suppress postfixOperator
    for (dev_iter = schemas_.begin(); dev_iter != schemas_.end(); dev_iter++) {
        MeasurementSchemaMap& ms_map =
            dev_iter->second->measurement_schema_map_;
        MeasurementSchemaMapIter ms_iter;
        for (ms_iter = ms_map.begin(); ms_iter != ms_map.end(); ms_iter++) {
            MeasurementSchema* ms = ms_iter->second;
            if (ms != nullptr) {
                if (ms->chunk_writer_ != nullptr) {
                    delete ms->chunk_writer_;
                    ms->chunk_writer_ = nullptr;
                }
                delete ms;
                ms_iter->second = nullptr;
            }
        }
        delete dev_iter->second;
        dev_iter->second = nullptr;
    }
    schemas_.clear();
    record_count_since_last_flush_ = 0;
}

int TsFileWriter::init(WriteFile* write_file) {
    if (write_file == nullptr) {
        return E_INVALID_ARG;
    } else if (!write_file->file_opened()) {
        return E_INVALID_ARG;
    }
    write_file_ = write_file;
    write_file_created_ = false;
    io_writer_owned_ = true;
    // Re-arm per-lifecycle state when the writer is reused after a
    // destroy().  enforce_recovered_last_time_order_ may have been set
    // true by a previous recovery init; without resetting it we'd refuse
    // valid writes whose timestamps don't satisfy a long-stale anchor.
    // unrecoverable_ from a previous partial-write failure would otherwise
    // make every operation on the new file fail immediately.
    // start_file_done_ is true after the previous lifecycle's first flush,
    // so without resetting it flush() would skip the magic/version write on
    // the new file and produce headerless output.
    enforce_recovered_last_time_order_ = false;
    unrecoverable_ = false;
    start_file_done_ = false;
    record_count_since_last_flush_ = 0;
    io_writer_ = new TsFileIOWriter();
    io_writer_->init(write_file_);
    return E_OK;
}

// -----------------------------------------------------------------------------
// Recovery init: rebuild schemas_ from recovered chunk group metas (aligned
// with Java). Use each CGM's actual device_id from file as key so tree and
// table model both get correct lookups. Table model can still lazy-create from
// table_schema_map_ in do_check_schema_table when a new device appears.
// All new MeasurementSchemaGroup/MeasurementSchema are freed in destroy().
// -----------------------------------------------------------------------------
int TsFileWriter::init(RestorableTsFileIOWriter* rw) {
    if (rw == nullptr || !rw->can_write()) {
        return E_INVALID_ARG;
    }
    write_file_ = rw->get_write_file();
    write_file_created_ = false;
    io_writer_owned_ = false;
    // Clear any unrecoverable_ latched from a previous lifecycle so the
    // re-init isn't immediately poisoned.
    unrecoverable_ = false;
    // Reject new writes whose timestamps fall back into the recovered range.
    enforce_recovered_last_time_order_ = true;
    io_writer_ = rw;

    const std::vector<ChunkGroupMeta*>& recovered =
        rw->get_recovered_chunk_group_metas();
    for (ChunkGroupMeta* cgm : recovered) {
        if (cgm == nullptr || cgm->device_id_ == nullptr) {
            continue;
        }
        std::shared_ptr<IDeviceID> device_id = cgm->device_id_;

        // Find existing group for same device (same device may have multiple
        // CGMs from multiple flushes).
        DeviceSchemasMapIter it = schemas_.begin();
        for (; it != schemas_.end(); ++it) {
            if (it->first != nullptr && *it->first == *device_id) {
                break;
            }
        }

        MeasurementSchemaGroup* group = nullptr;
        if (it != schemas_.end()) {
            group = it->second;
        } else {
            group = new MeasurementSchemaGroup;
            group->is_aligned_ =
                rw->is_device_aligned(device_id->get_table_name());
            schemas_.insert(std::make_pair(device_id, group));
        }

        // Add measurement schemas from this CGM (skip time column: empty name).
        for (auto iter = cgm->chunk_meta_list_.begin();
             iter != cgm->chunk_meta_list_.end(); iter++) {
            ChunkMeta* cm = iter.get();
            if (cm == nullptr) {
                continue;
            }
            // Track the highest end_time across recovered chunks so that
            // appending writes can refuse out-of-order timestamps.
            if (cm->statistic_ != nullptr && cm->statistic_->count_ > 0) {
                group->last_time_ =
                    std::max(group->last_time_, cm->statistic_->end_time_);
            }
            std::string mname = cm->measurement_name_.to_std_string();
            if (mname.empty()) {
                continue;
            }
            if (group->measurement_schema_map_.find(mname) !=
                group->measurement_schema_map_.end()) {
                continue;
            }
            MeasurementSchema* ms = new MeasurementSchema(
                mname, cm->data_type_, cm->encoding_, cm->compression_type_);
            group->measurement_schema_map_.insert(std::make_pair(mname, ms));
        }
    }

    start_file_done_ = true;
    return E_OK;
}

void TsFileWriter::set_generate_table_schema(bool generate_table_schema) {
    io_writer_->set_generate_table_schema(generate_table_schema);
}

int TsFileWriter::register_table(
    const std::shared_ptr<TableSchema>& table_schema) {
    if (!table_schema) return E_INVALID_ARG;

    // Empty table name or column name is not allowed.
    if (table_schema->get_table_name().empty()) {
        return E_INVALID_ARG;
    }
    for (const auto& name : table_schema->get_measurement_names()) {
        if (name.empty()) {
            return E_INVALID_ARG;
        }
    }

    // Because it is not possible to return an error code for duplicate name
    // checks during the construction phase of TabletSchema, the duplicate name
    // check has been moved to the table registration stage.

    // TODO: Add Debug INFO if ErrorCode is not enough to describe problems.
    if (table_schema->get_column_pos_index_num() !=
        table_schema->get_measurement_names().size()) {
        return E_INVALID_ARG;
    }

    if (io_writer_->get_schema()->table_schema_map_.find(
            table_schema->get_table_name()) !=
        io_writer_->get_schema()->table_schema_map_.end()) {
        return E_ALREADY_EXIST;
    }
    io_writer_->get_schema()->register_table_schema(table_schema);
    return E_OK;
}

int TsFileWriter::open(const std::string& file_path, int flags, mode_t mode) {
    flags |= O_CREAT | O_EXCL;
    write_file_ = new WriteFile;
    write_file_created_ = true;
    io_writer_ = new TsFileIOWriter;
    int ret = E_OK;
    if (RET_FAIL(write_file_->create(file_path, flags, mode))) {
    } else {
        io_writer_->init(write_file_);
    }
    return ret;
}

int TsFileWriter::open(const std::string& file_path) {
    return open(file_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
}

int TsFileWriter::register_aligned_timeseries(
    const std::string& device_id, const MeasurementSchema& measurement_schema) {
    MeasurementSchema* ms = new MeasurementSchema(
        measurement_schema.measurement_name_, measurement_schema.data_type_,
        measurement_schema.encoding_, measurement_schema.compression_type_);
    return register_timeseries(device_id, ms, true);
}

int TsFileWriter::register_aligned_timeseries(
    const std::string& device_id,
    const std::vector<MeasurementSchema*>& measurement_schemas) {
    int ret = E_OK;
    for (auto it : measurement_schemas) {
        ret = register_timeseries(device_id, it, true);
        if (ret != E_OK) {
            return ret;
        }
    }
    return ret;
}

int TsFileWriter::register_timeseries(
    const std::string& device_id, const MeasurementSchema& measurement_schema) {
    MeasurementSchema* ms = new MeasurementSchema(
        measurement_schema.measurement_name_, measurement_schema.data_type_,
        measurement_schema.encoding_, measurement_schema.compression_type_);
    return register_timeseries(device_id, ms, false);
}

int TsFileWriter::register_timeseries(const std::string& device_path,
                                      MeasurementSchema* measurement_schema,
                                      bool is_aligned) {
    std::shared_ptr<IDeviceID> device_id =
        std::make_shared<StringArrayDeviceID>(device_path);
    DeviceSchemasMapIter device_iter = schemas_.find(device_id);
    if (device_iter != schemas_.end()) {
        MeasurementSchemaMap& msm =
            device_iter->second->measurement_schema_map_;
        MeasurementSchemaMapInsertResult ins_res = msm.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        if (UNLIKELY(!ins_res.second)) {
            return E_ALREADY_EXIST;
        }
    } else {
        MeasurementSchemaGroup* ms_group = new MeasurementSchemaGroup;
        ms_group->is_aligned_ = is_aligned;
        ms_group->measurement_schema_map_.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        schemas_.insert(std::make_pair(device_id, ms_group));
    }
    return E_OK;
}

int TsFileWriter::register_timeseries(
    const std::string& device_id,
    const std::vector<MeasurementSchema*>& measurement_schema_vec) {
    int ret = E_OK;
    auto it = measurement_schema_vec.begin();
    for (; it != measurement_schema_vec.end();
         it++) {  // cppcheck-suppress postfixOperator
        ret = register_timeseries(device_id, *it);
        if (ret != E_OK) {
            return ret;
        }
    }
    return ret;
}

struct MeasurementSchemaMapNamesGetter {
   public:
    explicit MeasurementSchemaMapNamesGetter(
        const MeasurementSchemaMap& measurement_schema_map)
        : measurement_schema_map_(
              const_cast<MeasurementSchemaMap&>(measurement_schema_map)) {
        measurement_name_idx_ = measurement_schema_map_.begin();
    }

    FORCE_INLINE uint32_t get_count() const {
        return measurement_schema_map_.size();
    }

    FORCE_INLINE const std::string& next() {
        ASSERT(measurement_name_idx_ != measurement_schema_map_.end());
        std::string& ret = measurement_name_idx_->second->measurement_name_;
        measurement_name_idx_++;
        return ret;
    }

   private:
    MeasurementSchemaMap& measurement_schema_map_;
    MeasurementSchemaMap::iterator measurement_name_idx_;
};

struct MeasurementNamesFromRecord {
   public:
    explicit MeasurementNamesFromRecord(const TsRecord& record)
        : record_(record), measurement_name_idx_(0) {}
    FORCE_INLINE uint32_t get_count() const { return record_.points_.size(); }

    FORCE_INLINE const std::string& next() {
        return this->at(measurement_name_idx_++);
    }

   private:
    const TsRecord& record_;
    size_t measurement_name_idx_;
    FORCE_INLINE const std::string& at(size_t idx) const {
        ASSERT(idx < record_.points_.size());
        return record_.points_[idx].measurement_name_;
    }
};

struct MeasurementNamesFromTablet {
    explicit MeasurementNamesFromTablet(const Tablet& tablet)
        : tablet_(tablet), measurement_name_idx_(0) {}
    FORCE_INLINE uint32_t get_count() const {
        return tablet_.schema_vec_->size();
    }
    FORCE_INLINE const std::string& next() {
        return this->at(measurement_name_idx_++);
    }

   private:
    const Tablet& tablet_;
    size_t measurement_name_idx_;
    FORCE_INLINE const std::string& at(size_t idx) const {
        ASSERT(idx < tablet_.schema_vec_->size());
        return tablet_.schema_vec_->at(idx).measurement_name_;
    }
};

int TsFileWriter::do_check_and_prepare_tablet(Tablet& tablet) {
    if (tablet.column_categories_.empty()) {
        auto& schema_map = io_writer_->get_schema()->table_schema_map_;
        auto table_schema_it = schema_map.find(tablet.get_table_name());
        auto table_schema = table_schema_it->second;
        uint32_t column_cnt = tablet.get_column_count();
        for (uint32_t i = 0; i < column_cnt; i++) {
            auto& col_name = tablet.get_column_name(i);
            int col_index = table_schema->find_column_index(col_name);
            if (col_index == -1) {
                return E_COLUMN_NOT_EXIST;
            }
            if (table_schema->get_data_types()[col_index] !=
                tablet.schema_vec_->at(i).data_type_) {
                return E_TYPE_NOT_MATCH;
            }
            const common::ColumnCategory column_category =
                table_schema->get_column_categories()[col_index];
            tablet.column_categories_.emplace_back(column_category);
            if (column_category == ColumnCategory::TAG) {
                tablet.id_column_indexes_.push_back(i);
            }
        }
    }
    return common::E_OK;
}

std::shared_ptr<TableSchema> TsFileWriter::get_table_schema(
    const std::string& table_name) const {
    auto& schema_map = io_writer_->get_schema()->table_schema_map_;
    auto it = schema_map.find(table_name);
    if (it == schema_map.end()) return nullptr;
    return it->second;
}

template <typename MeasurementNamesGetter>
int TsFileWriter::do_check_schema(
    std::shared_ptr<IDeviceID> device_id,
    MeasurementNamesGetter& measurement_names,
    SimpleVector<ChunkWriter*>& chunk_writers,
    SimpleVector<common::TSDataType>& data_types) {
    int ret = E_OK;
    DeviceSchemasMapIter dev_it = schemas_.find(device_id);
    MeasurementSchemaGroup* device_schema = nullptr;
    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        return E_DEVICE_NOT_EXIST;
    }
    MeasurementSchemaMap& msm = device_schema->measurement_schema_map_;
    uint32_t measurement_count = measurement_names.get_count();
    // chunk_writers.reserve(measurement_count);
    for (uint32_t i = 0; i < measurement_count; i++) {
        auto ms_iter = msm.find(measurement_names.next());
        if (UNLIKELY(ms_iter == msm.end())) {
            chunk_writers.push_back(NULL);
            data_types.push_back(common::NULL_TYPE);
        } else {
            // In Java we will check data_type. But in C++, no check here.
            // Because checks are performed at the chunk layer and page layer
            MeasurementSchema* ms = ms_iter->second;
            if (IS_NULL(ms->chunk_writer_)) {
                ms->chunk_writer_ = new ChunkWriter;
                ret = ms->chunk_writer_->init(ms->measurement_name_,
                                              ms->data_type_, ms->encoding_,
                                              ms->compression_type_);
                if (IS_SUCC(ret)) {
                    chunk_writers.push_back(ms->chunk_writer_);
                } else {
                    for (size_t chunk_writer_idx = 0;
                         chunk_writer_idx < chunk_writers.size();
                         chunk_writer_idx++) {
                        if (!chunk_writers[chunk_writer_idx]) {
                            delete chunk_writers[chunk_writer_idx];
                        }
                    }
                    ret = common::E_INVALID_ARG;
                    return ret;
                }
            } else {
                chunk_writers.push_back(ms->chunk_writer_);
            }
            data_types.push_back(ms->data_type_);
        }
    }
    return ret;
}

template <typename MeasurementNamesGetter>
int TsFileWriter::do_check_schema_aligned(
    std::shared_ptr<IDeviceID> device_id,
    MeasurementNamesGetter& measurement_names,
    storage::TimeChunkWriter*& time_chunk_writer,
    common::SimpleVector<storage::ValueChunkWriter*>& value_chunk_writers,
    SimpleVector<common::TSDataType>& data_types) {
    int ret = E_OK;
    auto dev_it = schemas_.find(device_id);
    MeasurementSchemaGroup* device_schema = NULL;
    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        return E_DEVICE_NOT_EXIST;
    }
    if (IS_NULL(device_schema->time_chunk_writer_)) {
        device_schema->time_chunk_writer_ = new TimeChunkWriter();
        device_schema->time_chunk_writer_->init(
            "", g_config_value_.time_encoding_type_,
            g_config_value_.time_compress_type_);
    }
    time_chunk_writer = device_schema->time_chunk_writer_;
    MeasurementSchemaMap& msm = device_schema->measurement_schema_map_;
    uint32_t measurement_count = measurement_names.get_count();
    for (uint32_t i = 0; i < measurement_count; i++) {
        auto ms_iter = msm.find(measurement_names.next());
        if (UNLIKELY(ms_iter == msm.end())) {
            value_chunk_writers.push_back(NULL);
            data_types.push_back(common::NULL_TYPE);
        } else {
            // Here we may check data_type against ms_iter. But in Java
            // libtsfile, no check here.
            MeasurementSchema* ms = ms_iter->second;
            if (IS_NULL(ms->value_chunk_writer_)) {
                ms->value_chunk_writer_ = new ValueChunkWriter;
                ret = ms->value_chunk_writer_->init(
                    ms->measurement_name_, ms->data_type_, ms->encoding_,
                    ms->compression_type_);
                if (IS_SUCC(ret)) {
                    value_chunk_writers.push_back(ms->value_chunk_writer_);
                } else {
                    value_chunk_writers.push_back(NULL);
                    for (size_t chunk_writer_idx = 0;
                         chunk_writer_idx < value_chunk_writers.size();
                         chunk_writer_idx++) {
                        if (!value_chunk_writers[chunk_writer_idx]) {
                            delete value_chunk_writers[chunk_writer_idx];
                        }
                    }
                    ret = common::E_INVALID_ARG;
                    return ret;
                }
            } else {
                value_chunk_writers.push_back(ms->value_chunk_writer_);
            }
            data_types.push_back(ms->data_type_);
        }
    }
    return ret;
}

int TsFileWriter::do_check_schema_table(
    std::shared_ptr<IDeviceID> device_id, Tablet& tablet,
    storage::TimeChunkWriter*& time_chunk_writer,
    common::SimpleVector<storage::ValueChunkWriter*>& value_chunk_writers) {
    int ret = E_OK;

    auto dev_it = schemas_.find(device_id);
    MeasurementSchemaGroup* device_schema = NULL;

    auto& schema_map = io_writer_->get_schema()->table_schema_map_;
    auto table_schema_it = schema_map.find(tablet.get_table_name());
    if (UNLIKELY(table_schema_it == schema_map.end())) {
        return E_TABLE_NOT_EXIST;
    }
    auto table_schema = table_schema_it->second;

    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        device_schema = new MeasurementSchemaGroup;
        device_schema->is_aligned_ = true;
        device_schema->time_chunk_writer_ = new TimeChunkWriter();
        device_schema->time_chunk_writer_->init(
            "", g_config_value_.time_encoding_type_,
            g_config_value_.time_compress_type_);

        for (uint32_t i = 0; i < table_schema->get_measurement_schemas().size();
             ++i) {
            if (table_schema->get_column_categories().at(i) ==
                common::ColumnCategory::FIELD) {
                auto table_column_schema =
                    table_schema->get_measurement_schemas().at(i);
                auto device_column_schema = new MeasurementSchema(
                    table_column_schema->measurement_name_,
                    table_column_schema->data_type_,
                    table_column_schema->encoding_,
                    table_column_schema->compression_type_);
                if (!table_column_schema->props_.empty()) {
                    device_column_schema->props_ = table_column_schema->props_;
                }
                device_schema->measurement_schema_map_
                    [device_column_schema->measurement_name_] =
                    device_column_schema;
            }
        }
        schemas_[device_id] = device_schema;
    }

    // After recovery, device_schema may exist but time_chunk_writer_ not yet
    // created
    if (IS_NULL(device_schema->time_chunk_writer_)) {
        device_schema->time_chunk_writer_ = new TimeChunkWriter();
        device_schema->time_chunk_writer_->init(
            "", g_config_value_.time_encoding_type_,
            g_config_value_.time_compress_type_);
    }

    uint32_t column_cnt = tablet.get_column_count();
    time_chunk_writer = device_schema->time_chunk_writer_;
    MeasurementSchemaMap& msm = device_schema->measurement_schema_map_;

    for (uint32_t i = 0; i < column_cnt; i++) {
        if (tablet.column_categories_.at(i) != common::ColumnCategory::FIELD) {
            continue;
        }
        auto ms_iter = msm.find(tablet.get_column_name(i));
        if (UNLIKELY(ms_iter == msm.end())) {
            value_chunk_writers.push_back(NULL);
        } else {
            // Here we may check data_type against ms_iter. But in Java
            // libtsfile, no check here.
            MeasurementSchema* ms = ms_iter->second;
            if (IS_NULL(ms->value_chunk_writer_)) {
                ms->value_chunk_writer_ = new ValueChunkWriter;
                ret = ms->value_chunk_writer_->init(
                    ms->measurement_name_, ms->data_type_, ms->encoding_,
                    ms->compression_type_);
                if (IS_SUCC(ret)) {
                    value_chunk_writers.push_back(ms->value_chunk_writer_);
                } else {
                    value_chunk_writers.push_back(NULL);
                    for (size_t chunk_writer_idx = 0;
                         chunk_writer_idx < value_chunk_writers.size();
                         chunk_writer_idx++) {
                        if (!value_chunk_writers[chunk_writer_idx]) {
                            delete value_chunk_writers[chunk_writer_idx];
                        }
                    }
                    ret = common::E_INVALID_ARG;
                    return ret;
                }
            } else {
                value_chunk_writers.push_back(ms->value_chunk_writer_);
            }
        }
    }
    return ret;
}

int64_t TsFileWriter::calculate_mem_size_for_all_group() {
    int64_t mem_total_size = 0;
    DeviceSchemasMapIter device_iter;
    for (device_iter = schemas_.begin(); device_iter != schemas_.end();
         device_iter++) {
        MeasurementSchemaGroup* chunk_group = device_iter->second;
        MeasurementSchemaMap& map = chunk_group->measurement_schema_map_;
        for (MeasurementSchemaMapIter ms_iter = map.begin();
             ms_iter != map.end(); ms_iter++) {
            MeasurementSchema* m_schema = ms_iter->second;
            if (!chunk_group->is_aligned_) {
                ChunkWriter*& chunk_writer = m_schema->chunk_writer_;
                if (chunk_writer != nullptr) {
                    mem_total_size +=
                        chunk_writer->estimate_max_series_mem_size();
                }
            } else {
                ValueChunkWriter*& chunk_writer = m_schema->value_chunk_writer_;
                if (chunk_writer != nullptr) {
                    mem_total_size +=
                        chunk_writer->estimate_max_series_mem_size();
                }
            }
        }
        if (chunk_group->is_aligned_) {
            TimeChunkWriter*& time_chunk_writer =
                chunk_group->time_chunk_writer_;
            if (time_chunk_writer != nullptr) {
                mem_total_size +=
                    time_chunk_writer->estimate_max_series_mem_size();
            }
        }
    }
    return mem_total_size;
}

int64_t TsFileWriter::calculate_meta_mem_size() const {
    return io_writer_->get_meta_size();
}

/**
 * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush
 * them to given OutputStream.
 */
int TsFileWriter::check_memory_size_and_may_flush_chunks() {
    int ret = E_OK;
    if (record_count_since_last_flush_ >= record_count_for_next_mem_check_) {
        // chunk-writer memory drops to ~0 after flush, but chunk metadata
        // (ChunkMeta / ChunkGroupMeta / per-statistic PageArenas) keeps
        // accumulating until end_file().  Wide-schema or many-flush
        // workloads can pile up tens of MB of metadata that the old
        // threshold check ignored entirely — flush would never fire even
        // though total writer memory was well past chunk_group_size_threshold_.
        int64_t chunk_size = calculate_mem_size_for_all_group();
        int64_t meta_size = calculate_meta_mem_size();
        int64_t mem_size = chunk_size + meta_size;
        record_count_for_next_mem_check_ =
            record_count_since_last_flush_ *
            common::g_config_value_.chunk_group_size_threshold_ / mem_size;
        if (mem_size > common::g_config_value_.chunk_group_size_threshold_) {
            ret = flush();
        }
    }
    return ret;
}

int TsFileWriter::write_record(const TsRecord& record) {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    int ret = E_OK;
    auto device_id = std::make_shared<StringArrayDeviceID>(record.device_id_);
    // After recovery, refuse writes whose timestamp would land at or before
    // any already-flushed chunk's end_time for this device.
    if (enforce_recovered_last_time_order_) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr &&
            record.timestamp_ <= schema_it->second->last_time_) {
            return E_OUT_OF_ORDER;
        }
    }
    // std::vector<ChunkWriter*> chunk_writers;
    SimpleVector<ChunkWriter*> chunk_writers;
    SimpleVector<common::TSDataType> data_types;
    MeasurementNamesFromRecord mnames_getter(record);
    if (RET_FAIL(do_check_schema(device_id, mnames_getter, chunk_writers,
                                 data_types))) {
        return ret;
    }

    ASSERT(chunk_writers.size() == record.points_.size());
    for (uint32_t c = 0; c < chunk_writers.size(); c++) {
        ChunkWriter* chunk_writer = chunk_writers[c];
        if (IS_NULL(chunk_writer)) {
            continue;
        }
        // ignore point writer failure
        write_point(chunk_writer, record.timestamp_, data_types[c],
                    record.points_[c]);
    }

    if (enforce_recovered_last_time_order_) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr) {
            schema_it->second->last_time_ =
                std::max(schema_it->second->last_time_, record.timestamp_);
        }
    }
    record_count_since_last_flush_++;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

int TsFileWriter::write_record_aligned(const TsRecord& record) {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    int ret = E_OK;
    auto device_id = std::make_shared<StringArrayDeviceID>(record.device_id_);
    if (enforce_recovered_last_time_order_) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr &&
            record.timestamp_ <= schema_it->second->last_time_) {
            return E_OUT_OF_ORDER;
        }
    }
    SimpleVector<ValueChunkWriter*> value_chunk_writers;
    SimpleVector<common::TSDataType> data_types;
    TimeChunkWriter* time_chunk_writer;
    MeasurementNamesFromRecord mnames_getter(record);
    if (RET_FAIL(do_check_schema_aligned(device_id, mnames_getter,
                                         time_chunk_writer, value_chunk_writers,
                                         data_types))) {
        return ret;
    }
    if (value_chunk_writers.size() != record.points_.size()) {
        return E_INVALID_ARG;
    }
    // Snapshot page counters before the write so we can detect any column
    // that crossed a page boundary and seal the rest in lockstep.
    int32_t time_pages_before = time_chunk_writer->num_of_pages();
    std::vector<int32_t> value_pages_before(value_chunk_writers.size(), 0);
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (!IS_NULL(value_chunk_writer)) {
            value_pages_before[c] = value_chunk_writer->num_of_pages();
        }
    }
    // Time first: a rejected timestamp (E_OUT_OF_ORDER, OOM, etc.) must
    // not silently advance the value writers — that would leave the time
    // chunk one row behind every value chunk for the rest of the file.
    if (RET_FAIL(time_chunk_writer->write(record.timestamp_))) {
        return ret;
    }
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (IS_NULL(value_chunk_writer)) {
            continue;
        }
        if (RET_FAIL(write_point_aligned(value_chunk_writer, record.timestamp_,
                                         data_types[c], record.points_[c]))) {
            // Time wrote the row but at least one value column failed
            // mid-record; the per-column row counts no longer agree.
            // Mark the writer unrecoverable so flush/close refuses to
            // seal a misaligned chunk group.
            unrecoverable_ = true;
            return ret;
        }
    }
    if (RET_FAIL(maybe_seal_aligned_pages_together(
            time_chunk_writer, value_chunk_writers, time_pages_before,
            value_pages_before))) {
        unrecoverable_ = true;
        return ret;
    }
    if (enforce_recovered_last_time_order_) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr) {
            schema_it->second->last_time_ =
                std::max(schema_it->second->last_time_, record.timestamp_);
        }
    }
    return ret;
}

int TsFileWriter::write_point(ChunkWriter* chunk_writer, int64_t timestamp,
                              common::TSDataType data_type,
                              const DataPoint& point) {
    switch (data_type) {
        case common::BOOLEAN:
            return chunk_writer->write(timestamp, point.u_.bool_val_);
        case common::DATE:
        case common::INT32:
            return chunk_writer->write(timestamp, point.u_.i32_val_);
        case common::TIMESTAMP:
        case common::INT64:
            return chunk_writer->write(timestamp, point.u_.i64_val_);
        case common::FLOAT:
            return chunk_writer->write(timestamp, point.u_.float_val_);
        case common::DOUBLE:
            return chunk_writer->write(timestamp, point.u_.double_val_);
        case common::BLOB:
        case common::TEXT:
        case common::STRING:
            return chunk_writer->write(timestamp, point.text_val_);
        default:
            return E_INVALID_DATA_POINT;
    }
}

// After writing one record / batch to the time chunk and every value chunk,
// keep their page boundaries aligned: if any of them autosealed a page on
// memory pressure, seal the rest of the open pages too so an aligned reader
// can still pair position N across time + every value column.
int TsFileWriter::maybe_seal_aligned_pages_together(
    TimeChunkWriter* time_chunk_writer,
    common::SimpleVector<ValueChunkWriter*>& value_chunk_writers,
    int32_t time_pages_before, const std::vector<int32_t>& value_pages_before) {
    bool should_seal_all =
        time_chunk_writer->num_of_pages() > time_pages_before;
    for (uint32_t c = 0; c < value_chunk_writers.size() && !should_seal_all;
         c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (!IS_NULL(value_chunk_writer) &&
            value_chunk_writer->num_of_pages() > value_pages_before[c]) {
            should_seal_all = true;
            break;
        }
    }
    if (!should_seal_all) {
        return E_OK;
    }

    int ret = E_OK;
    if (time_chunk_writer->has_current_page_data() &&
        RET_FAIL(time_chunk_writer->seal_current_page())) {
        return ret;
    }
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (!IS_NULL(value_chunk_writer) &&
            value_chunk_writer->has_current_page_data() &&
            RET_FAIL(value_chunk_writer->seal_current_page())) {
            return ret;
        }
    }
    return ret;
}

int TsFileWriter::write_point_aligned(ValueChunkWriter* value_chunk_writer,
                                      int64_t timestamp,
                                      common::TSDataType data_type,
                                      const DataPoint& point) {
    bool isnull = point.isnull;
    switch (data_type) {
        case common::BOOLEAN:
            return value_chunk_writer->write(timestamp, point.u_.bool_val_,
                                             isnull);
        case common::INT32:
        case common::DATE:
            return value_chunk_writer->write(timestamp, point.u_.i32_val_,
                                             isnull);
        case common::TIMESTAMP:
        case common::INT64:
            return value_chunk_writer->write(timestamp, point.u_.i64_val_,
                                             isnull);
        case common::FLOAT:
            return value_chunk_writer->write(timestamp, point.u_.float_val_,
                                             isnull);
        case common::DOUBLE:
            return value_chunk_writer->write(timestamp, point.u_.double_val_,
                                             isnull);
        case common::BLOB:
        case common::TEXT:
        case common::STRING:
            return value_chunk_writer->write(timestamp, point.text_val_,
                                             isnull);
        default:
            return E_INVALID_DATA_POINT;
    }
}

int TsFileWriter::write_tablet_aligned(const Tablet& tablet) {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    int ret = E_OK;
    auto device_id =
        std::make_shared<StringArrayDeviceID>(tablet.insert_target_name_);
    const uint32_t total_rows = tablet.get_cur_row_size();
    if (enforce_recovered_last_time_order_ && total_rows > 0 &&
        tablet.timestamps_ != nullptr) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr &&
            tablet.timestamps_[0] <= schema_it->second->last_time_) {
            return E_OUT_OF_ORDER;
        }
    }
    SimpleVector<ValueChunkWriter*> value_chunk_writers;
    TimeChunkWriter* time_chunk_writer = nullptr;
    SimpleVector<common::TSDataType> data_types;
    MeasurementNamesFromTablet mnames_getter(tablet);
    if (RET_FAIL(do_check_schema_aligned(device_id, mnames_getter,
                                         time_chunk_writer, value_chunk_writers,
                                         data_types))) {
        return ret;
    }
    ASSERT(data_types.size() == tablet.get_column_count());
    for (uint32_t c = 0; c < data_types.size(); c++) {
        if (data_types[c] == common::NULL_TYPE) {
            continue;
        }
        if (data_types[c] != tablet.schema_vec_->at(c).data_type_) {
            return E_TYPE_NOT_MATCH;
        }
    }
    // Snapshot page counters before the batch so we can detect any column
    // that crossed a page boundary mid-tablet and seal the rest in lockstep.
    int32_t time_pages_before = time_chunk_writer->num_of_pages();
    std::vector<int32_t> value_pages_before(value_chunk_writers.size(), 0);
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (!IS_NULL(value_chunk_writer)) {
            value_pages_before[c] = value_chunk_writer->num_of_pages();
        }
    }
    // Suppress memory-driven page sealing on every column for the duration of
    // the batch. The count-driven seals inside write_batch still fire at the
    // same `page_writer_max_point_num_` boundary on every writer (time +
    // values), which keeps aligned page boundaries in lock-step. Re-enable
    // both before returning so subsequent record-by-record writes restore the
    // normal memory-pressure behavior, and let the final
    // maybe_seal_aligned_pages_together pick up any count-driven divergence
    // (e.g. when a sealed value column ended a page that the time column did
    // not).
    time_chunk_writer->set_enable_page_seal_if_full(false);
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (!IS_NULL(value_chunk_writer)) {
            value_chunk_writer->set_enable_page_seal_if_full(false);
        }
    }
    auto restore_seal = [&]() {
        time_chunk_writer->set_enable_page_seal_if_full(true);
        for (uint32_t k = 0; k < value_chunk_writers.size(); k++) {
            if (!IS_NULL(value_chunk_writers[k])) {
                value_chunk_writers[k]->set_enable_page_seal_if_full(true);
            }
        }
    };
    // Any failure (out-of-order timestamps, OOM, etc.) must abort before we
    // write a single value column — otherwise the time chunk would record
    // fewer rows than each value chunk and the chunk-group would deserialize
    // as misaligned data.
    if (RET_FAIL(time_write_column_batch(time_chunk_writer, tablet, 0,
                                         total_rows))) {
        restore_seal();
        return ret;
    }
    ASSERT(value_chunk_writers.size() == tablet.get_column_count());
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter* value_chunk_writer = value_chunk_writers[c];
        if (IS_NULL(value_chunk_writer)) {
            continue;
        }
        if (RET_FAIL(value_write_column_batch(value_chunk_writer, tablet, c, 0,
                                              total_rows))) {
            restore_seal();
            // Time chunk has the full row count but at least one value
            // column stopped early.  Mark the writer unrecoverable so no
            // later flush/close seals the divergent state.
            unrecoverable_ = true;
            return ret;
        }
    }
    restore_seal();
    if (RET_FAIL(maybe_seal_aligned_pages_together(
            time_chunk_writer, value_chunk_writers, time_pages_before,
            value_pages_before))) {
        unrecoverable_ = true;
        return ret;
    }
    if (enforce_recovered_last_time_order_ && total_rows > 0 &&
        tablet.timestamps_ != nullptr) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr) {
            schema_it->second->last_time_ =
                std::max(schema_it->second->last_time_,
                         tablet.timestamps_[total_rows - 1]);
        }
    }
    return ret;
}

int TsFileWriter::write_tablet(const Tablet& tablet) {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    int ret = E_OK;
    auto device_id =
        std::make_shared<StringArrayDeviceID>(tablet.insert_target_name_);
    // Use the actual filled row count — max_row_num_ is the buffer capacity
    // and would let uninitialized timestamps/values past the live range leak
    // into the chunk.
    const uint32_t total_rows = tablet.get_cur_row_size();
    if (enforce_recovered_last_time_order_ && total_rows > 0 &&
        tablet.timestamps_ != nullptr) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr &&
            tablet.timestamps_[0] <= schema_it->second->last_time_) {
            return E_OUT_OF_ORDER;
        }
    }
    SimpleVector<ChunkWriter*> chunk_writers;
    SimpleVector<common::TSDataType> data_types;
    MeasurementNamesFromTablet mnames_getter(tablet);
    if (RET_FAIL(do_check_schema(device_id, mnames_getter, chunk_writers,
                                 data_types))) {
        return ret;
    }
    ASSERT(data_types.size() == tablet.get_column_count());
    for (uint32_t c = 0; c < data_types.size(); c++) {
        if (data_types[c] == common::NULL_TYPE) {
            continue;
        }
        if (data_types[c] != tablet.schema_vec_->at(c).data_type_) {
            return E_TYPE_NOT_MATCH;
        }
    }
    ASSERT(chunk_writers.size() == tablet.get_column_count());
    uint32_t columns_written = 0;
    for (uint32_t c = 0; c < chunk_writers.size(); c++) {
        ChunkWriter* chunk_writer = chunk_writers[c];
        if (IS_NULL(chunk_writer)) {
            continue;
        }
        if (RET_FAIL(
                write_column_batch(chunk_writer, tablet, c, 0, total_rows))) {
            // Earlier columns already advanced their chunk writers; this
            // column failed mid-write, so per-column row counts diverge.
            // Mark unrecoverable so flush/close refuse to seal the
            // misaligned tree chunk group.
            if (columns_written > 0) unrecoverable_ = true;
            return ret;
        }
        columns_written++;
    }

    if (enforce_recovered_last_time_order_ && total_rows > 0 &&
        tablet.timestamps_ != nullptr) {
        auto schema_it = schemas_.find(device_id);
        if (schema_it != schemas_.end() && schema_it->second != nullptr) {
            schema_it->second->last_time_ =
                std::max(schema_it->second->last_time_,
                         tablet.timestamps_[total_rows - 1]);
        }
    }
    record_count_since_last_flush_ += total_rows;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

int TsFileWriter::write_tree(const Tablet& tablet) {
    auto device_id =
        std::make_shared<StringArrayDeviceID>(tablet.insert_target_name_);
    if (schemas_.find(device_id) != schemas_.end()) {
        auto schema = schemas_[device_id];
        if (schema->is_aligned_) {
            return this->write_tablet_aligned(tablet);
        }
        return this->write_tablet(tablet);
    }
    return E_NOT_EXIST;
}

int TsFileWriter::write_tree(const TsRecord& record) {
    auto device_id = std::make_shared<StringArrayDeviceID>(record.device_id_);
    if (schemas_.find(device_id) != schemas_.end()) {
        auto schema = schemas_[device_id];
        if (schema->is_aligned_) {
            return this->write_record_aligned(record);
        }
        return this->write_record(record);
    }
    return E_NOT_EXIST;
}

int TsFileWriter::write_table(Tablet& tablet) {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    int ret = E_OK;
    if (io_writer_->get_schema()->table_schema_map_.find(
            tablet.insert_target_name_) ==
        io_writer_->get_schema()->table_schema_map_.end()) {
        ret = E_TABLE_NOT_EXIST;
        return ret;
    }
    if (RET_FAIL(do_check_and_prepare_tablet(tablet))) {
        return ret;
    }

    auto device_id_end_index_pairs = split_tablet_by_device(tablet);

    if (table_aligned_) {
        struct ValueTask {
            ValueChunkWriter* vcw;
            uint32_t col_idx;
        };
        struct SegmentRange {
            uint32_t si;
            uint32_t ei;
        };
        struct DeviceWriteCtx {
            TimeChunkWriter* tcw;
            std::vector<ValueTask> value_tasks;
            std::vector<SegmentRange> segments;
            uint32_t initial_page_points;
        };

        const uint32_t page_max_points =
            std::max<uint32_t>(1, g_config_value_.page_writer_max_point_num_);

        std::vector<DeviceWriteCtx> device_ctxs;
        std::map<std::shared_ptr<IDeviceID>, size_t, IDeviceIDComparator>
            device_ctx_index;
        int start_idx = 0;
        for (auto& pair : device_id_end_index_pairs) {
            auto device_id = pair.first;
            int end_idx = pair.second;
            if (end_idx == 0) continue;

            const uint32_t si = static_cast<uint32_t>(start_idx);
            const uint32_t ei = static_cast<uint32_t>(end_idx);
            // Recovery: refuse any segment whose first timestamp would land
            // at or before a flushed chunk's end_time for this device. This
            // mirrors the per-record / per-tablet check on the tree path.
            if (enforce_recovered_last_time_order_ && tablet.timestamps_ &&
                ei > si) {
                auto schema_it = schemas_.find(device_id);
                if (schema_it != schemas_.end() &&
                    schema_it->second != nullptr &&
                    tablet.timestamps_[si] <= schema_it->second->last_time_) {
                    return E_OUT_OF_ORDER;
                }
            }
            auto idx_it = device_ctx_index.find(device_id);
            if (idx_it == device_ctx_index.end()) {
                SimpleVector<ValueChunkWriter*> value_chunk_writers;
                TimeChunkWriter* time_chunk_writer = nullptr;
                if (RET_FAIL(do_check_schema_table(device_id, tablet,
                                                   time_chunk_writer,
                                                   value_chunk_writers))) {
                    return ret;
                }

                // device_ctx_index tracks devices seen in *this* tablet, but
                // do_check_schema_table returns the device's persistent chunk
                // writer, which may already hold points from earlier
                // tablets/records in the same un-flushed chunk group — so
                // time_cur_points can be > 0 even on first sight in this
                // tablet.
                uint32_t time_cur_points = time_chunk_writer->get_point_numer();
                if (time_cur_points >= page_max_points) {
                    // Seal the time page first, then every value page in
                    // lockstep.  Any failure leaves columns at different
                    // page boundaries and the chunk group can no longer be
                    // sealed coherently — mark the writer unrecoverable.
                    if (time_chunk_writer->has_current_page_data()) {
                        if (RET_FAIL(time_chunk_writer->seal_current_page())) {
                            unrecoverable_ = true;
                            return ret;
                        }
                    }
                    for (uint32_t k = 0; k < value_chunk_writers.size(); k++) {
                        if (!IS_NULL(value_chunk_writers[k]) &&
                            value_chunk_writers[k]->has_current_page_data()) {
                            if (RET_FAIL(value_chunk_writers[k]
                                             ->seal_current_page())) {
                                unrecoverable_ = true;
                                return ret;
                            }
                        }
                    }
                    time_cur_points = 0;
                }

                DeviceWriteCtx ctx;
                ctx.tcw = time_chunk_writer;
                ctx.initial_page_points = time_cur_points;
                uint32_t field_col_count = 0;
                for (uint32_t i = 0; i < tablet.get_column_count(); ++i) {
                    if (tablet.column_categories_[i] ==
                        common::ColumnCategory::FIELD) {
                        ValueChunkWriter* vcw =
                            value_chunk_writers[field_col_count];
                        if (!IS_NULL(vcw)) {
                            ctx.value_tasks.push_back({vcw, i});
                        }
                        field_col_count++;
                    }
                }
                device_ctxs.push_back(std::move(ctx));
                idx_it = device_ctx_index
                             .insert(std::make_pair(device_id,
                                                    device_ctxs.size() - 1))
                             .first;
            }

            device_ctxs[idx_it->second].segments.push_back({si, ei});
            start_idx = end_idx;
        }

        auto write_time_segments =
            [this, &tablet, page_max_points](
                TimeChunkWriter* tcw, const std::vector<SegmentRange>& segments,
                uint32_t initial_page_points) -> int {
            int r = E_OK;
            tcw->set_enable_page_seal_if_full(false);
            // The caller seals and resets time_cur_points to 0 once it reaches
            // page_max_points, so initial_page_points is always in
            // [0, page_max_points): >0 means a partial page (room is the
            // leftover), ==0 means a fresh page (a full page of room).  The
            // `< page_max_points` guard is defensive; that case can't occur.
            uint32_t page_remaining =
                (initial_page_points > 0 &&
                 initial_page_points < page_max_points)
                    ? (page_max_points - initial_page_points)
                    : page_max_points;
            for (const auto& segment : segments) {
                uint32_t seg_pos = segment.si;
                while (seg_pos < segment.ei) {
                    uint32_t batch =
                        std::min(page_remaining, segment.ei - seg_pos);
                    if ((r = time_write_column_batch(
                             tcw, tablet, seg_pos, seg_pos + batch)) != E_OK) {
                        tcw->set_enable_page_seal_if_full(true);
                        return r;
                    }
                    seg_pos += batch;
                    page_remaining -= batch;
                    if (page_remaining == 0) {
                        if ((r = tcw->seal_current_page()) != E_OK) {
                            tcw->set_enable_page_seal_if_full(true);
                            return r;
                        }
                        page_remaining = page_max_points;
                    }
                }
            }
            tcw->set_enable_page_seal_if_full(true);
            return r;
        };

        auto write_value_segments =
            [this, &tablet, page_max_points](
                ValueChunkWriter* vcw, uint32_t col_idx,
                const std::vector<SegmentRange>& segments,
                uint32_t initial_page_points) -> int {
            int r = E_OK;
            vcw->set_enable_page_seal_if_full(false);
            uint32_t page_remaining =
                (initial_page_points > 0 &&
                 initial_page_points < page_max_points)
                    ? (page_max_points - initial_page_points)
                    : page_max_points;
            for (const auto& segment : segments) {
                uint32_t seg_pos = segment.si;
                while (seg_pos < segment.ei) {
                    uint32_t batch =
                        std::min(page_remaining, segment.ei - seg_pos);
                    if ((r = value_write_column_batch(
                             vcw, tablet, col_idx, seg_pos, seg_pos + batch)) !=
                        E_OK) {
                        vcw->set_enable_page_seal_if_full(true);
                        return r;
                    }
                    seg_pos += batch;
                    page_remaining -= batch;
                    if (page_remaining == 0) {
                        if (vcw->has_current_page_data() &&
                            (r = vcw->seal_current_page()) != E_OK) {
                            vcw->set_enable_page_seal_if_full(true);
                            return r;
                        }
                        page_remaining = page_max_points;
                    }
                }
            }
            vcw->set_enable_page_seal_if_full(true);
            return r;
        };

#ifdef ENABLE_THREADS
        if (g_config_value_.parallel_write_enabled_ &&
            common::g_thread_pool_ != nullptr) {
            std::vector<std::future<int>> futures;
            for (auto& ctx : device_ctxs) {
                // Capture the per-iteration state by pointer value. This
                // is equivalent in lifetime to the old by-reference
                // captures (each referred to its own vector element, and
                // device_ctxs outlives all future.get() calls below) — it
                // only makes the per-task address explicit. Truly task-
                // owned lifetime would require copying the task inputs.
                auto* ctx_ptr = &ctx;
                futures.push_back(common::g_thread_pool_->submit(
                    [&write_time_segments, ctx_ptr]() {
                        return write_time_segments(
                            ctx_ptr->tcw, ctx_ptr->segments,
                            ctx_ptr->initial_page_points);
                    }));
                for (auto& vt : ctx.value_tasks) {
                    auto* vt_ptr = &vt;
                    futures.push_back(common::g_thread_pool_->submit(
                        [&write_value_segments, vt_ptr, ctx_ptr]() {
                            return write_value_segments(
                                vt_ptr->vcw, vt_ptr->col_idx, ctx_ptr->segments,
                                ctx_ptr->initial_page_points);
                        }));
                }
            }
            for (auto& f : futures) {
                int r = f.get();
                if (r != E_OK && ret == E_OK) ret = r;
            }
            if (ret != E_OK) {
                // One task aborted mid-batch while others may have written
                // all of their rows; the per-column row counts no longer
                // line up.  Mark the writer unrecoverable so flush/close
                // can't seal a corrupt aligned chunk group.
                // TODO: a future PR should add per-device cleanup so that a
                // partial write can be rolled back rather than poisoning the
                // entire writer.
                unrecoverable_ = true;
                return ret;
            }
        } else
#endif
        {
            for (auto& ctx : device_ctxs) {
                if (RET_FAIL(write_time_segments(ctx.tcw, ctx.segments,
                                                 ctx.initial_page_points))) {
                    // Time wrote partial rows before failing; value columns
                    // still hold the prior count.  Same column-alignment
                    // hazard as the parallel path.
                    unrecoverable_ = true;
                    return ret;
                }
                for (auto& vt : ctx.value_tasks) {
                    if (RET_FAIL(write_value_segments(
                            vt.vcw, vt.col_idx, ctx.segments,
                            ctx.initial_page_points))) {
                        unrecoverable_ = true;
                        return ret;
                    }
                }
            }
        }
    } else {
        int start_idx = 0;
        for (auto& device_id_end_index_pair : device_id_end_index_pairs) {
            auto device_id = device_id_end_index_pair.first;
            int end_idx = device_id_end_index_pair.second;
            if (end_idx == 0) continue;

            const uint32_t si = static_cast<uint32_t>(start_idx);
            if (enforce_recovered_last_time_order_ && tablet.timestamps_ &&
                end_idx > start_idx) {
                auto schema_it = schemas_.find(device_id);
                if (schema_it != schemas_.end() &&
                    schema_it->second != nullptr &&
                    tablet.timestamps_[si] <= schema_it->second->last_time_) {
                    return E_OUT_OF_ORDER;
                }
            }
            MeasurementNamesFromTablet mnames_getter(tablet);
            SimpleVector<ChunkWriter*> chunk_writers;
            SimpleVector<common::TSDataType> data_types;
            if (RET_FAIL(do_check_schema(device_id, mnames_getter,
                                         chunk_writers, data_types))) {
                return ret;
            }
            ASSERT(chunk_writers.size() == tablet.get_column_count());

#ifdef ENABLE_THREADS
            if (chunk_writers.size() >= 2 &&
                g_config_value_.parallel_write_enabled_ &&
                common::g_thread_pool_ != nullptr) {
                const uint32_t si = start_idx;
                const uint32_t ei = device_id_end_index_pair.second;
                std::vector<std::future<int>> futures;
                for (uint32_t c = 0; c < chunk_writers.size(); c++) {
                    ChunkWriter* cw = chunk_writers[c];
                    if (IS_NULL(cw)) continue;
                    futures.push_back(common::g_thread_pool_->submit(
                        [this, cw, &tablet, c, si, ei]() {
                            return write_column_batch(cw, tablet, c, si, ei);
                        }));
                }
                for (auto& f : futures) {
                    int r = f.get();
                    if (r != E_OK && ret == E_OK) ret = r;
                }
                if (ret != E_OK) {
                    // One column aborted partway while sibling columns
                    // may have written all of their rows.  The per-column
                    // chunk writers now disagree on row count, so subsequent
                    // flush/close would seal a corrupt non-aligned chunk
                    // group.  Same hazard as the aligned parallel path —
                    // mark the writer unrecoverable so future ops refuse.
                    unrecoverable_ = true;
                    return ret;
                }
            } else
#endif
            {
                for (uint32_t c = 0; c < chunk_writers.size(); c++) {
                    ChunkWriter* chunk_writer = chunk_writers[c];
                    if (IS_NULL(chunk_writer)) continue;
                    if (RET_FAIL(write_column_batch(
                            chunk_writer, tablet, c, start_idx,
                            device_id_end_index_pair.second))) {
                        // Sequential path: earlier columns already wrote
                        // their batch, this column failed → divergent row
                        // counts.  Same unrecoverable contract.
                        if (c > 0) unrecoverable_ = true;
                        return ret;
                    }
                }
            }
            start_idx = device_id_end_index_pair.second;
        }
    }
    // After all device segments wrote successfully, advance recovery's
    // per-device last_time_ floor to the highest timestamp this tablet
    // contributed for each device.
    if (enforce_recovered_last_time_order_ && tablet.timestamps_) {
        int update_start = 0;
        for (auto& pair : device_id_end_index_pairs) {
            int end_idx = pair.second;
            if (end_idx == 0) continue;
            if (end_idx > update_start) {
                auto schema_it = schemas_.find(pair.first);
                if (schema_it != schemas_.end() &&
                    schema_it->second != nullptr) {
                    schema_it->second->last_time_ =
                        std::max(schema_it->second->last_time_,
                                 tablet.timestamps_[end_idx - 1]);
                }
            }
            update_start = end_idx;
        }
    }
    record_count_since_last_flush_ += tablet.cur_row_size_;
    // Reset string column buffers so the tablet can be reused for the next
    // batch without accumulating memory across writes.
    tablet.reset_string_columns();
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

std::vector<std::pair<std::shared_ptr<IDeviceID>, int>>
TsFileWriter::split_tablet_by_device(const Tablet& tablet) {
    std::vector<std::pair<std::shared_ptr<IDeviceID>, int>> result;

    if (tablet.id_column_indexes_.empty() || tablet.single_device_) {
        // No tag columns or caller guarantees single device — skip boundary
        // detection entirely.
        auto sentinel = std::make_shared<StringArrayDeviceID>("last_device_id");
        result.emplace_back(std::move(sentinel), 0);
        std::shared_ptr<IDeviceID> dev_id(tablet.get_device_id(0));
        result.emplace_back(std::move(dev_id), tablet.get_cur_row_size());
        return result;
    }

    const uint32_t row_count = tablet.get_cur_row_size();
    if (row_count == 0) return result;

    auto sentinel = std::make_shared<StringArrayDeviceID>("last_device_id");
    result.emplace_back(std::move(sentinel), 0);

    auto boundaries = tablet.find_all_device_boundaries();

    uint32_t seg_start = 0;
    for (uint32_t b : boundaries) {
        std::shared_ptr<IDeviceID> dev_id(tablet.get_device_id(seg_start));
        result.emplace_back(std::move(dev_id), b);
        seg_start = b;
    }
    std::shared_ptr<IDeviceID> last_id(tablet.get_device_id(seg_start));
    result.emplace_back(std::move(last_id), row_count);
    return result;
}

int TsFileWriter::write_column(ChunkWriter* chunk_writer, const Tablet& tablet,
                               int col_idx, uint32_t start_idx,
                               uint32_t end_idx) {
    common::TSDataType data_type = tablet.schema_vec_->at(col_idx).data_type_;
    int64_t* timestamps = tablet.timestamps_;
    Tablet::ValueMatrixEntry col_values = tablet.value_matrix_[col_idx];
    BitMap& col_notnull_bitmap = tablet.bitmaps_[col_idx];
    end_idx = std::min(end_idx, tablet.max_row_num_);

    // Cover every storage type (DATE->int32, TIMESTAMP->int64, TEXT/BLOB->
    // string).  This is the null fallback for the non-aligned batch path, so a
    // column of any type that contains a null lands here; the old if/else only
    // handled 6 types and ASSERT(false)'d (silently no-op in NDEBUG) on
    // DATE/TIMESTAMP/TEXT/BLOB, dropping those rows.
    switch (data_type) {
        case common::BOOLEAN:
            return write_typed_column(chunk_writer, timestamps,
                                      col_values.bool_data, col_notnull_bitmap,
                                      start_idx, end_idx);
        case common::INT32:
        case common::DATE:
            return write_typed_column(chunk_writer, timestamps,
                                      col_values.int32_data, col_notnull_bitmap,
                                      start_idx, end_idx);
        case common::INT64:
        case common::TIMESTAMP:
            return write_typed_column(chunk_writer, timestamps,
                                      col_values.int64_data, col_notnull_bitmap,
                                      start_idx, end_idx);
        case common::FLOAT:
            return write_typed_column(chunk_writer, timestamps,
                                      col_values.float_data, col_notnull_bitmap,
                                      start_idx, end_idx);
        case common::DOUBLE:
            return write_typed_column(chunk_writer, timestamps,
                                      col_values.double_data,
                                      col_notnull_bitmap, start_idx, end_idx);
        case common::STRING:
        case common::TEXT:
        case common::BLOB:
            return write_typed_column(chunk_writer, timestamps,
                                      col_values.string_col, col_notnull_bitmap,
                                      start_idx, end_idx);
        default:
            return E_NOT_SUPPORT;
    }
}

int TsFileWriter::time_write_column(TimeChunkWriter* time_chunk_writer,
                                    const Tablet& tablet, uint32_t start_idx,
                                    uint32_t end_idx) {
    int64_t* timestamps = tablet.timestamps_;
    int ret = E_OK;
    if (IS_NULL(time_chunk_writer) || IS_NULL(timestamps)) {
        return E_INVALID_ARG;
    }
    for (uint32_t r = start_idx; r < end_idx && r < tablet.max_row_num_; r++) {
        if (RET_FAIL(time_chunk_writer->write(timestamps[r]))) {
            break;
        }
    }
    return ret;
}

// Non-aligned numeric column: a null row contributes no point, so null rows
// are skipped.  Covers bool/int32/int64/float/double; instantiated only from
// write_column in this translation unit.
template <typename T>
int TsFileWriter::write_typed_column(ChunkWriter* chunk_writer,
                                     int64_t* timestamps, T* col_values,
                                     BitMap& col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    int ret = E_OK;
    for (uint32_t r = start_idx; r < end_idx; r++) {
        if (LIKELY(!col_notnull_bitmap.test(r))) {
            if (RET_FAIL(chunk_writer->write(timestamps[r], col_values[r]))) {
                return ret;
            }
        }
    }
    return ret;
}

int TsFileWriter::write_typed_column(ChunkWriter* chunk_writer,
                                     int64_t* timestamps,
                                     Tablet::StringColumn* string_col,
                                     BitMap& col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    int ret = E_OK;
    for (uint32_t r = start_idx; r < end_idx; r++) {
        if (LIKELY(!col_notnull_bitmap.test(r))) {
            common::String val(
                string_col->buffer + string_col->offsets[r],
                string_col->offsets[r + 1] - string_col->offsets[r]);
            if (RET_FAIL(chunk_writer->write(timestamps[r], val))) {
                return ret;
            }
        }
    }
    return ret;
}

int TsFileWriter::time_write_column_batch(TimeChunkWriter* time_chunk_writer,
                                          const Tablet& tablet,
                                          uint32_t start_idx,
                                          uint32_t end_idx) {
    int64_t* timestamps = tablet.timestamps_;
    int ret = E_OK;
    if (IS_NULL(time_chunk_writer) || IS_NULL(timestamps)) {
        return E_INVALID_ARG;
    }
    end_idx = std::min(end_idx, tablet.max_row_num_);
    uint32_t count = end_idx - start_idx;
    if (count == 0) return ret;
    return time_chunk_writer->write_batch(timestamps + start_idx, count);
}

int TsFileWriter::write_column_batch(ChunkWriter* chunk_writer,
                                     const Tablet& tablet, int col_idx,
                                     uint32_t start_idx, uint32_t end_idx) {
    int ret = E_OK;
    common::TSDataType data_type = tablet.schema_vec_->at(col_idx).data_type_;
    int64_t* timestamps = tablet.timestamps_;
    Tablet::ValueMatrixEntry col_values = tablet.value_matrix_[col_idx];
    BitMap& col_notnull_bitmap = tablet.bitmaps_[col_idx];
    end_idx = std::min(end_idx, tablet.max_row_num_);
    uint32_t count = end_idx - start_idx;
    if (count == 0) return ret;

    bool has_null = false;
    if (col_notnull_bitmap.may_have_set_bits()) {
        for (uint32_t r = start_idx; r < end_idx; r++) {
            if (col_notnull_bitmap.test(r)) {
                has_null = true;
                break;
            }
        }
    }

    if (!has_null) {
        switch (data_type) {
            case common::BOOLEAN:
                ret = chunk_writer->write_batch(
                    timestamps + start_idx, col_values.bool_data + start_idx,
                    count);
                break;
            case common::INT32:
            case common::DATE:
                ret = chunk_writer->write_batch(
                    timestamps + start_idx, col_values.int32_data + start_idx,
                    count);
                break;
            case common::INT64:
            case common::TIMESTAMP:
                ret = chunk_writer->write_batch(
                    timestamps + start_idx, col_values.int64_data + start_idx,
                    count);
                break;
            case common::FLOAT:
                ret = chunk_writer->write_batch(
                    timestamps + start_idx, col_values.float_data + start_idx,
                    count);
                break;
            case common::DOUBLE:
                ret = chunk_writer->write_batch(
                    timestamps + start_idx, col_values.double_data + start_idx,
                    count);
                break;
            case common::STRING:
            case common::TEXT:
            case common::BLOB: {
                auto* sc = col_values.string_col;
                // sc->offsets is int32_t* (Arrow Utf8/Binary spec);
                // write_string_batch still takes const uint32_t* through the
                // page/encoder stack.  Offsets are non-negative by
                // construction so the bit pattern is identical — cast at the
                // boundary until the downstream chain is converted in a
                // follow-up.
                ret = chunk_writer->write_string_batch(
                    timestamps + start_idx, sc->buffer,
                    reinterpret_cast<const uint32_t*>(sc->offsets), start_idx,
                    count);
                break;
            }
            default:
                ret = write_column(chunk_writer, tablet, col_idx, start_idx,
                                   end_idx);
                break;
        }
    } else {
        ret = write_column(chunk_writer, tablet, col_idx, start_idx, end_idx);
    }
    return ret;
}

int TsFileWriter::value_write_column_batch(ValueChunkWriter* value_chunk_writer,
                                           const Tablet& tablet, int col_idx,
                                           uint32_t start_idx,
                                           uint32_t end_idx) {
    int ret = E_OK;
    common::TSDataType data_type = tablet.schema_vec_->at(col_idx).data_type_;
    int64_t* timestamps = tablet.timestamps_;
    Tablet::ValueMatrixEntry col_values = tablet.value_matrix_[col_idx];
    BitMap& col_notnull_bitmap = tablet.bitmaps_[col_idx];
    end_idx = std::min(end_idx, tablet.max_row_num_);
    uint32_t count = end_idx - start_idx;
    if (count == 0) return ret;

    switch (data_type) {
        case common::BOOLEAN:
            ret = value_chunk_writer->write_batch(
                timestamps, col_values.bool_data, col_notnull_bitmap, start_idx,
                count);
            break;
        case common::DATE:
        case common::INT32:
            ret = value_chunk_writer->write_batch(
                timestamps, col_values.int32_data, col_notnull_bitmap,
                start_idx, count);
            break;
        case common::TIMESTAMP:
        case common::INT64:
            ret = value_chunk_writer->write_batch(
                timestamps, col_values.int64_data, col_notnull_bitmap,
                start_idx, count);
            break;
        case common::FLOAT:
            ret = value_chunk_writer->write_batch(
                timestamps, col_values.float_data, col_notnull_bitmap,
                start_idx, count);
            break;
        case common::DOUBLE:
            ret = value_chunk_writer->write_batch(
                timestamps, col_values.double_data, col_notnull_bitmap,
                start_idx, count);
            break;
        case common::STRING:
        case common::TEXT:
        case common::BLOB: {
            auto* sc = col_values.string_col;
            // See above: sc->offsets is int32_t*, downstream still uint32_t*.
            ret = value_chunk_writer->write_string_batch(
                timestamps, sc->buffer,
                reinterpret_cast<const uint32_t*>(sc->offsets),
                col_notnull_bitmap, start_idx, count);
            break;
        }
        default:
            ret = E_NOT_SUPPORT;
            break;
    }
    return ret;
}

// TODO make sure ret is meaningful to SDK user
int TsFileWriter::flush() {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    int ret = E_OK;
    if (!start_file_done_) {
        if (RET_FAIL(io_writer_->start_file())) {
            return ret;
        }
        start_file_done_ = true;
    }

    /* since @schemas_ used std::map which is rbtree underlying,
             so map itself is ordered by device name. */

    DeviceSchemasMapIter device_iter;
    for (device_iter = schemas_.begin(); device_iter != schemas_.end();
         device_iter++) {
        if (check_chunk_group_empty(device_iter->second,
                                    device_iter->second->is_aligned_)) {
            continue;
        }
        bool is_aligned = device_iter->second->is_aligned_;

        if (RET_FAIL(io_writer_->start_flush_chunk_group(device_iter->first,
                                                         is_aligned))) {
        } else if (RET_FAIL(
                       flush_chunk_group(device_iter->second, is_aligned))) {
        } else if (RET_FAIL(io_writer_->end_flush_chunk_group(is_aligned))) {
        }
    }

    record_count_since_last_flush_ = 0;
    return ret;
}

bool TsFileWriter::check_chunk_group_empty(MeasurementSchemaGroup* chunk_group,
                                           bool is_aligned) {
    if (chunk_group->is_aligned_ &&
        chunk_group->time_chunk_writer_ != nullptr &&
        chunk_group->time_chunk_writer_->hasData()) {
        return false;
    }
    MeasurementSchemaMap& map = chunk_group->measurement_schema_map_;
    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema* m_schema = ms_iter->second;
        if (is_aligned) {
            if (m_schema->value_chunk_writer_ != NULL &&
                m_schema->value_chunk_writer_->hasData()) {
                return false;
            }
        } else {
            if (m_schema->chunk_writer_ != NULL &&
                m_schema->chunk_writer_->hasData()) {
                // first condition is to avoid first flush empty chunk group
                // second condition is to avoid repeated flush
                return false;
            }
        }
    }
    return true;
}

#define FLUSH_CHUNK(writer, io_writer, name, data_type, encoding, compression, \
                    num_pages)                                                 \
    if (RET_FAIL(writer->end_encode_chunk())) {                                \
    } else if (RET_FAIL(io_writer->start_flush_chunk(                          \
                   writer->get_chunk_data(), name, data_type, encoding,        \
                   compression, num_pages))) {                                 \
    } else if (RET_FAIL(io_writer->flush_chunk(writer->get_chunk_data()))) {   \
    } else if (RET_FAIL(io_writer->end_flush_chunk(                            \
                   writer->get_chunk_statistic()))) {                          \
    } else {                                                                   \
        writer->reset();                                                       \
    }

// Write already-encoded chunk data to stream (no compression — done earlier).
#define FLUSH_CHUNK_ENCODED(writer, io_writer, name, data_type, encoding,     \
                            compression, num_pages)                           \
    if (RET_FAIL(io_writer->start_flush_chunk(writer->get_chunk_data(), name, \
                                              data_type, encoding,            \
                                              compression, num_pages))) {     \
    } else if (RET_FAIL(io_writer->flush_chunk(writer->get_chunk_data()))) {  \
    } else if (RET_FAIL(io_writer->end_flush_chunk(                           \
                   writer->get_chunk_statistic()))) {                         \
    } else {                                                                  \
        writer->reset();                                                      \
    }

int TsFileWriter::flush_chunk_group_encoded(MeasurementSchemaGroup* chunk_group,
                                            bool is_aligned) {
    int ret = E_OK;
    MeasurementSchemaMap& map = chunk_group->measurement_schema_map_;

    if (chunk_group->is_aligned_) {
        TimeChunkWriter*& time_chunk_writer = chunk_group->time_chunk_writer_;
        ChunkHeader chunk_header = time_chunk_writer->get_chunk_header();
        FLUSH_CHUNK_ENCODED(
            time_chunk_writer, io_writer_, chunk_header.measurement_name_,
            chunk_header.data_type_, chunk_header.encoding_type_,
            chunk_header.compression_type_, time_chunk_writer->num_of_pages())
    }

    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema* m_schema = ms_iter->second;
        // Skip registered-but-empty columns: a measurement that was never
        // written in this window would otherwise be sealed as an EMPTY chunk
        // (count=0, dataSize=0). Java readers (TsFileSequenceReader self-
        // check) treat such a file as crashed. Mirror the aligned branch's
        // hasData() check below.
        if (!chunk_group->is_aligned_ && m_schema->chunk_writer_ != nullptr &&
            m_schema->chunk_writer_->hasData()) {
            ChunkWriter*& chunk_writer = m_schema->chunk_writer_;
            FLUSH_CHUNK_ENCODED(
                chunk_writer, io_writer_, m_schema->measurement_name_,
                m_schema->data_type_, m_schema->encoding_,
                m_schema->compression_type_, chunk_writer->num_of_pages())
        } else if (m_schema->value_chunk_writer_ != nullptr &&
                   m_schema->value_chunk_writer_->hasData()) {
            ValueChunkWriter*& value_chunk_writer =
                m_schema->value_chunk_writer_;
            FLUSH_CHUNK_ENCODED(
                value_chunk_writer, io_writer_, m_schema->measurement_name_,
                m_schema->data_type_, m_schema->encoding_,
                m_schema->compression_type_, value_chunk_writer->num_of_pages())
        }
    }

    return ret;
}

int TsFileWriter::flush_chunk_group(MeasurementSchemaGroup* chunk_group,
                                    bool is_aligned) {
    int ret = E_OK;
    MeasurementSchemaMap& map = chunk_group->measurement_schema_map_;

    if (chunk_group->is_aligned_) {
        TimeChunkWriter*& time_chunk_writer = chunk_group->time_chunk_writer_;
        ChunkHeader chunk_header = time_chunk_writer->get_chunk_header();
        FLUSH_CHUNK(time_chunk_writer, io_writer_,
                    chunk_header.measurement_name_, chunk_header.data_type_,
                    chunk_header.encoding_type_, chunk_header.compression_type_,
                    time_chunk_writer->num_of_pages())
    }

    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema* m_schema = ms_iter->second;
        // See flush_chunk_group_encoded: never seal a registered-but-empty
        // column as a count=0 chunk.
        if (!chunk_group->is_aligned_ && m_schema->chunk_writer_ != nullptr &&
            m_schema->chunk_writer_->hasData()) {
            ChunkWriter*& chunk_writer = m_schema->chunk_writer_;
            FLUSH_CHUNK(chunk_writer, io_writer_, m_schema->measurement_name_,
                        m_schema->data_type_, m_schema->encoding_,
                        m_schema->compression_type_,
                        chunk_writer->num_of_pages())
        } else if (m_schema->value_chunk_writer_ != nullptr &&
                   m_schema->value_chunk_writer_->hasData()) {
            ValueChunkWriter*& value_chunk_writer =
                m_schema->value_chunk_writer_;
            FLUSH_CHUNK(value_chunk_writer, io_writer_,
                        m_schema->measurement_name_, m_schema->data_type_,
                        m_schema->encoding_, m_schema->compression_type_,
                        value_chunk_writer->num_of_pages())
        }
    }

    return ret;
}

int TsFileWriter::close() {
    if (UNLIKELY(unrecoverable_)) return E_DATA_INCONSISTENCY;
    return io_writer_->end_file();
}

int TsFileWriter::add_tsfile_property(const std::string& key,
                                      const uint8_t* value,
                                      uint32_t value_len) {
    if (io_writer_ == nullptr) {
        return E_FILE_WRITE_ERR;
    }
    return io_writer_->add_tsfile_property(key, value, value_len);
}

int TsFileWriter::add_tsfile_property(const std::string& key,
                                      const std::vector<uint8_t>& value) {
    if (io_writer_ == nullptr) {
        return E_FILE_WRITE_ERR;
    }
    return io_writer_->add_tsfile_property(key, value);
}

}  // end namespace storage
