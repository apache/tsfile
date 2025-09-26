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

#include <unistd.h>

#include "chunk_writer.h"
#include "common/config/config.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "utils/errno_define.h"

using namespace common;

namespace storage {

typedef std::map<std::string, MeasurementSchemaGroup *>::iterator
    DeviceSchemaIter;

int libtsfile_init() {
    static bool g_s_is_inited = false;
    if (g_s_is_inited) {
        return E_OK;
    }
    ModStat::get_instance().init();

    init_config_value();

    g_s_is_inited = true;
    return E_OK;
}

void libtsfile_destroy() { ModStat::get_instance().destroy(); }

void set_page_max_point_count(uint32_t page_max_ponint_count) {
    config_set_page_max_point_count(page_max_ponint_count);
}
void set_max_degree_of_index_node(uint32_t max_degree_of_index_node) {
    config_set_max_degree_of_index_node(max_degree_of_index_node);
}

TsFileWriter::TsFileWriter()
    : write_file_(nullptr),
      io_writer_(nullptr),
      schemas_(),
      record_count_since_last_flush_(0),
      record_count_for_next_mem_check_(
          g_config_value_.record_count_for_next_mem_check_),
      write_file_created_(false) {}

TsFileWriter::~TsFileWriter() { destroy(); }

void TsFileWriter::destroy() {
    if (write_file_created_ && write_file_ != nullptr) {
        delete write_file_;
        write_file_ = nullptr;
    }
    if (io_writer_) {
        delete io_writer_;
        io_writer_ = NULL;
    }
    std::map<std::string, MeasurementSchemaGroup *>::iterator dev_iter;
    // cppcheck-suppress postfixOperator
    for (dev_iter = schemas_.begin(); dev_iter != schemas_.end(); dev_iter++) {
        MeasurementSchemaMap &ms_map =
            dev_iter->second->measurement_schema_map_;
        MeasurementSchemaMapIter ms_iter;
        for (ms_iter = ms_map.begin(); ms_iter != ms_map.end(); ms_iter++) {
            MeasurementSchema *ms = ms_iter->second;
            if (ms != nullptr) {
                if (ms->chunk_writer_ != nullptr) {
                    delete ms->chunk_writer_;
                }
                delete ms;
            }
        }
        delete dev_iter->second;
    }
    schemas_.clear();
    record_count_since_last_flush_ = 0;
}

int TsFileWriter::init(WriteFile *write_file) {
    if (write_file == NULL) {
        return E_INVALID_ARG;
    } else if (!write_file->file_opened()) {
        return E_INVALID_ARG;
    }
    write_file_ = write_file;
    write_file_created_ = false;
    io_writer_ = new TsFileIOWriter();
    io_writer_->init(write_file_);
    return E_OK;
}

bool check_file_exist(const std::string &file_path) {
    return access(file_path.c_str(), F_OK) == 0;
}

int TsFileWriter::open(const std::string &file_path, int flags, mode_t mode) {
    if (check_file_exist(file_path)) {
        return E_ALREADY_EXIST;
    }
    write_file_ = new WriteFile;
    write_file_created_ = true;
    io_writer_ = new TsFileIOWriter;
    int ret = E_OK;
    if (RET_FAIL(write_file_->create(file_path, flags, mode))) {
    } else {
        io_writer_->init(write_file_);
        if (RET_FAIL(io_writer_->start_file())) {
            return ret;
        }
#if DEBUG_SE
        std::cout << "finish writing magic code" << std::endl;
#endif
    }
    return ret;
}

int TsFileWriter::register_timeseries(
    const std::string &device_path, const std::string &measurement_name,
    common::TSDataType data_type, common::TSEncoding encoding,
    common::CompressionType compression_type) {
    MeasurementSchema *ms = new MeasurementSchema(measurement_name, data_type,
                                                  encoding, compression_type);
    return register_timeseries(device_path, ms);
}

int TsFileWriter::register_timeseries(const std::string &device_path,
                                      MeasurementSchema *measurement_schema) {
    DeviceSchemaIter device_iter = schemas_.find(device_path);
    if (device_iter != schemas_.end()) {
        MeasurementSchemaMap &msm =
            device_iter->second->measurement_schema_map_;
        MeasurementSchemaMapInsertResult ins_res = msm.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        if (UNLIKELY(!ins_res.second)) {
            return E_ALREADY_EXIST;
        }
    } else {
        MeasurementSchemaGroup *ms_group = new MeasurementSchemaGroup;
        ms_group->measurement_schema_map_.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        schemas_.insert(std::make_pair(device_path, ms_group));
    }
    return E_OK;
}

int TsFileWriter::register_timeseries(
    const std::string &device_path,
    const std::vector<MeasurementSchema *> &measurement_schema_vec) {
    int ret = E_OK;
    std::vector<MeasurementSchema *>::const_iterator it =
        measurement_schema_vec.begin();
    for (; it != measurement_schema_vec.end();
         it++) {  // cppcheck-suppress postfixOperator
        ret = register_timeseries(device_path, *it);
        if (ret != E_OK) {
            return ret;
        }
    }
    return ret;
}

struct MeasurementNamesFromRecord {
    const TsRecord &record_;

    explicit MeasurementNamesFromRecord(const TsRecord &record)
        : record_(record) {}
    FORCE_INLINE uint32_t get_count() const { return record_.points_.size(); }
    FORCE_INLINE const std::string &at(size_t idx) const {
        ASSERT(idx < record_.points_.size());
        return record_.points_[idx].measurement_name_;
    }
};

struct MeasurementNamesFromTablet {
    const Tablet &tablet_;

    explicit MeasurementNamesFromTablet(const Tablet &tablet)
        : tablet_(tablet) {}
    FORCE_INLINE uint32_t get_count() const {
        return tablet_.schema_vec_->size();
    }
    FORCE_INLINE const std::string &at(size_t idx) const {
        ASSERT(idx < tablet_.schema_vec_->size());
        return tablet_.schema_vec_->at(idx).measurement_name_;
    }
};

template <typename MeasurementNamesGetter>
int TsFileWriter::do_check_schema(const std::string &device_name,
                                  MeasurementNamesGetter &measurement_names,
                                  SimpleVector<ChunkWriter *> &chunk_writers) {
    int ret = E_OK;
    DeviceSchemaIter dev_it = schemas_.find(device_name);
    MeasurementSchemaGroup *device_schema = NULL;
    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        return E_DEVICE_NOT_EXIST;
    }
    MeasurementSchemaMap &msm = device_schema->measurement_schema_map_;
    uint32_t measurement_count = measurement_names.get_count();
    // chunk_writers.reserve(measurement_count);
    for (uint32_t i = 0; i < measurement_count; i++) {
        MeasurementSchemaMapIter ms_iter = msm.find(measurement_names.at(i));
        if (UNLIKELY(ms_iter == msm.end())) {
            chunk_writers.push_back(NULL);
        } else {
            // In Java we will check data_type. But in C++, no check here.
            // Because checks are performed at the chunk layer and page layer
            MeasurementSchema *ms = ms_iter->second;
            if (IS_NULL(ms->chunk_writer_)) {
                ms->chunk_writer_ = new ChunkWriter;
                ret = ms->chunk_writer_->init(ms->measurement_name_,
                                              ms->data_type_, ms->encoding_,
                                              ms->compression_type_);
                if (IS_SUCC(ret)) {
                    chunk_writers.push_back(ms->chunk_writer_);
                } else {
                    std::cout << "get error when chunk init" << std::endl;
                    chunk_writers.push_back(NULL);
                }
            } else {
                chunk_writers.push_back(ms->chunk_writer_);
            }
        }
    }
    return ret;
}

int64_t TsFileWriter::calculate_mem_size_for_all_group() {
    int64_t mem_total_size = 0;
    DeviceSchemaIter device_iter;
    for (device_iter = schemas_.begin(); device_iter != schemas_.end();
         device_iter++) {
        MeasurementSchemaGroup *chunk_group = device_iter->second;
        MeasurementSchemaMap &map = chunk_group->measurement_schema_map_;
        for (MeasurementSchemaMapIter ms_iter = map.begin();
             ms_iter != map.end(); ms_iter++) {
            MeasurementSchema *m_schema = ms_iter->second;
            ChunkWriter *&chunk_writer = m_schema->chunk_writer_;
            if (chunk_writer != NULL) {
                mem_total_size += chunk_writer->estimate_max_series_mem_size();
            }
        }
    }
    return mem_total_size;
}

/**
 * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush
 * them to given OutputStream.
 */
int TsFileWriter::check_memory_size_and_may_flush_chunks() {
    int ret = E_OK;
    if (record_count_since_last_flush_ >= record_count_for_next_mem_check_) {
        int64_t mem_size = calculate_mem_size_for_all_group();
        record_count_for_next_mem_check_ =
            record_count_since_last_flush_ *
            common::g_config_value_.chunk_group_size_threshold_ / mem_size;
        if (mem_size > common::g_config_value_.chunk_group_size_threshold_) {
            ret = flush();
        }
    }
    return ret;
}

int TsFileWriter::write_record(const TsRecord &record) {
    int ret = E_OK;
    // std::vector<ChunkWriter*> chunk_writers;
    SimpleVector<ChunkWriter *> chunk_writers;
    MeasurementNamesFromRecord mnames_getter(record);
    if (RET_FAIL(do_check_schema(record.device_name_, mnames_getter,
                                 chunk_writers))) {
        return ret;
    }

    ASSERT(chunk_writers.size() == record.points_.size());
    for (uint32_t c = 0; c < chunk_writers.size(); c++) {
        ChunkWriter *chunk_writer = chunk_writers[c];
        if (IS_NULL(chunk_writer)) {
            continue;
        }
        // ignore point writer failure
        write_point(chunk_writer, record.timestamp_, record.points_[c]);
    }

    record_count_since_last_flush_++;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

int TsFileWriter::write_point(ChunkWriter *chunk_writer, int64_t timestamp,
                              const DataPoint &point) {
    switch (point.data_type_) {
        case common::BOOLEAN:
            return chunk_writer->write(timestamp, point.u_.bool_val_);
        case common::INT32:
            return chunk_writer->write(timestamp, point.u_.i32_val_);
        case common::INT64:
            return chunk_writer->write(timestamp, point.u_.i64_val_);
        case common::FLOAT:
            return chunk_writer->write(timestamp, point.u_.float_val_);
        case common::DOUBLE:
            return chunk_writer->write(timestamp, point.u_.double_val_);
        case common::TEXT:
            ASSERT(false);
            return E_OK;
        default:
            return E_INVALID_DATA_POINT;
    }
}

int TsFileWriter::write_tablet(const Tablet &tablet) {
    int ret = E_OK;
    // std::vector<ChunkWriter*> chunk_writers;
    SimpleVector<ChunkWriter *> chunk_writers;
    MeasurementNamesFromTablet mnames_getter(tablet);
    if (RET_FAIL(do_check_schema(tablet.device_name_, mnames_getter,
                                 chunk_writers))) {
        return ret;
    }
    ASSERT(chunk_writers.size() == tablet.get_column_count());
    for (uint32_t c = 0; c < chunk_writers.size(); c++) {
        ChunkWriter *chunk_writer = chunk_writers[c];
        if (IS_NULL(chunk_writer)) {
            continue;
        }
        // ignore writer failure
        write_column(chunk_writer, tablet, c);
    }

    record_count_since_last_flush_ += tablet.max_rows_;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

int TsFileWriter::write_column(ChunkWriter *chunk_writer, const Tablet &tablet,
                               int col_idx) {
    int ret = E_OK;

    TSDataType data_type = tablet.schema_vec_->at(col_idx).data_type_;
    int64_t *timestamps = tablet.timestamps_;
    void *col_values = tablet.value_matrix_[col_idx];
    BitMap &col_bitmap = tablet.bitmaps_[col_idx];
    int32_t row_count = tablet.max_rows_;

    if (data_type == common::BOOLEAN) {
        ret = write_typed_column(chunk_writer, timestamps, (bool *)col_values,
                                 col_bitmap, row_count);
    } else if (data_type == common::INT32) {
        ret = write_typed_column(chunk_writer, timestamps,
                                 (int32_t *)col_values, col_bitmap, row_count);
    } else if (data_type == common::INT64) {
        ret = write_typed_column(chunk_writer, timestamps,
                                 (int64_t *)col_values, col_bitmap, row_count);
    } else if (data_type == common::FLOAT) {
        ret = write_typed_column(chunk_writer, timestamps, (float *)col_values,
                                 col_bitmap, row_count);
    } else if (data_type == common::DOUBLE) {
        ret = write_typed_column(chunk_writer, timestamps, (double *)col_values,
                                 col_bitmap, row_count);
    } else {
        ASSERT(false);
    }
    return ret;
}

#define DO_WRITE_TYPED_COLUMN()                                          \
    do {                                                                 \
        int ret = E_OK;                                                  \
        for (int r = 0; r < row_count; r++) {                            \
            if (LIKELY(col_bitmap.test(r))) {                            \
                ret = chunk_writer->write(timestamps[r], col_values[r]); \
            }                                                            \
        }                                                                \
        return ret;                                                      \
    } while (false)

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, bool *col_values,
                                     BitMap &col_bitmap, int32_t row_count) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, int32_t *col_values,
                                     BitMap &col_bitmap, int32_t row_count) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, int64_t *col_values,
                                     BitMap &col_bitmap, int32_t row_count) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, float *col_values,
                                     BitMap &col_bitmap, int32_t row_count) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, double *col_values,
                                     BitMap &col_bitmap, int32_t row_count) {
    DO_WRITE_TYPED_COLUMN();
}

// TODO make sure ret is meaningful to SDK user
int TsFileWriter::flush() {
    int ret = E_OK;

    /* since @schemas_ used std::map which is rbtree underlying,
       so map itself is ordered by device name. */
    std::map<std::string, MeasurementSchemaGroup *>::iterator device_iter;
    for (device_iter = schemas_.begin(); device_iter != schemas_.end();
         device_iter++) {
        if (check_chunk_group_empty(device_iter->second)) {
            continue;
        }

        if (RET_FAIL(io_writer_->start_flush_chunk_group(device_iter->first))) {
            return ret;
        } else if (RET_FAIL(flush_chunk_group(device_iter->second))) {
            return ret;
        } else if (RET_FAIL(io_writer_->end_flush_chunk_group())) {
            return ret;
        }
    }
    record_count_since_last_flush_ = 0;
    return ret;
}

bool TsFileWriter::check_chunk_group_empty(
    MeasurementSchemaGroup *chunk_group) {
    MeasurementSchemaMap &map = chunk_group->measurement_schema_map_;
    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema *m_schema = ms_iter->second;
        if (m_schema->chunk_writer_ != NULL &&
            m_schema->chunk_writer_->num_of_pages() > 0) {
            // first condition is to avoid first flush empty chunk group
            // second condition is to avoid repeated flush
            return false;
        }
    }
    return true;
}

int TsFileWriter::flush_chunk_group(MeasurementSchemaGroup *chunk_group) {
    int ret = E_OK;
    MeasurementSchemaMap &map = chunk_group->measurement_schema_map_;
    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema *m_schema = ms_iter->second;
        ChunkWriter *&chunk_writer = m_schema->chunk_writer_;
        if (RET_FAIL(chunk_writer->end_encode_chunk())) {
        } else if (RET_FAIL(io_writer_->start_flush_chunk(
                       chunk_writer->get_chunk_data(),
                       m_schema->measurement_name_, m_schema->data_type_,
                       m_schema->encoding_, m_schema->compression_type_,
                       chunk_writer->num_of_pages()))) {
            return ret;
        } else if (RET_FAIL(io_writer_->flush_chunk(
                       chunk_writer->get_chunk_data()))) {
            return ret;
        } else if (RET_FAIL(io_writer_->end_flush_chunk(
                       chunk_writer->get_chunk_statistic()))) {
            return ret;
        } else {
            chunk_writer->destroy();
            chunk_writer = nullptr;
        }
    }
    return ret;
}

int TsFileWriter::close() { return io_writer_->end_file(); }

}  // end namespace storage
