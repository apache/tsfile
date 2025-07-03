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
#ifndef WRITER_TSFILE_WRITER_H
#define WRITER_TSFILE_WRITER_H

#include <fcntl.h>

#include <climits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/container/simple_vector.h"
#include "common/device_id.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"

namespace storage {
class WriteFile;
class ChunkWriter;
class TsFileIOWriter;
}  // namespace storage

namespace storage {

extern int libtsfile_init();
extern void libtsfile_destroy();
extern void set_page_max_point_count(uint32_t page_max_ponint_count);
extern void set_max_degree_of_index_node(uint32_t max_degree_of_index_node);

class TsFileWriter {
   public:
    TsFileWriter();
    ~TsFileWriter();
    void destroy();

    int open(const std::string &file_path, int flags, mode_t mode);
    int open(const std::string &file_path);
    int init(storage::WriteFile *write_file);

    void set_generate_table_schema(bool generate_table_schema);
    int register_timeseries(const std::string &device_id,
                            const MeasurementSchema &measurement_schema);
    int register_timeseries(
        const std::string &device_path,
        const std::vector<MeasurementSchema *> &measurement_schema_vec);
    int register_aligned_timeseries(
        const std::string &device_id,
        const MeasurementSchema &measurement_schema);
    int register_aligned_timeseries(
        const std::string &device_id,
        const std::vector<MeasurementSchema *> &measurement_schemas);
    int register_table(const std::shared_ptr<TableSchema> &table_schema);
    int write_record(const TsRecord &record);
    int write_tablet(const Tablet &tablet);
    int write_record_aligned(const TsRecord &record);
    int write_tablet_aligned(const Tablet &tablet);
    int write_table(Tablet &tablet);

    typedef std::map<std::shared_ptr<IDeviceID>, MeasurementSchemaGroup *,
                     IDeviceIDComparator>
        DeviceSchemasMap;
    typedef std::map<std::shared_ptr<IDeviceID>, MeasurementSchemaGroup *,
                     IDeviceIDComparator>::iterator DeviceSchemasMapIter;

    typedef std::unordered_map<std::string, std::shared_ptr<TableSchema>>
        TableSchemasMap;
    typedef std::unordered_map<std::string,
                               std::shared_ptr<TableSchema>>::iterator
        TableSchemasMapIter;

    DeviceSchemasMap *get_schema_group_map() { return &schemas_; }
    int64_t calculate_mem_size_for_all_group();
    int check_memory_size_and_may_flush_chunks();
    /*
     * Flush buffer to disk file, but do not writer file index part.
     * TsFileWriter allows user to flush many times.
     */
    int flush();

    /*
     * Flush file index part of the whole file (it may be flushed many times
     * before close, the index part should cover all data in disk file).
     */
    int close();

   private:
    int write_point(storage::ChunkWriter *chunk_writer, int64_t timestamp,
                    common::TSDataType data_type, const DataPoint &point);
    bool check_chunk_group_empty(MeasurementSchemaGroup *chunk_group,
                                 bool is_aligned);
    int write_point_aligned(ValueChunkWriter *value_chunk_writer,
                            int64_t timestamp, common::TSDataType data_type,
                            const DataPoint &point);
    int flush_chunk_group(MeasurementSchemaGroup *chunk_group, bool is_aligned);

    int write_typed_column(storage::ChunkWriter *chunk_writer,
                           int64_t *timestamps, bool *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);
    int write_typed_column(storage::ChunkWriter *chunk_writer,
                           int64_t *timestamps, int32_t *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);
    int write_typed_column(storage::ChunkWriter *chunk_writer,
                           int64_t *timestamps, int64_t *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);
    int write_typed_column(storage::ChunkWriter *chunk_writer,
                           int64_t *timestamps, float *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);
    int write_typed_column(storage::ChunkWriter *chunk_writer,
                           int64_t *timestamps, double *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);
    int write_typed_column(ChunkWriter *chunk_writer, int64_t *timestamps,
                           common::String *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);

    template <typename MeasurementNamesGetter>
    int do_check_schema(
        std::shared_ptr<IDeviceID> device_id,
        MeasurementNamesGetter &measurement_names,
        common::SimpleVector<storage::ChunkWriter *> &chunk_writers,
        common::SimpleVector<common::TSDataType> &data_types);

    template <typename MeasurementNamesGetter>
    int do_check_schema_aligned(
        std::shared_ptr<IDeviceID> device_id,
        MeasurementNamesGetter &measurement_names,
        storage::TimeChunkWriter *&time_chunk_writer,
        common::SimpleVector<storage::ValueChunkWriter *> &value_chunk_writers,
        common::SimpleVector<common::TSDataType> &data_types);
    int do_check_schema_table(
        std::shared_ptr<IDeviceID> device_id, Tablet &tablet,
        storage::TimeChunkWriter *&time_chunk_writer,
        common::SimpleVector<storage::ValueChunkWriter *> &value_chunk_writers);

    int do_check_and_prepare_tablet(Tablet &tablet);
    // std::vector<storage::ChunkWriter*> &chunk_writers);
    int write_column(storage::ChunkWriter *chunk_writer, const Tablet &,
                     int col_idx, uint32_t start_idx = 0,
                     uint32_t end_idx = UINT32_MAX);
    int time_write_column(TimeChunkWriter *time_chunk_writer,
                          const Tablet &tablet, uint32_t start_idx = 0,
                          uint32_t end_idx = UINT32_MAX);
    int register_timeseries(const std::string &device_path,
                            MeasurementSchema *measurement_schema,
                            bool is_aligned = false);
    std::vector<std::pair<std::shared_ptr<IDeviceID>, int>>
    split_tablet_by_device(const Tablet &tablet);

   private:
    storage::WriteFile *write_file_;
    storage::TsFileIOWriter *io_writer_;
    // device_id -> MeasurementSchemaGroup
    DeviceSchemasMap schemas_;
    bool start_file_done_;
    // record count since last flush
    int64_t record_count_since_last_flush_;
    // record count for next memory check
    int64_t record_count_for_next_mem_check_;
    bool write_file_created_;
    bool table_aligned_ = true;

    int write_typed_column(ValueChunkWriter *value_chunk_writer,
                           int64_t *timestamps, bool *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);

    int write_typed_column(ValueChunkWriter *value_chunk_writer,
                           int64_t *timestamps, double *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);
    int write_typed_column(ValueChunkWriter *value_chunk_writer,
                           int64_t *timestamps, common::String *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);

    int write_typed_column(ValueChunkWriter *value_chunk_writer,
                           int64_t *timestamps, float *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);

    int write_typed_column(ValueChunkWriter *value_chunk_writer,
                           int64_t *timestamps, int32_t *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);

    int write_typed_column(ValueChunkWriter *value_chunk_writer,
                           int64_t *timestamps, int64_t *col_values,
                           common::BitMap &col_notnull_bitmap,
                           uint32_t start_idx, uint32_t end_idx);

    int value_write_column(ValueChunkWriter *value_chunk_writer,
                           const Tablet &tablet, int col_idx,
                           uint32_t start_idx, uint32_t end_idx);
};

}  // end namespace storage

#endif  // WRITER_TSFILE_WRITER_H
