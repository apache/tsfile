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

#ifndef DATASET_DATASET_INDEX_H
#define DATASET_DATASET_INDEX_H

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

namespace storage {
namespace dataset {

static const uint16_t DATASET_INDEX_VERSION_MAJOR = 1;
static const uint16_t DATASET_INDEX_VERSION_MINOR = 0;
static const uint32_t DATASET_INDEX_HEADER_SIZE = 64;
static const uint32_t DATASET_INDEX_DIRECTORY_ENTRY_SIZE = 32;
static const uint32_t DATASET_INDEX_SECTION_COUNT = 13;
static const uint64_t DATASET_INDEX_ALIGNMENT = 64;

enum class DatasetIndexStatus {
    OK = 0,
    INVALID_ARGUMENT,
    IO_ERROR,
    OUT_OF_MEMORY,
    BAD_MAGIC,
    UNSUPPORTED_VERSION,
    BAD_HEADER,
    BAD_DIRECTORY,
    BAD_SECTION,
    BAD_CHECKSUM,
    BAD_REFERENCE,
    NOT_FOUND,
};

const char* dataset_index_status_name(DatasetIndexStatus status);

enum class DatasetIndexSectionType : uint32_t {
    STRING_OFFSETS = 1,
    STRING_BYTES = 2,
    TABLE_NAME_INDEX = 3,
    TABLE_RECORD = 4,
    DEVICE_NAME_INDEX = 5,
    DEVICE_RECORD = 6,
    COLUMN_NAME_INDEX = 7,
    COLUMN_SCHEMA = 8,
    LOGICAL_SERIES = 9,
    TSFILE_RECORD = 10,
    DEVICE_FILE_SPAN = 11,
    SERIES_FILE_SPAN = 12,
    SERIES_LOCATOR = 13,
};

#pragma pack(push, 1)
struct DatasetIndexHeader {
    char magic[8];
    uint16_t version_major;
    uint16_t version_minor;
    uint32_t header_size;
    uint64_t directory_offset;
    uint32_t section_count;
    uint32_t directory_entry_size;
    uint64_t file_length;
    uint32_t header_crc32c;
    uint8_t reserved[20];
};

struct DatasetIndexDirectoryEntry {
    uint32_t section_type;
    uint32_t record_size;
    uint64_t offset;
    uint64_t length;
    uint32_t count;
    uint32_t crc32c;
};

struct TableNameIndexRecord {
    uint64_t name_hash;
    uint32_t name_sid;
    uint32_t table_id;
};

struct TableRecord {
    uint32_t name_sid;
    uint32_t reserved0;
    uint32_t first_device_name_index;
    uint32_t device_count;
    uint32_t first_column_name_index;
    uint32_t column_count;
    uint64_t reserved1;
};

struct DeviceNameIndexRecord {
    uint32_t table_id;
    uint32_t device_id;
    uint64_t name_hash;
    uint32_t name_sid;
    uint32_t reserved;
};

struct DeviceRecord {
    uint32_t table_id;
    uint32_t name_sid;
    uint32_t reserved0;
    uint32_t reserved1;
    uint32_t first_series_id;
    uint32_t series_count;
    uint32_t first_file_span;
    uint32_t file_span_count;
    int64_t min_time;
    int64_t max_time;
};

struct ColumnNameIndexRecord {
    uint32_t table_id;
    uint32_t column_id;
    uint64_t name_hash;
    uint32_t name_sid;
    uint32_t reserved;
};

struct ColumnSchemaRecord {
    uint32_t table_id;
    uint32_t name_sid;
    uint32_t column_ordinal;
    uint16_t logical_type;
    uint16_t physical_type;
    uint16_t encoding;
    uint16_t compression;
    uint16_t role;
    uint16_t nullable;
    uint64_t reserved;
};

struct LogicalSeriesRecord {
    uint32_t device_id;
    uint32_t column_id;
    uint32_t first_file_span;
    uint32_t file_span_count;
    int64_t min_time;
    int64_t max_time;
};

struct TsFileRecord {
    uint32_t path_sid;
    uint32_t reserved0;
    uint64_t file_size;
    uint64_t file_fingerprint;
    uint64_t reserved1;
};

struct DeviceFileSpanRecord {
    uint32_t device_id;
    uint32_t file_id;
    uint64_t time_meta_offset;
    uint32_t time_meta_length;
    uint16_t layout;
    uint16_t flags;
    uint64_t row_count;
};

struct SeriesFileSpanRecord {
    uint32_t series_id;
    uint32_t file_id;
    uint32_t locator_id;
    uint32_t reserved;
    int64_t min_time;
    int64_t max_time;
    uint64_t row_count;
};

struct SeriesLocatorRecord {
    uint32_t device_file_span_id;
    uint16_t locator_kind;
    uint16_t flags;
    uint64_t timeseries_meta_offset;
    uint32_t timeseries_meta_length;
    uint32_t padding;
};
#pragma pack(pop)

static_assert(sizeof(DatasetIndexHeader) == 64,
              "DatasetIndexHeader must be 64 bytes");
static_assert(sizeof(DatasetIndexDirectoryEntry) == 32,
              "DatasetIndexDirectoryEntry must be 32 bytes");
static_assert(sizeof(TableNameIndexRecord) == 16,
              "TableNameIndexRecord must be 16 bytes");
static_assert(sizeof(TableRecord) == 32, "TableRecord must be 32 bytes");
static_assert(sizeof(DeviceNameIndexRecord) == 24,
              "DeviceNameIndexRecord must be 24 bytes");
static_assert(sizeof(DeviceRecord) == 48, "DeviceRecord must be 48 bytes");
static_assert(sizeof(ColumnNameIndexRecord) == 24,
              "ColumnNameIndexRecord must be 24 bytes");
static_assert(sizeof(ColumnSchemaRecord) == 32,
              "ColumnSchemaRecord must be 32 bytes");
static_assert(sizeof(LogicalSeriesRecord) == 32,
              "LogicalSeriesRecord must be 32 bytes");
static_assert(sizeof(TsFileRecord) == 32, "TsFileRecord must be 32 bytes");
static_assert(sizeof(DeviceFileSpanRecord) == 32,
              "DeviceFileSpanRecord must be 32 bytes");
static_assert(sizeof(SeriesFileSpanRecord) == 40,
              "SeriesFileSpanRecord must be 40 bytes");
static_assert(sizeof(SeriesLocatorRecord) == 24,
              "SeriesLocatorRecord must be 24 bytes");

struct DatasetIndexSectionData {
    DatasetIndexSectionType type;
    uint32_t record_size;
    uint32_t count;
    std::vector<uint8_t> bytes;
};

struct DatasetIndexSectionView {
    const uint8_t* data;
    uint64_t length;
    uint32_t record_size;
    uint32_t count;

    DatasetIndexSectionView()
        : data(nullptr), length(0), record_size(0), count(0) {}
};

struct DatasetIndexStringView {
    const char* data;
    uint32_t length;

    DatasetIndexStringView() : data(nullptr), length(0) {}
    std::string to_string() const {
        return data == nullptr ? std::string() : std::string(data, length);
    }
};

uint32_t dataset_index_crc32c(const void* data, size_t length);
uint64_t dataset_index_name_hash(const char* data, size_t length);

class DatasetIndexWriter {
   public:
    static DatasetIndexStatus write_atomic(
        const std::string& output_path,
        const std::vector<DatasetIndexSectionData>& sections,
        std::string& error_message);
};

class MappedDatasetIndex {
   public:
    MappedDatasetIndex();
    ~MappedDatasetIndex();

    DatasetIndexStatus open(const std::string& path);
    void close();
    bool is_open() const { return mapping_ != nullptr; }
    const std::string& error_message() const { return error_message_; }
    const std::string& path() const { return path_; }
    uint64_t file_length() const { return mapping_size_; }
    const DatasetIndexHeader* header() const { return header_; }

    DatasetIndexStatus section(DatasetIndexSectionType type,
                               DatasetIndexSectionView& result) const;
    DatasetIndexStatus string(uint32_t sid,
                              DatasetIndexStringView& result) const;

    template <typename T>
    DatasetIndexStatus record(DatasetIndexSectionType type, uint32_t id,
                              const T*& result) const {
        DatasetIndexSectionView view;
        DatasetIndexStatus status = section(type, view);
        if (status != DatasetIndexStatus::OK) {
            result = nullptr;
            return status;
        }
        if (view.record_size != sizeof(T) || id >= view.count) {
            result = nullptr;
            return DatasetIndexStatus::NOT_FOUND;
        }
        result = reinterpret_cast<const T*>(
            view.data + static_cast<uint64_t>(id) * view.record_size);
        return DatasetIndexStatus::OK;
    }

    DatasetIndexStatus find_table_ids(const std::string& name,
                                      std::vector<uint32_t>& table_ids) const;
    DatasetIndexStatus find_device_id(uint32_t table_id,
                                      const std::string& name,
                                      uint32_t& device_id) const;
    DatasetIndexStatus find_column_id(uint32_t table_id,
                                      const std::string& name,
                                      uint32_t& column_id) const;
    DatasetIndexStatus find_series_id(uint32_t device_id, uint32_t column_id,
                                      uint32_t& series_id) const;

   private:
    DatasetIndexStatus validate();
    DatasetIndexStatus fail(DatasetIndexStatus status,
                            const std::string& message);

    std::string path_;
    std::string error_message_;
    const uint8_t* mapping_;
    uint64_t mapping_size_;
    const DatasetIndexHeader* header_;
    const DatasetIndexDirectoryEntry* directory_;
#ifdef _WIN32
    void* file_handle_;
    void* mapping_handle_;
#else
    int fd_;
#endif
};

}  // namespace dataset
}  // namespace storage

#endif  // DATASET_DATASET_INDEX_H
