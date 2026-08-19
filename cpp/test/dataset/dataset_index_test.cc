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

#include "dataset/dataset_index.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace storage {
namespace dataset {
namespace {

template <typename T>
DatasetIndexSectionData fixed_section(DatasetIndexSectionType type,
                                      const std::vector<T>& records) {
    DatasetIndexSectionData section;
    section.type = type;
    section.record_size = sizeof(T);
    section.count = static_cast<uint32_t>(records.size());
    section.bytes.resize(records.size() * sizeof(T));
    if (!records.empty()) {
        std::memcpy(section.bytes.data(), records.data(), section.bytes.size());
    }
    return section;
}

std::vector<DatasetIndexSectionData> make_minimal_sections() {
    const std::string strings[] = {"table", "device", "value", "/tmp/a.tsfile"};
    std::vector<uint32_t> offsets(1, 0);
    std::vector<uint8_t> bytes;
    for (const std::string& value : strings) {
        bytes.insert(bytes.end(), value.begin(), value.end());
        offsets.push_back(static_cast<uint32_t>(bytes.size()));
    }

    DatasetIndexSectionData string_bytes;
    string_bytes.type = DatasetIndexSectionType::STRING_BYTES;
    string_bytes.record_size = 0;
    string_bytes.count = static_cast<uint32_t>(bytes.size());
    string_bytes.bytes = bytes;

    TableNameIndexRecord table_name = {
        dataset_index_name_hash(strings[0].data(), strings[0].size()), 0, 0};
    TableRecord table = {0, 0, 0, 1, 0, 1, 0};
    DeviceNameIndexRecord device_name = {
        0, 0, dataset_index_name_hash(strings[1].data(), strings[1].size()), 1,
        0};
    DeviceRecord device = {0, 1, 0, 0, 0, 1, 0, 1, 0, 9};
    ColumnNameIndexRecord column_name = {
        0, 0, dataset_index_name_hash(strings[2].data(), strings[2].size()), 2,
        0};
    ColumnSchemaRecord column = {0, 2, 0, 2, 2, 0, 0, 0, 1, 0};
    LogicalSeriesRecord series = {0, 0, 0, 1, 0, 9};
    TsFileRecord file = {3, 0, 4096, 0x5678, 0};
    DeviceFileSpanRecord device_span = {0, 0, 0, 0, 0, 0, 0};
    SeriesFileSpanRecord series_span = {0, 0, 0, 0, 0, 9, 10};
    SeriesLocatorRecord locator = {0, 0, 0, 128, 16, 0};

    std::vector<DatasetIndexSectionData> sections;
    sections.push_back(
        fixed_section(DatasetIndexSectionType::STRING_OFFSETS, offsets));
    sections.push_back(string_bytes);
    sections.push_back(
        fixed_section(DatasetIndexSectionType::TABLE_NAME_INDEX,
                      std::vector<TableNameIndexRecord>(1, table_name)));
    sections.push_back(fixed_section(DatasetIndexSectionType::TABLE_RECORD,
                                     std::vector<TableRecord>(1, table)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::DEVICE_NAME_INDEX,
                      std::vector<DeviceNameIndexRecord>(1, device_name)));
    sections.push_back(fixed_section(DatasetIndexSectionType::DEVICE_RECORD,
                                     std::vector<DeviceRecord>(1, device)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::COLUMN_NAME_INDEX,
                      std::vector<ColumnNameIndexRecord>(1, column_name)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::COLUMN_SCHEMA,
                      std::vector<ColumnSchemaRecord>(1, column)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::LOGICAL_SERIES,
                      std::vector<LogicalSeriesRecord>(1, series)));
    sections.push_back(fixed_section(DatasetIndexSectionType::TSFILE_RECORD,
                                     std::vector<TsFileRecord>(1, file)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::DEVICE_FILE_SPAN,
                      std::vector<DeviceFileSpanRecord>(1, device_span)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::SERIES_FILE_SPAN,
                      std::vector<SeriesFileSpanRecord>(1, series_span)));
    sections.push_back(
        fixed_section(DatasetIndexSectionType::SERIES_LOCATOR,
                      std::vector<SeriesLocatorRecord>(1, locator)));
    return sections;
}

int current_process_id() {
#ifdef _WIN32
    return _getpid();
#else
    return static_cast<int>(getpid());
#endif
}

class DatasetIndexTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::ostringstream stream;
        stream << "dataset_index_test_" << current_process_id() << ".tsidx";
        path_ = stream.str();
        std::remove(path_.c_str());
        std::ostringstream temp;
        temp << path_ << ".tmp." << current_process_id();
        std::remove(temp.str().c_str());
    }

    void TearDown() override { std::remove(path_.c_str()); }

    void write_valid() {
        std::string error;
        ASSERT_EQ(DatasetIndexStatus::OK,
                  DatasetIndexWriter::write_atomic(
                      path_, make_minimal_sections(), error))
            << error;
    }

    template <typename T>
    void overwrite(uint64_t offset, const T& value) {
        std::fstream file(path_.c_str(),
                          std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file.good());
        file.seekp(static_cast<std::streamoff>(offset));
        file.write(reinterpret_cast<const char*>(&value), sizeof(value));
        ASSERT_TRUE(file.good());
    }

    std::string path_;
};

TEST_F(DatasetIndexTest, WritesMapsAndLooksUpMinimalIndex) {
    write_valid();
    MappedDatasetIndex index;
    ASSERT_EQ(DatasetIndexStatus::OK, index.open(path_))
        << index.error_message();
    EXPECT_EQ(DATASET_INDEX_SECTION_COUNT, index.header()->section_count);

    std::vector<uint32_t> tables;
    ASSERT_EQ(DatasetIndexStatus::OK, index.find_table_ids("table", tables));
    ASSERT_EQ(1U, tables.size());
    EXPECT_EQ(0U, tables[0]);

    uint32_t device_id = 99;
    uint32_t column_id = 99;
    uint32_t series_id = 99;
    EXPECT_EQ(DatasetIndexStatus::OK,
              index.find_device_id(0, "device", device_id));
    EXPECT_EQ(DatasetIndexStatus::OK,
              index.find_column_id(0, "value", column_id));
    EXPECT_EQ(DatasetIndexStatus::OK,
              index.find_series_id(device_id, column_id, series_id));
    EXPECT_EQ(0U, device_id);
    EXPECT_EQ(0U, column_id);
    EXPECT_EQ(0U, series_id);

    DatasetIndexStringView path;
    ASSERT_EQ(DatasetIndexStatus::OK, index.string(3, path));
    EXPECT_EQ("/tmp/a.tsfile", path.to_string());
    EXPECT_EQ(DatasetIndexStatus::NOT_FOUND,
              index.find_device_id(0, "missing", device_id));
}

TEST_F(DatasetIndexTest, RejectsDuplicateCanonicalTableNames) {
    std::vector<DatasetIndexSectionData> sections = make_minimal_sections();
    for (DatasetIndexSectionData& section : sections) {
        if (section.type == DatasetIndexSectionType::TABLE_NAME_INDEX) {
            const TableNameIndexRecord duplicate = {
                dataset_index_name_hash("table", 5), 0, 1};
            const uint8_t* data = reinterpret_cast<const uint8_t*>(&duplicate);
            section.bytes.insert(section.bytes.end(), data,
                                 data + sizeof(duplicate));
            ++section.count;
        } else if (section.type == DatasetIndexSectionType::TABLE_RECORD) {
            const TableRecord duplicate = {0, 0, 0, 0, 0, 0, 0};
            const uint8_t* data = reinterpret_cast<const uint8_t*>(&duplicate);
            section.bytes.insert(section.bytes.end(), data,
                                 data + sizeof(duplicate));
            ++section.count;
        }
    }
    std::string error;
    ASSERT_EQ(DatasetIndexStatus::OK,
              DatasetIndexWriter::write_atomic(path_, sections, error))
        << error;

    MappedDatasetIndex index;
    EXPECT_EQ(DatasetIndexStatus::BAD_REFERENCE, index.open(path_));
    EXPECT_NE(std::string::npos,
              index.error_message().find("duplicate table names"));
}

TEST_F(DatasetIndexTest, RejectsUnsupportedVersionBeforePublishingViews) {
    write_valid();
    uint16_t version = 2;
    overwrite(offsetof(DatasetIndexHeader, version_major), version);
    MappedDatasetIndex index;
    EXPECT_EQ(DatasetIndexStatus::UNSUPPORTED_VERSION, index.open(path_));
    EXPECT_FALSE(index.is_open());
}

TEST_F(DatasetIndexTest, RejectsHeaderChecksumMismatch) {
    write_valid();
    uint64_t wrong_length = 1;
    overwrite(offsetof(DatasetIndexHeader, file_length), wrong_length);
    MappedDatasetIndex index;
    EXPECT_EQ(DatasetIndexStatus::BAD_HEADER, index.open(path_));
    EXPECT_FALSE(index.is_open());
}

TEST_F(DatasetIndexTest, RejectsSectionChecksumMismatch) {
    write_valid();
    MappedDatasetIndex valid;
    ASSERT_EQ(DatasetIndexStatus::OK, valid.open(path_));
    DatasetIndexSectionView strings;
    ASSERT_EQ(DatasetIndexStatus::OK,
              valid.section(DatasetIndexSectionType::STRING_BYTES, strings));
    const uint64_t string_offset = static_cast<uint64_t>(
        strings.data - reinterpret_cast<const uint8_t*>(valid.header()));
    valid.close();
    const uint8_t changed = 'X';
    overwrite(string_offset, changed);

    MappedDatasetIndex index;
    EXPECT_EQ(DatasetIndexStatus::BAD_CHECKSUM, index.open(path_));
}

TEST_F(DatasetIndexTest, WriterRejectsMalformedSectionCount) {
    std::vector<DatasetIndexSectionData> sections = make_minimal_sections();
    sections.pop_back();
    std::string error;
    EXPECT_EQ(DatasetIndexStatus::INVALID_ARGUMENT,
              DatasetIndexWriter::write_atomic(path_, sections, error));
    EXPECT_FALSE(error.empty());
}

TEST(DatasetIndexChecksumTest, MatchesKnownCrc32cVector) {
    const char* value = "123456789";
    EXPECT_EQ(0xE3069283U, dataset_index_crc32c(value, 9));
}

TEST(DatasetIndexCrossLanguageTest, OpensExternalIndexWhenConfigured) {
    const char* path = std::getenv("TSFILE_DATASET_INDEX_TEST_PATH");
    if (path == nullptr || path[0] == '\0') {
        GTEST_SKIP() << "TSFILE_DATASET_INDEX_TEST_PATH is not set";
    }

    MappedDatasetIndex index;
    ASSERT_EQ(DatasetIndexStatus::OK, index.open(path))
        << index.error_message();
    DatasetIndexSectionView files;
    DatasetIndexSectionView series;
    ASSERT_EQ(DatasetIndexStatus::OK,
              index.section(DatasetIndexSectionType::TSFILE_RECORD, files));
    ASSERT_EQ(DatasetIndexStatus::OK,
              index.section(DatasetIndexSectionType::LOGICAL_SERIES, series));
    EXPECT_GT(files.count, 0U);
    EXPECT_GT(series.count, 0U);
}

}  // namespace
}  // namespace dataset
}  // namespace storage
