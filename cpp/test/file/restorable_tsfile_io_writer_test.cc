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

/**
 * Unit tests for RestorableTsFileIOWriter.
 * Covers: empty/invalid/complete file open, truncate recovery, continued write
 * with TsFileWriter/TsFileTreeWriter/TsFileTableWriter, and read-back verify.
 */

#include "file/restorable_tsfile_io_writer.h"

#include <gtest/gtest.h>

#include <fstream>
#include <random>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"
#include "writer/tsfile_writer.h"

namespace storage {
class ResultSet;
}
using namespace storage;
using namespace common;

// -----------------------------------------------------------------------------
// Helpers used by multiple tests (file flags, file size, corrupt tail)
// -----------------------------------------------------------------------------

static int GetWriteCreateFlags() {
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    return flags;
}

static int64_t GetFileSize(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    return static_cast<int64_t>(f.tellg());
}

/** Overwrite the last num_bytes of the file with zeros to simulate corruption.
 */
static void CorruptFileTail(const std::string& path, int num_bytes) {
    const int64_t full_size = GetFileSize(path);
    std::ofstream out(path, std::ios::binary | std::ios::in);
    out.seekp(full_size - static_cast<std::streamoff>(num_bytes));
    for (int i = 0; i < num_bytes; ++i) {
        out.put(0);
    }
    out.close();
}

/** Query tree reader and return row count; destroys query result. */
static int CountTreeReaderRows(
    TsFileTreeReader& reader, const std::vector<std::string>& measurement_ids) {
    auto device_ids = reader.get_all_device_ids();
    ResultSet* result = nullptr;
    int ret =
        reader.query(device_ids, measurement_ids, INT64_MIN, INT64_MAX, result);
    if (ret != E_OK || result == nullptr) {
        return -1;
    }
    int count = 0;
    for (auto it = result->iterator(); it.hasNext(); it.next()) {
        ++count;
    }
    reader.destroy_query_data_set(result);
    return count;
}

// -----------------------------------------------------------------------------
// Test fixture
// -----------------------------------------------------------------------------

class RestorableTsFileIOWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("restorable_tsfile_io_writer_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    int64_t GetCurrentFileSize() const { return GetFileSize(file_name_); }
    void CorruptCurrentFileTail(int num_bytes) {
        CorruptFileTail(file_name_, num_bytes);
    }

    std::string file_name_;

    static std::string generate_random_string(int length) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string s;
        s.reserve(static_cast<size_t>(length));
        for (int i = 0; i < length; ++i) {
            s += chars[static_cast<size_t>(dis(gen))];
        }
        return s;
    }
};

// -----------------------------------------------------------------------------
// Open behavior: empty file, bad magic, complete file, truncated file,
// header-only
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileIOWriterTest, OpenEmptyFile) {
    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), 0);
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenBadMagicFile) {
    std::ofstream f(file_name_);
    f.write("BadFile", 7);
    f.close();

    RestorableTsFileIOWriter writer;
    EXPECT_NE(writer.open(file_name_, true), E_OK);
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_INCOMPATIBLE);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenCompleteFile) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    record.timestamp_ = 2;
    tw.write_record(record);
    tw.flush();
    tw.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_FALSE(writer.can_write());
    EXPECT_FALSE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_COMPLETE);
    EXPECT_EQ(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenTruncatedFile) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, RLE, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    tw.flush();
    tw.close();

    const int64_t full_size = GetCurrentFileSize();
    CorruptCurrentFileTail(5);

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_GE(writer.get_truncated_size(),
              static_cast<int64_t>(MAGIC_STRING_TSFILE_LEN + 1));
    EXPECT_LE(writer.get_truncated_size(), full_size);
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenFileWithOnlyHeader) {
    int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    WriteFile wf;
    ASSERT_EQ(wf.create(file_name_, flags, 0666), E_OK);
    wf.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
    wf.write(&VERSION_NUM_BYTE, 1);
    wf.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), MAGIC_STRING_TSFILE_LEN + 1);
    writer.close();
}

// -----------------------------------------------------------------------------
// Recovery + continued write: TsFileWriter::init(rw) rebuilds schemas_ from
// recovered chunk group metas using actual device_id from file (not
// table_name), so both tree and table model get correct lookups.
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileIOWriterTest, TruncateRecoversAndProvidesWriter) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    record.timestamp_ = 2;
    tw.write_record(record);
    tw.flush();
    tw.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());
    ASSERT_NE(rw.get_tsfile_io_writer(), nullptr);
    ASSERT_NE(rw.get_write_file(), nullptr);
    EXPECT_EQ(rw.get_file_path(), file_name_);

    TsFileWriter tw2;
    ASSERT_EQ(tw2.init(&rw), E_OK);
    TsRecord record2(3, "d1");
    record2.add_point("s1", 3.0f);
    ASSERT_EQ(tw2.write_record(record2), E_OK);
    tw2.close();
    rw.close();
}

// Multi-segment device path: recovery must use actual device_id from file so
// that subsequent write to the same path finds the schema (no table_name key).
TEST_F(RestorableTsFileIOWriterTest,
       TreeModelMultiSegmentDeviceRecoverAndWrite) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "root.d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "root.d1");
    record.add_point("s1", 1.0f);
    ASSERT_EQ(tw.write_record(record), E_OK);
    tw.flush();
    tw.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileWriter tw2;
    ASSERT_EQ(tw2.init(&rw), E_OK);
    TsRecord record2(2, "root.d1");
    record2.add_point("s1", 2.0f);
    ASSERT_EQ(tw2.write_record(record2), E_OK);
    tw2.flush();
    tw2.close();
    rw.close();

    TsFileTreeReader reader;
    reader.open(file_name_);
    ASSERT_EQ(reader.get_all_device_ids().size(), 1u);
    ASSERT_EQ(CountTreeReaderRows(reader, {"s1"}), 2);
    reader.close();
}

// -----------------------------------------------------------------------------
// Recovery + continued write with TsFileTreeWriter, then read-back verify
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileIOWriterTest, MultiDeviceRecoverAndWriteWithTreeWriter) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries("d1", MeasurementSchema("s1", FLOAT));
    tw.register_timeseries("d1", MeasurementSchema("s2", INT32));
    tw.register_timeseries("d2", MeasurementSchema("s1", FLOAT));
    tw.register_timeseries("d2", MeasurementSchema("s2", DOUBLE));

    TsRecord r1(1, "d1");
    r1.add_point("s1", 1.0f);
    r1.add_point("s2", 10);
    ASSERT_EQ(tw.write_record(r1), E_OK);
    TsRecord r2(2, "d2");
    r2.add_point("s1", 2.0f);
    r2.add_point("s2", 20.0);
    ASSERT_EQ(tw.write_record(r2), E_OK);
    tw.flush();
    tw.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileTreeWriter tree_writer(&rw);
    TsRecord r3(3, "d1");
    r3.add_point("s1", 3.0f);
    r3.add_point("s2", 30);
    ASSERT_EQ(tree_writer.write(r3), E_OK);
    TsRecord r4(4, "d2");
    r4.add_point("s1", 4.0f);
    r4.add_point("s2", 40.0);
    ASSERT_EQ(tree_writer.write(r4), E_OK);
    tree_writer.flush();
    tree_writer.close();

    TsFileTreeReader reader;
    reader.open(file_name_);
    ASSERT_EQ(reader.get_all_device_ids().size(), 2u);
    ASSERT_EQ(CountTreeReaderRows(reader, {"s1", "s2"}), 4);
    reader.close();
}

// -----------------------------------------------------------------------------
// Tree model + Recovery + continued write with aligned timeseries, then
// read-back verify
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileIOWriterTest, AlignedTimeseriesRecoverAndWrite) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    std::vector<MeasurementSchema*> aligned_schemas;
    aligned_schemas.push_back(new MeasurementSchema("s1", FLOAT));
    aligned_schemas.push_back(new MeasurementSchema("s2", FLOAT));
    tw.register_aligned_timeseries("d1", aligned_schemas);

    TsRecord r1(1, "d1");
    r1.add_point("s1", 1.0f);
    r1.add_point("s2", 2.0f);
    ASSERT_EQ(tw.write_record_aligned(r1), E_OK);
    TsRecord r2(2, "d1");
    r2.add_point("s1", 3.0f);
    r2.add_point("s2", 4.0f);
    ASSERT_EQ(tw.write_record_aligned(r2), E_OK);
    tw.flush();
    tw.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileTreeWriter tw2(&rw);
    TsRecord r3(3, "d1");
    r3.add_point("s1", 5.0f);
    r3.add_point("s2", 6.0f);
    ASSERT_EQ(tw2.write(r3), E_OK);
    tw2.flush();
    tw2.close();

    TsFileTreeReader reader;
    reader.open(file_name_);
    ASSERT_EQ(reader.get_all_device_ids().size(), 1u);
    ASSERT_EQ(CountTreeReaderRows(reader, {"s1", "s2"}), 3);
    reader.close();
}

// -----------------------------------------------------------------------------
// Recovery + continued write with TsFileTableWriter (table model), then
// read-back
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileIOWriterTest, TableWriterRecoverAndWrite) {
    std::vector<MeasurementSchema*> measurement_schemas;
    measurement_schemas.push_back(new MeasurementSchema("device", STRING));
    measurement_schemas.push_back(new MeasurementSchema("value", DOUBLE));
    std::vector<ColumnCategory> column_categories = {ColumnCategory::TAG,
                                                     ColumnCategory::FIELD};
    TableSchema table_schema("test_table", measurement_schemas,
                             column_categories);

    WriteFile write_file;
    write_file.create(file_name_, GetWriteCreateFlags(), 0666);
    TsFileTableWriter table_writer(&write_file, &table_schema);
    const std::string table_name = "test_table";

    {
        Tablet tablet(table_schema.get_measurement_names(),
                      table_schema.get_data_types(), 10);
        tablet.set_table_name(table_name);
        for (int i = 0; i < 10; i++) {
            tablet.add_timestamp(i, static_cast<int64_t>(i));
            tablet.add_value(i, "device", "device0");
            tablet.add_value(i, "value", i * 1.1);
        }
        ASSERT_EQ(table_writer.write_table(tablet), E_OK);
        ASSERT_EQ(table_writer.flush(), E_OK);
    }
    {
        Tablet tablet(table_schema.get_measurement_names(),
                      table_schema.get_data_types(), 10);
        tablet.set_table_name(table_name);
        for (int i = 0; i < 10; i++) {
            tablet.add_timestamp(i, static_cast<int64_t>(i + 10));
            tablet.add_value(i, "device", "device1");
            tablet.add_value(i, "value", i * 1.1);
        }
        ASSERT_EQ(table_writer.write_table(tablet), E_OK);
        ASSERT_EQ(table_writer.flush(), E_OK);
    }

    table_writer.close();
    write_file.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileTableWriter table_writer2(&rw);
    std::vector<std::string> value_col = {"__level1", "value"};
    std::vector<TSDataType> value_types = {STRING, DOUBLE};
    {
        Tablet tablet2(value_col, value_types, 10);
        tablet2.set_table_name(table_name);
        for (int i = 0; i < 10; i++) {
            tablet2.add_timestamp(i, static_cast<int64_t>(i + 20));
            tablet2.add_value(i, "__level1", "device0");
            tablet2.add_value(i, "value", (i + 10) * 1.1);
        }
        ASSERT_EQ(table_writer2.write_table(tablet2), E_OK);
        table_writer2.flush();
    }
    {
        Tablet tablet2(value_col, value_types, 10);
        tablet2.set_table_name(table_name);
        for (int i = 0; i < 10; i++) {
            tablet2.add_timestamp(i, static_cast<int64_t>(i + 30));
            tablet2.add_value(i, "__level1", "device1");
            tablet2.add_value(i, "value", (i + 10) * 1.1);
        }
        ASSERT_EQ(table_writer2.write_table(tablet2), E_OK);
        table_writer2.flush();
    }

    table_writer2.close();

    TsFileReader table_reader;
    ASSERT_EQ(table_reader.open(file_name_), E_OK);
    ResultSet* tmp_result_set = nullptr;
    table_reader.query("test_table", {"__level1", "value"}, 0, 10000,
                       tmp_result_set, nullptr);
    auto* table_result_set = static_cast<TableResultSet*>(tmp_result_set);
    bool has_next = false;
    int64_t row_num = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        (void)table_result_set->get_row_record();
        row_num++;
    }
    ASSERT_EQ(row_num, 40);
    table_reader.destroy_query_data_set(tmp_result_set);
    table_reader.close();
}
