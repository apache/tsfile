/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/memory_random_access_read_file.h"
#include "file/memory_random_access_write_file.h"
#include "file/restorable_tsfile_io_writer.h"
#include "file/write_file.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/result_set.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;

namespace {

const int64_t kTimestampBase = 1000000;

TableSchema make_table_schema() {
    std::vector<ColumnSchema> columns;
    columns.emplace_back("device", TSDataType::STRING, ColumnCategory::TAG);
    columns.emplace_back("value", TSDataType::INT64, ColumnCategory::FIELD);
    return TableSchema("memory_sink_table", columns);
}

Tablet make_tablet(int row_count) {
    std::vector<std::string> names = {"device", "value"};
    std::vector<TSDataType> types = {TSDataType::STRING, TSDataType::INT64};
    Tablet tablet(names, types, static_cast<uint32_t>(row_count));
    for (int i = 0; i < row_count; ++i) {
        tablet.add_timestamp(i, kTimestampBase + i);
        tablet.add_value(i, "device", "d0");
        tablet.add_value(i, "value", static_cast<int64_t>(i) * 10);
    }
    return tablet;
}

std::unique_ptr<RandomAccessReadFile> as_read_file(
    MemoryRandomAccessWriteFile& sink) {
    return std::unique_ptr<RandomAccessReadFile>(new MemoryRandomAccessReadFile(
        sink.release_data(), "memory://written.tsfile"));
}

}  // namespace

class WriteSinkEndToEndTest : public ::testing::Test {
   protected:
    void SetUp() override { libtsfile_init(); }
    void TearDown() override { libtsfile_destroy(); }
};

TEST_F(WriteSinkEndToEndTest, TableModelRoundTrip) {
    const int kRows = 200;
    TableSchema schema = make_table_schema();
    Tablet tablet = make_tablet(kRows);

    MemoryRandomAccessWriteFile sink("memory://table.tsfile");
    TsFileTableWriter writer(&sink, &schema);
    ASSERT_EQ(writer.get_error(), E_OK);
    ASSERT_EQ(writer.write_table(tablet), E_OK);
    ASSERT_EQ(writer.flush(), E_OK);
    ASSERT_EQ(writer.close(), E_OK);
    EXPECT_FALSE(sink.is_opened());
    EXPECT_GT(sink.size(), 0);

    TsFileReader reader;
    ASSERT_EQ(reader.open(as_read_file(sink)), E_OK);
    ResultSet* result = nullptr;
    ASSERT_EQ(
        reader.query("memory_sink_table", {"value"}, 0, INT64_MAX, result),
        E_OK);
    ASSERT_NE(result, nullptr);
    auto* table_result = static_cast<TableResultSet*>(result);
    bool has_next = false;
    int rows = 0;
    while (table_result->next(has_next) == E_OK && has_next) {
        EXPECT_EQ(table_result->get_value<int64_t>(1), kTimestampBase + rows);
        EXPECT_EQ(table_result->get_value<int64_t>(2),
                  static_cast<int64_t>(rows) * 10);
        ++rows;
    }
    EXPECT_EQ(rows, kRows);
    reader.destroy_query_data_set(result);
    reader.close();
}

TEST_F(WriteSinkEndToEndTest, TreeModelRoundTrip) {
    const int kRows = 100;
    const std::string device = "root.memory.d0";

    MemoryRandomAccessWriteFile sink;
    {
        TsFileWriter writer;
        ASSERT_EQ(writer.init(&sink), E_OK);
        ASSERT_EQ(
            writer.register_timeseries(
                device, MeasurementSchema("s0", INT32, PLAIN, UNCOMPRESSED)),
            E_OK);
        for (int i = 0; i < kRows; ++i) {
            TsRecord record(kTimestampBase + i, device);
            ASSERT_EQ(record.add_point("s0", static_cast<int32_t>(i)), E_OK);
            ASSERT_EQ(writer.write_record(record), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    EXPECT_FALSE(sink.is_opened());

    TsFileReader reader;
    ASSERT_EQ(reader.open(as_read_file(sink)), E_OK);
    std::vector<std::string> paths = {device + ".s0"};
    ResultSet* result = nullptr;
    ASSERT_EQ(reader.query(paths, 0, INT64_MAX, result), E_OK);
    ASSERT_NE(result, nullptr);
    auto* qds = static_cast<QDSWithoutTimeGenerator*>(result);
    bool has_next = false;
    int rows = 0;
    while (qds->next(has_next) == E_OK && has_next) {
        EXPECT_EQ(qds->get_value<int64_t>(1), kTimestampBase + rows);
        EXPECT_EQ(qds->get_value<int32_t>(2), rows);
        ++rows;
    }
    EXPECT_EQ(rows, kRows);
    reader.destroy_query_data_set(result);
    reader.close();
}

TEST_F(WriteSinkEndToEndTest, AlignedTreeModelRoundTrip) {
    const int kRows = 64;
    const std::string device = "root.memory.aligned";

    MemoryRandomAccessWriteFile sink;
    {
        TsFileWriter writer;
        ASSERT_EQ(writer.init(&sink), E_OK);
        std::vector<MeasurementSchema*> schemas;
        schemas.push_back(
            new MeasurementSchema("s0", INT64, PLAIN, UNCOMPRESSED));
        schemas.push_back(
            new MeasurementSchema("s1", DOUBLE, PLAIN, UNCOMPRESSED));
        ASSERT_EQ(writer.register_aligned_timeseries(device, schemas), E_OK);
        for (int i = 0; i < kRows; ++i) {
            TsRecord record(kTimestampBase + i, device);
            ASSERT_EQ(record.add_point("s0", static_cast<int64_t>(i)), E_OK);
            ASSERT_EQ(record.add_point("s1", static_cast<double>(i) + 0.5),
                      E_OK);
            ASSERT_EQ(writer.write_record_aligned(record), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    EXPECT_FALSE(sink.is_opened());

    TsFileReader reader;
    ASSERT_EQ(reader.open(as_read_file(sink)), E_OK);
    std::vector<std::string> paths = {device + ".s0", device + ".s1"};
    ResultSet* result = nullptr;
    ASSERT_EQ(reader.query(paths, 0, INT64_MAX, result), E_OK);
    ASSERT_NE(result, nullptr);
    auto* qds = static_cast<QDSWithoutTimeGenerator*>(result);
    bool has_next = false;
    int rows = 0;
    while (qds->next(has_next) == E_OK && has_next) {
        EXPECT_EQ(qds->get_value<int64_t>(1), kTimestampBase + rows);
        EXPECT_EQ(qds->get_value<int64_t>(2), static_cast<int64_t>(rows));
        EXPECT_DOUBLE_EQ(qds->get_value<double>(3),
                         static_cast<double>(rows) + 0.5);
        ++rows;
    }
    EXPECT_EQ(rows, kRows);
    reader.destroy_query_data_set(result);
    reader.close();
}

// The two backends must produce the same bytes for the same workload; this is
// what makes an in-memory sink a faithful stand-in for a local file.
TEST_F(WriteSinkEndToEndTest, MemorySinkMatchesLocalFileByteForByte) {
    const int kRows = 500;
    TableSchema schema = make_table_schema();
    Tablet memory_tablet = make_tablet(kRows);
    Tablet local_tablet = make_tablet(kRows);

    MemoryRandomAccessWriteFile sink;
    {
        TsFileTableWriter writer(&sink, &schema);
        ASSERT_EQ(writer.get_error(), E_OK);
        ASSERT_EQ(writer.write_table(memory_tablet), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }

    const std::string path = "write_sink_e2e_local.tsfile";
    std::remove(path.c_str());
    WriteFile local_file;
    const int flags = O_RDWR | O_CREAT | O_TRUNC;
    ASSERT_EQ(local_file.create(path, flags, 0666), E_OK);
    {
        TsFileTableWriter writer(&local_file, &schema);
        ASSERT_EQ(writer.get_error(), E_OK);
        ASSERT_EQ(writer.write_table(local_tablet), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }

    std::ifstream in(path, std::ios::binary);
    std::vector<char> local_bytes((std::istreambuf_iterator<char>(in)),
                                  std::istreambuf_iterator<char>());
    in.close();
    std::remove(path.c_str());

    ASSERT_GT(local_bytes.size(), 0u);
    ASSERT_EQ(local_bytes.size(), sink.data().size());
    EXPECT_TRUE(std::equal(local_bytes.begin(), local_bytes.end(),
                           sink.data().begin()));
}

// The memory sink cannot back the recovery path (self-check reads the
// existing bytes by path), so recovery must keep working on a local file and
// expose its sink through the interface.
TEST_F(WriteSinkEndToEndTest, RestorableWriterStillLocalOnly) {
    const std::string path = "write_sink_e2e_restorable.tsfile";
    std::remove(path.c_str());

    {
        RestorableTsFileIOWriter restorable;
        ASSERT_EQ(restorable.open(path), E_OK);
        ASSERT_TRUE(restorable.can_write());
        RandomAccessWriteFile* sink = restorable.get_write_file();
        ASSERT_NE(sink, nullptr);
        ASSERT_TRUE(sink->is_opened());
        EXPECT_EQ(sink->file_path(), path);

        TsFileWriter writer;
        ASSERT_EQ(writer.init(&restorable), E_OK);
        ASSERT_EQ(writer.register_timeseries(
                      "root.memory.restore",
                      MeasurementSchema("s0", INT64, PLAIN, UNCOMPRESSED)),
                  E_OK);
        TsRecord record(kTimestampBase, "root.memory.restore");
        ASSERT_EQ(record.add_point("s0", static_cast<int64_t>(7)), E_OK);
        ASSERT_EQ(writer.write_record(record), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
        restorable.close();
    }

    TsFileReader reader;
    ASSERT_EQ(reader.open(path), E_OK);
    reader.close();
    std::remove(path.c_str());
}
