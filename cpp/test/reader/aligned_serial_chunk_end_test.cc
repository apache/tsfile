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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "common/config/config.h"
#include "common/global.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "file/local_random_access_read_file.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/aligned_chunk_reader.h"
#include "writer/tsfile_table_writer.h"

namespace storage {
namespace {

using namespace common;

// The serial (non page-plan) path must stop at the end of a multi-page
// aligned chunk with E_NO_MORE_DATA instead of decoding an empty page header.
class AlignedSerialChunkEndTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        saved_page_rows_ = g_config_value_.page_writer_max_point_num_;
        g_config_value_.page_writer_max_point_num_ = 8;
        path_ =
            "aligned_serial_chunk_end_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
        std::remove(path_.c_str());
        const CompressionType compression = get_default_compressor();
        schema_ = std::make_shared<TableSchema>(
            "t", std::vector<ColumnSchema>{
                     {"v", INT64, compression, PLAIN, ColumnCategory::FIELD},
                     {"w", INT64, compression, PLAIN, ColumnCategory::FIELD}});
        WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(path_, flags, 0666), E_OK);
        TsFileTableWriter writer(&file, schema_.get());
        Tablet tablet("t", {"v", "w"}, {INT64, INT64},
                      {ColumnCategory::FIELD, ColumnCategory::FIELD}, 40);
        for (int row = 0; row < 40; ++row) {
            ASSERT_EQ(tablet.add_timestamp(row, row), E_OK);
            ASSERT_EQ(tablet.add_value(row, "v", static_cast<int64_t>(row)),
                      E_OK);
            ASSERT_EQ(tablet.add_value(row, "w", static_cast<int64_t>(row * 2)),
                      E_OK);
        }
        ASSERT_EQ(writer.write_table(tablet), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }

    void TearDown() override {
        std::remove(path_.c_str());
        g_config_value_.page_writer_max_point_num_ = saved_page_rows_;
        libtsfile_destroy();
    }

    std::string path_;
    std::shared_ptr<TableSchema> schema_;
    uint32_t saved_page_rows_ = 0;
};

TEST_F(AlignedSerialChunkEndTest, SerialReadStopsAtChunkEnd) {
    LocalRandomAccessReadFile read_file;
    ASSERT_EQ(read_file.open(path_), E_OK);
    TsFileIOReader io;
    ASSERT_EQ(io.init(&read_file), E_OK);
    TsFileMeta* metadata = nullptr;
    ASSERT_EQ(io.get_tsfile_meta(metadata), E_OK);
    PageArena arena;
    arena.init(512, MOD_TSFILE_READER);
    auto device =
        std::make_shared<StringArrayDeviceID>(std::vector<std::string>{"t"});
    std::vector<ITimeseriesIndex*> indexes(2);
    ASSERT_EQ(io.get_timeseries_indexes(device, {"v", "w"}, indexes, arena),
              E_OK);
    ChunkMeta* time_meta = nullptr;
    std::map<std::string, ChunkMeta*> value_metas;
    for (auto* index : indexes) {
        ASSERT_NE(index, nullptr);
        time_meta = index->get_time_chunk_meta_list()->begin().get();
        value_metas[index->get_measurement_name().to_std_string()] =
            index->get_value_chunk_meta_list()->begin().get();
    }
    ASSERT_NE(time_meta, nullptr);
    ASSERT_NE(value_metas["v"], nullptr);
    ASSERT_NE(value_metas["w"], nullptr);

    void* memory = mem_alloc(sizeof(AlignedChunkReader), MOD_CHUNK_READER);
    ASSERT_NE(memory, nullptr);
    auto* reader = new (memory) AlignedChunkReader();
    ASSERT_EQ(reader->init(&read_file, String("v", 1), INT64, nullptr), E_OK);
    // No field filter and no decode pool: this exercises the serial path.
    std::vector<ChunkMeta*> columns{value_metas["v"], value_metas["w"]};
    ASSERT_EQ(reader->load_by_aligned_meta_multi(time_meta, columns), E_OK);

    TupleDesc tuple;
    tuple.push_back(g_time_column_schema);
    tuple.push_back(schema_->get_column_schema("v"));
    tuple.push_back(schema_->get_column_schema("w"));
    TsBlock block(&tuple, 40);
    ASSERT_EQ(block.init(), E_OK);

    int rows = 0;
    bool saw_end = false;
    for (int attempt = 0; attempt < 50; ++attempt) {
        block.reset();
        const int ret = reader->get_next_page(&block, nullptr, arena);
        ASSERT_TRUE(ret == E_OK || ret == E_NO_MORE_DATA)
            << "attempt=" << attempt << " ret=" << ret;
        RowIterator it(&block);
        while (it.has_next()) {
            uint32_t size = 0;
            bool is_null = false;
            char* data = it.read(0, &size, &is_null);
            ASSERT_FALSE(is_null);
            ASSERT_EQ(size, sizeof(int64_t));
            int64_t ts = 0;
            std::memcpy(&ts, data, sizeof(ts));
            EXPECT_EQ(ts, rows);
            data = it.read(1, &size, &is_null);
            ASSERT_FALSE(is_null);
            int64_t v = 0;
            std::memcpy(&v, data, sizeof(v));
            EXPECT_EQ(v, rows);
            data = it.read(2, &size, &is_null);
            ASSERT_FALSE(is_null);
            int64_t w = 0;
            std::memcpy(&w, data, sizeof(w));
            EXPECT_EQ(w, rows * 2);
            ++rows;
            it.next();
        }
        if (ret == E_NO_MORE_DATA) {
            saw_end = true;
            break;
        }
        // Each call must make progress; E_OK with zero rows would loop forever.
        ASSERT_GT(block.get_row_count(), 0U) << "attempt=" << attempt;
    }
    EXPECT_TRUE(saw_end);
    EXPECT_EQ(rows, 40);

    reader->destroy();
    mem_free(reader);
}

}  // namespace
}  // namespace storage
