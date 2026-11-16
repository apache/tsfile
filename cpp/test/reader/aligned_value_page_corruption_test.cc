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

// An aligned value page begins with a uint32 row count that sizes the
// not-null bitmap. These tests corrupt a real file's value-page payload in
// place (the page is
// written UNCOMPRESSED, so its payload is stored verbatim) and read it back
// through AlignedChunkReader.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <new>
#include <string>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "common/allocator/alloc_base.h"
#include "common/config/config.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/aligned_chunk_reader.h"
#include "writer/tsfile_table_writer.h"

namespace storage {
namespace {

constexpr int kRows = 1000;

class AlignedValuePageCorruptionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        using namespace common;
        libtsfile_init();
        saved_ = g_config_value_;
        // One page for the whole chunk, so the value page payload is a single
        // contiguous run in the file that can be patched in place.
        g_config_value_.page_writer_max_point_num_ = kRows;

        schema_ = std::make_shared<TableSchema>(
            "t", std::vector<ColumnSchema>{
                     {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}});
        path_ = TempName("valid");
        {
            WriteFile file;
            int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
            flags |= O_BINARY;
#endif
            ASSERT_EQ(file.create(path_, flags, 0666), E_OK);
            TsFileTableWriter writer(&file, schema_.get());
            Tablet tablet("t", {"v"}, {INT64}, {ColumnCategory::FIELD}, kRows);
            for (int r = 0; r < kRows; ++r) {
                ASSERT_EQ(tablet.add_timestamp(r, 1000 + r), E_OK);
                ASSERT_EQ(tablet.add_value(r, "v", static_cast<int64_t>(r)),
                          E_OK);
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
            ASSERT_EQ(writer.close(), E_OK);
        }

        LocalRandomAccessReadFile file;
        ASSERT_EQ(file.open(path_), E_OK);
        TsFileIOReader io;
        ASSERT_EQ(io.init(&file), E_OK);
        TsFileMeta* metadata = nullptr;
        ASSERT_EQ(io.get_tsfile_meta(metadata), E_OK);
        PageArena arena;
        arena.init(512, MOD_TSFILE_READER);
        auto device = std::make_shared<StringArrayDeviceID>(
            std::vector<std::string>{"t"});
        std::vector<ITimeseriesIndex*> indexes(1, nullptr);
        ASSERT_EQ(io.get_timeseries_indexes(device, {"v"}, indexes, arena),
                  E_OK);
        ASSERT_NE(indexes[0], nullptr);
        time_meta_ = indexes[0]->get_time_chunk_meta_list()->begin().get();
        value_meta_ = indexes[0]->get_value_chunk_meta_list()->begin().get();
        ASSERT_NE(time_meta_, nullptr);
        ASSERT_NE(value_meta_, nullptr);
        value_payload_offset_ = PayloadOffset(file, value_meta_);
        ASSERT_GT(value_payload_offset_, 0);
    }

    void TearDown() override {
        std::remove(path_.c_str());
        std::remove(corrupt_path_.c_str());
        common::g_config_value_ = saved_;
        libtsfile_destroy();
    }

    static std::string TempName(const char* tag) {
        static std::atomic<uint64_t> counter{0};
#ifdef _WIN32
        const auto process_id = static_cast<uint64_t>(_getpid());
#else
        const auto process_id = static_cast<uint64_t>(getpid());
#endif
        return "aligned_value_page_" + std::string(tag) + "_" +
               std::to_string(std::chrono::steady_clock::now()
                                  .time_since_epoch()
                                  .count()) +
               "_" + std::to_string(process_id) + "_" +
               std::to_string(counter.fetch_add(1)) + ".tsfile";
    }

    // Offset of the first value page's payload: just past the chunk header and
    // the page header. A one-page chunk omits the page statistics, so the
    // header is only the two var-int sizes.
    static int64_t PayloadOffset(RandomAccessReadFile& file, ChunkMeta* meta) {
        std::vector<char> bytes(4096);
        int32_t actual = 0;
        EXPECT_EQ(file.read(meta->offset_of_chunk_header_, bytes.data(),
                            static_cast<int32_t>(bytes.size()), actual),
                  common::E_OK);
        common::ByteStream in;
        in.wrap_from(bytes.data(), actual);
        ChunkHeader chunk;
        EXPECT_EQ(chunk.deserialize_from(in), common::E_OK);
        PageHeader page;
        EXPECT_EQ(page.deserialize_from(
                      in,
                      (chunk.chunk_type_ & ONLY_ONE_PAGE_CHUNK_HEADER_MARKER) !=
                          ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
                      chunk.data_type_),
                  common::E_OK);
        return meta->offset_of_chunk_header_ + in.read_pos();
    }

    // Copies the valid file with its value-page row count replaced by
    // @row_count (stored big-endian, as SerializationUtil::write_ui32 does).
    void WriteCorruptedFile(uint32_t row_count) {
        std::ifstream input(path_, std::ios::binary);
        ASSERT_TRUE(input.is_open());
        std::vector<char> bytes((std::istreambuf_iterator<char>(input)),
                                std::istreambuf_iterator<char>());
        ASSERT_GE(bytes.size(), static_cast<size_t>(value_payload_offset_) + 4);
        for (int i = 0; i < 4; ++i) {
            bytes[value_payload_offset_ + i] =
                static_cast<char>(row_count >> (24 - i * 8));
        }
        corrupt_path_ = TempName("corrupt");
        std::ofstream output(corrupt_path_, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output.is_open());
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        output.close();
    }

    // Reads the single-series ("v") chunk through AlignedChunkReader and
    // returns the first non-OK status, or E_NO_MORE_DATA after every row.
    int ReadAllRows(const std::string& path, int* rows_read) {
        using namespace common;
        LocalRandomAccessReadFile file;
        if (file.open(path) != E_OK) {
            return E_FILE_OPEN_ERR;
        }
        TsFileIOReader io;
        if (io.init(&file) != E_OK) {
            return -1;
        }
        TsFileMeta* metadata = nullptr;
        if (io.get_tsfile_meta(metadata) != E_OK) {
            return -2;
        }
        PageArena arena;
        arena.init(512, MOD_TSFILE_READER);
        auto device = std::make_shared<StringArrayDeviceID>(
            std::vector<std::string>{"t"});
        std::vector<ITimeseriesIndex*> indexes(1, nullptr);
        if (io.get_timeseries_indexes(device, {"v"}, indexes, arena) != E_OK ||
            indexes[0] == nullptr) {
            return -3;
        }
        ChunkMeta* time_meta =
            indexes[0]->get_time_chunk_meta_list()->begin().get();
        ChunkMeta* value_meta =
            indexes[0]->get_value_chunk_meta_list()->begin().get();

        void* memory =
            common::mem_alloc(sizeof(AlignedChunkReader), MOD_CHUNK_READER);
        if (memory == nullptr) {
            return E_OOM;
        }
        auto* reader = new (memory) AlignedChunkReader();
        struct ReaderGuard {
            AlignedChunkReader* reader;
            ~ReaderGuard() {
                reader->destroy();
                common::mem_free(reader);
            }
        } guard{reader};
        if (reader->init(&file, common::String("v", 1), INT64, nullptr) !=
            E_OK) {
            return E_OOM;
        }
        if (reader->load_by_aligned_meta(time_meta, value_meta) != E_OK) {
            return -4;
        }

        TupleDesc tuple;
        tuple.push_back(g_time_column_schema);
        tuple.push_back(schema_->get_column_schema("v"));
        TsBlock block(&tuple, 64);
        if (block.init() != E_OK) {
            return E_OOM;
        }
        int rows = 0;
        for (int call = 0; call < 1000; ++call) {
            block.reset();
            const int ret = reader->get_next_page(&block, nullptr, arena);
            rows += static_cast<int>(block.get_row_count());
            if (ret != E_OK) {
                if (rows_read != nullptr) *rows_read = rows;
                return ret == E_NO_MORE_DATA ? E_NO_MORE_DATA : ret;
            }
            // The chunk is exhausted once no page is left; like the series
            // scan iterator, stop here instead of asking for another page.
            if (!reader->has_more_data()) {
                if (rows_read != nullptr) *rows_read = rows;
                return E_NO_MORE_DATA;
            }
        }
        if (rows_read != nullptr) *rows_read = rows;
        return E_NO_MORE_DATA;
    }

    common::ConfigValue saved_;
    std::shared_ptr<TableSchema> schema_;
    std::string path_;
    std::string corrupt_path_;
    ChunkMeta* time_meta_ = nullptr;
    ChunkMeta* value_meta_ = nullptr;
    int64_t value_payload_offset_ = -1;
};

TEST_F(AlignedValuePageCorruptionTest, ValidFileReadsEveryRow) {
    int rows = 0;
    EXPECT_EQ(ReadAllRows(path_, &rows), common::E_NO_MORE_DATA);
    EXPECT_EQ(rows, kRows);
}

TEST_F(AlignedValuePageCorruptionTest, OversizedRowCountIsRejected) {
    // Two orders of magnitude more rows than the page can hold: the bitmap
    // alone (1 MiB / 8) does not fit in the 8129-byte page payload.
    WriteCorruptedFile(1u << 20);
    int rows = 0;
    EXPECT_EQ(ReadAllRows(corrupt_path_, &rows), common::E_TSFILE_CORRUPTED);
    EXPECT_EQ(rows, 0);
}

TEST_F(AlignedValuePageCorruptionTest, RowCountThatWrapsBitmapSizeIsRejected) {
    // UINT32_MAX + 7 wraps to 6 in 32-bit arithmetic, so the bitmap size used
    // to become zero and the page was silently read as all-null rows.
    WriteCorruptedFile(UINT32_MAX);
    int rows = 0;
    EXPECT_EQ(ReadAllRows(corrupt_path_, &rows), common::E_TSFILE_CORRUPTED);
    EXPECT_EQ(rows, 0);
}

TEST_F(AlignedValuePageCorruptionTest,
       CountReducedWithinBitmapPaddingIsRejected) {
    // 999 rows still needs a 125-byte bitmap, so a bitmap-capacity check
    // alone accepts all 1000 time rows; the declared count must bound rows.
    WriteCorruptedFile(999);
    int rows = 0;
    EXPECT_EQ(ReadAllRows(corrupt_path_, &rows), common::E_TSFILE_CORRUPTED);
    EXPECT_LT(rows, kRows);
}

}  // namespace
}  // namespace storage
