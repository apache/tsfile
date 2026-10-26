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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/config/config.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/aligned_chunk_reader.h"
#include "reader/block/single_device_tsblock_reader.h"
#include "reader/filter/time_operator.h"
#include "writer/tsfile_table_writer.h"
#ifdef ENABLE_THREADS
#include "common/thread_pool.h"
#endif

namespace storage {
namespace {

class TempTsFile {
   public:
    explicit TempTsFile(const char* path) : path_(path) {
        std::remove(path_.c_str());
        std::ofstream out(path_, std::ios::binary | std::ios::trunc);
        out.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
        out.put(VERSION_NUM_BYTE);
        out.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
    }

    ~TempTsFile() { std::remove(path_.c_str()); }

    const std::string& path() const { return path_; }

   private:
    std::string path_;
};

AlignedChunkReader* allocate_reader(RandomAccessReadFile* read_file) {
    void* memory =
        common::mem_alloc(sizeof(AlignedChunkReader), common::MOD_CHUNK_READER);
    if (memory == nullptr) {
        return nullptr;
    }
    auto* reader = new (memory) AlignedChunkReader();
    if (reader->init(read_file, common::String("s", 1), common::INT64,
                     nullptr) != common::E_OK) {
        reader->destroy();
        common::mem_free(reader);
        return nullptr;
    }
    return reader;
}

void free_reader(AlignedChunkReader* reader) {
    reader->destroy();
    common::mem_free(reader);
}

TEST(ChunkReaderResourceTest, AlignedInitialShortReadReleasesBuffer) {
    TempTsFile temp_file("aligned_initial_short_read.tsfile");
    LocalRandomAccessReadFile read_file;
    ASSERT_EQ(read_file.open(temp_file.path()), common::E_OK);
    AlignedChunkReader* reader = allocate_reader(&read_file);
    ASSERT_NE(reader, nullptr);
    ChunkMeta time_meta;
    time_meta.offset_of_chunk_header_ = read_file.file_size();
    ChunkMeta value_meta;
    int64_t memory_before =
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER);

    EXPECT_EQ(reader->load_by_aligned_meta(&time_meta, &value_meta),
              common::E_TSFILE_CORRUPTED);
    EXPECT_EQ(
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER),
        memory_before);

    free_reader(reader);
}

TEST(ChunkReaderResourceTest, MultiAlignedInitialShortReadReleasesBuffer) {
    TempTsFile temp_file("multi_aligned_initial_short_read.tsfile");
    LocalRandomAccessReadFile read_file;
    ASSERT_EQ(read_file.open(temp_file.path()), common::E_OK);
    AlignedChunkReader* reader = allocate_reader(&read_file);
    ASSERT_NE(reader, nullptr);
    ChunkMeta time_meta;
    time_meta.offset_of_chunk_header_ = read_file.file_size();
    ChunkMeta value_meta;
    std::vector<ChunkMeta*> value_metas{&value_meta};
    int64_t memory_before =
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER);

    EXPECT_EQ(reader->load_by_aligned_meta_multi(&time_meta, value_metas),
              common::E_TSFILE_CORRUPTED);
    EXPECT_EQ(
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER),
        memory_before);

    free_reader(reader);
}

// A one-shot failure lets a retry reach healthy storage. The reader must still
// return the original hard error rather than reuse partial decoded state.
class PageFailureReadFile : public LocalRandomAccessReadFile {
   public:
    int64_t target = -1;
    uint32_t corrupt_row_count = 0;
    bool corrupt = false;
    std::atomic<bool> fired{false};
    std::atomic<int> reads{0};
    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        ++reads;
        if (offset == target && !fired.exchange(true) && !corrupt) {
            read_size = 0;
            return common::E_FILE_READ_ERR;
        }
        int ret =
            LocalRandomAccessReadFile::read(offset, buffer, size, read_size);
        if (ret == common::E_OK && corrupt && offset == target &&
            read_size >= 4) {
            for (unsigned i = 0; i < 4; ++i)
                buffer[i] =
                    static_cast<char>(corrupt_row_count >> (24 - i * 8));
        }
        return ret;
    }
};

class AlignedReaderFailureTest : public ::testing::Test {
   protected:
    void SetUp() override {
        using namespace common;
        libtsfile_init();
        saved_ = g_config_value_;
        g_config_value_.page_writer_max_point_num_ = 8;
        schema_ = std::make_shared<TableSchema>(
            "t", std::vector<ColumnSchema>{
                     {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
                     {"w", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
                     {"s", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
                     {"missing", INT64, UNCOMPRESSED, PLAIN,
                      ColumnCategory::FIELD}});
        // CTest runs the cases in separate processes sharing one directory.
        // Each case needs its own file, including concurrent/repeated runs.
        path_ =
            "aligned_reader_failure_" +
            std::string(::testing::UnitTest::GetInstance()
                            ->current_test_info()
                            ->name()) +
            "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
        WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(path_, flags, 0666), E_OK);
        TsFileTableWriter writer(&file, schema_.get());
        Tablet tablet("t", {"v", "w", "s"}, {INT64, INT64, STRING},
                      {ColumnCategory::FIELD, ColumnCategory::FIELD,
                       ColumnCategory::FIELD},
                      40);
        for (int r = 0; r < 40; ++r) {
            ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
            ASSERT_EQ(tablet.add_value(r, "v", int64_t(r)), E_OK);
            ASSERT_EQ(tablet.add_value(r, "w", int64_t(r * 2)), E_OK);
            ASSERT_EQ(tablet.add_value(r, "s", "value"), E_OK);
        }
        ASSERT_EQ(writer.write_table(tablet), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }
    void TearDown() override {
        std::remove(path_.c_str());
        common::g_config_value_ = saved_;
        libtsfile_destroy();
    }
    int64_t payload_offset(PageFailureReadFile& file, ChunkMeta* meta,
                           int page_number = 0) {
        std::vector<char> bytes(1024);
        int32_t actual = 0;
        EXPECT_EQ(file.read(meta->offset_of_chunk_header_, bytes.data(),
                            bytes.size(), actual),
                  common::E_OK);
        common::ByteStream in;
        in.wrap_from(bytes.data(), actual);
        ChunkHeader chunk;
        EXPECT_EQ(chunk.deserialize_from(in), common::E_OK);
        for (int p = 0; p <= page_number; ++p) {
            PageHeader page;
            EXPECT_EQ(
                page.deserialize_from(
                    in,
                    (chunk.chunk_type_ & ONLY_ONE_PAGE_CHUNK_HEADER_MARKER) !=
                        ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
                    chunk.data_type_),
                common::E_OK);
            if (p == page_number)
                return meta->offset_of_chunk_header_ + in.read_pos();
            in.wrapped_buf_advance_read_pos(page.compressed_size_);
        }
        return -1;
    }
    void check_failure(bool parallel, const std::string& target, bool corrupt,
                       uint32_t data_num = 0, bool time_boundary = false,
                       int page_number = 0) {
        using namespace common;
        PageFailureReadFile file;
        ASSERT_EQ(file.open(path_), E_OK);
        TsFileIOReader io;
        ASSERT_EQ(io.init(&file), E_OK);
        TsFileMeta* metadata = nullptr;
        ASSERT_EQ(io.get_tsfile_meta(metadata), E_OK);
        PageArena arena;
        arena.init(512, MOD_TSFILE_READER);
        auto device = std::make_shared<StringArrayDeviceID>(
            std::vector<std::string>{"t"});
        std::vector<ITimeseriesIndex*> indexes(3);
        ASSERT_EQ(
            io.get_timeseries_indexes(device, {"v", "w", "s"}, indexes, arena),
            E_OK);
        ChunkMeta* time = nullptr;
        std::map<std::string, ChunkMeta*> values;
        for (auto* index : indexes) {
            ASSERT_NE(index, nullptr);
            time = index->get_time_chunk_meta_list()->begin().get();
            values[index->get_measurement_name().to_std_string()] =
                index->get_value_chunk_meta_list()->begin().get();
        }
        const auto target_offset = payload_offset(
            file, target == "time" ? time : values.at(target), page_number);
        auto filter =
            AlignedFieldFilter::compare("v", AlignedFieldFilter::CompareOp::GE,
                                        AlignedFieldFilter::Literal::int64(3));
        auto* reader = allocate_reader(&file);
        ASSERT_NE(reader, nullptr);
        // The production reader uses placement new and explicit destroy().
        std::unique_ptr<AlignedChunkReader, decltype(&free_reader)> owned(
            reader, free_reader);
        reader->set_field_filter(filter.get());
#ifdef ENABLE_THREADS
        // This pool lives in the caller executable. Decoder ownership must
        // also work when its inline worker TLS is distinct from the DLL's.
        common::ThreadPool pool(2);
        if (parallel) reader->set_decode_pool(&pool);
#else
        (void)parallel;
#endif
        const std::vector<ChunkMeta*> columns{values.at("v"), values.at("w"),
                                              values.at("s")};
        ASSERT_EQ(reader->load_by_aligned_meta_multi(time, columns), E_OK);
        TupleDesc tuple;
        tuple.push_back(g_time_column_schema);
        for (const auto& name : {"v", "w", "s"})
            tuple.push_back(schema_->get_column_schema(name));
        TsBlock block(&tuple, 2);
        ASSERT_EQ(block.init(), E_OK);
        TimeBetween range(6, 35, false);
        Filter* time_filter = time_boundary ? &range : nullptr;
        file.target = target_offset;
        file.corrupt = corrupt;
        file.corrupt_row_count = data_num;
        const int expected = corrupt ? E_TSFILE_CORRUPTED : E_FILE_READ_ERR;
        bool saw_error = false;
        int prefix_rows = 0;
        for (int attempt = 0; attempt < 50; ++attempt) {
            block.reset();
            const int ret = reader->get_next_page(&block, time_filter, arena);
            if (ret == expected) {
                saw_error = true;
                break;
            }
            ASSERT_EQ(ret, E_OK);
            prefix_rows += block.get_row_count();
        }
        ASSERT_TRUE(saw_error);
        ASSERT_TRUE(file.fired);
        if (page_number > 0) EXPECT_GT(prefix_rows, 0);
        const int reads_after_error = file.reads;
        file.target = -1;
        for (int retry = 0; retry < 3; ++retry) {
            block.reset();
            EXPECT_EQ(reader->get_next_page(&block, time_filter, arena),
                      expected);
            EXPECT_EQ(block.get_row_count(), 0U);
            EXPECT_EQ(file.reads, reads_after_error);
        }
        int offset = 0, limit = -1;
        EXPECT_EQ(reader->get_next_page(&block, time_filter, arena, INT64_MIN,
                                        offset, limit),
                  expected);
        EXPECT_EQ(file.reads, reads_after_error);
        // Reset is the recovery boundary. The same reader can load healthy
        // data again, including continuation through small output blocks.
        reader->reset();
        ASSERT_EQ(reader->load_by_aligned_meta_multi(time, columns), E_OK);
        int count = 0;
        for (int attempt = 0; attempt < 50; ++attempt) {
            block.reset();
            const int ret = reader->get_next_page(&block, nullptr, arena);
            ASSERT_TRUE(ret == E_OK || ret == E_NO_MORE_DATA) << ret;
            RowIterator rows(&block);
            while (rows.has_next()) {
                const int64_t expected_row = count + 3;
                for (uint32_t c = 0; c < 3; ++c) {
                    uint32_t size = 0;
                    bool is_null = false;
                    char* data = rows.read(c, &size, &is_null);
                    ASSERT_FALSE(is_null);
                    ASSERT_EQ(size, sizeof(int64_t));
                    int64_t value = 0;
                    std::memcpy(&value, data, sizeof(value));
                    EXPECT_EQ(value, c == 2 ? expected_row * 2 : expected_row);
                }
                uint32_t size = 0;
                bool is_null = false;
                char* data = rows.read(3, &size, &is_null);
                ASSERT_FALSE(is_null);
                EXPECT_EQ(std::string(data, size), "value");
                ++count;
                rows.next();
            }
            if (ret == E_NO_MORE_DATA) break;
        }
        EXPECT_EQ(count, 37);
    }
    common::ConfigValue saved_;
    std::string path_;
    std::shared_ptr<TableSchema> schema_;
};

TEST_F(AlignedReaderFailureTest, HardErrorsStayTerminalUntilReset) {
    for (bool parallel : {false, true}) {
        for (const std::string target : {"time", "v", "w", "s"}) {
            SCOPED_TRACE(::testing::Message() << parallel << ":" << target);
            check_failure(parallel, target, false);
        }
        // Fail while building a boundary-page plan, before decoding starts.
        check_failure(parallel, "time", false, 0, true);
        // Output-only failures on a later page happen after rows have already
        // been returned, in both eager and lazy decoding.
        check_failure(parallel, "w", false, 0, false, 1);
        check_failure(parallel, "s", false, 0, false, 1);
    }
}

TEST_F(AlignedReaderFailureTest, RejectsValuePageRowCountMismatch) {
    for (bool parallel : {false, true}) {
        for (const std::string target : {"v", "w", "s"}) {
            for (uint32_t count : {0U, 1U, 7U, UINT32_MAX}) {
                SCOPED_TRACE(::testing::Message()
                             << parallel << ":" << target << ":" << count);
                check_failure(parallel, target, true, count);
            }
        }
        // A clipped time range exercises nonzero prefix counting.
        check_failure(parallel, "v", true, 1, true);
    }
}

TEST_F(AlignedReaderFailureTest, CloseIsSafeBeforeAndAfterExhaustion) {
    using namespace common;
    for (bool exhaust : {false, true}) {
        for (bool time_only : {false, true}) {
            LocalRandomAccessReadFile file;
            ASSERT_EQ(file.open(path_), E_OK);
            TsFileIOReader io;
            ASSERT_EQ(io.init(&file), E_OK);
            TsFileMeta* metadata = nullptr;
            ASSERT_EQ(io.get_tsfile_meta(metadata), E_OK);
            auto mapping = std::make_shared<ColumnMapping>();
            const std::vector<std::string> names =
                time_only ? std::vector<std::string>{"missing"}
                          : std::vector<std::string>{"v", "w", "v"};
            for (size_t c = 0; c < names.size(); ++c)
                ASSERT_EQ(mapping->add(names[c], c, *schema_), E_OK);
            auto device = std::make_shared<StringArrayDeviceID>(
                std::vector<std::string>{"t"});
            DeviceQueryTask task(device, names, mapping, nullptr, schema_);
            SingleDeviceTsBlockReader reader(&task, 7, nullptr, &io, nullptr,
                                             nullptr);
            ASSERT_EQ(reader.init(&task, 7, nullptr, nullptr), E_OK);
            if (exhaust) {
                int rows = 0;
                for (int n = 0; n < 50; ++n) {
                    TsBlock* block = nullptr;
                    const int ret = reader.next(block);
                    if (ret == E_NO_MORE_DATA) break;
                    ASSERT_EQ(ret, E_OK);
                    ASSERT_NE(block, nullptr);
                    rows += block->get_row_count();
                }
                EXPECT_EQ(rows, 40);
            }
            reader.close();
            reader.close();
            // The destructor closes a third time.
        }
    }
}

TEST_F(AlignedReaderFailureTest, PartiallyResolvedMultiScanReleasesOwnership) {
    using namespace common;
    TsFileIOReader io;
    ASSERT_EQ(io.init(path_), E_OK);
    PageArena arena;
    arena.init(512, MOD_TSFILE_READER);
    auto device =
        std::make_shared<StringArrayDeviceID>(std::vector<std::string>{"t"});
    // Under LSan, each unsuccessful allocation must release the vector built
    // for the first two valid names, as well as the SSI and its arena.
    for (int attempt = 0; attempt < 32; ++attempt) {
        TsFileSeriesScanIterator* ssi = nullptr;
        EXPECT_EQ(io.alloc_multi_ssi(device, {"v", "w", "missing"}, ssi, arena),
                  E_NOT_EXIST);
        EXPECT_EQ(ssi, nullptr);
    }
    TsFileSeriesScanIterator* ssi = nullptr;
    ASSERT_EQ(io.alloc_multi_ssi(device, {"v", "w", "s"}, ssi, arena), E_OK);
    io.revert_ssi(ssi);
}

}  // namespace
}  // namespace storage
