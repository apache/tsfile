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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/config/config.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "file/local_random_access_read_file.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/aligned_chunk_reader.h"
#include "reader/filter/aligned_field_filter.h"
#include "reader/filter/time_operator.h"
#include "writer/tsfile_table_writer.h"
#ifdef ENABLE_THREADS
#include "common/thread_pool.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
#endif

using namespace common;

// A multi-column reader uses the page-plan path when a decode pool is set
// (what a parallel read does) or when an offset / FIELD filter is present.
// The no-filter bulk-copy and direct-decode tests need the pool, so they are
// skipped in a build without threads; the FIELD filter test reaches the plan
// either way.
#ifdef ENABLE_THREADS
#define SKIP_WITHOUT_THREADS() ((void)0)
#else
#define SKIP_WITHOUT_THREADS() \
    GTEST_SKIP() << "page-plan decode needs a decode pool (ENABLE_THREADS)"
#endif

namespace storage {
namespace {

// 8 rows per page and 40 rows per chunk: five pages, so a time window can start
// and end inside a page and still leave whole pages in between.
constexpr int kPagePoints = 8;
constexpr int kRowCount = 40;
constexpr int64_t kBaseTime = 1000;

struct Row {
    int64_t time = 0;
    int64_t v = 0;
    int64_t w = 0;
    std::string s;
};

class AlignedBulkCopyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_config_ = common::g_config_value_;
        common::g_config_value_.page_writer_max_point_num_ = kPagePoints;
        schema_ = std::make_shared<TableSchema>(
            "t",
            std::vector<ColumnSchema>{
                {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
                {"w", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
                {"s", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}});
        // Cases of parallel runs (ctest -j, repeated runs) share a working
        // directory, so the name carries the case and a wall-clock suffix.
        path_ =
            std::string("aligned_bulk_copy_") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            "_" + std::to_string([] {
#ifdef _WIN32
                return static_cast<uint64_t>(_getpid());
#else
                return static_cast<uint64_t>(getpid());
#endif
            }()) +
            "_" + std::to_string([] {
                static std::atomic<uint64_t> counter{0};
                return counter.fetch_add(1);
            }()) +
            ".tsfile";
        std::remove(path_.c_str());
        {
            WriteFile file;
            ASSERT_EQ(file.create(path_, O_WRONLY | O_CREAT | O_TRUNC, 0666),
                      E_OK);
            TsFileTableWriter writer(&file, schema_.get());
            Tablet tablet("t", {"v", "w", "s"}, {INT64, INT64, STRING},
                          {ColumnCategory::FIELD, ColumnCategory::FIELD,
                           ColumnCategory::FIELD},
                          kRowCount);
            for (int r = 0; r < kRowCount; r++) {
                ASSERT_EQ(tablet.add_timestamp(r, kBaseTime + r), E_OK);
                ASSERT_EQ(tablet.add_value(r, "v", static_cast<int64_t>(r)),
                          E_OK);
                ASSERT_EQ(tablet.add_value(r, "w", static_cast<int64_t>(r) * 2),
                          E_OK);
                ASSERT_EQ(tablet.add_value(r, "s", "s" + std::to_string(r)),
                          E_OK);
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
            ASSERT_EQ(writer.close(), E_OK);
        }

        ASSERT_EQ(read_file_.open(path_), E_OK);
        ASSERT_EQ(io_.init(&read_file_), E_OK);
        TsFileMeta* tsfile_meta = nullptr;
        ASSERT_EQ(io_.get_tsfile_meta(tsfile_meta), E_OK);
        arena_.init(512, MOD_TSFILE_READER);
        auto device = std::make_shared<StringArrayDeviceID>(
            std::vector<std::string>{"t"});
        std::vector<ITimeseriesIndex*> indexes(3, nullptr);
        ASSERT_EQ(io_.get_timeseries_indexes(device, {"v", "w", "s"}, indexes,
                                             arena_),
                  E_OK);
        for (ITimeseriesIndex* index : indexes) {
            ASSERT_NE(index, nullptr);
            // The aligned time column is shared by every measurement.
            ChunkMeta* time_meta =
                index->get_time_chunk_meta_list()->begin().get();
            if (time_meta_ == nullptr) {
                time_meta_ = time_meta;
            } else {
                EXPECT_EQ(time_meta_, time_meta)
                    << "measurements disagree on the shared time chunk";
            }
            value_metas_[index->get_measurement_name().to_std_string()] =
                index->get_value_chunk_meta_list()->begin().get();
        }
        ASSERT_EQ(value_metas_.size(), 3u);

        reader_ = allocate_reader();
        ASSERT_NE(reader_, nullptr);
#ifdef ENABLE_THREADS
        pool_.reset(new common::ThreadPool(2));
        reader_->set_decode_pool(pool_.get());
#endif
    }

    void TearDown() override {
        if (reader_ != nullptr) {
            reader_->destroy();
            common::mem_free(reader_);
            reader_ = nullptr;
        }
        std::remove(path_.c_str());
        common::g_config_value_ = saved_config_;
        libtsfile_destroy();
    }

    AlignedChunkReader* allocate_reader() {
        void* memory = common::mem_alloc(sizeof(AlignedChunkReader),
                                         common::MOD_CHUNK_READER);
        if (memory == nullptr) {
            return nullptr;
        }
        // The production reader is placement-new'ed and destroyed explicitly.
        auto* reader = new (memory) AlignedChunkReader();
        if (reader->init(&read_file_, common::String("v", 1), common::INT64,
                         nullptr) != E_OK) {
            reader->destroy();
            common::mem_free(reader);
            return nullptr;
        }
        return reader;
    }

    // Reads the chunk through AlignedChunkReader and returns every row in
    // output order. `max_direct_pages` reports the largest direct-decode prefix
    // the reader decided on, `bulk_pages` the number of pages copied by the
    // bulk path.
    // Reads the whole chunk once; the optional out parameters are written,
    // not accumulated into.
    std::vector<Row> read_all(const std::vector<std::string>& columns,
                              Filter* time_filter, uint32_t block_rows,
                              size_t* max_direct_pages = nullptr,
                              size_t* bulk_pages = nullptr) {
        if (max_direct_pages != nullptr) *max_direct_pages = 0;
        if (bulk_pages != nullptr) *bulk_pages = 0;
        std::vector<ChunkMeta*> value_metas;
        for (const auto& name : columns) {
            value_metas.push_back(value_metas_[name]);
        }
        if (reader_->load_by_aligned_meta_multi(time_meta_, value_metas) !=
            E_OK) {
            ADD_FAILURE() << "load_by_aligned_meta_multi failed";
            return {};
        }

        TupleDesc tuple;
        tuple.push_back(common::g_time_column_schema);
        for (const auto& name : columns) {
            tuple.push_back(schema_->get_column_schema(name));
        }
        common::TsBlock block(&tuple, block_rows);
        if (block.init() != common::E_OK) {
            ADD_FAILURE() << "TsBlock init failed";
            return {};
        }

        std::vector<Row> rows;
        size_t direct_pages = 0;
        size_t last_bulk_seen = 0;
        for (int call = 0; call < 64; call++) {
            block.reset();
            const int ret = reader_->get_next_page(&block, time_filter, arena_);
            EXPECT_TRUE(ret == E_OK || ret == E_NO_MORE_DATA)
                << "get_next_page returned " << ret;
            direct_pages =
                std::max(direct_pages,
                         static_cast<size_t>(reader_->TEST_direct_page_end()));
            if (bulk_pages != nullptr) {
                // The reader's counter is cumulative.
                const size_t seen = reader_->TEST_bulk_page_count();
                *bulk_pages += seen - last_bulk_seen;
                last_bulk_seen = seen;
            }
            common::RowIterator iter(&block);
            while (iter.has_next()) {
                Row row;
                uint32_t len = 0;
                bool is_null = false;
                std::memcpy(&row.time, iter.read(0, &len, &is_null),
                            sizeof(row.time));
                std::memcpy(&row.v, iter.read(1, &len, &is_null),
                            sizeof(row.v));
                if (columns.size() > 1) {
                    std::memcpy(&row.w, iter.read(2, &len, &is_null),
                                sizeof(row.w));
                }
                if (columns.size() > 2) {
                    const char* s = iter.read(3, &len, &is_null);
                    if (!is_null) {
                        row.s.assign(s, len);
                    }
                }
                rows.push_back(std::move(row));
                iter.next();
            }
            if (ret == E_NO_MORE_DATA) {
                break;
            }
        }
        if (max_direct_pages != nullptr) {
            *max_direct_pages = direct_pages;
        }
        return rows;
    }

    // Rows of the source table whose timestamp satisfies @range.
    std::vector<Row> expected(TimeBetween& range) {
        std::vector<Row> rows;
        for (int r = 0; r < kRowCount; r++) {
            if (!range.satisfy_start_end_time(kBaseTime + r, kBaseTime + r)) {
                continue;
            }
            Row row;
            row.time = kBaseTime + r;
            row.v = r;
            row.w = static_cast<int64_t>(r) * 2;
            row.s = "s" + std::to_string(r);
            rows.push_back(row);
        }
        return rows;
    }

    static void expect_rows_equal(const std::vector<Row>& got,
                                  const std::vector<Row>& want) {
        ASSERT_EQ(got.size(), want.size());
        for (size_t i = 0; i < got.size(); i++) {
            EXPECT_EQ(got[i].time, want[i].time) << i;
            EXPECT_EQ(got[i].v, want[i].v) << i;
            EXPECT_EQ(got[i].w, want[i].w) << i;
        }
    }

    common::ConfigValue saved_config_;
#ifdef ENABLE_THREADS
    std::unique_ptr<common::ThreadPool> pool_;
#endif
    std::shared_ptr<TableSchema> schema_;
    std::string path_;
    LocalRandomAccessReadFile read_file_;
    TsFileIOReader io_;
    PageArena arena_;
    ChunkMeta* time_meta_ = nullptr;
    std::map<std::string, ChunkMeta*> value_metas_;
    AlignedChunkReader* reader_ = nullptr;
};

// A window whose endpoints fall inside pages leaves whole pages in the middle.
// Those are BOUNDARY pages restricted to [0, data_num), and a contiguous time
// filter proves every row in that range matches, so they are bulk-copied.
TEST_F(AlignedBulkCopyTest, ContiguousWindowBulkCopiesInnerPages) {
    SKIP_WITHOUT_THREADS();
    TimeBetween range(kBaseTime + 10, kBaseTime + 25, /*not_between=*/false);
    size_t bulk_pages = 0;
    const std::vector<Row> rows =
        read_all({"v", "w"}, &range, kRowCount, nullptr, &bulk_pages);
    expect_rows_equal(rows, expected(range));
    EXPECT_GT(bulk_pages, 0u);
}

// A window that starts and ends inside one page still leaves a contiguous run
// of matching rows, so the retained slice is bulk-copied even though the page
// is not fully covered.
TEST_F(AlignedBulkCopyTest, WindowInsideOnePageIsBulkCopied) {
    SKIP_WITHOUT_THREADS();
    TimeBetween range(kBaseTime + 9, kBaseTime + 11, /*not_between=*/false);
    size_t bulk_pages = 0;
    const std::vector<Row> rows =
        read_all({"v", "w"}, &range, kRowCount, nullptr, &bulk_pages);
    expect_rows_equal(rows, expected(range));
    EXPECT_EQ(bulk_pages, 1u);
}

// A discrete time predicate may leave holes inside a page's retained range, so
// boundary pages must keep re-checking every timestamp.
TEST_F(AlignedBulkCopyTest, DiscreteTimeFilterStaysOnSlowPath) {
    SKIP_WITHOUT_THREADS();
    TimeIn in({kBaseTime + 1, kBaseTime + 2, kBaseTime + 20}, /*not_in=*/false);
    size_t bulk_pages = 0;
    const std::vector<Row> rows =
        read_all({"v", "w"}, &in, kRowCount, nullptr, &bulk_pages);
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[0].time, kBaseTime + 1);
    EXPECT_EQ(rows[1].time, kBaseTime + 2);
    EXPECT_EQ(rows[2].time, kBaseTime + 20);
    EXPECT_EQ(bulk_pages, 0u);
}

// Without a filter every page is fully retained and the columns are
// fixed-length, so the whole plan can be decoded straight into the destination
// block's column buffers.
TEST_F(AlignedBulkCopyTest, NoFilterDecodesDirectlyIntoBlock) {
    SKIP_WITHOUT_THREADS();
    size_t direct_pages = 0;
    const std::vector<Row> rows =
        read_all({"v", "w"}, nullptr, kRowCount, &direct_pages);
    ASSERT_EQ(rows.size(), static_cast<size_t>(kRowCount));
    for (int r = 0; r < kRowCount; r++) {
        EXPECT_EQ(rows[r].time, kBaseTime + r);
        EXPECT_EQ(rows[r].v, r);
        EXPECT_EQ(rows[r].w, static_cast<int64_t>(r) * 2);
    }
    EXPECT_GT(direct_pages, 0u);
}

// Variable-length columns keep their per-page staging buffers: the destination
// rows of a page are not fixed-size, so the decoder cannot write in place.
TEST_F(AlignedBulkCopyTest, VariableLengthColumnSkipsDirectDecode) {
    SKIP_WITHOUT_THREADS();
    size_t direct_pages = 0;
    const std::vector<Row> rows =
        read_all({"v", "w", "s"}, nullptr, kRowCount, &direct_pages);
    ASSERT_EQ(rows.size(), static_cast<size_t>(kRowCount));
    for (int r = 0; r < kRowCount; r++) {
        EXPECT_EQ(rows[r].v, r);
        EXPECT_EQ(rows[r].s, "s" + std::to_string(r));
    }
    EXPECT_EQ(direct_pages, 0u);
}

// Rows resume across block swaps: only the pages that fit in the first block
// can be decoded in place, the rest are staged page by page.
TEST_F(AlignedBulkCopyTest, RowsSpanMultipleBlocks) {
    SKIP_WITHOUT_THREADS();
    const std::vector<Row> rows = read_all({"v", "w"}, nullptr, 16);
    ASSERT_EQ(rows.size(), static_cast<size_t>(kRowCount));
    for (int r = 0; r < kRowCount; r++) {
        EXPECT_EQ(rows[r].time, kBaseTime + r);
        EXPECT_EQ(rows[r].v, r);
    }
}

// A field predicate can still reject rows inside a page's retained range, so a
// partially matching page must stay on the row-by-row path. Page 1 (rows 8..15)
// is only half retained; if it were bulk-copied, rows 8..11 would leak out.
TEST_F(AlignedBulkCopyTest, FieldFilterKeepsPartialPageOnSlowPath) {
    auto field_filter =
        AlignedFieldFilter::compare("v", AlignedFieldFilter::CompareOp::GE,
                                    AlignedFieldFilter::Literal::int64(12));
    ASSERT_NE(field_filter, nullptr);
    reader_->set_field_filter(field_filter.get());
    const std::vector<Row> rows = read_all({"v", "w"}, nullptr, kRowCount);
    ASSERT_EQ(rows.size(), static_cast<size_t>(kRowCount - 12));
    for (int r = 12; r < kRowCount; r++) {
        EXPECT_EQ(rows[r - 12].time, kBaseTime + r);
        EXPECT_EQ(rows[r - 12].v, r);
    }
}

}  // namespace
}  // namespace storage
