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
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <set>
#include <thread>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "common/arrow_batch.h"
#include "common/tablet.h"
#include "common/thread_pool.h"
#include "compress/uncompressed_compressor.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/arrow/scan_page.h"
#include "reader/filter/aligned_field_filter.h"
#include "reader/table_query_executor.h"
#include "writer/tsfile_table_writer.h"

namespace {
using namespace common;
using namespace storage;
TEST(ArrowBatchTest, ExportSharesBuffersAndOwnsData) {
    auto batch = std::make_shared<ArrowBatch>(
        std::vector<std::string>{"v", "flag", "text", "missing"},
        std::vector<TSDataType>{INT64, BOOLEAN, STRING, INVALID_DATATYPE}, 3);
    int64_t value = 42;
    ASSERT_EQ(batch->columns[0]->set_native(0, INT64,
                                            reinterpret_cast<char*>(&value), 8),
              E_OK);
    bool flag = true;
    ASSERT_EQ(batch->columns[1]->set_native(2, BOOLEAN,
                                            reinterpret_cast<char*>(&flag), 1),
              E_OK);
    ASSERT_EQ(batch->columns[2]->set_native(1, STRING, "abc", 3), E_OK);
    batch->row_count = 3;
    const char* original = batch->columns[0]->values();
    ArrowArray array{};
    ArrowSchema schema{};
    ASSERT_EQ(batch->export_to(&array, &schema), E_OK);
    EXPECT_EQ(array.children[0]->buffers[1], original);
    batch.reset();
    EXPECT_EQ(static_cast<const int64_t*>(array.children[0]->buffers[1])[0],
              42);
    EXPECT_EQ(array.children[0]->null_count, 2);
    EXPECT_EQ(static_cast<const uint8_t*>(array.children[1]->buffers[1])[0], 4);
    auto* offsets = static_cast<const int32_t*>(array.children[2]->buffers[1]);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 3);
    EXPECT_EQ(offsets[3], 3);
    EXPECT_STREQ(schema.children[3]->format, "n");
    EXPECT_EQ(array.children[3]->n_buffers, 0);
    EXPECT_EQ(array.children[3]->null_count, 3);
    array.release(&array);
    schema.release(&schema);
    EXPECT_EQ(array.release, nullptr);
    EXPECT_EQ(schema.release, nullptr);
}
class ArrowQueryScanTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_ = g_config_value_;
        g_config_value_.page_writer_max_point_num_ = 3;
        g_config_value_.time_compress_type_ = UNCOMPRESSED;
#ifdef _WIN32
        const auto process_id = _getpid();
#else
        const auto process_id = getpid();
#endif
        filename_ =
            std::string("arrow_query_scan_") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "_" + std::to_string(process_id) + ".tsfile";
#ifdef ENABLE_GZIP
        const CompressionType label_compression = GZIP;
#else
        const CompressionType label_compression = UNCOMPRESSED;
#endif
        const std::vector<ColumnSchema> columns{
            {"id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG},
            {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"p", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"label", STRING, label_compression, DICTIONARY,
             ColumnCategory::FIELD}};
        TableSchema schema("t", columns);
        WriteFile file;
        ASSERT_EQ(file.create(filename_, O_WRONLY | O_CREAT | O_TRUNC, 0666),
                  E_OK);
        TsFileTableWriter writer(&file, &schema);
        for (int d = 0; d < 2; ++d) {
            Tablet tablet("t", {"id", "v", "p", "label"},
                          {STRING, INT64, INT64, STRING},
                          {ColumnCategory::TAG, ColumnCategory::FIELD,
                           ColumnCategory::FIELD, ColumnCategory::FIELD},
                          7);
            for (int r = 0; r < 7; ++r) {
                ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
                ASSERT_EQ(tablet.add_value(r, "id", d == 0 ? "a" : "b"), E_OK);
                if (r != 3)
                    ASSERT_EQ(tablet.add_value(r, "v", int64_t(d * 100 + r)),
                              E_OK);
                ASSERT_EQ(tablet.add_value(r, "p", int64_t(r % 2)), E_OK);
                if (r % 3 != 1)
                    ASSERT_EQ(
                        tablet.add_value(r, "label", r % 3 == 0 ? "" : "value"),
                        E_OK);
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    void TearDown() override {
        std::remove(filename_.c_str());
        g_config_value_ = saved_;
        libtsfile_destroy();
    }
    std::string filename_;
    ConfigValue saved_;
};
TEST_F(ArrowQueryScanTest, ExactBatchesCrossDevicesAndRetainOwnership) {
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TableQueryExecutor executor(&file, 5);
    ResultSet* raw = nullptr;
    ASSERT_EQ(executor.query("t", {"id", "v"}, nullptr, nullptr, nullptr, raw),
              E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    std::vector<std::shared_ptr<ArrowBatch>> held;
    for (uint32_t expected : {5U, 5U, 4U}) {
        std::shared_ptr<ArrowBatch> batch;
        ASSERT_EQ(table->get_next_arrow_batch(batch), E_OK);
        ASSERT_EQ(batch->row_count, expected);
        held.push_back(batch);
    }
    std::shared_ptr<ArrowBatch> end;
    EXPECT_EQ(table->get_next_arrow_batch(end), E_NO_MORE_DATA);
    result.reset();
    file.close();
    char scratch[8];
    uint32_t size;
    int64_t value;
    std::memcpy(&value, held[1]->columns[2]->native_value(2, scratch, size), 8);
    EXPECT_EQ(value, 100);
    EXPECT_TRUE(held[0]->columns[2]->is_null(3));
    EXPECT_FALSE(held[0]->columns[2]->is_null(2));
}
TEST_F(ArrowQueryScanTest, DictionaryStringsPreserveEmptyValuesAndNulls) {
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TableQueryExecutor executor(&file, 4);
    ResultSet* raw = nullptr;
    ASSERT_EQ(executor.query("t", {"label"}, nullptr, nullptr, nullptr, raw),
              E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    unsigned rows = 0;
    std::shared_ptr<ArrowBatch> batch;
    int ret;
    while ((ret = table->get_next_arrow_batch(batch)) == E_OK) {
        for (unsigned r = 0; r < batch->row_count; ++r, ++rows) {
            char scratch[8];
            uint32_t size = 0;
            const char* value =
                batch->columns[1]->native_value(r, scratch, size);
            const unsigned pattern = rows % 7 % 3;
            if (pattern == 1) {
                EXPECT_EQ(value, nullptr);
            } else {
                ASSERT_NE(value, nullptr);
                EXPECT_EQ(std::string(value, size),
                          pattern == 0 ? "" : "value");
            }
        }
    }
    EXPECT_EQ(ret, E_NO_MORE_DATA);
    EXPECT_EQ(rows, 14U);
}
TEST_F(ArrowQueryScanTest,
       UnprojectedPredicateOffsetAndLimitCountSelectedRows) {
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TableQueryExecutor executor(&file, 3);
    auto filter =
        AlignedFieldFilter::compare("p", AlignedFieldFilter::CompareOp::EQ,
                                    AlignedFieldFilter::Literal::int64(1));
    ResultSet* raw = nullptr;
    ASSERT_EQ(executor.query("t", {"v"}, nullptr, nullptr, filter.release(), 1,
                             4, raw),
              E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    std::shared_ptr<ArrowBatch> first, last;
    ASSERT_EQ(table->get_next_arrow_batch(first), E_OK);
    ASSERT_EQ(table->get_next_arrow_batch(last), E_OK);
    EXPECT_EQ(first->row_count, 3);
    EXPECT_EQ(last->row_count, 1);
    EXPECT_TRUE(first->columns[1]->is_null(0));
    EXPECT_TRUE(last->columns[1]->is_null(0));
    char scratch[8];
    uint32_t size;
    int64_t value;
    std::memcpy(&value, first->columns[1]->native_value(2, scratch, size), 8);
    EXPECT_EQ(value, 101);
}
// Observe concurrency at the byte source, so decoding-only parallelism cannot
// satisfy this regression. A short delay lets independent reads overlap.
class ObservedReadFile : public RandomAccessReadFile {
   public:
    LocalRandomAccessReadFile source;
    std::atomic<int> active{0};
    std::atomic<int> peak{0};
    std::atomic<int> rejected{0};
    std::set<int64_t> forbidden;
    std::map<int64_t, int> reads;
    std::mutex reads_mutex;
    int64_t peak_compressor_bytes = 0;
    bool observe = false;
    bool is_opened() const override { return source.is_opened(); }
    int64_t file_size() const override { return source.file_size(); }
    const std::string& file_path() const override { return source.file_path(); }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        return source.generation(size, fingerprint);
    }
    void close() override { source.close(); }
    int read(int64_t offset, char* data, int32_t size,
             int32_t& actual) override {
        {
            std::lock_guard<std::mutex> lock(reads_mutex);
            ++reads[offset];
            peak_compressor_bytes =
                std::max(peak_compressor_bytes,
                         ModStat::get_instance().get_stat(MOD_COMPRESSOR_OBJ));
        }
        if (forbidden.count(offset)) {
            ++rejected;
            actual = 0;
            return E_FILE_READ_ERR;
        }
        int now = ++active;
        int maximum = peak.load();
        while (maximum < now && !peak.compare_exchange_weak(maximum, now)) {
        }
        if (observe) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        int ret = source.read(offset, data, size, actual);
        --active;
        return ret;
    }
};
TEST_F(ArrowQueryScanTest, MetadataLookupsUseTaskOwnedMemory) {
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TsFileIOReader metadata;
    ASSERT_EQ(metadata.init(&file), E_OK);
    TsFileMeta* file_meta = nullptr;
    ASSERT_EQ(metadata.get_tsfile_meta(file_meta), E_OK);
    auto root = file_meta->table_metadata_index_node_map_.at("t");
    auto entry = std::dynamic_pointer_cast<DeviceMetaIndexEntry>(
        root->children_.front());
    ASSERT_NE(entry, nullptr);
    const auto footer_bytes = root->pa_->get_total_used_bytes();
    for (int query = 0; query < 8; ++query) {
        PageArena arena;
        arena.init(1024, MOD_TSFILE_READER);
        std::vector<ITimeseriesIndex*> indexes(1, nullptr);
        ASSERT_EQ(metadata.get_timeseries_indexes(entry->device_id_, {"v"},
                                                  indexes, arena),
                  E_OK);
        auto* index = dynamic_cast<AlignedTimeseriesIndex*>(indexes[0]);
        ASSERT_NE(index, nullptr);
        TimeseriesIndex* time = nullptr;
        ASSERT_EQ(
            metadata.get_device_time_index(entry->device_id_, time, arena),
            E_OK);
        ASSERT_NE(time, nullptr);
        time->~TimeseriesIndex();
        index->time_ts_idx_->~TimeseriesIndex();
        index->value_ts_idx_->~TimeseriesIndex();
        index->~AlignedTimeseriesIndex();
        EXPECT_EQ(root->pa_->get_total_used_bytes(), footer_bytes);
    }
}
class WindowReadFile : public RandomAccessReadFile {
   public:
    std::string data = std::string(256 * 1024, '\0');
    std::string path = "memory";
    std::vector<std::pair<int64_t, int32_t>> reads;
    bool fail = false;
    bool short_read = false;
    WindowReadFile() {
        for (size_t i = 0; i < data.size(); ++i) data[i] = char(i % 251);
    }
    bool is_opened() const override { return true; }
    int64_t file_size() const override { return data.size(); }
    const std::string& file_path() const override { return path; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        size = data.size();
        fingerprint = 1;
        return E_OK;
    }
    void close() override {}
    int read(int64_t offset, char* output, int32_t size,
             int32_t& actual) override {
        reads.emplace_back(offset, size);
        actual = fail ? 0 : short_read ? size - 1 : size;
        if (actual) std::memcpy(output, data.data() + offset, actual);
        return fail ? E_FILE_READ_ERR : E_OK;
    }
};
TEST(ArrowInputRangeTest, ChargesSharedBytesAndReleasesReservationOnError) {
    WindowReadFile file;
    auto budget = std::make_shared<std::atomic<size_t>>(0);
    std::shared_ptr<ArrowInputRange> input;
    ASSERT_EQ(ArrowInputRange::read(file, budget, 17, 64, input), E_OK);
    ASSERT_TRUE(input);
    EXPECT_EQ(std::memcmp(input->bytes.get(), file.data.data() + 17, 64), 0);
    auto held = input;
    input.reset();
    EXPECT_EQ(budget->load(), 64U);
    held.reset();
    EXPECT_EQ(budget->load(), 0U);
    budget->store(ArrowInputRange::MAX_QUERY_BYTES - 10);
    EXPECT_EQ(ArrowInputRange::read(file, budget, 17, 11, input), E_OK);
    EXPECT_FALSE(input);
    EXPECT_EQ(file.reads.size(), 1U);
    budget->store(0);
    file.short_read = true;
    EXPECT_EQ(ArrowInputRange::read(file, budget, 17, 64, input),
              E_FILE_READ_ERR);
    EXPECT_FALSE(input);
    EXPECT_EQ(budget->load(), 0U);
}
TEST(ArrowReadWindowTest, CoalescesWithinBoundsAndBypassesLargeReads) {
    WindowReadFile file;
    ArrowReadWindow window;
    std::vector<char> output(100 * 1024);
    ASSERT_EQ(window.read(file, 100, output.data(), 100, 200000), E_OK);
    ASSERT_EQ(file.reads.size(), 1U);
    EXPECT_EQ(file.reads[0], std::make_pair(int64_t(100), int32_t(64 * 1024)));
    ASSERT_EQ(window.read(file, 60000, output.data(), 100, 200000), E_OK);
    EXPECT_EQ(std::memcmp(output.data(), file.data.data() + 60000, 100), 0);
    EXPECT_EQ(file.reads.size(), 1U);
    ASSERT_EQ(window.read(file, 70000, output.data(), 100, 70123), E_OK);
    EXPECT_EQ(file.reads.back(), std::make_pair(int64_t(70000), int32_t(123)));
    ASSERT_EQ(window.read(file, 80000, output.data(), output.size(), 200000),
              E_OK);
    EXPECT_EQ(file.reads.back().second, int32_t(output.size()));
    EXPECT_EQ(
        std::memcmp(output.data(), file.data.data() + 80000, output.size()), 0);
    EXPECT_EQ(window.read(file, 70000, output.data(), 100, 70099),
              E_TSFILE_CORRUPTED);
}
TEST(ArrowReadWindowTest, FailedAndShortReadsAreNeverCached) {
    WindowReadFile file;
    ArrowReadWindow window;
    char output[100];
    for (bool short_read : {false, true}) {
        file.fail = !short_read;
        file.short_read = short_read;
        EXPECT_EQ(window.read(file, 100, output, 100, 1000), E_FILE_READ_ERR);
    }
    file.fail = file.short_read = false;
    EXPECT_EQ(window.read(file, 100, output, 100, 1000), E_OK);
    EXPECT_EQ(file.reads.size(), 3U);
    EXPECT_EQ(std::memcmp(output, file.data.data() + 100, 100), 0);
}
#ifdef ENABLE_THREADS
TEST_F(ArrowQueryScanTest, QueryParallelismIncludesFileReads) {
    ASSERT_NE(g_thread_pool_, nullptr);
    ASSERT_GT(g_thread_pool_->num_threads(), 1U);
    g_config_value_.parallel_read_enabled_ = true;
    ObservedReadFile file;
    ASSERT_EQ(file.source.open(filename_), E_OK);
    file.observe = true;
    TableQueryExecutor executor(&file, 10);
    ResultSet* raw = nullptr;
    ASSERT_EQ(
        executor.query("t", {"id", "v", "p"}, nullptr, nullptr, nullptr, raw),
        E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    std::shared_ptr<ArrowBatch> batch;
    ASSERT_EQ(table->get_next_arrow_batch(batch), E_OK);
    EXPECT_EQ(batch->row_count, 10U);
    EXPECT_GT(file.peak.load(), 1);
}
#endif
TEST_F(ArrowQueryScanTest,
       PredicatePruningDoesNotReadOutputPayloadAndErrorsAreTerminal) {
    ObservedReadFile file;
    ASSERT_EQ(file.source.open(filename_), E_OK);
    {
        TsFileIOReader metadata;
        ASSERT_EQ(metadata.init(&file), E_OK);
        TsFileMeta* file_meta = nullptr;
        ASSERT_EQ(metadata.get_tsfile_meta(file_meta), E_OK);
        PageArena arena;
        arena.init(1024, MOD_TSFILE_READER);
        std::vector<ITimeseriesIndex*> indexes(1, nullptr);
        auto root = file_meta->table_metadata_index_node_map_.at("t");
        ASSERT_FALSE(root->children_.empty());
        auto entry = std::dynamic_pointer_cast<DeviceMetaIndexEntry>(
            root->children_.front());
        ASSERT_NE(entry, nullptr);
        auto id = entry->device_id_;
        EXPECT_EQ(id->get_device_name(), "t.a");
        ASSERT_EQ(metadata.get_timeseries_indexes(id, {"v"}, indexes, arena),
                  E_OK);
        auto* index = dynamic_cast<AlignedTimeseriesIndex*>(indexes[0]);
        ASSERT_NE(index, nullptr);
        auto* chunks = index->get_value_chunk_meta_list();
        std::vector<ArrowPageLocation> pages;
        for (auto it = chunks->begin(); it != chunks->end(); it++)
            ASSERT_EQ(plan_arrow_pages(file, *it.get(), index->get_statistic(),
                                       pages),
                      E_OK);
        ASSERT_FALSE(pages.empty());
        for (auto& page : pages) file.forbidden.insert(page.offset);
        index->time_ts_idx_->~TimeseriesIndex();
        index->value_ts_idx_->~TimeseriesIndex();
        index->~AlignedTimeseriesIndex();
    }
    g_config_value_.parallel_read_enabled_ = true;
    TableQueryExecutor executor(&file, 5);
    for (int64_t threshold : {99, 1, 0}) {
        auto filter = AlignedFieldFilter::compare(
            "p", AlignedFieldFilter::CompareOp::GT,
            AlignedFieldFilter::Literal::int64(threshold == 1 ? 0 : threshold));
        if (threshold == 1)
            filter = AlignedFieldFilter::and_filter(
                std::move(filter), AlignedFieldFilter::compare(
                                       "p", AlignedFieldFilter::CompareOp::EQ,
                                       AlignedFieldFilter::Literal::int64(0)));
        ResultSet* raw = nullptr;
        ASSERT_EQ(
            executor.query("t", {"v"}, nullptr, nullptr, filter.release(), raw),
            E_OK);
        std::unique_ptr<ResultSet> result(raw);
        auto* table = dynamic_cast<TableResultSet*>(raw);
        std::shared_ptr<ArrowBatch> batch;
        if (threshold != 0) {
            EXPECT_EQ(table->get_next_arrow_batch(batch), E_NO_MORE_DATA);
            EXPECT_EQ(file.rejected.load(), 0);
        } else {
            EXPECT_EQ(table->get_next_arrow_batch(batch), E_FILE_READ_ERR);
            EXPECT_FALSE(batch);
            int reads = file.rejected.load();
            EXPECT_GT(reads, 0);
            EXPECT_EQ(table->get_next_arrow_batch(batch), E_FILE_READ_ERR);
            EXPECT_EQ(file.rejected.load(), reads);
        }
    }
}

TEST_F(ArrowQueryScanTest, SmallBatchesResumeEachPayloadWithoutRereading) {
    ObservedReadFile file;
    ASSERT_EQ(file.source.open(filename_), E_OK);
    TableQueryExecutor executor(&file, 1);
    ResultSet* raw = nullptr;
    ASSERT_EQ(executor.query("t", {"v", "p"}, nullptr, nullptr, nullptr, raw),
              E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    for (int row = 0; row < 14; ++row) {
        std::shared_ptr<ArrowBatch> batch;
        ASSERT_EQ(table->get_next_arrow_batch(batch), E_OK);
        ASSERT_EQ(batch->row_count, 1U);
        char scratch[8];
        uint32_t size;
        int64_t value;
        std::memcpy(&value, batch->columns[2]->native_value(0, scratch, size),
                    8);
        EXPECT_EQ(value, (row % 7) % 2);
        EXPECT_EQ(batch->columns[1]->is_null(0), row % 7 == 3);
        // Metadata may be visited by more than one planner. Payload offsets
        // are first seen only when a result batch needs their values; a later
        // batch within the same page must do no additional file I/O.
        if (row == 0) {
            std::lock_guard<std::mutex> lock(file.reads_mutex);
            file.reads.clear();
        } else if (row < 3) {
            std::lock_guard<std::mutex> lock(file.reads_mutex);
            EXPECT_TRUE(file.reads.empty());
        }
    }
    std::shared_ptr<ArrowBatch> end;
    EXPECT_EQ(table->get_next_arrow_batch(end), E_NO_MORE_DATA);
}

#ifdef ENABLE_MEM_STAT
TEST_F(ArrowQueryScanTest, LimitReleasesUnfinishedPageBuffers) {
    g_config_value_.parallel_read_enabled_ = true;
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TableQueryExecutor executor(&file, 5);
    ResultSet* raw = nullptr;
    ASSERT_EQ(
        executor.query("t", {"v", "p"}, nullptr, nullptr, nullptr, 0, 1, raw),
        E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    const int64_t baseline =
        ModStat::get_instance().get_stat(MOD_COMPRESSOR_OBJ);
    std::shared_ptr<ArrowBatch> batch;
    ASSERT_EQ(table->get_next_arrow_batch(batch), E_OK);
    ASSERT_EQ(batch->row_count, 1U);
    EXPECT_EQ(ModStat::get_instance().get_stat(MOD_COMPRESSOR_OBJ), baseline);
    std::shared_ptr<ArrowBatch> end;
    EXPECT_EQ(table->get_next_arrow_batch(end), E_NO_MORE_DATA);
}

TEST_F(ArrowQueryScanTest, SparseBatchesReleaseCompletedPageBuffers) {
    g_config_value_.parallel_read_enabled_ = false;
    ObservedReadFile file;
    ASSERT_EQ(file.source.open(filename_), E_OK);
    TableQueryExecutor executor(&file, 3);
    auto filter =
        AlignedFieldFilter::compare("p", AlignedFieldFilter::CompareOp::EQ,
                                    AlignedFieldFilter::Literal::int64(0));
    ResultSet* raw = nullptr;
    ASSERT_EQ(executor.query("t", {"v", "v", "p"}, nullptr, nullptr,
                             filter.release(), raw),
              E_OK);
    std::unique_ptr<ResultSet> result(raw);
    auto* table = dynamic_cast<TableResultSet*>(raw);
    const int64_t baseline =
        ModStat::get_instance().get_stat(MOD_COMPRESSOR_OBJ);
    const int64_t expected_values[] = {0, 2, 4, 6, 100, 102, 104, 106};
    uint32_t row = 0;
    for (uint32_t expected : {3U, 3U, 2U}) {
        std::shared_ptr<ArrowBatch> batch;
        ASSERT_EQ(table->get_next_arrow_batch(batch), E_OK);
        ASSERT_EQ(batch->row_count, expected);
        EXPECT_EQ(batch->columns[1], batch->columns[2]);
        for (uint32_t i = 0; i < expected; ++i, ++row) {
            char scratch[8];
            uint32_t size;
            int64_t value;
            ASSERT_FALSE(batch->columns[1]->is_null(i));
            std::memcpy(&value,
                        batch->columns[1]->native_value(i, scratch, size), 8);
            EXPECT_EQ(value, expected_values[row]);
        }
        // Every batch ends at the last selected row of a page, even though
        // some pages still have rejected physical rows. No payload is needed
        // across these boundaries, including while the Arrow batch is held.
        EXPECT_EQ(ModStat::get_instance().get_stat(MOD_COMPRESSOR_OBJ),
                  baseline);
    }
    // The next read allocates its compressor before requesting its payload.
    // In serial execution all previous payloads must already be released,
    // even when several pages contribute to the same exact-size batch.
    EXPECT_LE(file.peak_compressor_bytes,
              baseline + int64_t(sizeof(UncompressedCompressor)));
    std::shared_ptr<ArrowBatch> end;
    EXPECT_EQ(table->get_next_arrow_batch(end), E_NO_MORE_DATA);
}
#endif

}  // namespace
