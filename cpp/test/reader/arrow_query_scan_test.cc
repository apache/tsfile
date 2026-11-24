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

#include "common/arrow_batch.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/filter/aligned_field_filter.h"
#include "reader/table_query_executor.h"
#include "writer/tsfile_table_writer.h"

namespace {
using namespace common;
using namespace storage;
TEST(ArrowBatchTest, ExportSharesBuffersAndOwnsData) {
    auto batch = std::make_shared<ArrowBatch>(
        std::vector<std::string>{"v", "flag", "text"},
        std::vector<TSDataType>{INT64, BOOLEAN, STRING}, 3);
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
        filename_ = "arrow_query_scan_" +
                    std::to_string(reinterpret_cast<uintptr_t>(this)) +
                    ".tsfile";
        const std::vector<ColumnSchema> columns{
            {"id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG},
            {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"p", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}};
        TableSchema schema("t", columns);
        WriteFile file;
        ASSERT_EQ(file.create(filename_, O_WRONLY | O_CREAT | O_TRUNC, 0666),
                  E_OK);
        TsFileTableWriter writer(&file, &schema);
        for (int d = 0; d < 2; ++d) {
            Tablet tablet("t", {"id", "v", "p"}, {STRING, INT64, INT64},
                          {ColumnCategory::TAG, ColumnCategory::FIELD,
                           ColumnCategory::FIELD},
                          7);
            for (int r = 0; r < 7; ++r) {
                ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
                ASSERT_EQ(tablet.add_value(r, "id", d == 0 ? "a" : "b"), E_OK);
                if (r != 3)
                    ASSERT_EQ(tablet.add_value(r, "v", int64_t(d * 100 + r)),
                              E_OK);
                ASSERT_EQ(tablet.add_value(r, "p", int64_t(r % 2)), E_OK);
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
}  // namespace
