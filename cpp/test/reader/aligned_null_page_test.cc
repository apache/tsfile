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
#include <cstdio>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/path.h"
#include "encoding/encoder_factory.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {
namespace {

using namespace common;

// Recreate the older C++ representation so reader compatibility is tested
// independently of the corrected empty-page writer.
void use_legacy_null_pages(ValueChunkWriter* writer, TSDataType type,
                           TSEncoding encoding, uint32_t page_rows) {
    ByteStream rewritten(1024, MOD_DEFAULT);
    ByteStream& data = writer->get_chunk_data();
    std::unique_ptr<Encoder, decltype(&EncoderFactory::free)> encoder(
        EncoderFactory::alloc_value_encoder(encoding, type),
        EncoderFactory::free);
    std::unique_ptr<Statistic, decltype(&StatisticFactory::free)> empty_stat(
        StatisticFactory::alloc_statistic(type), StatisticFactory::free);
    ASSERT_NE(encoder, nullptr);
    ASSERT_NE(empty_stat, nullptr);
    for (int page = 0; page < writer->num_of_pages(); ++page) {
        PageHeader header;
        ASSERT_EQ(header.deserialize_from(data, true, type), E_OK);
        if (header.uncompressed_size_ == 0) {
            ByteStream payload(1024, MOD_DEFAULT);
            encoder->reset();
            ASSERT_EQ(encoder->flush(payload), E_OK);
            std::vector<char> bitmap((page_rows + 7) / 8, 0);
            const uint32_t size =
                sizeof(uint32_t) + bitmap.size() + payload.total_size();
            ASSERT_EQ(SerializationUtil::write_var_uint(size, rewritten), E_OK);
            ASSERT_EQ(SerializationUtil::write_var_uint(size, rewritten), E_OK);
            ASSERT_EQ(empty_stat->serialize_to(rewritten), E_OK);
            ASSERT_EQ(SerializationUtil::write_ui32(page_rows, rewritten),
                      E_OK);
            ASSERT_EQ(rewritten.write_buf(bitmap.data(), bitmap.size()), E_OK);
            ASSERT_EQ(merge_byte_stream(rewritten, payload), E_OK);
        } else {
            ASSERT_EQ(SerializationUtil::write_var_uint(
                          header.uncompressed_size_, rewritten),
                      E_OK);
            ASSERT_EQ(SerializationUtil::write_var_uint(header.compressed_size_,
                                                        rewritten),
                      E_OK);
            ASSERT_EQ(header.statistic_->serialize_to(rewritten), E_OK);
            std::vector<char> payload(header.compressed_size_);
            uint32_t read = 0;
            ASSERT_EQ(data.read_buf(payload.data(), payload.size(), read),
                      E_OK);
            ASSERT_EQ(read, payload.size());
            ASSERT_EQ(rewritten.write_buf(payload.data(), payload.size()),
                      E_OK);
        }
    }
    ASSERT_EQ(data.remaining_size(), 0u);
    data.reset();
    ASSERT_EQ(merge_byte_stream(data, rewritten), E_OK);
}

// Leading and intermediate all-null value pages must not prevent reading
// subsequent pages, regardless of the value codec or write API.
class AlignedNullPageReadTest
    : public ::testing::TestWithParam<
          std::tuple<TSDataType, uint32_t, int, bool>> {
   protected:
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        saved_page_rows_ = g_config_value_.page_writer_max_point_num_;
        saved_block_memory_ = g_config_value_.tsblock_max_memory_;
        g_config_value_.page_writer_max_point_num_ = std::get<1>(GetParam());
        // The larger pages span multiple result blocks.
        g_config_value_.tsblock_max_memory_ = 4096;
        file_ = "aligned_null_page_" +
                std::to_string(static_cast<int>(std::get<0>(GetParam()))) +
                "_" + std::to_string(std::get<1>(GetParam())) + "_" +
                std::to_string(std::get<2>(GetParam())) + "_" +
                std::to_string(std::get<3>(GetParam())) + ".tsfile";
        std::remove(file_.c_str());
    }

    void TearDown() override {
        std::remove(file_.c_str());
        g_config_value_.page_writer_max_point_num_ = saved_page_rows_;
        g_config_value_.tsblock_max_memory_ = saved_block_memory_;
        libtsfile_destroy();
    }

    bool is_null(int row) const {
        int page = row / std::get<1>(GetParam());
        return page == 0 || page == 2 || page == 3 || page == 6;
    }

    void add_value(TsRecord& record, int row) {
        switch (std::get<0>(GetParam())) {
            case INT32:
                record.add_point("value", static_cast<int32_t>(1000 + row));
                break;
            case INT64:
                record.add_point("value", static_cast<int64_t>(1000 + row));
                break;
            case FLOAT:
                record.add_point("value", static_cast<float>(1000.25 + row));
                break;
            case DOUBLE:
                record.add_point("value", 1000.25 + row);
                break;
            case STRING:
                record_string_ = "value-" + std::to_string(row);
                record.add_point("value", String(record_string_));
                break;
            default:
                FAIL() << "Unexpected type";
        }
    }

    void add_value(Tablet& tablet, int index, int row) {
        switch (std::get<0>(GetParam())) {
            case INT32:
                ASSERT_EQ(tablet.add_value(index, 1,
                                           static_cast<int32_t>(1000 + row)),
                          E_OK);
                break;
            case INT64:
                ASSERT_EQ(tablet.add_value(index, 1,
                                           static_cast<int64_t>(1000 + row)),
                          E_OK);
                break;
            case FLOAT:
                ASSERT_EQ(tablet.add_value(index, 1,
                                           static_cast<float>(1000.25 + row)),
                          E_OK);
                break;
            case DOUBLE:
                ASSERT_EQ(tablet.add_value(index, 1, 1000.25 + row), E_OK);
                break;
            case STRING: {
                std::string value = "value-" + std::to_string(row);
                ASSERT_EQ(tablet.add_value(index, 1, value.c_str()), E_OK);
                break;
            }
            default:
                FAIL() << "Unexpected type";
        }
    }

    void check_value(Field* field, int row) {
        if (is_null(row)) {
            EXPECT_EQ(field->type_, NULL_TYPE) << "row=" << row;
            return;
        }
        ASSERT_EQ(field->type_, std::get<0>(GetParam())) << "row=" << row;
        switch (field->type_) {
            case INT32:
                EXPECT_EQ(field->value_.ival_, 1000 + row);
                break;
            case INT64:
                EXPECT_EQ(field->value_.lval_, 1000 + row);
                break;
            case FLOAT:
                EXPECT_FLOAT_EQ(field->value_.fval_, 1000.25f + row);
                break;
            case DOUBLE:
                EXPECT_DOUBLE_EQ(field->value_.dval_, 1000.25 + row);
                break;
            case STRING:
                EXPECT_EQ(field->get_string_value()->to_std_string(),
                          "value-" + std::to_string(row));
                break;
            default:
                FAIL() << "Unexpected type";
        }
    }

    std::string file_;
    std::string record_string_;
    uint32_t saved_page_rows_ = 0;
    uint32_t saved_block_memory_ = 0;
};

TEST_P(AlignedNullPageReadTest,
       ReadsValuesAfterLeadingAndIntermediateNullPages) {
    const TSDataType type = std::get<0>(GetParam());
    const TSEncoding encoding = type == STRING ? DICTIONARY : GORILLA;
    const std::string device = "root.null_pages";
    const int rows = 7 * std::get<1>(GetParam());
    const int mode = std::get<2>(GetParam());
    {
        TsFileWriter writer;
        ASSERT_EQ(writer.open(file_), E_OK);
        std::vector<MeasurementSchema*> schemas{
            new MeasurementSchema("anchor", INT64, PLAIN, UNCOMPRESSED),
            new MeasurementSchema("value", type, encoding, UNCOMPRESSED)};
        ASSERT_EQ(writer.register_aligned_timeseries(device, schemas), E_OK);
        for (int row = 0; row < rows;) {
            if (mode == 0 || (mode == 2 && row % 3 == 0)) {
                TsRecord record(row, device);
                record.add_point("anchor", static_cast<int64_t>(row));
                if (is_null(row))
                    record.points_.emplace_back("value");
                else
                    add_value(record, row);
                ASSERT_EQ(writer.write_record_aligned(record), E_OK);
                ++row;
            } else {
                const int count = std::min(11, rows - row);
                auto schema =
                    std::make_shared<std::vector<MeasurementSchema>>();
                schema->emplace_back("anchor", INT64, PLAIN, UNCOMPRESSED);
                schema->emplace_back("value", type, encoding, UNCOMPRESSED);
                Tablet tablet(device, schema, count);
                ASSERT_EQ(tablet.err_code_, E_OK);
                for (int index = 0; index < count; ++index) {
                    ASSERT_EQ(tablet.add_timestamp(index, row + index), E_OK);
                    ASSERT_EQ(tablet.add_value(
                                  index, 0, static_cast<int64_t>(row + index)),
                              E_OK);
                    if (!is_null(row + index))
                        add_value(tablet, index, row + index);
                }
                ASSERT_EQ(writer.write_tablet_aligned(tablet), E_OK);
                row += count;
            }
        }
        if (std::get<3>(GetParam())) {
            ASSERT_NO_FATAL_FAILURE(
                use_legacy_null_pages(schemas[1]->value_chunk_writer_, type,
                                      encoding, std::get<1>(GetParam())));
        }
        ASSERT_EQ(writer.flush(), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_), E_OK);
    ResultSet* result = nullptr;
    std::vector<Path> paths{Path(device + ".anchor"), Path(device + ".value")};
    ASSERT_EQ(reader.query(QueryExpression::create(paths, nullptr), result),
              E_OK);
    int row = 0;
    bool more = false;
    while (true) {
        ASSERT_EQ(result->next(more), E_OK);
        if (!more) break;
        auto* record = result->get_row_record();
        ASSERT_LT(row, rows);
        EXPECT_EQ(record->get_timestamp(), row);
        EXPECT_EQ(record->get_field(1)->value_.lval_, row);
        check_value(record->get_field(2), row);
        ++row;
    }
    EXPECT_EQ(row, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(reader.close(), E_OK);
}

INSTANTIATE_TEST_SUITE_P(
    CodecsAndPageBoundaries, AlignedNullPageReadTest,
    ::testing::Combine(::testing::Values(INT32, INT64, FLOAT, DOUBLE, STRING),
                       ::testing::Values(1u, 7u, 256u),
                       ::testing::Values(0, 1, 2), ::testing::Bool()));

}  // namespace
}  // namespace storage
