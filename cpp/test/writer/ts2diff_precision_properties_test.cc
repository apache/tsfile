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

#include <chrono>
#include <cmath>
#include <cstdio>
#include <tuple>

#include "encoding/ts2diff_decoder.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

namespace {
using namespace common;
using namespace storage;

// FLOAT/DOUBLE, aligned/nonaligned, copied/owned schema, tablet/record,
// explicit zero/four/default precision. All calls exist before this fix.
using PrecisionCase = std::tuple<bool, bool, bool, bool, int>;

class TS2DiffPrecisionProperties
    : public testing::TestWithParam<PrecisionCase> {
   protected:
    std::string path_;
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        g_config_value_.time_compress_type_ = UNCOMPRESSED;
        ASSERT_EQ(set_page_max_point_count(17), E_OK);
        path_ =
            testing::TempDir() + "tsfile-precision-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
    }
    void TearDown() override {
        std::remove(path_.c_str());
        libtsfile_destroy();
    }
};

TEST_P(TS2DiffPrecisionProperties, KeepsPrecisionAcrossPagesAndChunks) {
    const bool is_float = std::get<0>(GetParam());
    const bool aligned = std::get<1>(GetParam());
    const bool copied = std::get<2>(GetParam());
    const bool batch = std::get<3>(GetParam());
    const int precision = std::get<4>(GetParam());
    const auto type = is_float ? FLOAT : DOUBLE;
    MeasurementSchema schema("v", type, TS_2DIFF, UNCOMPRESSED);
    if (precision >= 0)
        schema.props_["max_point_number"] = std::to_string(precision);
    {
        TsFileWriter writer;
        ASSERT_EQ(writer.open(path_), E_OK);
        writer.set_generate_table_schema(false);
        if (copied) {
            ASSERT_EQ(aligned
                          ? writer.register_aligned_timeseries("root.d", schema)
                          : writer.register_timeseries("root.d", schema),
                      E_OK);
        } else {
            auto* owned =
                new MeasurementSchema("v", type, TS_2DIFF, UNCOMPRESSED);
            owned->props_ = schema.props_;
            std::vector<MeasurementSchema*> schemas{owned};
            ASSERT_EQ(
                aligned ? writer.register_aligned_timeseries("root.d", schemas)
                        : writer.register_timeseries("root.d", schemas),
                E_OK);
        }
        ASSERT_EQ(writer.get_schema_group_map()->size(), 1U);
        EXPECT_EQ(writer.get_schema_group_map()
                      ->begin()
                      ->second->measurement_schema_map_.at("v")
                      ->props_,
                  schema.props_);
        for (int chunk = 0; chunk < 2; ++chunk) {
            if (batch) {
                auto schemas =
                    std::make_shared<std::vector<MeasurementSchema>>();
                schemas->emplace_back("v", type, TS_2DIFF, UNCOMPRESSED);
                Tablet tablet("root.d", schemas, 260);
                for (int row = 0; row < 260; ++row) {
                    ASSERT_EQ(tablet.add_timestamp(row, chunk * 260 + row),
                              E_OK);
                    if (is_float)
                        ASSERT_EQ(tablet.add_value(row, "v", 1.23456f), E_OK);
                    else
                        ASSERT_EQ(tablet.add_value(row, "v", 1.23456), E_OK);
                }
                ASSERT_EQ(aligned ? writer.write_tablet_aligned(tablet)
                                  : writer.write_tablet(tablet),
                          E_OK);
            } else {
                for (int row = 0; row < 260; ++row) {
                    TsRecord record(chunk * 260 + row, "root.d");
                    if (is_float)
                        record.add_point("v", 1.23456f);
                    else
                        record.add_point("v", 1.23456);
                    ASSERT_EQ(aligned ? writer.write_record_aligned(record)
                                      : writer.write_record(record),
                              E_OK);
                }
            }
            ASSERT_EQ(writer.flush(), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    TsFileReader reader;
    ASSERT_EQ(reader.open(path_), E_OK);
    ResultSet* result = nullptr;
    std::vector<std::string> paths{"root.d.v"};
    ASSERT_EQ(reader.query(paths, 0, 520, result), E_OK);
    int rows = 0;
    std::vector<double> actual;
    for (;;) {
        bool has_next = false;
        ASSERT_EQ(result->next(has_next), E_OK);
        if (!has_next) break;
        EXPECT_EQ(result->get_value<int64_t>(1), rows);
        actual.push_back(is_float ? result->get_value<float>(2)
                                  : result->get_value<double>(2));
        ++rows;
    }
    EXPECT_EQ(rows, 520);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(reader.close(), E_OK);
    const double expected = precision == 0   ? 1.0
                            : precision == 4 ? 1.2346
                                             : 1.23;
    for (double value : actual) {
        ASSERT_EQ(value, is_float
                             ? static_cast<double>(static_cast<float>(expected))
                             : expected);
    }
}

class TS2DiffTablePrecision : public testing::Test {
   protected:
    std::string path_;
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        g_config_value_.time_compress_type_ = UNCOMPRESSED;
        path_ =
            testing::TempDir() + "tsfile-table-precision-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
    }
    void TearDown() override {
        std::remove(path_.c_str());
        libtsfile_destroy();
    }
};

TEST_F(TS2DiffTablePrecision, ConfiguresTableValueChunk) {
    auto* column = new MeasurementSchema("v", FLOAT, TS_2DIFF, UNCOMPRESSED);
    column->props_["max_point_number"] = "4";
    auto schema = std::make_shared<TableSchema>(
        "audit", std::vector<MeasurementSchema*>{column},
        std::vector<ColumnCategory>{ColumnCategory::FIELD});
    TsFileWriter writer;
    ASSERT_EQ(writer.open(path_), E_OK);
    ASSERT_EQ(writer.register_table(schema), E_OK);
    Tablet tablet("audit", schema->get_measurement_names(),
                  schema->get_data_types(), schema->get_column_categories(), 1);
    ASSERT_EQ(tablet.add_timestamp(0, 1), E_OK);
    ASSERT_EQ(tablet.add_value(0, "v", 1.23456f), E_OK);
    ASSERT_EQ(writer.write_table(tablet), E_OK);
    auto* chunk = writer.get_schema_group_map()
                      ->begin()
                      ->second->measurement_schema_map_.at("v")
                      ->value_chunk_writer_;
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->end_encode_chunk(), E_OK);
    std::vector<char> bytes(chunk->get_chunk_data().total_size());
    ASSERT_EQ(
        copy_bs_to_buf(chunk->get_chunk_data(), bytes.data(), bytes.size()),
        E_OK);
    ByteStream page;
    page.wrap_from(bytes.data(), bytes.size());
    PageHeader header;
    ASSERT_EQ(header.deserialize_from(page, false, FLOAT), E_OK);
    uint32_t rows = 0;
    uint8_t bitmap = 0;
    ASSERT_EQ(SerializationUtil::read_ui32(rows, page), E_OK);
    ASSERT_EQ(SerializationUtil::read_ui8(bitmap, page), E_OK);
    EXPECT_EQ(rows, 1U);
    EXPECT_EQ(bitmap, 0x80);
    FloatTS2DIFFDecoder decoder;
    float value = 0;
    ASSERT_EQ(decoder.read_float(value, page), E_OK);
    EXPECT_EQ(value, 1.2346f);
    ASSERT_EQ(writer.close(), E_OK);
}

INSTANTIATE_TEST_SUITE_P(WriterPaths, TS2DiffPrecisionProperties,
                         testing::Combine(testing::Bool(), testing::Bool(),
                                          testing::Bool(), testing::Bool(),
                                          testing::Values(0, 4, -1)));

}  // namespace
