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

#include "writer/tsfile_writer.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"

using namespace common;
using namespace storage;

namespace {

class SchemaCheckCacheTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        writer_ = new TsFileWriter();
        file_name_ = "tsfile_schema_cache_test_" + unique_suffix() + ".tsfile";
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(writer_->open(file_name_, flags, 0666), E_OK);
    }

    void TearDown() override {
        delete writer_;
        ASSERT_EQ(remove(file_name_.c_str()), 0);
        libtsfile_destroy();
    }

    static std::string unique_suffix() {
        static std::atomic<uint64_t> counter{0};
#ifdef _WIN32
        const auto process_id = static_cast<uint64_t>(_getpid());
#else
        const auto process_id = static_cast<uint64_t>(getpid());
#endif
        return std::to_string(process_id) + "_" +
               std::to_string(counter.fetch_add(1));
    }

    static MeasurementSchema schema(const std::string& name) {
        return MeasurementSchema(name, TSDataType::INT32, TSEncoding::PLAIN,
                                 CompressionType::UNCOMPRESSED);
    }

    static std::vector<MeasurementSchema> schemas(
        const std::vector<std::string>& names) {
        std::vector<MeasurementSchema> result;
        for (const auto& name : names) {
            result.push_back(schema(name));
        }
        return result;
    }

    static std::string field_to_string(Field* field) {
        if (field->type_ == TEXT || field->type_ == STRING ||
            field->type_ == BLOB) {
            return std::string(field->value_.sval_);
        }
        std::stringstream stream;
        switch (field->type_) {
            case BOOLEAN:
                stream << (field->value_.bval_ ? "true" : "false");
                break;
            case INT32:
            case DATE:
                stream << field->value_.ival_;
                break;
            case INT64:
            case TIMESTAMP:
                stream << field->value_.lval_;
                break;
            case FLOAT:
                stream << field->value_.fval_;
                break;
            case DOUBLE:
                stream << field->value_.dval_;
                break;
            case NULL_TYPE:
                stream << "NULL";
                break;
            default:
                ADD_FAILURE() << "Unexpected field type: " << field->type_;
        }
        return stream.str();
    }

    std::vector<std::vector<std::string>> read_rows(
        const std::vector<Path>& paths) const {
        TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);
        QueryExpression* expression = QueryExpression::create(paths, nullptr);
        ResultSet* result = nullptr;
        EXPECT_EQ(reader.query(expression, result), E_OK);
        auto* query = static_cast<QDSWithoutTimeGenerator*>(result);

        std::vector<std::vector<std::string>> rows;
        bool has_next = false;
        while (IS_SUCC(query->next(has_next)) && has_next) {
            RowRecord* record = query->get_row_record();
            std::vector<std::string> row;
            row.push_back(std::to_string(record->get_timestamp()));
            for (size_t i = 1; i < record->get_fields()->size(); ++i) {
                row.push_back(field_to_string(record->get_field(i)));
            }
            rows.push_back(row);
        }
        reader.destroy_query_data_set(query);
        reader.close();
        return rows;
    }

    static Path path(const std::string& device,
                     const std::string& measurement) {
        std::string device_copy = device;
        std::string measurement_copy = measurement;
        return Path(device_copy, measurement_copy);
    }

    TsFileWriter* writer_ = nullptr;
    std::string file_name_;
};

TEST_F(SchemaCheckCacheTest, RepeatedSameSchemaSurvivesFlush) {
    const std::string device = "root.cache_hit";
    const std::vector<std::string> names = {"s0", "s1", "s2"};
    for (const auto& name : names) {
        ASSERT_EQ(writer_->register_timeseries(device, schema(name)), E_OK);
    }

    for (int row = 0; row < 5; ++row) {
        auto tablet_schema = schemas(names);
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(tablet_schema), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 1000 + row), E_OK);
        for (uint32_t column = 0; column < names.size(); ++column) {
            ASSERT_EQ(tablet.add_value(
                          0, column, static_cast<int32_t>(row * 100 + column)),
                      E_OK);
        }
        ASSERT_EQ(writer_->write_tablet(tablet), E_OK);
        if (row == 2) {
            ASSERT_EQ(writer_->flush(), E_OK);
        }
    }
    ASSERT_EQ(writer_->flush(), E_OK);
    ASSERT_EQ(writer_->close(), E_OK);

    auto rows =
        read_rows({path(device, "s0"), path(device, "s1"), path(device, "s2")});
    ASSERT_EQ(rows.size(), 5u);
    for (int row = 0; row < 5; ++row) {
        EXPECT_EQ(rows[row][0], std::to_string(1000 + row));
        for (int column = 0; column < 3; ++column) {
            EXPECT_EQ(rows[row][column + 1],
                      std::to_string(row * 100 + column));
        }
    }
}

TEST_F(SchemaCheckCacheTest, SameCountDifferentNameOrderDoesNotCrossWire) {
    const std::string device = "root.cache_reorder";
    for (const auto& name : {"s0", "s1", "s2"}) {
        ASSERT_EQ(writer_->register_timeseries(device, schema(name)), E_OK);
    }

    {
        auto tablet_schema = schemas({"s0", "s1"});
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(tablet_schema), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 0), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, static_cast<int32_t>(10)), E_OK);
        ASSERT_EQ(tablet.add_value(0, 1, static_cast<int32_t>(11)), E_OK);
        ASSERT_EQ(writer_->write_tablet(tablet), E_OK);
    }
    {
        auto tablet_schema = schemas({"s1", "s0"});
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(tablet_schema), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 1), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, static_cast<int32_t>(21)), E_OK);
        ASSERT_EQ(tablet.add_value(0, 1, static_cast<int32_t>(20)), E_OK);
        ASSERT_EQ(writer_->write_tablet(tablet), E_OK);
    }
    {
        auto tablet_schema = schemas({"s0", "s2"});
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(tablet_schema), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 2), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, static_cast<int32_t>(30)), E_OK);
        ASSERT_EQ(tablet.add_value(0, 1, static_cast<int32_t>(32)), E_OK);
        ASSERT_EQ(writer_->write_tablet(tablet), E_OK);
    }
    ASSERT_EQ(writer_->flush(), E_OK);
    ASSERT_EQ(writer_->close(), E_OK);

    auto rows =
        read_rows({path(device, "s0"), path(device, "s1"), path(device, "s2")});
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[0][1], "10");
    EXPECT_EQ(rows[0][2], "11");
    EXPECT_EQ(rows[1][1], "20");
    EXPECT_EQ(rows[1][2], "21");
    EXPECT_EQ(rows[2][1], "30");
    EXPECT_EQ(rows[2][3], "32");
}

TEST_F(SchemaCheckCacheTest, LateRegistrationIsNotMaskedByCache) {
    const std::string device = "root.cache_late_register";
    ASSERT_EQ(writer_->register_timeseries(device, schema("s0")), E_OK);

    auto first_schema = schemas({"s0", "s1"});
    Tablet first(device,
                 std::make_shared<std::vector<MeasurementSchema>>(first_schema),
                 1);
    ASSERT_EQ(first.add_timestamp(0, 0), E_OK);
    ASSERT_EQ(first.add_value(0, 0, static_cast<int32_t>(100)), E_OK);
    ASSERT_EQ(first.add_value(0, 1, static_cast<int32_t>(101)), E_OK);
    ASSERT_EQ(writer_->write_tablet(first), E_OK);

    ASSERT_EQ(writer_->register_timeseries(device, schema("s1")), E_OK);
    auto second_schema = schemas({"s0", "s1"});
    Tablet second(
        device, std::make_shared<std::vector<MeasurementSchema>>(second_schema),
        1);
    ASSERT_EQ(second.add_timestamp(0, 1), E_OK);
    ASSERT_EQ(second.add_value(0, 0, static_cast<int32_t>(200)), E_OK);
    ASSERT_EQ(second.add_value(0, 1, static_cast<int32_t>(201)), E_OK);
    ASSERT_EQ(writer_->write_tablet(second), E_OK);

    ASSERT_EQ(writer_->flush(), E_OK);
    ASSERT_EQ(writer_->close(), E_OK);
    auto rows = read_rows({path(device, "s1")});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0][0], "1");
    EXPECT_EQ(rows[0][1], "201");
}

TEST_F(SchemaCheckCacheTest, AlignedCacheIsIndependentAndHandlesReorder) {
    const std::string device = "root.cache_aligned";
    for (const auto& name : {"a0", "a1"}) {
        ASSERT_EQ(writer_->register_aligned_timeseries(device, schema(name)),
                  E_OK);
    }

    for (int row = 0; row < 4; ++row) {
        const std::vector<std::string> names =
            row == 3 ? std::vector<std::string>{"a1", "a0"}
                     : std::vector<std::string>{"a0", "a1"};
        auto tablet_schema = schemas(names);
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(tablet_schema), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 500 + row), E_OK);
        if (row == 3) {
            ASSERT_EQ(tablet.add_value(0, 0, static_cast<int32_t>(19)), E_OK);
            ASSERT_EQ(tablet.add_value(0, 1, static_cast<int32_t>(9)), E_OK);
        } else {
            ASSERT_EQ(tablet.add_value(0, 0, row), E_OK);
            ASSERT_EQ(tablet.add_value(0, 1, 10 + row), E_OK);
        }
        ASSERT_EQ(writer_->write_tablet_aligned(tablet), E_OK);
    }
    ASSERT_EQ(writer_->flush(), E_OK);
    ASSERT_EQ(writer_->close(), E_OK);

    auto rows = read_rows({path(device, "a0"), path(device, "a1")});
    ASSERT_EQ(rows.size(), 4u);
    for (int row = 0; row < 3; ++row) {
        EXPECT_EQ(rows[row][1], std::to_string(row));
        EXPECT_EQ(rows[row][2], std::to_string(10 + row));
    }
    EXPECT_EQ(rows[3][1], "9");
    EXPECT_EQ(rows[3][2], "19");
}

TEST_F(SchemaCheckCacheTest, InterleavedDevicesDoNotCrossWire) {
    const std::string plain_device0 = "root.cache_plain0";
    const std::string plain_device1 = "root.cache_plain1";
    const std::string aligned_device = "root.cache_aligned_interleaved";
    for (const auto& name : {"m0", "m1"}) {
        ASSERT_EQ(writer_->register_timeseries(plain_device0, schema(name)),
                  E_OK);
        ASSERT_EQ(writer_->register_timeseries(plain_device1, schema(name)),
                  E_OK);
        ASSERT_EQ(
            writer_->register_aligned_timeseries(aligned_device, schema(name)),
            E_OK);
    }

    for (int row = 0; row < 3; ++row) {
        for (const auto& device : {plain_device0, plain_device1}) {
            auto plain_schema = schemas({"m0", "m1"});
            Tablet plain_tablet(
                device,
                std::make_shared<std::vector<MeasurementSchema>>(plain_schema),
                1);
            ASSERT_EQ(plain_tablet.add_timestamp(0, 700 + row), E_OK);
            const int device_offset = device == plain_device0 ? 0 : 100;
            ASSERT_EQ(plain_tablet.add_value(
                          0, 0, static_cast<int32_t>(device_offset + row)),
                      E_OK);
            ASSERT_EQ(plain_tablet.add_value(
                          0, 1, static_cast<int32_t>(device_offset + 10 + row)),
                      E_OK);
            ASSERT_EQ(writer_->write_tablet(plain_tablet), E_OK);
        }

        auto aligned_schema = schemas({"m0", "m1"});
        Tablet aligned_tablet(
            aligned_device,
            std::make_shared<std::vector<MeasurementSchema>>(aligned_schema),
            1);
        ASSERT_EQ(aligned_tablet.add_timestamp(0, 700 + row), E_OK);
        ASSERT_EQ(
            aligned_tablet.add_value(0, 0, static_cast<int32_t>(1000 + row)),
            E_OK);
        ASSERT_EQ(
            aligned_tablet.add_value(0, 1, static_cast<int32_t>(1010 + row)),
            E_OK);
        ASSERT_EQ(writer_->write_tablet_aligned(aligned_tablet), E_OK);
    }

    ASSERT_EQ(writer_->flush(), E_OK);
    ASSERT_EQ(writer_->close(), E_OK);
    for (const auto& device : {plain_device0, plain_device1}) {
        auto rows = read_rows({path(device, "m0"), path(device, "m1")});
        ASSERT_EQ(rows.size(), 3u);
        const int device_offset = device == plain_device0 ? 0 : 100;
        for (int row = 0; row < 3; ++row) {
            EXPECT_EQ(rows[row][1], std::to_string(device_offset + row));
            EXPECT_EQ(rows[row][2], std::to_string(device_offset + 10 + row));
        }
    }
    auto aligned_rows =
        read_rows({path(aligned_device, "m0"), path(aligned_device, "m1")});
    ASSERT_EQ(aligned_rows.size(), 3u);
    for (int row = 0; row < 3; ++row) {
        EXPECT_EQ(aligned_rows[row][1], std::to_string(1000 + row));
        EXPECT_EQ(aligned_rows[row][2], std::to_string(1010 + row));
    }
}

TEST_F(SchemaCheckCacheTest, RecordPathsUseIndependentCaches) {
    const std::string plain_device = "root.cache_record_plain";
    const std::string aligned_device = "root.cache_record_aligned";
    for (const auto& name : {"m0", "m1"}) {
        ASSERT_EQ(writer_->register_timeseries(plain_device, schema(name)),
                  E_OK);
        ASSERT_EQ(
            writer_->register_aligned_timeseries(aligned_device, schema(name)),
            E_OK);
    }

    for (int row = 0; row < 4; ++row) {
        TsRecord plain_record(800 + row, plain_device);
        plain_record.add_point("m0", static_cast<int32_t>(row));
        plain_record.add_point("m1", static_cast<int32_t>(10 + row));
        ASSERT_EQ(writer_->write_record(plain_record), E_OK);

        TsRecord aligned_record(800 + row, aligned_device);
        aligned_record.add_point("m0", static_cast<int32_t>(100 + row));
        aligned_record.add_point("m1", static_cast<int32_t>(110 + row));
        ASSERT_EQ(writer_->write_record_aligned(aligned_record), E_OK);
    }

    ASSERT_EQ(writer_->flush(), E_OK);
    ASSERT_EQ(writer_->close(), E_OK);
    auto plain_rows =
        read_rows({path(plain_device, "m0"), path(plain_device, "m1")});
    auto aligned_rows =
        read_rows({path(aligned_device, "m0"), path(aligned_device, "m1")});
    ASSERT_EQ(plain_rows.size(), 4u);
    ASSERT_EQ(aligned_rows.size(), 4u);
    for (int row = 0; row < 4; ++row) {
        EXPECT_EQ(plain_rows[row][1], std::to_string(row));
        EXPECT_EQ(plain_rows[row][2], std::to_string(10 + row));
        EXPECT_EQ(aligned_rows[row][1], std::to_string(100 + row));
        EXPECT_EQ(aligned_rows[row][2], std::to_string(110 + row));
    }
}

}  // namespace
