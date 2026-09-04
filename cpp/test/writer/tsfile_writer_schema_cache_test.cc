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

// Tests for the per-device schema-check cache in do_check_schema /
// do_check_schema_aligned (issue #885). The cache resolves chunk writers and
// data types once per device and reuses them while the tablet's measurement
// NAME SEQUENCE is unchanged. These tests pin the behaviors the cache must
// preserve:
//  1. repeated same-schema writes round-trip every row (cache hit path);
//  2. a same-column-count tablet with different names/order re-resolves and
//     writes each value into the right column (cache invalidation);
//  3. a column that was unregistered at first write is NOT masked by a cached
//     NULL after it is registered (only fully-resolved results are cached);
//  4. the aligned path keeps its own cache with the same guarantees;
//  5. per-device caches never cross-wire two devices.
#include <gtest/gtest.h>

#include "writer/tsfile_writer.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"

using namespace storage;
using namespace common;

namespace {

class SchemaCheckCacheTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        tsfile_writer_ = new TsFileWriter();
        file_name_ = std::string("tsfile_schema_cache_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(tsfile_writer_->open(file_name_, flags, 0666), common::E_OK);
    }
    void TearDown() override {
        delete tsfile_writer_;
        ASSERT_EQ(0, remove(file_name_.c_str()));
        libtsfile_destroy();
    }

    std::string file_name_;
    TsFileWriter* tsfile_writer_ = nullptr;

   public:
    static std::string generate_random_string(int length) {
        static std::atomic<uint64_t> counter{0};
        std::mt19937 gen(static_cast<unsigned int>(
            std::chrono::system_clock::now().time_since_epoch().count()));
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string random_string;
        for (int i = 0; i < length; ++i) {
            random_string += chars[dis(gen)];
        }
#ifdef _WIN32
        const auto process_id = static_cast<uint64_t>(_getpid());
#else
        const auto process_id = static_cast<uint64_t>(getpid());
#endif
        random_string += "_" + std::to_string(process_id) + "_" +
                         std::to_string(counter.fetch_add(1));
        return random_string;
    }

    // Reads back (device, measurement) pairs and returns one row per
    // timestamp: {timestamp, value-string per series}. Row count is asserted
    // by the caller so dropped rows cannot pass silently.
    std::vector<std::vector<std::string>> query_all(
        const std::vector<Path>& select_list) {
        storage::TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);
        QueryExpression* query_expr =
            QueryExpression::create(select_list, nullptr);
        ResultSet* tmp_qds = nullptr;
        EXPECT_EQ(reader.query(query_expr, tmp_qds), E_OK);
        auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

        std::vector<std::vector<std::string>> rows;
        bool has_next = false;
        while (IS_SUCC(qds->next(has_next)) && has_next) {
            RowRecord* record = qds->get_row_record();
            std::vector<std::string> row;
            row.push_back(std::to_string(record->get_timestamp()));
            // field(0) is the timestamp; value fields start at 1.
            for (size_t i = 1; i < record->get_fields()->size(); ++i) {
                row.push_back(field_to_string(record->get_field(i)));
            }
            rows.push_back(row);
        }
        reader.destroy_query_data_set(qds);
        return rows;
    }

    MeasurementSchema int32_schema(const std::string& name) {
        return MeasurementSchema(name, TSDataType::INT32, TSEncoding::PLAIN,
                                 CompressionType::UNCOMPRESSED);
    }

    static std::string field_to_string(storage::Field* value) {
        if (value->type_ == common::TEXT || value->type_ == STRING ||
            value->type_ == BLOB) {
            return std::string(value->value_.sval_);
        }
        std::stringstream ss;
        switch (value->type_) {
            case common::BOOLEAN:
                ss << (value->value_.bval_ ? "true" : "false");
                break;
            case common::INT32:
                ss << value->value_.ival_;
                break;
            case common::INT64:
            case common::TIMESTAMP:
                ss << value->value_.lval_;
                break;
            case common::FLOAT:
                ss << value->value_.fval_;
                break;
            case common::DOUBLE:
                ss << value->value_.dval_;
                break;
            case common::NULL_TYPE:
                ss << "NULL";
                break;
            default:
                ASSERT(false);
                break;
        }
        return ss.str();
    }

    // Path's two-part ctor takes non-const std::string&, so route every
    // construction through copies.
    Path make_path(const std::string& device, const std::string& measurement) {
        std::string dev = device;
        std::string meas = measurement;
        return Path(dev, meas);
    }
};

// 1. Cache hit: the same tablet schema written repeatedly (with a flush in
// between, so chunk writers survive a seal and are re-resolved from the
// cache) must round-trip every row of every column.
TEST_F(SchemaCheckCacheTest, RepeatedSameSchemaRoundTrip) {
    const std::string device = "root.cache_hit";
    const std::vector<std::string> names = {"s0", "s1", "s2"};
    for (const auto& name : names) {
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(device, int32_schema(name)),
            E_OK);
    }

    const int num_tablets = 5;
    for (int t = 0; t < num_tablets; t++) {
        std::vector<MeasurementSchema> schema_vec;
        for (const auto& name : names) schema_vec.push_back(int32_schema(name));
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 1000 + t), E_OK);
        for (uint32_t j = 0; j < names.size(); j++) {
            ASSERT_EQ(tablet.add_value(0, j, t * 100 + (int32_t)j), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        if (t == 2) {
            ASSERT_EQ(tsfile_writer_->flush(), E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<Path> select_list;
    for (const auto& name : names)
        select_list.push_back(make_path(device, name));
    auto rows = query_all(select_list);
    ASSERT_EQ(rows.size(), (size_t)num_tablets);
    for (int t = 0; t < num_tablets; t++) {
        ASSERT_EQ(rows[t][0], std::to_string(1000 + t));
        for (uint32_t j = 0; j < names.size(); j++) {
            ASSERT_EQ(rows[t][j + 1], std::to_string(t * 100 + j))
                << "row " << t << " column " << j;
        }
    }
}

// 2. Invalidation by name sequence: same column count, different names and
// order. Values must land in the column their NAME says, not the position
// the previous tablet used.
TEST_F(SchemaCheckCacheTest, SameCountDifferentNamesAndOrder) {
    const std::string device = "root.cache_inval";
    for (const auto& name : {"s0", "s1", "s2"}) {
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(device, int32_schema(name)),
            E_OK);
    }

    // Tablet 1: [s0, s1] at t=0.
    {
        std::vector<MeasurementSchema> schema_vec = {int32_schema("s0"),
                                                     int32_schema("s1")};
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 0), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, 10), E_OK);  // s0 = 10
        ASSERT_EQ(tablet.add_value(0, 1, 11), E_OK);  // s1 = 11
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }
    // Tablet 2: same count, REVERSED order, at t=1.
    {
        std::vector<MeasurementSchema> schema_vec = {int32_schema("s1"),
                                                     int32_schema("s0")};
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 1), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, 21), E_OK);  // s1 = 21
        ASSERT_EQ(tablet.add_value(0, 1, 20), E_OK);  // s0 = 20
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }
    // Tablet 3: same count, one column swapped for an unseen name, at t=2.
    {
        std::vector<MeasurementSchema> schema_vec = {int32_schema("s0"),
                                                     int32_schema("s2")};
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 2), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, 30), E_OK);  // s0 = 30
        ASSERT_EQ(tablet.add_value(0, 1, 32), E_OK);  // s2 = 32
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<Path> select_list;
    for (const auto& name : {"s0", "s1", "s2"}) {
        select_list.push_back(make_path(device, name));
    }
    auto rows = query_all(select_list);
    ASSERT_EQ(rows.size(), (size_t)3);
    // t=0
    EXPECT_EQ(rows[0][0], "0");
    EXPECT_EQ(rows[0][1], "10");
    EXPECT_EQ(rows[0][2], "11");
    // t=1: swapped order must not swap values
    EXPECT_EQ(rows[1][0], "1");
    EXPECT_EQ(rows[1][1], "20");
    EXPECT_EQ(rows[1][2], "21");
    // t=2: s1 has no point at t=2
    EXPECT_EQ(rows[2][0], "2");
    EXPECT_EQ(rows[2][1], "30");
    EXPECT_EQ(rows[2][3], "32");
}

// 3. A measurement missing at first write resolves to a NULL chunk writer
// (column skipped). After it is registered, the same tablet schema must
// write that column: the cache must not pin the stale NULL.
TEST_F(SchemaCheckCacheTest, ColumnRegisteredAfterFirstWriteIsNotMasked) {
    const std::string device = "root.cache_late_register";
    ASSERT_EQ(tsfile_writer_->register_timeseries(device, int32_schema("s0")),
              E_OK);
    // Deliberately NOT registering s1 yet.

    // First write: s1 unresolved -> NULL chunk writer, column skipped.
    {
        std::vector<MeasurementSchema> schema_vec = {int32_schema("s0"),
                                                     int32_schema("s1")};
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 0), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, 100), E_OK);
        ASSERT_EQ(tablet.add_value(0, 1, 101), E_OK);
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }
    // Now register s1 and write the same schema again.
    ASSERT_EQ(tsfile_writer_->register_timeseries(device, int32_schema("s1")),
              E_OK);
    {
        std::vector<MeasurementSchema> schema_vec = {int32_schema("s0"),
                                                     int32_schema("s1")};
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 1), E_OK);
        ASSERT_EQ(tablet.add_value(0, 0, 200), E_OK);
        ASSERT_EQ(tablet.add_value(0, 1, 201), E_OK);
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    auto rows = query_all({make_path(device, "s1")});
    // Without the fully-resolved guard the cached NULL would drop this
    // column forever and this query would return zero rows.
    ASSERT_EQ(rows.size(), (size_t)1);
    EXPECT_EQ(rows[0][0], "1");
    EXPECT_EQ(rows[0][1], "201");
}

// 4. Aligned path: same-schema repeated writes round-trip, and a reordered
// tablet re-resolves instead of reusing positions.
TEST_F(SchemaCheckCacheTest, AlignedRepeatedAndReordered) {
    const std::string device = "root.cache_aligned";
    for (const auto& name : {"a0", "a1"}) {
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(
                      device, int32_schema(name)),
                  E_OK);
    }

    const int num_tablets = 4;
    for (int t = 0; t < num_tablets; t++) {
        // Last tablet reverses the column order.
        std::vector<MeasurementSchema> schema_vec;
        if (t < num_tablets - 1) {
            schema_vec = {int32_schema("a0"), int32_schema("a1")};
        } else {
            schema_vec = {int32_schema("a1"), int32_schema("a0")};
        }
        Tablet tablet(
            device,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec), 1);
        ASSERT_EQ(tablet.add_timestamp(0, 500 + t), E_OK);
        if (t < num_tablets - 1) {
            ASSERT_EQ(tablet.add_value(0, 0, t), E_OK);       // a0
            ASSERT_EQ(tablet.add_value(0, 1, 10 + t), E_OK);  // a1
        } else {
            ASSERT_EQ(tablet.add_value(0, 0, 19), E_OK);  // a1
            ASSERT_EQ(tablet.add_value(0, 1, 9), E_OK);   // a0
        }
        ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<Path> select_list;
    for (const auto& name : {"a0", "a1"}) {
        select_list.push_back(make_path(device, name));
    }
    auto rows = query_all(select_list);
    ASSERT_EQ(rows.size(), (size_t)num_tablets);
    for (int t = 0; t < num_tablets - 1; t++) {
        EXPECT_EQ(rows[t][1], std::to_string(t));
        EXPECT_EQ(rows[t][2], std::to_string(10 + t));
    }
    // Reordered final tablet: a0=9, a1=19.
    EXPECT_EQ(rows[num_tablets - 1][1], "9");
    EXPECT_EQ(rows[num_tablets - 1][2], "19");
}

// 5. Per-device caches are independent: two devices with identical
// measurement names, interleaved writes, different values.
TEST_F(SchemaCheckCacheTest, MultiDeviceCachesIndependent) {
    const std::string devices[2] = {"root.cache_dev0", "root.cache_dev1"};
    for (const auto& device : devices) {
        for (const auto& name : {"m0", "m1"}) {
            ASSERT_EQ(
                tsfile_writer_->register_timeseries(device, int32_schema(name)),
                E_OK);
        }
    }

    for (int t = 0; t < 3; t++) {
        for (int d = 0; d < 2; d++) {
            std::vector<MeasurementSchema> schema_vec = {int32_schema("m0"),
                                                         int32_schema("m1")};
            Tablet tablet(
                devices[d],
                std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                1);
            ASSERT_EQ(tablet.add_timestamp(0, 700 + t), E_OK);
            // d*1000 separates the two devices' value spaces.
            ASSERT_EQ(tablet.add_value(0, 0, d * 1000 + t), E_OK);
            ASSERT_EQ(tablet.add_value(0, 1, d * 1000 + 10 + t), E_OK);
            ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    for (int d = 0; d < 2; d++) {
        auto rows = query_all(
            {make_path(devices[d], "m0"), make_path(devices[d], "m1")});
        ASSERT_EQ(rows.size(), (size_t)3);
        for (int t = 0; t < 3; t++) {
            EXPECT_EQ(rows[t][1], std::to_string(d * 1000 + t));
            EXPECT_EQ(rows[t][2], std::to_string(d * 1000 + 10 + t));
        }
    }
}

}  // namespace
