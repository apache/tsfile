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

// Writer-owned schema lookup cache (issue #885): tree records/tablets and
// table FIELD columns share one positional cache. Exercise repeated writes,
// name/width changes, late registration, device/writer isolation and lifecycle
// changes by checking the values and timestamps read from the resulting files.
#include <gtest/gtest.h>

#include "writer/tsfile_writer.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <fstream>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/restorable_tsfile_io_writer.h"
#include "file/write_file.h"
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
        const std::vector<Path>& select_list,
        const std::string& file_name = "") {
        storage::TsFileReader reader;
        EXPECT_EQ(reader.open(file_name.empty() ? file_name_ : file_name),
                  E_OK);
        QueryExpression* query_expr =
            QueryExpression::create(select_list, nullptr);
        ResultSet* tmp_qds = nullptr;
        EXPECT_EQ(reader.query(query_expr, tmp_qds), E_OK);
        auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

        std::vector<std::vector<std::string>> rows;
        bool has_next = false;
        int ret = E_OK;
        while (IS_SUCC(ret = qds->next(has_next)) && has_next) {
            RowRecord* record = qds->get_row_record();
            std::vector<std::string> row;
            row.push_back(std::to_string(record->get_timestamp()));
            // field(0) is the timestamp; value fields start at 1.
            for (size_t i = 1; i < record->get_fields()->size(); ++i) {
                row.push_back(field_to_string(record->get_field(i)));
            }
            rows.push_back(row);
        }
        EXPECT_EQ(ret, E_OK);
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

// 5. Switching devices must discard pointers from the previous device even
// when their measurement names are identical.
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

class TreeSchemaCacheTest : public SchemaCheckCacheTest,
                            public ::testing::WithParamInterface<bool> {};

TEST_P(TreeSchemaCacheTest, ChangingWidthsAcrossRecordsAndTablets) {
    const std::string device = "root.cache_width";
    const std::vector<std::string> names = {"i", "d", "l", "unregistered"};
    const std::vector<TSDataType> types = {INT32, DOUBLE, INT64, INT32};
    for (size_t i = 0; i < 3; ++i) {
        MeasurementSchema schema(names[i], types[i], PLAIN, UNCOMPRESSED);
        ASSERT_EQ(
            GetParam()
                ? tsfile_writer_->register_aligned_timeseries(device, schema)
                : tsfile_writer_->register_timeseries(device, schema),
            E_OK);
    }
    std::vector<std::vector<int>> orders = {{0, 1, 2}, {1}, {2, 0},
                                            {2, 1, 0}, {0}, {0, 1, 2}};
    std::vector<std::vector<std::string>> expected;
    int64_t time = 0;
    for (const auto& order : orders) {
        std::vector<std::string> cols;
        std::vector<TSDataType> col_types;
        for (int i : order) {
            cols.push_back(names[i]);
            col_types.push_back(types[i]);
        }
        // Same schema first as a record, then as a tablet: both callers share
        // the cache but must preserve the registered type and column order.
        for (int repeat = 0; repeat < 2; ++repeat, ++time) {
            Tablet tablet(device, &cols, &col_types, 1);
            TsRecord record(device, time);
            ASSERT_EQ(tablet.add_timestamp(0, time), E_OK);
            std::vector<std::string> row = {std::to_string(time), "NULL",
                                            "NULL", "NULL"};
            for (int i : order) {
                if (i == 0) {
                    ASSERT_EQ(record.add_point(names[i], int32_t(10 + time)),
                              E_OK);
                    ASSERT_EQ(tablet.add_value(0, names[i], int32_t(10 + time)),
                              E_OK);
                    row[i + 1] = std::to_string(10 + time);
                } else if (i == 1) {
                    ASSERT_EQ(record.add_point(names[i], double(20.5 + time)),
                              E_OK);
                    ASSERT_EQ(
                        tablet.add_value(0, names[i], double(20.5 + time)),
                        E_OK);
                    std::ostringstream value;
                    value << 20.5 + time;
                    row[i + 1] = value.str();
                } else if (i == 2) {
                    ASSERT_EQ(record.add_point(names[i],
                                               int64_t(5000000000LL + time)),
                              E_OK);
                    ASSERT_EQ(tablet.add_value(0, names[i],
                                               int64_t(5000000000LL + time)),
                              E_OK);
                    row[i + 1] = std::to_string(5000000000LL + time);
                } else {
                    ASSERT_EQ(record.add_point(names[i], int32_t(7)), E_OK);
                    ASSERT_EQ(tablet.add_value(0, names[i], int32_t(7)), E_OK);
                }
            }
            ASSERT_EQ(repeat == 0 ? tsfile_writer_->write_tree(record)
                                  : tsfile_writer_->write_tree(tablet),
                      E_OK);
            expected.push_back(row);
        }
        // A field omitted for an entire chunk must retain its time position.
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    EXPECT_EQ(query_all({make_path(device, "i"), make_path(device, "d"),
                         make_path(device, "l")}),
              expected);
}

TEST_P(TreeSchemaCacheTest, WritersWithSameNamesKeepTheirOwnTypesAndValues) {
    const std::string second_file = file_name_ + ".second";
    const std::string device = "root.same";
    {
        TsFileWriter second;
        ASSERT_EQ(second.open(second_file), E_OK);
        MeasurementSchema small("s", INT32, PLAIN, UNCOMPRESSED);
        MeasurementSchema wide("s", INT64, PLAIN, UNCOMPRESSED);
        ASSERT_EQ(
            GetParam()
                ? tsfile_writer_->register_aligned_timeseries(device, small)
                : tsfile_writer_->register_timeseries(device, small),
            E_OK);
        ASSERT_EQ(GetParam() ? second.register_aligned_timeseries(device, wide)
                             : second.register_timeseries(device, wide),
                  E_OK);
        for (int t = 0; t < 4; ++t) {
            TsRecord first_record(device, t);
            TsRecord second_record(device, t);
            ASSERT_EQ(first_record.add_point("s", int32_t(10 + t)), E_OK);
            ASSERT_EQ(second_record.add_point("s", int64_t(5000000000LL + t)),
                      E_OK);
            ASSERT_EQ(tsfile_writer_->write_tree(first_record), E_OK);
            ASSERT_EQ(second.write_tree(second_record), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
        ASSERT_EQ(tsfile_writer_->close(), E_OK);
        ASSERT_EQ(second.flush(), E_OK);
        ASSERT_EQ(second.close(), E_OK);
    }
    EXPECT_EQ(query_all({make_path(device, "s")}),
              (std::vector<std::vector<std::string>>{
                  {"0", "10"}, {"1", "11"}, {"2", "12"}, {"3", "13"}}));
    EXPECT_EQ(query_all({make_path(device, "s")}, second_file),
              (std::vector<std::vector<std::string>>{{"0", "5000000000"},
                                                     {"1", "5000000001"},
                                                     {"2", "5000000002"},
                                                     {"3", "5000000003"}}));
    EXPECT_EQ(remove(second_file.c_str()), 0);
}

TEST_P(TreeSchemaCacheTest, DestroyAndInitDoNotReuseOldSchemaPointers) {
    const std::string device = "root.reused";
    MeasurementSchema first("s", INT32, PLAIN, UNCOMPRESSED);
    ASSERT_EQ(GetParam()
                  ? tsfile_writer_->register_aligned_timeseries(device, first)
                  : tsfile_writer_->register_timeseries(device, first),
              E_OK);
    TsRecord record(device, 1);
    ASSERT_EQ(record.add_point("s", int32_t(1)), E_OK);
    ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    tsfile_writer_->destroy();
    ASSERT_EQ(remove(file_name_.c_str()), 0);
    WriteFile file;
    ASSERT_EQ(file.create(file_name_, O_RDWR | O_CREAT | O_TRUNC, 0666), E_OK);
    ASSERT_EQ(tsfile_writer_->init(&file), E_OK);
    MeasurementSchema second("s", INT64, PLAIN, UNCOMPRESSED);
    ASSERT_EQ(GetParam()
                  ? tsfile_writer_->register_aligned_timeseries(device, second)
                  : tsfile_writer_->register_timeseries(device, second),
              E_OK);
    TsRecord next(device, 0);
    ASSERT_EQ(next.add_point("s", int64_t(5000000000LL)), E_OK);
    ASSERT_EQ(tsfile_writer_->write_tree(next), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    tsfile_writer_->destroy();
    EXPECT_EQ(query_all({make_path(device, "s")}),
              (std::vector<std::vector<std::string>>{{"0", "5000000000"}}));
}

TEST_P(TreeSchemaCacheTest, EvictedDevicesCanBeWrittenAgain) {
    const int device_count = 80;  // Exceeds the writer's bounded cache.
    for (int d = 0; d < device_count; ++d) {
        const std::string device = "root.eviction.d" + std::to_string(d);
        ASSERT_EQ(GetParam() ? tsfile_writer_->register_aligned_timeseries(
                                   device, int32_schema("s"))
                             : tsfile_writer_->register_timeseries(
                                   device, int32_schema("s")),
                  E_OK);
    }
    for (int t = 0; t < 2; ++t) {
        for (int d = 0; d < device_count; ++d) {
            const std::string device = "root.eviction.d" + std::to_string(d);
            TsRecord record(device, t);
            ASSERT_EQ(record.add_point("s", int32_t(t * 1000 + d)), E_OK);
            ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    for (int d = 0; d < device_count; ++d) {
        EXPECT_EQ(
            query_all({make_path("root.eviction.d" + std::to_string(d), "s")}),
            (std::vector<std::vector<std::string>>{
                {"0", std::to_string(d)}, {"1", std::to_string(1000 + d)}}));
    }
}

TEST_F(SchemaCheckCacheTest, TableWriteContextsSurviveCacheEviction) {
    const std::vector<ColumnSchema> columns = {
        ColumnSchema("tag", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG),
        ColumnSchema("i", INT32, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD),
        ColumnSchema("l", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD)};
    auto schema = std::make_shared<TableSchema>("eviction", columns);
    ASSERT_EQ(tsfile_writer_->register_table(schema), E_OK);
    const int device_count = 80;
    for (int batch = 0; batch < 2; ++batch) {
        Tablet tablet("eviction", schema->get_measurement_names(),
                      schema->get_data_types(), schema->get_column_categories(),
                      device_count);
        for (int d = 0; d < device_count; ++d) {
            const std::string name = "d" + std::to_string(d);
            String tag(const_cast<char*>(name.data()), name.size());
            const int64_t time = batch * 1000 + d;
            ASSERT_EQ(tablet.add_timestamp(d, time), E_OK);
            ASSERT_EQ(tablet.add_value(d, "tag", tag), E_OK);
            ASSERT_EQ(tablet.add_value(d, "i", int32_t(d)), E_OK);
            ASSERT_EQ(tablet.add_value(d, "l", int64_t(5000000000LL + time)),
                      E_OK);
        }
        // The first device's cache entry is evicted before its saved write
        // context is consumed; the actual schema/chunk writers must survive.
        ASSERT_EQ(tsfile_writer_->write_table(tablet), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* result = nullptr;
    ASSERT_EQ(reader.query("eviction", {"tag", "i", "l"}, INT64_MIN, INT64_MAX,
                           result),
              E_OK);
    std::set<int64_t> seen;
    bool has_next = false;
    int ret = E_OK;
    while ((ret = result->next(has_next)) == E_OK && has_next) {
        const int64_t time = result->get_value<int64_t>(1);
        EXPECT_TRUE(time / 1000 == 0 || time / 1000 == 1);
        EXPECT_GE(time % 1000, 0);
        EXPECT_LT(time % 1000, device_count);
        EXPECT_TRUE(seen.insert(time).second);
        const auto* tag = result->get_value<String*>(2);
        EXPECT_EQ(std::string(tag->buf_, tag->len_),
                  "d" + std::to_string(time % 1000));
        EXPECT_EQ(result->get_value<int32_t>(3), time % 1000);
        EXPECT_EQ(result->get_value<int64_t>(4), 5000000000LL + time);
    }
    EXPECT_EQ(ret, E_OK);
    EXPECT_EQ(seen.size(), 2U * device_count);
    reader.destroy_query_data_set(result);
}

INSTANTIATE_TEST_SUITE_P(PlainAndAligned, TreeSchemaCacheTest,
                         ::testing::Bool());

TEST_F(SchemaCheckCacheTest,
       TableFieldsReorderAcrossDevicesTablesAndTreeWrites) {
    const std::vector<ColumnSchema> columns = {
        ColumnSchema("tag", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG),
        ColumnSchema("i", INT32, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD),
        ColumnSchema("l", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD)};
    for (const auto& table : {"cache_a", "cache_b"}) {
        ASSERT_EQ(tsfile_writer_->register_table(
                      std::make_shared<TableSchema>(table, columns)),
                  E_OK);
    }
    ASSERT_EQ(
        tsfile_writer_->register_timeseries("root.tree", int32_schema("i")),
        E_OK);
    const std::vector<std::vector<std::string>> orders = {
        {"tag", "i", "l"}, {"tag", "i", "l"}, {"l", "tag", "i"},
        {"l", "i", "tag"}, {"tag", "i"},      {"tag", "i", "l"}};
    for (size_t batch = 0; batch < orders.size(); ++batch) {
        const auto& names = orders[batch];
        std::vector<TSDataType> types;
        std::vector<ColumnCategory> categories;
        for (const auto& name : names) {
            types.push_back(name == "tag" ? STRING
                            : name == "i" ? INT32
                                          : INT64);
            categories.push_back(name == "tag" ? ColumnCategory::TAG
                                               : ColumnCategory::FIELD);
        }
        for (const auto& table : {"cache_a", "cache_b"}) {
            // Repeating a single device exercises hits; A/B/A in one tablet
            // also checks the write contexts survive cache replacement.
            for (int repeat = 0; repeat < 2; ++repeat) {
                const int count = batch == 2 ? 3 : 1;
                auto write_names = names;
                auto write_types = types;
                auto write_categories = categories;
                if (batch == 3 && repeat == 1) {
                    // Reorder FIELDs while this table/device's cache is warm.
                    std::rotate(write_names.begin(), write_names.begin() + 1,
                                write_names.end());
                    std::rotate(write_types.begin(), write_types.begin() + 1,
                                write_types.end());
                    std::rotate(write_categories.begin(),
                                write_categories.begin() + 1,
                                write_categories.end());
                }
                Tablet tablet(table, write_names, write_types, write_categories,
                              count);
                for (int r = 0; r < count; ++r) {
                    int64_t time = batch * 10 + repeat * 3 + r;
                    String tag(r == 1 ? "b" : "a", 1);
                    ASSERT_EQ(tablet.add_timestamp(r, time), E_OK);
                    ASSERT_EQ(tablet.add_value(r, "tag", tag), E_OK);
                    ASSERT_EQ(tablet.add_value(r, "i", int32_t(100 + time)),
                              E_OK);
                    if (batch != 4) {
                        ASSERT_EQ(tablet.add_value(
                                      r, "l", int64_t(5000000000LL + time)),
                                  E_OK);
                    }
                }
                ASSERT_EQ(tsfile_writer_->write_table(tablet), E_OK);
            }
        }
        TsRecord tree("root.tree", batch);
        ASSERT_EQ(tree.add_point("i", int32_t(batch)), E_OK);
        ASSERT_EQ(tsfile_writer_->write_tree(tree), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    for (const auto& table : {"cache_a", "cache_b"}) {
        ResultSet* result = nullptr;
        ASSERT_EQ(reader.query(table, {"tag", "i", "l"}, INT64_MIN, INT64_MAX,
                               result),
                  E_OK);
        bool has_next = false;
        int ret = E_OK;
        int rows = 0;
        std::set<int64_t> remaining_times = {0,  3,  10, 13, 20, 21, 22, 23,
                                             24, 25, 30, 33, 40, 43, 50, 53};
        while ((ret = result->next(has_next)) == E_OK && has_next) {
            const int64_t time = result->get_value<int64_t>(1);
            EXPECT_EQ(remaining_times.erase(time), 1U);
            EXPECT_EQ(result->get_value<int32_t>(3), 100 + time);
            EXPECT_EQ(result->is_null(4), time / 10 == 4);
            if (!result->is_null(4))
                EXPECT_EQ(result->get_value<int64_t>(4), 5000000000LL + time);
            const auto* tag = result->get_value<String*>(2);
            EXPECT_EQ(std::string(tag->buf_, tag->len_),
                      time == 21 || time == 24 ? "b" : "a");
            ++rows;
        }
        EXPECT_EQ(ret, E_OK);
        EXPECT_EQ(rows, 16);
        EXPECT_TRUE(remaining_times.empty());
        reader.destroy_query_data_set(result);
    }
    EXPECT_EQ(query_all({make_path("root.tree", "i")}),
              (std::vector<std::vector<std::string>>{{"0", "0"},
                                                     {"1", "1"},
                                                     {"2", "2"},
                                                     {"3", "3"},
                                                     {"4", "4"},
                                                     {"5", "5"}}));
}

class SparseAlignedSchemaTest : public SchemaCheckCacheTest,
                                public ::testing::WithParamInterface<bool> {
   protected:
    ConfigValue saved_config_;
    void SetUp() override {
        saved_config_ = g_config_value_;
        SchemaCheckCacheTest::SetUp();
        g_config_value_.page_writer_max_point_num_ = 4;
        g_config_value_.page_writer_max_memory_bytes_ = 1024 * 1024;
        g_config_value_.parallel_write_enabled_ = GetParam();
        g_config_value_.parallel_read_enabled_ = GetParam();
    }
    void TearDown() override {
        SchemaCheckCacheTest::TearDown();
        g_config_value_ = saved_config_;
    }
};

INSTANTIATE_TEST_SUITE_P(SerialAndParallel, SparseAlignedSchemaTest,
                         ::testing::Bool());

TEST_P(SparseAlignedSchemaTest, MissingFieldsWithinPagesAndAcrossChunks) {
    const std::string device = "root.sparse";
    for (const auto& name : {"x", "y"}) {
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(
                      device, int32_schema(name)),
                  E_OK);
    }
    const std::vector<ColumnSchema> columns = {
        ColumnSchema("tag", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG),
        ColumnSchema("x", INT32, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD),
        ColumnSchema("y", INT32, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD)};
    ASSERT_EQ(tsfile_writer_->register_table(
                  std::make_shared<TableSchema>("sparse", columns)),
              E_OK);
    const std::vector<std::vector<std::string>> fields = {
        {"x"}, {"x"}, {"y", "x"}, {"y"}, {}, {"x"}};
    std::vector<std::vector<std::string>> expected_tree;
    for (size_t batch = 0; batch < fields.size(); ++batch) {
        const auto& names = fields[batch];
        const std::vector<TSDataType> types(names.size(), INT32);
        Tablet tree(device, &names, &types, 7);
        auto table_names = names;
        auto table_types = types;
        std::vector<ColumnCategory> categories(names.size(),
                                               ColumnCategory::FIELD);
        table_names.push_back("tag");
        table_types.push_back(STRING);
        categories.push_back(ColumnCategory::TAG);
        Tablet table("sparse", table_names, table_types, categories, 14);
        for (int r = 0; r < 7; ++r) {
            const int64_t time = batch * 7 + r;
            TsRecord record(device, time);
            ASSERT_EQ(tree.add_timestamp(r, time), E_OK);
            std::vector<std::string> expected = {std::to_string(time), "NULL",
                                                 "NULL"};
            for (const auto& name : names) {
                const int32_t value = (name == "x" ? 100 : 200) + time;
                ASSERT_EQ(record.add_point(name, value), E_OK);
                ASSERT_EQ(tree.add_value(r, name, value), E_OK);
                expected[name == "x" ? 1 : 2] = std::to_string(value);
            }
            if (batch % 2 == 0)
                ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
            expected_tree.push_back(expected);
            for (int d = 0; d < 2; ++d) {
                const int row = r * 2 + d;
                ASSERT_EQ(table.add_timestamp(row, time), E_OK);
                ASSERT_EQ(
                    table.add_value(row, "tag", String(d == 0 ? "a" : "b", 1)),
                    E_OK);
                for (const auto& name : names) {
                    ASSERT_EQ(table.add_value(
                                  row, name,
                                  int32_t((name == "x" ? 100 : 200) + time)),
                              E_OK);
                }
            }
        }
        if (batch % 2 == 1) ASSERT_EQ(tsfile_writer_->write_tree(tree), E_OK);
        ASSERT_EQ(tsfile_writer_->write_table(table), E_OK);
        if (batch % 2 == 1) ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    EXPECT_EQ(query_all({make_path(device, "x"), make_path(device, "y")}),
              expected_tree);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    // A sparse-only projection must preserve even the all-null table rows.
    for (const auto& projection : std::vector<std::vector<std::string>>{
             {"tag", "x", "y"}, {"tag", "y"}}) {
        for (const auto& range :
             std::vector<std::pair<int64_t, int64_t>>{{0, 41}, {5, 36}}) {
            ResultSet* result = nullptr;
            ASSERT_EQ(reader.query("sparse", projection, range.first,
                                   range.second, result),
                      E_OK);
            std::set<std::pair<int64_t, std::string>> rows;
            bool next = false;
            int ret = E_OK;
            while ((ret = result->next(next)) == E_OK && next) {
                const int64_t time = result->get_value<int64_t>(1);
                EXPECT_GE(time, range.first);
                EXPECT_LE(time, range.second);
                const auto* tag = result->get_value<String*>(2);
                EXPECT_TRUE(
                    rows.emplace(time, std::string(tag->buf_, tag->len_))
                        .second);
                for (size_t i = 1; i < projection.size(); ++i) {
                    const auto& name = projection[i];
                    const bool present = name == "x"
                                             ? time / 7 <= 2 || time / 7 == 5
                                             : time / 7 == 2 || time / 7 == 3;
                    EXPECT_EQ(result->is_null(i + 2), !present)
                        << time << ":" << name;
                    if (present && !result->is_null(i + 2)) {
                        EXPECT_EQ(result->get_value<int32_t>(i + 2),
                                  (name == "x" ? 100 : 200) + time);
                    }
                }
            }
            EXPECT_EQ(ret, E_OK);
            EXPECT_EQ(rows.size(), 2U * (range.second - range.first + 1));
            reader.destroy_query_data_set(result);
        }
    }
}

TEST_P(SparseAlignedSchemaTest, LateFieldBackfillsSealedAndOpenPages) {
    std::vector<std::string> devices;
    for (int count : {2, 4, 9, 13}) {
        const std::string device = "root.late" + std::to_string(count);
        devices.push_back(device);
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(
                      device, int32_schema("x")),
                  E_OK);
        for (int64_t time = 0; time < count; ++time) {
            TsRecord record(device, time);
            ASSERT_EQ(record.add_point("x", int32_t(100 + time)), E_OK);
            // The unresolved cache entry must be rechecked after registration.
            ASSERT_EQ(record.add_point("y", int32_t(200 + time)), E_OK);
            ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(
                      device, int32_schema("y")),
                  E_OK);
        for (int64_t time = count; time < 13; ++time) {
            TsRecord record(device, time);
            ASSERT_EQ(record.add_point("x", int32_t(100 + time)), E_OK);
            if (time % 2)
                ASSERT_EQ(record.add_point("y", int32_t(200 + time)), E_OK);
            ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    for (const auto& device : devices) {
        EXPECT_EQ(tsfile_writer_->register_aligned_timeseries(
                      device, int32_schema("z")),
                  E_NOT_SUPPORT);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    for (int count : {2, 4, 9, 13}) {
        const std::string device = "root.late" + std::to_string(count);
        std::vector<std::vector<std::string>> expected;
        for (int64_t time = 0; time < 13; ++time) {
            expected.push_back(
                {std::to_string(time), std::to_string(100 + time),
                 time >= count && time % 2 ? std::to_string(200 + time)
                                           : "NULL"});
        }
        EXPECT_EQ(query_all({make_path(device, "x"), make_path(device, "y")}),
                  expected);
    }
}

TEST_P(SparseAlignedSchemaTest,
       RecoveryKeepsSingleEmptyPageAndFollowingChunks) {
    const std::string device = "root.recovered_late";
    ASSERT_EQ(
        tsfile_writer_->register_aligned_timeseries(device, int32_schema("x")),
        E_OK);
    for (int64_t time = 0; time < 4; ++time) {
        TsRecord record(device, time);
        ASSERT_EQ(record.add_point("x", int32_t(100 + time)), E_OK);
        ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
    }
    // Exactly one time page is sealed. Registration followed immediately by
    // flush produces a value chunk containing just one empty-page header.
    ASSERT_EQ(
        tsfile_writer_->register_aligned_timeseries(device, int32_schema("y")),
        E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    TsRecord record(device, 4);
    ASSERT_EQ(record.add_point("x", int32_t(104)), E_OK);
    ASSERT_EQ(record.add_point("y", int32_t(204)), E_OK);
    ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    // Leave the flushed chunks without a footer to exercise crash recovery.
    tsfile_writer_->destroy();
    int64_t flushed_size = 0;
    {
        std::ifstream file(file_name_, std::ios::binary | std::ios::ate);
        ASSERT_TRUE(file.is_open());
        flushed_size = static_cast<int64_t>(file.tellg());
    }
    {
        RestorableTsFileIOWriter recovered;
        ASSERT_EQ(recovered.open(file_name_, true), E_OK);
        ASSERT_EQ(recovered.get_truncated_size(), flushed_size);
        ASSERT_EQ(recovered.get_recovered_chunk_group_metas().size(), 2U);
        TsFileWriter resumed;
        ASSERT_EQ(resumed.init(&recovered), E_OK);
        ASSERT_EQ(resumed.close(), E_OK);
    }
    std::vector<std::vector<std::string>> expected;
    for (int64_t time = 0; time < 5; ++time) {
        expected.push_back({std::to_string(time), std::to_string(100 + time),
                            time >= 4 ? std::to_string(200 + time) : "NULL"});
    }
    EXPECT_EQ(query_all({make_path(device, "x"), make_path(device, "y")}),
              expected);
}

TEST_P(SparseAlignedSchemaTest,
       RecordAfterFullTabletKeepsOmittedColumnAligned) {
    const std::string device = "root.tablet_record";
    ASSERT_EQ(
        tsfile_writer_->register_aligned_timeseries(device, int32_schema("x")),
        E_OK);
    ASSERT_EQ(
        tsfile_writer_->register_aligned_timeseries(device, int32_schema("y")),
        E_OK);
    const std::vector<std::string> names = {"x"};
    const std::vector<TSDataType> types = {INT32};
    Tablet tablet(device, &names, &types, 4);
    for (int r = 0; r < 4; ++r) {
        ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
        ASSERT_EQ(tablet.add_value(r, "x", int32_t(100 + r)), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tree(tablet), E_OK);
    std::vector<std::vector<std::string>> expected;
    for (int64_t time = 0; time < 7; ++time) {
        if (time >= 4) {
            TsRecord record(device, time);
            ASSERT_EQ(record.add_point("x", int32_t(100 + time)), E_OK);
            if (time == 5) ASSERT_EQ(record.add_point("y", int32_t(205)), E_OK);
            ASSERT_EQ(tsfile_writer_->write_tree(record), E_OK);
        }
        expected.push_back({std::to_string(time), std::to_string(100 + time),
                            time == 5 ? "205" : "NULL"});
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    EXPECT_EQ(query_all({make_path(device, "x"), make_path(device, "y")}),
              expected);
}

}  // namespace
