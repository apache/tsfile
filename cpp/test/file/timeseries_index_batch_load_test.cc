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

#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/tsfile_io_reader.h"
#include "writer/tsfile_writer.h"

using namespace common;

namespace storage {
namespace {

// Above the batch-load threshold in TsFileIOReader::get_timeseries_indexes(),
// so a request for every column takes the bulk path.
constexpr int kMeasurementCount = 40;
// Below the threshold, so the per-measurement path handles it.
constexpr int kNarrowMeasurementCount = 5;

const char* kNonAlignedDevice = "root.sg.batch_plain";
const char* kAlignedDevice = "root.sg.batch_aligned";

std::string MeasurementName(int i) { return "m" + std::to_string(i); }

// CTest runs each case as its own process in a shared working directory (and
// running the binary directly runs them all in one process), so each case
// writes to a file named after itself.
std::string TestFileName() {
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string("timeseries_index_batch_load_") +
           (info != nullptr ? info->name() : "unknown") + ".tsfile";
}

class TimeseriesIndexBatchLoadTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = TestFileName();
        std::remove(file_name_.c_str());
        TsFileWriter writer;
        ASSERT_EQ(writer.open(file_name_, O_WRONLY | O_CREAT | O_TRUNC, 0666),
                  E_OK);

        for (int i = 0; i < kMeasurementCount; i++) {
            ASSERT_EQ(writer.register_timeseries(
                          kNonAlignedDevice,
                          MeasurementSchema(MeasurementName(i), INT32, PLAIN,
                                            UNCOMPRESSED)),
                      E_OK);
        }
        for (int64_t row = 0; row < 2; row++) {
            TsRecord record(1000 + row, kNonAlignedDevice);
            for (int i = 0; i < kMeasurementCount; i++) {
                record.add_point(MeasurementName(i),
                                 static_cast<int32_t>(i * 10 + row));
            }
            ASSERT_EQ(writer.write_record(record), E_OK);
        }

        std::vector<MeasurementSchema> aligned_schemas;
        for (int i = 0; i < kMeasurementCount; i++) {
            aligned_schemas.emplace_back(MeasurementName(i), INT64, PLAIN,
                                         UNCOMPRESSED);
        }
        {
            // The writer takes ownership of the schemas of a successful
            // registration, so the raw pointers are released on success only.
            std::vector<std::unique_ptr<MeasurementSchema>> owned;
            std::vector<MeasurementSchema*> to_register;
            for (const auto& schema : aligned_schemas) {
                owned.emplace_back(new MeasurementSchema(schema));
                to_register.push_back(owned.back().get());
            }
            ASSERT_EQ(
                writer.register_aligned_timeseries(kAlignedDevice, to_register),
                E_OK);
            for (auto& schema : owned) {
                schema.release();
            }
        }
        Tablet tablet(
            kAlignedDevice,
            std::make_shared<std::vector<MeasurementSchema>>(aligned_schemas),
            4);
        for (int64_t row = 0; row < 4; row++) {
            ASSERT_EQ(tablet.add_timestamp(row, 2000 + row), E_OK);
            for (int i = 0; i < kMeasurementCount; i++) {
                ASSERT_EQ(tablet.add_value(row, static_cast<uint32_t>(i),
                                           static_cast<int64_t>(i * 100 + row)),
                          E_OK);
            }
        }
        ASSERT_EQ(writer.write_tablet_aligned(tablet), E_OK);
        ASSERT_EQ(writer.flush(), E_OK);
        ASSERT_EQ(writer.close(), E_OK);

        reader_ = std::make_shared<TsFileIOReader>();
        ASSERT_EQ(reader_->init(file_name_), E_OK);
        // The metadata must be loaded before the index lookups below; a
        // TsFileReader does this during its own init().
        TsFileMeta* tsfile_meta = nullptr;
        ASSERT_EQ(reader_->get_tsfile_meta(tsfile_meta), E_OK);
        pa_.init(512, MOD_TSFILE_READER);
    }

    void TearDown() override {
        reader_.reset();
        pa_.destroy();
        std::remove(file_name_.c_str());
        libtsfile_destroy();
    }

    // Names of every measurement of @device, all in one request set.
    static std::unordered_set<std::string> AllMeasurementNames() {
        std::unordered_set<std::string> names;
        for (int i = 0; i < kMeasurementCount; i++) {
            names.insert(MeasurementName(i));
        }
        return names;
    }

    std::string file_name_;
    std::shared_ptr<TsFileIOReader> reader_;
    PageArena pa_;
};

TEST_F(TimeseriesIndexBatchLoadTest, LoadsEveryMeasurementOfWideDevice) {
    const std::unordered_set<std::string> names = AllMeasurementNames();
    std::vector<ITimeseriesIndex*> indexes(names.size(), nullptr);
    ASSERT_EQ(reader_->get_timeseries_indexes(
                  std::make_shared<StringArrayDeviceID>(kNonAlignedDevice),
                  names, indexes, pa_),
              E_OK);

    std::map<std::string, ITimeseriesIndex*> by_name;
    for (ITimeseriesIndex* index : indexes) {
        ASSERT_NE(index, nullptr);
        EXPECT_FALSE(index->is_aligned());
        EXPECT_EQ(index->get_data_type(), INT32);
        SimpleList<ChunkMeta*>* chunks = index->get_chunk_meta_list();
        ASSERT_NE(chunks, nullptr);
        EXPECT_GE(chunks->size(), 1u);
        by_name[index->get_measurement_name().to_std_string()] = index;
    }
    EXPECT_EQ(by_name.size(), names.size());
    for (int i = 0; i < kMeasurementCount; i++) {
        EXPECT_NE(by_name.find(MeasurementName(i)), by_name.end());
    }
}

TEST_F(TimeseriesIndexBatchLoadTest, MissingMeasurementBecomesNull) {
    std::unordered_set<std::string> names = AllMeasurementNames();
    names.erase(MeasurementName(0));
    names.insert("does_not_exist");
    std::vector<ITimeseriesIndex*> indexes(names.size(), nullptr);
    ASSERT_EQ(reader_->get_timeseries_indexes(
                  std::make_shared<StringArrayDeviceID>(kNonAlignedDevice),
                  names, indexes, pa_),
              E_OK);

    int null_count = 0;
    for (ITimeseriesIndex* index : indexes) {
        if (index == nullptr) {
            null_count++;
        } else {
            EXPECT_NE(index->get_measurement_name().to_std_string(),
                      MeasurementName(0));
        }
    }
    EXPECT_EQ(null_count, 1);
}

TEST_F(TimeseriesIndexBatchLoadTest, AlignedDeviceSharesTheTimeIndex) {
    const std::unordered_set<std::string> names = AllMeasurementNames();
    std::vector<ITimeseriesIndex*> indexes(names.size(), nullptr);
    ASSERT_EQ(reader_->get_timeseries_indexes(
                  std::make_shared<StringArrayDeviceID>(kAlignedDevice), names,
                  indexes, pa_),
              E_OK);

    std::map<std::string, ITimeseriesIndex*> by_name;
    for (ITimeseriesIndex* index : indexes) {
        ASSERT_NE(index, nullptr);
        // An aligned measurement is wrapped with the device's shared time
        // index and reports the value column's type, not the time column's.
        EXPECT_TRUE(index->is_aligned());
        EXPECT_EQ(index->get_data_type(), INT64);
        ASSERT_NE(index->get_time_chunk_meta_list(), nullptr);
        ASSERT_NE(index->get_value_chunk_meta_list(), nullptr);
        EXPECT_EQ(index->get_value_chunk_meta_list()->size(), 1u);
        by_name[index->get_measurement_name().to_std_string()] = index;
    }
    EXPECT_EQ(by_name.size(), names.size());
}

TEST_F(TimeseriesIndexBatchLoadTest, NarrowRequestUsesPerMeasurementPath) {
    std::unordered_set<std::string> names;
    for (int i = 0; i < kNarrowMeasurementCount; i++) {
        names.insert(MeasurementName(i));
    }
    names.insert("absent_measurement");
    std::vector<ITimeseriesIndex*> indexes(names.size(), nullptr);
    ASSERT_EQ(reader_->get_timeseries_indexes(
                  std::make_shared<StringArrayDeviceID>(kNonAlignedDevice),
                  names, indexes, pa_),
              E_OK);

    int found = 0;
    for (ITimeseriesIndex* index : indexes) {
        if (index == nullptr) continue;
        found++;
        EXPECT_EQ(index->get_measurement_name().to_std_string().substr(0, 1),
                  "m");
        EXPECT_EQ(index->get_data_type(), INT32);
    }
    EXPECT_EQ(found, kNarrowMeasurementCount);
}

// The bulk path must return the same chunk metadata as the per-measurement
// path (a different loader: binary search + deserialize + clone per column).
TEST_F(TimeseriesIndexBatchLoadTest, MatchesPerMeasurementPath) {
    const std::unordered_set<std::string> names = AllMeasurementNames();
    std::vector<ITimeseriesIndex*> bulk(names.size(), nullptr);
    ASSERT_EQ(reader_->get_timeseries_indexes(
                  std::make_shared<StringArrayDeviceID>(kNonAlignedDevice),
                  names, bulk, pa_),
              E_OK);

    std::unordered_set<std::string> narrow_names;
    for (int i = 0; i < kNarrowMeasurementCount; i++) {
        narrow_names.insert(MeasurementName(i));
    }
    std::vector<ITimeseriesIndex*> narrow(narrow_names.size(), nullptr);
    ASSERT_EQ(reader_->get_timeseries_indexes(
                  std::make_shared<StringArrayDeviceID>(kNonAlignedDevice),
                  narrow_names, narrow, pa_),
              E_OK);

    std::map<std::string, ITimeseriesIndex*> expected;
    for (ITimeseriesIndex* index : narrow) {
        ASSERT_NE(index, nullptr);
        expected[index->get_measurement_name().to_std_string()] = index;
    }
    EXPECT_EQ(expected.size(), narrow_names.size());

    for (ITimeseriesIndex* index : bulk) {
        ASSERT_NE(index, nullptr);
        const std::string name = index->get_measurement_name().to_std_string();
        auto it = expected.find(name);
        if (it == expected.end()) continue;
        SimpleList<ChunkMeta*>* got = index->get_chunk_meta_list();
        SimpleList<ChunkMeta*>* want = it->second->get_chunk_meta_list();
        ASSERT_EQ(got->size(), want->size()) << name;
        SimpleList<ChunkMeta*>::Iterator got_it = got->begin();
        SimpleList<ChunkMeta*>::Iterator want_it = want->begin();
        while (got_it != got->end()) {
            EXPECT_EQ(got_it.get()->offset_of_chunk_header_,
                      want_it.get()->offset_of_chunk_header_)
                << name;
            got_it++;
            want_it++;
        }
    }
}

}  // namespace
}  // namespace storage
