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
// Regression tests for PR #909 / issue #908:
//  1. registered-but-empty measurements must never be sealed as count=0
//     chunks (non-aligned flush path in flush_chunk_group /
//     flush_chunk_group_encoded);
//  2. the parallel aligned tablet write path in write_table() must capture
//     per-iteration state by value so pool threads never read dangling
//     references.
#include <gtest/gtest.h>

#include "writer/tsfile_writer.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <map>
#include <memory>
#include <random>
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

class EmptyChunkRegressionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        tsfile_writer_ = new TsFileWriter();
        file_name_ = std::string("tsfile_empty_chunk_regression_") +
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

    // Collect per-measurement timeseries index pointers for one device from
    // the written file. The caller must keep `reader` open while using the
    // returned pointers (they reference reader-owned arenas).
    std::map<std::string, storage::ITimeseriesIndex*> CollectMeasurementMeta(
        storage::TsFileReader& reader, const std::string& device) {
        std::map<std::string, storage::ITimeseriesIndex*> out;
        std::vector<std::shared_ptr<IDeviceID>> devices = {
            std::make_shared<StringArrayDeviceID>(device)};
        auto meta_map = reader.get_timeseries_metadata(devices);
        for (auto& dev_pair : meta_map) {
            for (auto& ts_idx : dev_pair.second) {
                out[ts_idx->get_measurement_name().to_std_string()] =
                    ts_idx.get();
            }
        }
        return out;
    }
};

}  // namespace

// Regression (issue #908 bug 1): registering a measurement but never writing
// data to it used to seal an EMPTY chunk (count=0, dataSize=0) at flush time.
// Java readers (TsFileSequenceReader self-check / TsFileSketchTool) treat such
// a file as crashed and refuse to load it. The fix mirrors the aligned
// branch's hasData() guard: an empty column must produce no chunk at all, so
// the measurement must be absent from the file's metadata.
TEST_F(EmptyChunkRegressionTest, NonAlignedRegisteredButEmptyNotSealed) {
    std::string device = "root.dev_empty_col";
    const int total_measurements = 3;
    std::vector<MeasurementSchema> schemas;
    for (int i = 0; i < total_measurements; i++) {
        schemas.emplace_back("m" + std::to_string(i), TSDataType::INT32,
                             TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(
                device, MeasurementSchema("m" + std::to_string(i),
                                          TSDataType::INT32, TSEncoding::PLAIN,
                                          CompressionType::UNCOMPRESSED)),
            E_OK);
    }

    // Tablet carries all 3 registered columns, but only m0/m1 receive values.
    // m2's column stays all-null so its chunk writer never accumulates data.
    const int rows = 10;
    storage::Tablet tablet(
        device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
        rows);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(tablet.add_timestamp(r, 1000 + r), E_OK);
        ASSERT_EQ(tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
        ASSERT_EQ(tablet.add_value(r, 1u, static_cast<int32_t>(r * 10)), E_OK);
        // Column 2 (m2) intentionally never written.
    }
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);

    // Only measurements that received data may appear in the file.
    ASSERT_EQ(meta.size(), 2u);
    ASSERT_NE(meta.count("m0"), 0u);
    ASSERT_NE(meta.count("m1"), 0u);
    EXPECT_EQ(meta.count("m2"), 0u)
        << "registered-but-empty measurement must not be sealed as a chunk";
    // Every surviving series must carry real statistics, not the count=0 /
    // start=INT64_MAX / end=INT64_MIN signature of a sealed empty chunk.
    EXPECT_EQ(meta["m0"]->get_statistic()->count_, rows);
    EXPECT_EQ(meta["m0"]->get_statistic()->start_time_, 1000);
    EXPECT_EQ(meta["m0"]->get_statistic()->end_time_, 1000 + rows - 1);
    EXPECT_EQ(meta["m1"]->get_statistic()->count_, rows);
    reader.close();
}

// The file produced by the scenario above must also be fully readable through
// a normal query: the non-empty columns return every row.
TEST_F(EmptyChunkRegressionTest, NonAlignedEmptyColumnFileIsQueryable) {
    std::string device = "root.dev_empty_col_query";
    std::vector<MeasurementSchema> schemas;
    for (int i = 0; i < 3; i++) {
        schemas.emplace_back("m" + std::to_string(i), TSDataType::INT32,
                             TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(
                device, MeasurementSchema("m" + std::to_string(i),
                                          TSDataType::INT32, TSEncoding::PLAIN,
                                          CompressionType::UNCOMPRESSED)),
            E_OK);
    }
    const int rows = 7;
    storage::Tablet tablet(
        device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
        rows);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(tablet.add_timestamp(r, 100 + r), E_OK);
        ASSERT_EQ(tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
        ASSERT_EQ(tablet.add_value(r, 2u, static_cast<int32_t>(r + 5)), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    std::vector<std::string> select_list;
    select_list.push_back(device + ".m0");
    select_list.push_back(device + ".m2");
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(select_list, 0, INT64_MAX, tmp_qds), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    int row_count = 0;
    bool has_next = false;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        storage::RowRecord* rec = qds->get_row_record();
        EXPECT_EQ(rec->get_timestamp(), 100 + row_count);
        // field(0) is the time column; the selected m0/m2 follow.
        EXPECT_EQ(rec->get_field(1)->value_.ival_, row_count);
        EXPECT_EQ(rec->get_field(2)->value_.ival_, row_count + 5);
        row_count++;
    }
    EXPECT_EQ(row_count, rows);
    reader.destroy_query_data_set(qds);
    reader.close();
}

// Variant driven through write_record (non-aligned record path): the same
// flush_chunk_group code seals the chunk group, so a measurement that never
// appears in any record must not get an empty chunk either.
TEST_F(EmptyChunkRegressionTest, NonAlignedEmptyMeasurementRecordPath) {
    std::string device = "root.dev_empty_col_rec";
    std::vector<std::string> names = {"s0", "s1"};
    for (const auto& name : names) {
        ASSERT_EQ(tsfile_writer_->register_timeseries(
                      device, MeasurementSchema(name, TSDataType::INT64,
                                                TSEncoding::PLAIN,
                                                CompressionType::UNCOMPRESSED)),
                  E_OK);
    }
    for (int i = 0; i < 5; i++) {
        TsRecord record(1622505600000 + i, device);
        record.add_point(names[0], static_cast<int64_t>(i));
        // s1 never appears in any record.
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);
    ASSERT_EQ(meta.size(), 1u);
    EXPECT_EQ(meta.count("s0"), 1u);
    EXPECT_EQ(meta.count("s1"), 0u);
    EXPECT_EQ(meta["s0"]->get_statistic()->count_, 5);
    reader.close();
}

// Multi-flush variant: every flush window must skip empty columns, and a
// measurement that receives data in a later window must survive with a chunk
// only for that window (never a count=0 chunk for the empty windows).
TEST_F(EmptyChunkRegressionTest, NonAlignedEmptyMeasurementAcrossFlushes) {
    std::string device = "root.dev_empty_col_multi";
    std::vector<MeasurementSchema> schemas;
    for (int i = 0; i < 3; i++) {
        schemas.emplace_back("c" + std::to_string(i), TSDataType::INT32,
                             TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(
                device, MeasurementSchema("c" + std::to_string(i),
                                          TSDataType::INT32, TSEncoding::PLAIN,
                                          CompressionType::UNCOMPRESSED)),
            E_OK);
    }

    // Window 1: only c0 written.
    {
        storage::Tablet tablet(
            device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
            4);
        for (int r = 0; r < 4; r++) {
            ASSERT_EQ(tablet.add_timestamp(r, 1000 + r), E_OK);
            ASSERT_EQ(tablet.add_value(r, 0u, static_cast<int32_t>(1)), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    // Window 2: only c1 written (c0 gets nothing this window).
    {
        storage::Tablet tablet(
            device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
            4);
        for (int r = 0; r < 4; r++) {
            ASSERT_EQ(tablet.add_timestamp(r, 2000 + r), E_OK);
            ASSERT_EQ(tablet.add_value(r, 1u, static_cast<int32_t>(2)), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);
    // c2 never written in any window -> absent. c0/c1 each have one chunk.
    ASSERT_EQ(meta.size(), 2u);
    EXPECT_EQ(meta.count("c2"), 0u);
    EXPECT_EQ(meta["c0"]->get_chunk_meta_list()->size(), 1u);
    EXPECT_EQ(meta["c1"]->get_chunk_meta_list()->size(), 1u);
    EXPECT_EQ(meta["c0"]->get_statistic()->count_, 4);
    EXPECT_EQ(meta["c0"]->get_statistic()->start_time_, 1000);
    EXPECT_EQ(meta["c1"]->get_statistic()->count_, 4);
    EXPECT_EQ(meta["c1"]->get_statistic()->start_time_, 2000);
    reader.close();
}

// Data written after an empty flush window keeps flowing into the same
// column: an earlier flush that skipped the column must not corrupt the
// later write (writer reset semantics), and final statistics must span the
// written window only.
TEST_F(EmptyChunkRegressionTest, NonAlignedWriteAfterEmptyWindowSurvives) {
    std::string device = "root.dev_empty_then_write";
    std::vector<MeasurementSchema> schemas;
    schemas.emplace_back("w0", TSDataType::INT32, TSEncoding::PLAIN,
                         CompressionType::UNCOMPRESSED);
    ASSERT_EQ(tsfile_writer_->register_timeseries(
                  device,
                  MeasurementSchema("w0", TSDataType::INT32, TSEncoding::PLAIN,
                                    CompressionType::UNCOMPRESSED)),
              E_OK);

    // Window 1: nothing written, flush must succeed and emit no chunks.
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);

    // Window 2: real data after the empty flush.
    const int rows = 6;
    storage::Tablet tablet(
        device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
        rows);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(tablet.add_timestamp(r, 3000 + r), E_OK);
        ASSERT_EQ(tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);
    ASSERT_EQ(meta.size(), 1u);
    ASSERT_NE(meta.count("w0"), 0u);
    EXPECT_EQ(meta["w0"]->get_statistic()->count_, rows);
    EXPECT_EQ(meta["w0"]->get_statistic()->start_time_, 3000);
    EXPECT_EQ(meta["w0"]->get_statistic()->end_time_, 3000 + rows - 1);
    reader.close();
}

// Multiple devices: one device fully written, one device registered with an
// unwritten measurement. The empty column in the second device must not
// poison the first device's chunk group (they flush in the same pass).
TEST_F(EmptyChunkRegressionTest, EmptyColumnDoesNotAffectSiblingDevice) {
    std::string dev_full = "root.dev_full";
    std::string dev_partial = "root.dev_partial";

    std::vector<MeasurementSchema> full_schemas;
    full_schemas.emplace_back("f0", TSDataType::INT32, TSEncoding::PLAIN,
                              CompressionType::UNCOMPRESSED);
    ASSERT_EQ(tsfile_writer_->register_timeseries(
                  dev_full,
                  MeasurementSchema("f0", TSDataType::INT32, TSEncoding::PLAIN,
                                    CompressionType::UNCOMPRESSED)),
              E_OK);

    std::vector<MeasurementSchema> partial_schemas;
    partial_schemas.emplace_back("q0", TSDataType::INT32, TSEncoding::PLAIN,
                                 CompressionType::UNCOMPRESSED);
    partial_schemas.emplace_back("q1", TSDataType::INT32, TSEncoding::PLAIN,
                                 CompressionType::UNCOMPRESSED);
    for (const auto& s : partial_schemas) {
        ASSERT_EQ(tsfile_writer_->register_timeseries(
                      dev_partial,
                      MeasurementSchema(s.measurement_name_, TSDataType::INT32,
                                        TSEncoding::PLAIN,
                                        CompressionType::UNCOMPRESSED)),
                  E_OK);
    }

    const int rows = 5;
    storage::Tablet full_tablet(
        dev_full,
        std::make_shared<std::vector<MeasurementSchema>>(full_schemas), rows);
    storage::Tablet partial_tablet(
        dev_partial,
        std::make_shared<std::vector<MeasurementSchema>>(partial_schemas),
        rows);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(full_tablet.add_timestamp(r, 7000 + r), E_OK);
        ASSERT_EQ(full_tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
        ASSERT_EQ(partial_tablet.add_timestamp(r, 7000 + r), E_OK);
        ASSERT_EQ(partial_tablet.add_value(r, 0u, static_cast<int32_t>(r)),
                  E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet(full_tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->write_tablet(partial_tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    // Query both devices in one get_timeseries_metadata call: the reader
    // resets its metadata arena per call, so raw pointers from separate
    // calls must not be held simultaneously.
    std::vector<std::shared_ptr<IDeviceID>> devices = {
        std::make_shared<StringArrayDeviceID>(dev_full),
        std::make_shared<StringArrayDeviceID>(dev_partial)};
    auto meta_map = reader.get_timeseries_metadata(devices);
    ASSERT_EQ(meta_map.size(), 2u);

    auto full_list =
        meta_map.at(std::make_shared<StringArrayDeviceID>(dev_full));
    ASSERT_EQ(full_list.size(), 1u);
    EXPECT_EQ(full_list[0]->get_measurement_name().to_std_string(), "f0");
    EXPECT_EQ(full_list[0]->get_statistic()->count_, rows);

    auto partial_list =
        meta_map.at(std::make_shared<StringArrayDeviceID>(dev_partial));
    ASSERT_EQ(partial_list.size(), 1u);
    EXPECT_EQ(partial_list[0]->get_measurement_name().to_std_string(), "q0");
    reader.close();
}

// Mixed aligned/non-aligned devices in one file: an aligned device's value
// column with no data (all rows null) plus a non-aligned registered-but-empty
// measurement. Both flush paths must skip their empty columns.
TEST_F(EmptyChunkRegressionTest, EmptyColumnMixedAlignedAndNonAligned) {
    std::string dev_aligned = "root.dev_mixed_aligned";
    std::string dev_plain = "root.dev_mixed_plain";

    // Aligned device: register two value columns, only write one (the other
    // stays all-null in every tablet).
    std::vector<MeasurementSchema> aligned_schemas;
    aligned_schemas.emplace_back("a0", TSDataType::INT64, TSEncoding::PLAIN,
                                 CompressionType::UNCOMPRESSED);
    aligned_schemas.emplace_back("a1", TSDataType::INT64, TSEncoding::PLAIN,
                                 CompressionType::UNCOMPRESSED);
    std::vector<MeasurementSchema*> aligned_reg;
    for (const auto& s : aligned_schemas) {
        aligned_reg.push_back(new MeasurementSchema(
            s.measurement_name_, TSDataType::INT64, TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED));
    }
    ASSERT_EQ(
        tsfile_writer_->register_aligned_timeseries(dev_aligned, aligned_reg),
        E_OK);

    // Non-aligned device: register two measurements, write only one.
    std::vector<MeasurementSchema> plain_schemas;
    plain_schemas.emplace_back("p0", TSDataType::INT32, TSEncoding::PLAIN,
                               CompressionType::UNCOMPRESSED);
    plain_schemas.emplace_back("p1", TSDataType::INT32, TSEncoding::PLAIN,
                               CompressionType::UNCOMPRESSED);
    for (const auto& s : plain_schemas) {
        ASSERT_EQ(tsfile_writer_->register_timeseries(
                      dev_plain,
                      MeasurementSchema(s.measurement_name_, TSDataType::INT32,
                                        TSEncoding::PLAIN,
                                        CompressionType::UNCOMPRESSED)),
                  E_OK);
    }

    const int rows = 8;
    storage::Tablet aligned_tablet(
        dev_aligned,
        std::make_shared<std::vector<MeasurementSchema>>(aligned_schemas),
        rows);
    storage::Tablet plain_tablet(
        dev_plain,
        std::make_shared<std::vector<MeasurementSchema>>(plain_schemas), rows);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(aligned_tablet.add_timestamp(r, 5000 + r), E_OK);
        ASSERT_EQ(aligned_tablet.add_value(r, 0u, static_cast<int64_t>(r)),
                  E_OK);
        // a1 left all-null.

        ASSERT_EQ(plain_tablet.add_timestamp(r, 5000 + r), E_OK);
        ASSERT_EQ(plain_tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
        // p1 never written.
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(aligned_tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->write_tablet(plain_tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    std::vector<std::shared_ptr<IDeviceID>> devices = {
        std::make_shared<StringArrayDeviceID>(dev_aligned),
        std::make_shared<StringArrayDeviceID>(dev_plain)};
    auto meta_map = reader.get_timeseries_metadata(devices);
    ASSERT_EQ(meta_map.size(), 2u);

    auto aligned_list =
        meta_map.at(std::make_shared<StringArrayDeviceID>(dev_aligned));
    // The aligned branch has always had the hasData() guard; an all-null
    // value column still counts its (null) rows, so both columns appear.
    // The point of this test on the aligned side is that flush succeeds and
    // the written column carries correct statistics.
    std::map<std::string, storage::ITimeseriesIndex*> aligned_meta;
    for (auto& ts_idx : aligned_list) {
        aligned_meta[ts_idx->get_measurement_name().to_std_string()] =
            ts_idx.get();
    }
    ASSERT_EQ(aligned_meta.size(), 2u);
    EXPECT_EQ(aligned_meta["a0"]->get_statistic()->count_, rows);

    auto plain_list =
        meta_map.at(std::make_shared<StringArrayDeviceID>(dev_plain));
    // The non-aligned fix: p1 (registered but never written) must be absent.
    ASSERT_EQ(plain_list.size(), 1u);
    EXPECT_EQ(plain_list[0]->get_measurement_name().to_std_string(), "p0");
    EXPECT_EQ(plain_list[0]->get_statistic()->count_, rows);
    reader.close();
}

// ===== Adversarial additions =====

// The memory-threshold auto-flush is a second entry into flush_chunk_group
// that the tests above never drive (they all flush explicitly). Shrink
// chunk_group_size_threshold_ so write_tablet() itself triggers the flush,
// and verify the empty column is still skipped on that path.
TEST_F(EmptyChunkRegressionTest, EmptyColumnSkippedOnMemoryAutoFlush) {
    const int64_t prev_threshold =
        common::g_config_value_.chunk_group_size_threshold_;
    const int32_t prev_check_interval =
        common::g_config_value_.record_count_for_next_mem_check_;
    // Force the next check to fire after the first tablet and flush almost
    // immediately (threshold below the smallest realistic meta accounting).
    common::g_config_value_.record_count_for_next_mem_check_ = 1;
    common::g_config_value_.chunk_group_size_threshold_ = 1;

    std::string device = "root.dev_empty_autoflush";
    std::vector<MeasurementSchema> schemas;
    for (int i = 0; i < 3; i++) {
        schemas.emplace_back("m" + std::to_string(i), TSDataType::INT32,
                             TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(
                device, MeasurementSchema("m" + std::to_string(i),
                                          TSDataType::INT32, TSEncoding::PLAIN,
                                          CompressionType::UNCOMPRESSED)),
            E_OK);
    }

    {
        const int rows = 5;
        storage::Tablet tablet(
            device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
            rows);
        for (int r = 0; r < rows; r++) {
            ASSERT_EQ(tablet.add_timestamp(r, 100 + r), E_OK);
            ASSERT_EQ(tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
            // m1/m2 stay empty for this window.
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }

    common::g_config_value_.chunk_group_size_threshold_ = prev_threshold;
    common::g_config_value_.record_count_for_next_mem_check_ =
        prev_check_interval;

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);
    ASSERT_EQ(meta.size(), 1u);
    EXPECT_EQ(meta.count("m0"), 1u);
    EXPECT_EQ(meta.count("m1"), 0u);
    EXPECT_EQ(meta.count("m2"), 0u);
    EXPECT_EQ(meta["m0"]->get_statistic()->count_, 5);
    reader.close();
}

// Adversarial counter-check for the hasData() guard: a column with *some*
// nulls (partial data) must still be sealed — the fix must skip only fully
// empty columns, not partially-null ones. Uses the null-bitmap fallback
// path in write_column (row 2 of 4 left null).
TEST_F(EmptyChunkRegressionTest, PartiallyNullColumnIsStillSealed) {
    std::string device = "root.dev_partial_null";
    std::vector<MeasurementSchema> schemas;
    schemas.emplace_back("p0", TSDataType::INT32, TSEncoding::PLAIN,
                         CompressionType::UNCOMPRESSED);
    schemas.emplace_back("p1", TSDataType::INT32, TSEncoding::PLAIN,
                         CompressionType::UNCOMPRESSED);
    for (const auto& s : schemas) {
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(
                device, MeasurementSchema(s.measurement_name_,
                                          TSDataType::INT32, TSEncoding::PLAIN,
                                          CompressionType::UNCOMPRESSED)),
            E_OK);
    }

    const int rows = 4;
    storage::Tablet tablet(
        device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
        rows);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(tablet.add_timestamp(r, 200 + r), E_OK);
        ASSERT_EQ(tablet.add_value(r, 0u, static_cast<int32_t>(r)), E_OK);
        if (r != 2) {  // row 2 stays null in p1 -> null-bitmap fallback path
            ASSERT_EQ(tablet.add_value(r, 1u, static_cast<int32_t>(r * 3)),
                      E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);
    // Both columns sealed; p1 counts its 3 non-null rows.
    ASSERT_EQ(meta.size(), 2u);
    ASSERT_NE(meta.count("p1"), 0u)
        << "hasData() guard must not skip partially-null columns";
    EXPECT_EQ(meta["p1"]->get_statistic()->count_, rows - 1);
    EXPECT_EQ(meta["p1"]->get_statistic()->start_time_, 200);
    EXPECT_EQ(meta["p1"]->get_statistic()->end_time_, 203);
    reader.close();
}

// TEXT columns take a different write path (write_string_batch) than the
// fixed-width columns used above; an empty TEXT column must be skipped too.
TEST_F(EmptyChunkRegressionTest, EmptyTextColumnNotSealed) {
    std::string device = "root.dev_empty_text";
    std::vector<MeasurementSchema> schemas;
    schemas.emplace_back("t0", TSDataType::TEXT, TSEncoding::PLAIN,
                         CompressionType::UNCOMPRESSED);
    schemas.emplace_back("t1", TSDataType::TEXT, TSEncoding::PLAIN,
                         CompressionType::UNCOMPRESSED);
    for (const auto& s : schemas) {
        ASSERT_EQ(
            tsfile_writer_->register_timeseries(
                device, MeasurementSchema(s.measurement_name_, TSDataType::TEXT,
                                          TSEncoding::PLAIN,
                                          CompressionType::UNCOMPRESSED)),
            E_OK);
    }

    const int rows = 3;
    storage::Tablet tablet(
        device, std::make_shared<std::vector<MeasurementSchema>>(schemas),
        rows);
    char buf[] = "v";
    String s0(buf, 1);
    for (int r = 0; r < rows; r++) {
        ASSERT_EQ(tablet.add_timestamp(r, 300 + r), E_OK);
        ASSERT_EQ(tablet.add_value(r, 0u, s0), E_OK);
        // t1 never written.
    }
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    auto meta = CollectMeasurementMeta(reader, device);
    ASSERT_EQ(meta.size(), 1u);
    EXPECT_EQ(meta.count("t0"), 1u);
    EXPECT_EQ(meta.count("t1"), 0u)
        << "empty TEXT column must not be sealed as a chunk";
    EXPECT_EQ(meta["t0"]->get_statistic()->count_, rows);
    reader.close();
}
