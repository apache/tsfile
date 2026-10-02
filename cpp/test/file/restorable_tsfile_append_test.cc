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

/**
 * Unit tests for RestorableTsFileIOWriter::open_for_append().
 * Covers: appending to a file that was closed normally, in both the tree and
 * the table model, the no-op append, repeated append/close cycles, recovery of
 * a file that never got a footer, and the rule that a refused append leaves the
 * file byte-identical.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/restorable_tsfile_io_writer.h"
#include "file/write_file.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"
#include "writer/tsfile_writer.h"

namespace storage {
class ResultSet;
}  // namespace storage

using namespace storage;
using namespace common;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

static int GetWriteCreateFlags() {
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    return flags;
}

static int64_t GetFileSize(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    return static_cast<int64_t>(f.tellg());
}

/** Full file contents, for proving a refusal changed nothing. */
static std::string ReadWholeFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
}

/** Overwrite num_bytes of the file starting at offset. */
static void PokeFile(const std::string& path, int64_t offset,
                     const std::vector<char>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::in);
    out.seekp(static_cast<std::streamoff>(offset));
    for (size_t i = 0; i < bytes.size(); ++i) {
        out.put(bytes[i]);
    }
    out.close();
}

/** Trim the footer so the file looks like a write that never completed. */
static void CorruptFileTail(const std::string& path, int num_bytes) {
    PokeFile(path, GetFileSize(path) - num_bytes,
             std::vector<char>(static_cast<size_t>(num_bytes), 0));
}

/** Rows the tree reader returns for one device over a time window. */
static int CountRowsInWindow(TsFileTreeReader& reader,
                             const std::vector<std::string>& device_ids,
                             const std::vector<std::string>& measurement_ids,
                             int64_t start_time, int64_t end_time) {
    ResultSet* result = nullptr;
    int ret =
        reader.query(device_ids, measurement_ids, start_time, end_time, result);
    if (ret != E_OK || result == nullptr) {
        return -1;
    }
    int count = 0;
    for (auto it = result->iterator(); it.hasNext(); it.next()) {
        ++count;
    }
    reader.destroy_query_data_set(result);
    return count;
}

class RestorableTsFileAppendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("restorable_tsfile_append_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    /** Write a closed tree file: device "d1", s1=FLOAT at t=1..row_count. */
    void WriteCompleteTreeFile(int row_count) {
        TsFileWriter tw;
        ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
        tw.register_timeseries(
            "d1", MeasurementSchema("s1", FLOAT, GORILLA,
                                    CompressionType::UNCOMPRESSED));
        for (int i = 1; i <= row_count; ++i) {
            TsRecord record(i, "d1");
            record.add_point("s1", static_cast<float>(i));
            ASSERT_EQ(tw.write_record(record), E_OK);
        }
        tw.flush();
        tw.close();
    }

    std::string file_name_;

    static std::string generate_random_string(int length) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string s;
        s.reserve(static_cast<size_t>(length));
        for (int i = 0; i < length; ++i) {
            s += chars[static_cast<size_t>(dis(gen))];
        }
        return s;
    }
};

// -----------------------------------------------------------------------------
// open() and open_for_append() disagree on purpose
// -----------------------------------------------------------------------------

// The entry point is separate precisely so that open() keeps reporting a closed
// file as read-only. Pinning both here means a later change cannot quietly
// merge the two behaviours.
TEST_F(RestorableTsFileAppendTest, OpenStillRefusesACompleteFile) {
    WriteCompleteTreeFile(2);

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_FALSE(writer.can_write());
    EXPECT_FALSE(writer.has_crashed());
    EXPECT_FALSE(writer.is_append_on_complete());
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_COMPLETE);
    EXPECT_EQ(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileAppendTest, AppendToCompleteFileTrimsMetadataTail) {
    WriteCompleteTreeFile(2);
    const int64_t closed_size = GetFileSize(file_name_);

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open_for_append(file_name_), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.is_append_on_complete());
    // A file that was closed normally lost data: the append path must not
    // report that it did.
    EXPECT_FALSE(writer.has_crashed());
    // Bytes removed is the footer, so it is positive and smaller than the file.
    EXPECT_GT(writer.get_truncated_size(), 0);
    EXPECT_LT(writer.get_truncated_size(), closed_size);
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

// -----------------------------------------------------------------------------
// Append then read back: tree model
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileAppendTest, TreeAppendKeepsOldRowsAndAddsNew) {
    WriteCompleteTreeFile(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open_for_append(file_name_), E_OK);

    TsFileTreeWriter tree_writer(&rw);
    for (int i = 4; i <= 6; ++i) {
        TsRecord record(i, "d1");
        record.add_point("s1", static_cast<float>(i));
        ASSERT_EQ(tree_writer.write(record), E_OK);
    }
    ASSERT_EQ(tree_writer.flush(), E_OK);
    tree_writer.close();
    rw.close();

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    EXPECT_EQ(reader.get_all_device_ids().size(), 1u);
    EXPECT_EQ(CountRowsInWindow(reader, {"d1"}, {"s1"}, INT64_MIN, INT64_MAX),
              6);
    reader.close();
}

// A new device and a new measurement on the recovered device both go through
// the ordinary registration paths after an append.
TEST_F(RestorableTsFileAppendTest, TreeAppendAddsDeviceAndMeasurement) {
    WriteCompleteTreeFile(2);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open_for_append(file_name_), E_OK);

    TsFileWriter tw;
    ASSERT_EQ(tw.init(&rw), E_OK);
    tw.register_timeseries("d2", MeasurementSchema("s1", FLOAT));
    tw.register_timeseries("d1", MeasurementSchema("s2", INT32));

    TsRecord r1(3, "d1");
    r1.add_point("s2", 30);
    ASSERT_EQ(tw.write_record(r1), E_OK);
    TsRecord r2(4, "d2");
    r2.add_point("s1", 4.0f);
    ASSERT_EQ(tw.write_record(r2), E_OK);
    tw.flush();
    tw.close();
    rw.close();

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    EXPECT_EQ(reader.get_all_device_ids().size(), 2u);
    EXPECT_EQ(
        CountRowsInWindow(reader, {"d1"}, {"s1", "s2"}, INT64_MIN, INT64_MAX),
        3);
    EXPECT_EQ(CountRowsInWindow(reader, {"d2"}, {"s1"}, INT64_MIN, INT64_MAX),
              1);
    reader.close();
}

// The recovered per-device last_time_ is what rejects a timestamp that falls
// back into the range the file already covers.
TEST_F(RestorableTsFileAppendTest, TreeAppendRejectsOlderTimestamp) {
    WriteCompleteTreeFile(5);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open_for_append(file_name_), E_OK);

    TsFileTreeWriter tree_writer(&rw);
    TsRecord record(3, "d1");
    record.add_point("s1", 3.0f);
    EXPECT_NE(tree_writer.write(record), E_OK);
    tree_writer.close();
    rw.close();
}

// -----------------------------------------------------------------------------
// No-op append: the tail is rewritten from scanned chunk metadata, so a silent
// loss there has to show up as a query that prunes differently, not as a file
// that fails to open.
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileAppendTest, NoOpAppendKeepsStatisticsUsable) {
    WriteCompleteTreeFile(10);

    TsFileTreeReader before;
    ASSERT_EQ(before.open(file_name_), E_OK);
    const int all_before =
        CountRowsInWindow(before, {"d1"}, {"s1"}, INT64_MIN, INT64_MAX);
    // Time windows are answered from the footer's statistics: an inner window,
    // a lower bound window and an upper bound window each cover a different
    // pruning decision.
    const int mid_before = CountRowsInWindow(before, {"d1"}, {"s1"}, 4, 7);
    const int low_before = CountRowsInWindow(before, {"d1"}, {"s1"}, 1, 3);
    const int high_before = CountRowsInWindow(before, {"d1"}, {"s1"}, 8, 10);
    before.close();

    ASSERT_EQ(all_before, 10);
    ASSERT_EQ(mid_before, 4);
    ASSERT_EQ(low_before, 3);
    ASSERT_EQ(high_before, 3);

    {
        RestorableTsFileIOWriter rw;
        ASSERT_EQ(rw.open_for_append(file_name_), E_OK);
        TsFileTreeWriter tree_writer(&rw);
        tree_writer.close();
        rw.close();
    }

    // The tail that comes back is regenerated from the chunks the scan read,
    // not copied from the tail that was removed, so opening the file at all is
    // already proof the regenerated footer parses. What the windows prove is
    // that its statistics still describe the same data.
    TsFileTreeReader after;
    ASSERT_EQ(after.open(file_name_), E_OK);
    EXPECT_EQ(CountRowsInWindow(after, {"d1"}, {"s1"}, INT64_MIN, INT64_MAX),
              all_before);
    EXPECT_EQ(CountRowsInWindow(after, {"d1"}, {"s1"}, 4, 7), mid_before);
    EXPECT_EQ(CountRowsInWindow(after, {"d1"}, {"s1"}, 1, 3), low_before);
    EXPECT_EQ(CountRowsInWindow(after, {"d1"}, {"s1"}, 8, 10), high_before);
    after.close();
}

// Each close() writes another operation-index-range segment ahead of its
// separator, so accumulation is only benign if it stays benign over cycles.
TEST_F(RestorableTsFileAppendTest, RepeatedAppendCloseCycles) {
    WriteCompleteTreeFile(1);

    for (int cycle = 0; cycle < 4; ++cycle) {
        RestorableTsFileIOWriter rw;
        ASSERT_EQ(rw.open_for_append(file_name_), E_OK) << "cycle " << cycle;
        ASSERT_TRUE(rw.is_append_on_complete());

        TsFileTreeWriter tree_writer(&rw);
        TsRecord record(cycle + 2, "d1");
        record.add_point("s1", static_cast<float>(cycle + 2));
        ASSERT_EQ(tree_writer.write(record), E_OK) << "cycle " << cycle;
        tree_writer.flush();
        tree_writer.close();
        rw.close();

        TsFileTreeReader reader;
        ASSERT_EQ(reader.open(file_name_), E_OK);
        EXPECT_EQ(
            CountRowsInWindow(reader, {"d1"}, {"s1"}, INT64_MIN, INT64_MAX),
            cycle + 2)
            << "cycle " << cycle;
        reader.close();
    }
}

// -----------------------------------------------------------------------------
// Table model
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileAppendTest, TableAppendKeepsOldRowsAndAddsNew) {
    std::vector<MeasurementSchema*> measurement_schemas;
    measurement_schemas.push_back(new MeasurementSchema("device", STRING));
    measurement_schemas.push_back(new MeasurementSchema("value", DOUBLE));
    std::vector<ColumnCategory> column_categories = {ColumnCategory::TAG,
                                                     ColumnCategory::FIELD};
    TableSchema table_schema("test_table", measurement_schemas,
                             column_categories);

    {
        WriteFile write_file;
        ASSERT_EQ(write_file.create(file_name_, GetWriteCreateFlags(), 0666),
                  E_OK);
        TsFileTableWriter table_writer(&write_file, &table_schema);
        Tablet tablet(table_schema.get_measurement_names(),
                      table_schema.get_data_types(), 5);
        tablet.set_table_name("test_table");
        for (int i = 0; i < 5; ++i) {
            tablet.add_timestamp(i, static_cast<int64_t>(i));
            tablet.add_value(i, "device", "device0");
            tablet.add_value(i, "value", i * 1.1);
        }
        ASSERT_EQ(table_writer.write_table(tablet), E_OK);
        ASSERT_EQ(table_writer.flush(), E_OK);
        table_writer.close();
        write_file.close();
    }

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open_for_append(file_name_), E_OK);
    ASSERT_TRUE(rw.is_append_on_complete());

    {
        TsFileTableWriter table_writer2(&rw);
        std::vector<std::string> value_col = {"__level1", "value"};
        std::vector<TSDataType> value_types = {STRING, DOUBLE};
        Tablet tablet2(value_col, value_types, 5);
        tablet2.set_table_name("test_table");
        for (int i = 0; i < 5; ++i) {
            tablet2.add_timestamp(i, static_cast<int64_t>(i + 10));
            tablet2.add_value(i, "__level1", "device1");
            tablet2.add_value(i, "value", (i + 10) * 1.1);
        }
        ASSERT_EQ(table_writer2.write_table(tablet2), E_OK);
        ASSERT_EQ(table_writer2.flush(), E_OK);
        table_writer2.close();
    }
    rw.close();

    TsFileReader table_reader;
    ASSERT_EQ(table_reader.open(file_name_), E_OK);
    ResultSet* tmp_result_set = nullptr;
    ASSERT_EQ(table_reader.query("test_table", {"__level1", "value"}, 0, 10000,
                                 tmp_result_set, nullptr),
              E_OK);
    auto* table_result_set = static_cast<TableResultSet*>(tmp_result_set);
    bool has_next = false;
    int64_t row_num = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        ++row_num;
    }
    EXPECT_EQ(row_num, 10);
    table_reader.destroy_query_data_set(tmp_result_set);
    table_reader.close();
}

// -----------------------------------------------------------------------------
// Refusals must not touch the file
// -----------------------------------------------------------------------------

// A footer whose metadata length does not describe the file cannot be located,
// and the chunk region is only read to find out: the caller's data has to still
// be there afterwards.
TEST_F(RestorableTsFileAppendTest, AppendRefusesBadMetadataLength) {
    WriteCompleteTreeFile(4);
    const std::string bytes_before = ReadWholeFile(file_name_);
    const int64_t size = GetFileSize(file_name_);

    // The i32 length of the metadata body sits directly before the tail magic.
    std::vector<char> garbage(4, static_cast<char>(0x7f));
    PokeFile(file_name_, size - MAGIC_STRING_TSFILE_LEN - 4, garbage);

    RestorableTsFileIOWriter writer;
    EXPECT_NE(writer.open_for_append(file_name_), E_OK);
    EXPECT_FALSE(writer.can_write());
    EXPECT_FALSE(writer.is_append_on_complete());
    writer.close();

    // Reading the damaged file back is deliberately not attempted: the reader
    // takes this length at face value, so proving the append refused is the
    // point here, and the untouched bytes below are the evidence.
    EXPECT_EQ(ReadWholeFile(file_name_), bytes_before);
}

TEST_F(RestorableTsFileAppendTest, AppendRefusesBytesThatAreNotATsFile) {
    {
        std::ofstream f(file_name_);
        f.write("BadFile", 7);
        f.close();
    }
    const std::string bytes_before = ReadWholeFile(file_name_);

    RestorableTsFileIOWriter writer;
    EXPECT_NE(writer.open_for_append(file_name_), E_OK);
    EXPECT_FALSE(writer.can_write());
    writer.close();
    EXPECT_EQ(ReadWholeFile(file_name_), bytes_before);
}

// Chunk bytes that stop making sense part-way through the file: an incomplete
// file is what this is, so it is recovered the way open() recovers it, and the
// append still works.
TEST_F(RestorableTsFileAppendTest, AppendToIncompleteFileSalvagesThenWrites) {
    WriteCompleteTreeFile(3);
    CorruptFileTail(5);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open_for_append(file_name_), E_OK);
    EXPECT_TRUE(rw.can_write());
    EXPECT_TRUE(rw.has_crashed());
    EXPECT_FALSE(rw.is_append_on_complete());

    TsFileTreeWriter tree_writer(&rw);
    TsRecord record(4, "d1");
    record.add_point("s1", 4.0f);
    ASSERT_EQ(tree_writer.write(record), E_OK);
    tree_writer.flush();
    tree_writer.close();
    rw.close();

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    EXPECT_GE(CountRowsInWindow(reader, {"d1"}, {"s1"}, INT64_MIN, INT64_MAX),
              1);
    reader.close();
}

// Files with nothing to trim: an empty file and a header-only file both become
// writable through the same path open() uses for them.
TEST_F(RestorableTsFileAppendTest, AppendToEmptyAndHeaderOnlyFile) {
    {
        RestorableTsFileIOWriter writer;
        ASSERT_EQ(writer.open_for_append(file_name_), E_OK);
        EXPECT_TRUE(writer.can_write());
        EXPECT_FALSE(writer.is_append_on_complete());
        writer.close();
        remove(file_name_.c_str());
    }

    int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    {
        WriteFile wf;
        ASSERT_EQ(wf.create(file_name_, flags, 0666), E_OK);
        wf.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
        wf.write(&VERSION_NUM_BYTE, 1);
        wf.close();
    }

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open_for_append(file_name_), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_FALSE(writer.is_append_on_complete());
    writer.close();

    // And it really does append: the header stays a header, the rows arrive.
    RestorableTsFileIOWriter rw2;
    ASSERT_EQ(rw2.open_for_append(file_name_), E_OK);
    TsFileTreeWriter tree_writer(&rw2);
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    ASSERT_EQ(tree_writer.write(record), E_OK);
    tree_writer.flush();
    tree_writer.close();
    rw2.close();

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    EXPECT_EQ(CountRowsInWindow(reader, {"d1"}, {"s1"}, INT64_MIN, INT64_MAX),
              1);
    reader.close();
}

// A handle already holding a file must not open a second one.
TEST_F(RestorableTsFileAppendTest, AppendWhileAlreadyOpenIsRefused) {
    WriteCompleteTreeFile(2);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open_for_append(file_name_), E_OK);
    EXPECT_EQ(rw.open_for_append(file_name_), E_ALREADY_EXIST);
    rw.close();
}
