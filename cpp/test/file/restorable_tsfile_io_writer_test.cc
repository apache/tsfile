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

#include "file/restorable_tsfile_io_writer.h"

#include <gtest/gtest.h>

#include <fstream>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;

class RestorableTsFileIOWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = "restorable_tsfile_io_writer_test.tsfile";
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
};

TEST_F(RestorableTsFileIOWriterTest, OpenEmptyFile) {
    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), 0);
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenBadMagicFile) {
    std::ofstream f(file_name_);
    f.write("BadFile", 7);
    f.close();

    RestorableTsFileIOWriter writer;
    EXPECT_NE(writer.open(file_name_, true), E_OK);
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_INCOMPATIBLE);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenCompleteFile) {
    TsFileWriter tw;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(tw.open(file_name_, flags, 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);

    record.timestamp_ = 2;
    tw.write_record(record);

    tw.flush();
    tw.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_FALSE(writer.can_write());
    EXPECT_FALSE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_COMPLETE);
    EXPECT_EQ(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenTruncatedFile) {
    TsFileWriter tw;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(tw.open(file_name_, flags, 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, RLE, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    tw.flush();
    tw.close();

    std::streampos full_size;
    {
        std::ifstream f(file_name_, std::ios::binary | std::ios::ate);
        full_size = f.tellg();
    }

    std::ofstream corrupt(file_name_, std::ios::binary | std::ios::in);
    corrupt.seekp(full_size - std::streamoff(5));
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_GE(writer.get_truncated_size(),
              static_cast<int64_t>(MAGIC_STRING_TSFILE_LEN + 1));
    EXPECT_LE(writer.get_truncated_size(), static_cast<int64_t>(full_size));
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, OpenFileWithOnlyHeader) {
    WriteFile wf;
    int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(wf.create(file_name_, flags, 0666), E_OK);
    wf.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
    wf.write(&VERSION_NUM_BYTE, 1);
    wf.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), MAGIC_STRING_TSFILE_LEN + 1);
    writer.close();
}

TEST_F(RestorableTsFileIOWriterTest, TruncateRecoversAndProvidesWriter) {
    TsFileWriter tw;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(tw.open(file_name_, flags, 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);

    record.timestamp_ = 2;
    tw.write_record(record);

    tw.flush();
    tw.close();

    std::streampos full_size;
    {
        std::ifstream f(file_name_, std::ios::binary | std::ios::ate);
        full_size = f.tellg();
    }

    std::ofstream corrupt(file_name_, std::ios::binary | std::ios::in);
    corrupt.seekp(full_size - std::streamoff(3));
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.close();

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());
    ASSERT_NE(rw.get_tsfile_io_writer(), nullptr);
    ASSERT_NE(rw.get_write_file(), nullptr);
    EXPECT_EQ(rw.get_file_path(), file_name_);

    TsFileWriter tw2;
    ASSERT_EQ(tw2.init(&rw), E_OK);
    TsRecord record2(3, "d1");
    record2.add_point("s1", 3.0f);
    ASSERT_EQ(tw2.write_record(record2), E_OK);
    tw2.close();
    rw.close();
}

TEST_F(RestorableTsFileIOWriterTest, MultiDeviceRecoverAndWriteWithTreeWriter) {
    TsFileWriter tw;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(tw.open(file_name_, flags, 0666), E_OK);
    tw.register_timeseries("d1", MeasurementSchema("s1", FLOAT));
    tw.register_timeseries("d1", MeasurementSchema("s2", INT32));
    tw.register_timeseries("d2", MeasurementSchema("s1", FLOAT));
    tw.register_timeseries("d2", MeasurementSchema("s2", DOUBLE));

    TsRecord r1(1, "d1");
    r1.add_point("s1", 1.0f);
    r1.add_point("s2", 10);
    ASSERT_EQ(tw.write_record(r1), E_OK);

    TsRecord r2(2, "d2");
    r2.add_point("s1", 2.0f);
    r2.add_point("s2", 20.0);
    ASSERT_EQ(tw.write_record(r2), E_OK);

    tw.flush();
    tw.close();

    std::streampos full_size;
    {
        std::ifstream f(file_name_, std::ios::binary | std::ios::ate);
        full_size = f.tellg();
    }

    std::ofstream corrupt(file_name_, std::ios::binary | std::ios::in);
    corrupt.seekp(full_size - std::streamoff(3));
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.close();

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileTreeWriter tree_writer(&rw);
    TsRecord r3(3, "d1");
    r3.add_point("s1", 3.0f);
    r3.add_point("s2", 30);
    ASSERT_EQ(tree_writer.write(r3), E_OK);

    TsRecord r4(4, "d2");
    r4.add_point("s1", 4.0f);
    r4.add_point("s2", 40.0);
    ASSERT_EQ(tree_writer.write(r4), E_OK);

    tree_writer.close();
    rw.close();
}

TEST_F(RestorableTsFileIOWriterTest, AlignedTimeseriesRecoverAndWrite) {
    TsFileWriter tw;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(tw.open(file_name_, flags, 0666), E_OK);

    std::vector<MeasurementSchema*> aligned_schemas;
    aligned_schemas.push_back(new MeasurementSchema("s1", FLOAT));
    aligned_schemas.push_back(new MeasurementSchema("s2", FLOAT));
    tw.register_aligned_timeseries("d1", aligned_schemas);

    TsRecord r1(1, "d1");
    r1.add_point("s1", 1.0f);
    r1.add_point("s2", 2.0f);
    ASSERT_EQ(tw.write_record_aligned(r1), E_OK);

    TsRecord r2(2, "d1");
    r2.add_point("s1", 3.0f);
    r2.add_point("s2", 4.0f);
    ASSERT_EQ(tw.write_record_aligned(r2), E_OK);

    tw.flush();
    tw.close();

    std::streampos full_size;
    {
        std::ifstream f(file_name_, std::ios::binary | std::ios::ate);
        full_size = f.tellg();
    }

    std::ofstream corrupt(file_name_, std::ios::binary | std::ios::in);
    corrupt.seekp(full_size - std::streamoff(3));
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.close();

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileWriter tw2;
    ASSERT_EQ(tw2.init(&rw), E_OK);

    TsRecord r3(3, "d1");
    r3.add_point("s1", 5.0f);
    r3.add_point("s2", 6.0f);
    ASSERT_EQ(tw2.write_record_aligned(r3), E_OK);

    tw2.close();
    rw.close();
}

TEST_F(RestorableTsFileIOWriterTest, TableWriterRecoverAndWrite) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.push_back(new MeasurementSchema("device", STRING));
    measurement_schemas.push_back(new MeasurementSchema("value", DOUBLE));
    column_categories.push_back(ColumnCategory::TAG);
    column_categories.push_back(ColumnCategory::FIELD);
    TableSchema table_schema("test_table", measurement_schemas,
                             column_categories);

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    WriteFile write_file;
    write_file.create(file_name_, flags, 0666);

    TsFileTableWriter table_writer(&write_file, &table_schema);
    Tablet tablet(table_schema.get_measurement_names(),
                  table_schema.get_data_types(), 10);
    std::string table_name = "test_table";
    tablet.set_table_name(table_name);
    for (int i = 0; i < 10; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(i));
        tablet.add_value(i, "device", "device0");
        tablet.add_value(i, "value", i * 1.1);
    }
    ASSERT_EQ(table_writer.write_table(tablet), E_OK);
    ASSERT_EQ(table_writer.flush(), E_OK);
    table_writer.close();
    write_file.close();

    std::streampos full_size;
    {
        std::ifstream f(file_name_, std::ios::binary | std::ios::ate);
        full_size = f.tellg();
    }
    std::ofstream corrupt(file_name_, std::ios::binary | std::ios::in);
    corrupt.seekp(full_size - std::streamoff(3));
    corrupt.put(0);
    corrupt.put(0);
    corrupt.put(0);
    corrupt.close();

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileTableWriter table_writer2(&rw);
    // Java 规则：key=device_id.get_table_name()="device0"；1 segment 时无
    // __level 列，仅有 FIELD "value"
    std::vector<std::string> value_col = {"value"};
    std::vector<TSDataType> value_types = {DOUBLE};
    Tablet tablet2(value_col, value_types, 10);
    tablet2.set_table_name(table_name);
    for (int i = 0; i < 10; i++) {
        tablet2.add_timestamp(i, static_cast<int64_t>(i + 10));
        tablet.add_value(i, "device", "device0");
        tablet2.add_value(i, "value", (i + 10) * 1.1);
    }
    ASSERT_EQ(table_writer2.write_table(tablet2), E_OK);
    table_writer2.close();
    // rw.close();
}
