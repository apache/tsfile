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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

namespace cwrapper_public_writer {

class CWrapperPublicWriterTest : public testing::Test {};

// CTest runs gtest-discovered cases in parallel processes. Keep writer files
// unique per process/case so one test cannot remove another test's file while
// it is being written or read.
inline std::string unique_writer_path(const char* stem) {
    static unsigned counter = 0;
#ifdef _WIN32
    long pid = static_cast<long>(_getpid());
#else
    long pid = static_cast<long>(getpid());
#endif
    std::ostringstream path;
    path << stem << "_" << pid << "_" << counter++ << ".tsfile";
    return path.str();
}

// Every public generic writer entry point must reject a null writer handle
// without dereferencing it: ERRNO-returning calls report RET_INVALID_ARG,
// and tsfile_generic_writer_close follows the public tsfile_writer_close
// convention of treating a null handle as already closed.
TEST_F(CWrapperPublicWriterTest, NullWriterHandlesReturnInvalidArg) {
    ColumnSchema column = {const_cast<char*>("s1"), TS_DATATYPE_INT64, FIELD};
    TableSchema schema = {const_cast<char*>("table1"), &column, 1};

    TimeseriesSchema ts_schema;
    ts_schema.timeseries_name = const_cast<char*>("s1");
    ts_schema.data_type = TS_DATATYPE_INT64;
    ts_schema.encoding = TS_ENCODING_PLAIN;
    ts_schema.compression = TS_COMPRESSION_UNCOMPRESSED;

    DeviceSchema device_schema;
    device_schema.device_name = const_cast<char*>("root.d1");
    device_schema.timeseries_schema = &ts_schema;
    device_schema.timeseries_num = 1;

    EXPECT_EQ(tsfile_generic_writer_register_table(nullptr, &schema),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_register_timeseries(nullptr, "root.d1",
                                                        &ts_schema),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_register_device(nullptr, &device_schema),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_write_tree_tablet(nullptr, nullptr),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_write_table_tablet(nullptr, nullptr),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_flush(nullptr), RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_close(nullptr), RET_OK);

    char key[] = "test_key";
    const uint8_t value = 0;
    EXPECT_EQ(
        tsfile_generic_writer_add_tsfile_property(
            nullptr, key, static_cast<uint32_t>(std::strlen(key)), &value, 1),
        RET_INVALID_ARG);
}

// tsfile_generic_writer_new must never dereference a null pathname or a null
// err_code, and it must surface open() failures through *err_code with a
// NULL handle instead of leaking a half-open writer.
TEST_F(CWrapperPublicWriterTest, WriterNewInvalidArgs) {
    ERRNO err = RET_OK;

    EXPECT_EQ(tsfile_generic_writer_new(nullptr, 1 << 20, &err), nullptr);
    EXPECT_EQ(err, RET_INVALID_ARG);

    // A null err_code is accepted: the call reports failure only through
    // the NULL return and must not crash.
    const std::string filename = unique_writer_path("cwrapper_public_writer");
    EXPECT_EQ(tsfile_generic_writer_new(filename.c_str(), 1 << 20, nullptr),
              nullptr);
    remove(filename.c_str());

    // A path under a non-existent directory fails to open; the error code
    // must be reported and the handle must be NULL.
    const char* bad_path = "cwrapper_public_writer_nonexistent_dir/x.tsfile";
    err = RET_OK;
    EXPECT_EQ(tsfile_generic_writer_new(bad_path, 1 << 20, &err), nullptr);
    EXPECT_NE(err, RET_OK);
}

TEST_F(CWrapperPublicWriterTest, QueryTreeInvalidArgs) {
    ERRNO err = RET_OK;
    char path[] = "root.d1.s1";
    char* paths[] = {path};

    EXPECT_EQ(tsfile_reader_query_tree(nullptr, paths, 1, 0, 1, &err), nullptr);
    EXPECT_EQ(err, RET_INVALID_ARG);
    EXPECT_EQ(tsfile_reader_query_tree(nullptr, paths, 1, 0, 1, nullptr),
              nullptr);
}

// tablet_new_with_target_name must reject arguments that would make the
// private delegate read out-of-bounds memory or hit the native Tablet
// max_rows assertion, returning NULL instead.
TEST_F(CWrapperPublicWriterTest, TabletNewWithTargetNameInvalidArgs) {
    char column_names[] = "s1";
    char* column_name_list[] = {column_names};
    TSDataType data_types[] = {TS_DATATYPE_INT64};

    // Null element inside an otherwise valid list.
    char* null_entry[] = {nullptr};

    // Lists present, but element counts/limits invalid.
    EXPECT_EQ(
        tablet_new_with_target_name("t1", column_name_list, data_types, 1, 0),
        nullptr);
    EXPECT_EQ(
        tablet_new_with_target_name("t1", column_name_list, data_types, 1, -1),
        nullptr);
    EXPECT_EQ(
        tablet_new_with_target_name("t1", column_name_list, data_types, -1, 2),
        nullptr);

    // column_num > 0 requires both lists and every element.
    EXPECT_EQ(tablet_new_with_target_name("t1", nullptr, data_types, 1, 2),
              nullptr);
    EXPECT_EQ(
        tablet_new_with_target_name("t1", column_name_list, nullptr, 1, 2),
        nullptr);
    EXPECT_EQ(tablet_new_with_target_name("t1", null_entry, data_types, 1, 2),
              nullptr);

    // Zero columns with null lists and a null target is a valid tablet.
    Tablet zero_columns =
        tablet_new_with_target_name(nullptr, nullptr, nullptr, 0, 16);
    ASSERT_NE(nullptr, zero_columns);
    free_tablet(&zero_columns);
    EXPECT_EQ(nullptr, zero_columns);

    // The nominal one-column construction still succeeds.
    Tablet tablet =
        tablet_new_with_target_name("t1", column_name_list, data_types, 1, 2);
    ASSERT_NE(nullptr, tablet);
    free_tablet(&tablet);
    EXPECT_EQ(nullptr, tablet);
}

// Invalid arguments on a live writer handle must be reported, not crashed
// on, and must not close the writer.
TEST_F(CWrapperPublicWriterTest, InvalidArgsOnLiveWriter) {
    const std::string filename = unique_writer_path("cwrapper_public_writer");
    remove(filename.c_str());

    ERRNO err = RET_OK;
    TsFileGenericWriter writer =
        tsfile_generic_writer_new(filename.c_str(), 128 * 1024 * 1024, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, writer);

    TimeseriesSchema ts_schema;
    ts_schema.timeseries_name = const_cast<char*>("s1");
    ts_schema.data_type = TS_DATATYPE_INT64;
    ts_schema.encoding = TS_ENCODING_PLAIN;
    ts_schema.compression = TS_COMPRESSION_UNCOMPRESSED;

    TimeseriesSchema null_name_schema = ts_schema;
    null_name_schema.timeseries_name = nullptr;

    EXPECT_EQ(tsfile_generic_writer_register_table(writer, nullptr),
              RET_INVALID_ARG);
    EXPECT_EQ(
        tsfile_generic_writer_register_timeseries(writer, nullptr, &ts_schema),
        RET_INVALID_ARG);
    EXPECT_EQ(
        tsfile_generic_writer_register_timeseries(writer, "root.d1", nullptr),
        RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_register_timeseries(writer, "root.d1",
                                                        &null_name_schema),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_write_tree_tablet(writer, nullptr),
              RET_INVALID_ARG);
    EXPECT_EQ(tsfile_generic_writer_write_table_tablet(writer, nullptr),
              RET_INVALID_ARG);

    DeviceSchema no_name;
    no_name.device_name = nullptr;
    no_name.timeseries_schema = &ts_schema;
    no_name.timeseries_num = 1;
    EXPECT_EQ(tsfile_generic_writer_register_device(writer, &no_name),
              RET_INVALID_ARG);

    DeviceSchema no_array;
    no_array.device_name = const_cast<char*>("root.d1");
    no_array.timeseries_schema = nullptr;
    no_array.timeseries_num = 1;
    EXPECT_EQ(tsfile_generic_writer_register_device(writer, &no_array),
              RET_INVALID_ARG);

    DeviceSchema negative_count;
    negative_count.device_name = const_cast<char*>("root.d1");
    negative_count.timeseries_schema = &ts_schema;
    negative_count.timeseries_num = -1;
    EXPECT_EQ(tsfile_generic_writer_register_device(writer, &negative_count),
              RET_INVALID_ARG);

    // The writer must still be usable: a valid registration and close
    // succeed.
    EXPECT_EQ(tsfile_generic_writer_register_timeseries(writer, "root.d1",
                                                        &ts_schema),
              RET_OK);
    EXPECT_EQ(tsfile_generic_writer_close(writer), RET_OK);
    remove(filename.c_str());
}

TEST_F(CWrapperPublicWriterTest, WriteAndQueryTreeTablet) {
    const std::string filename = unique_writer_path("cwrapper_public_writer");
    remove(filename.c_str());

    const char* device_id = "root.d1";
    const char* measurement = "s1";

    // Write one INT64/PLAIN/UNCOMPRESSED timeseries with the public API.
    ERRNO err = RET_OK;
    TsFileGenericWriter writer =
        tsfile_generic_writer_new(filename.c_str(), 128 * 1024 * 1024, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, writer);

    TimeseriesSchema schema;
    schema.timeseries_name = const_cast<char*>(measurement);
    schema.data_type = TS_DATATYPE_INT64;
    schema.encoding = TS_ENCODING_PLAIN;
    schema.compression = TS_COMPRESSION_UNCOMPRESSED;
    ASSERT_EQ(
        tsfile_generic_writer_register_timeseries(writer, device_id, &schema),
        RET_OK);

    char* column_names[] = {const_cast<char*>(measurement)};
    TSDataType data_types[] = {TS_DATATYPE_INT64};
    Tablet tablet =
        tablet_new_with_target_name(device_id, column_names, data_types, 1, 2);
    ASSERT_NE(nullptr, tablet);

    ASSERT_EQ(tablet_add_timestamp(tablet, 0, 1), RET_OK);
    ASSERT_EQ(tablet_add_timestamp(tablet, 1, 2), RET_OK);
    ASSERT_EQ(tablet_add_value_by_name_int64_t(tablet, 0, measurement, 11),
              RET_OK);
    ASSERT_EQ(tablet_add_value_by_name_int64_t(tablet, 1, measurement, 22),
              RET_OK);

    ASSERT_EQ(tsfile_generic_writer_write_tree_tablet(writer, tablet), RET_OK);
    ASSERT_EQ(tsfile_generic_writer_flush(writer), RET_OK);
    ASSERT_EQ(tsfile_generic_writer_close(writer), RET_OK);

    free_tablet(&tablet);
    EXPECT_EQ(nullptr, tablet);

    // Read the file back with the public reader/query-by-row API.
    ERRNO code = RET_OK;
    TsFileReader reader = tsfile_reader_new(filename.c_str(), &code);
    ASSERT_EQ(RET_OK, code);
    ASSERT_NE(nullptr, reader);

    char device_ids[] = "root.d1";
    char measurement_names[] = "s1";
    char* device_id_list[] = {device_ids};
    char* measurement_name_list[] = {measurement_names};

    ResultSet result_set = tsfile_reader_query_tree_by_row(
        reader, device_id_list, 1, measurement_name_list, 1, 0, -1, &code);
    ASSERT_EQ(RET_OK, code);
    ASSERT_NE(nullptr, result_set);

    const int64_t expected_values[] = {11, 22};
    int row = 0;
    while (tsfile_result_set_next(result_set, &code)) {
        ASSERT_EQ(RET_OK, code);
        ASSERT_LT(row, 2);
        EXPECT_EQ(static_cast<int64_t>(row + 1),
                  tsfile_result_set_get_value_by_index_int64_t(result_set, 1));
        EXPECT_EQ(expected_values[row],
                  tsfile_result_set_get_value_by_index_int64_t(result_set, 2));
        row++;
    }
    ASSERT_EQ(RET_OK, code);
    ASSERT_EQ(2, row);

    free_tsfile_result_set(&result_set);
    ASSERT_EQ(tsfile_reader_close(reader), RET_OK);
    remove(filename.c_str());
}

}  // namespace cwrapper_public_writer
