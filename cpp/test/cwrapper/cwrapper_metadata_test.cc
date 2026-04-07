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
#include <unistd.h>

#include <cstring>
#include <string>

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

namespace cwrapper_metadata {

class CWrapperMetadataTest : public testing::Test {};

TEST_F(CWrapperMetadataTest, GetAllDevicesAndMetadataWithStatistic) {
    ERRNO code = RET_OK;
    const char* filename = "cwrapper_metadata_stat.tsfile";
    remove(filename);

    const char* device = "root.sg.d1";
    char* m_int = strdup("s_int");
    timeseries_schema sch{};
    sch.timeseries_name = m_int;
    sch.data_type = TS_DATATYPE_INT32;
    sch.encoding = TS_ENCODING_PLAIN;
    sch.compression = TS_COMPRESSION_UNCOMPRESSED;

    auto* writer = static_cast<void*>(
        _tsfile_writer_new(filename, 128 * 1024 * 1024, &code));
    ASSERT_EQ(RET_OK, code);
    ASSERT_EQ(RET_OK, _tsfile_writer_register_timeseries(writer, device, &sch));

    for (int row = 0; row < 3; row++) {
        auto* record = static_cast<TsRecord>(
            _ts_record_new(device, static_cast<int64_t>(row + 1), 1));
        const int32_t v = static_cast<int32_t>((row + 1) * 10);
        ASSERT_EQ(RET_OK, _insert_data_into_ts_record_by_name_int32_t(
                              record, m_int, v));
        ASSERT_EQ(RET_OK, _tsfile_writer_write_ts_record(writer, record));
        _free_tsfile_ts_record(reinterpret_cast<TsRecord*>(&record));
    }
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(filename, &code);
    ASSERT_EQ(RET_OK, code);
    ASSERT_NE(nullptr, reader);

    DeviceID* details = nullptr;
    uint32_t n_det = 0;
    ASSERT_EQ(RET_OK, tsfile_reader_get_all_devices(reader, &details, &n_det));
    ASSERT_EQ(1u, n_det);
    ASSERT_NE(nullptr, details);
    ASSERT_STREQ(device, details[0].path);
    ASSERT_NE(nullptr, details[0].table_name);
    EXPECT_STREQ("root.sg", details[0].table_name);
    EXPECT_EQ(2u, details[0].segment_count);
    ASSERT_NE(nullptr, details[0].segments);
    EXPECT_STREQ("root.sg", details[0].segments[0]);
    EXPECT_STREQ("d1", details[0].segments[1]);
    tsfile_free_device_id_array(details, n_det);

    DeviceTimeseriesMetadataMap map{};
    ASSERT_EQ(RET_OK, tsfile_reader_get_timeseries_metadata_all(reader, &map));
    ASSERT_EQ(1u, map.device_count);
    ASSERT_NE(nullptr, map.entries);
    ASSERT_STREQ(device, map.entries[0].device.path);
    ASSERT_NE(nullptr, map.entries[0].device.table_name);
    EXPECT_STREQ("root.sg", map.entries[0].device.table_name);
    EXPECT_EQ(2u, map.entries[0].device.segment_count);
    ASSERT_NE(nullptr, map.entries[0].device.segments);
    EXPECT_STREQ("root.sg", map.entries[0].device.segments[0]);
    EXPECT_STREQ("d1", map.entries[0].device.segments[1]);
    ASSERT_EQ(1u, map.entries[0].timeseries_count);
    ASSERT_NE(nullptr, map.entries[0].timeseries);
    TimeseriesMetadata& tm = map.entries[0].timeseries[0];
    ASSERT_STREQ(m_int, tm.measurement_name);
    ASSERT_EQ(TS_DATATYPE_INT32, tm.data_type);
    TsFileStatisticBase* sb = tsfile_statistic_base(&tm.statistic);
    ASSERT_TRUE(sb->has_statistic);
    EXPECT_EQ(3, sb->row_count);
    EXPECT_EQ(1, sb->start_time);
    EXPECT_EQ(3, sb->end_time);
    EXPECT_DOUBLE_EQ(60.0, tm.statistic.u.int_s.sum);
    ASSERT_EQ(TS_DATATYPE_INT32, sb->type);
    EXPECT_EQ(10, tm.statistic.u.int_s.min_int64);
    EXPECT_EQ(30, tm.statistic.u.int_s.max_int64);
    EXPECT_EQ(10, tm.statistic.u.int_s.first_int64);
    EXPECT_EQ(30, tm.statistic.u.int_s.last_int64);

    tsfile_free_device_timeseries_metadata_map(&map);

    DeviceTimeseriesMetadataMap empty{};
    ASSERT_EQ(RET_OK, tsfile_reader_get_timeseries_metadata_for_devices(
                          reader, nullptr, 0, &empty));
    EXPECT_EQ(0u, empty.device_count);
    EXPECT_EQ(nullptr, empty.entries);

    DeviceID q{};
    q.path = const_cast<char*>(device);
    q.table_name = nullptr;
    q.segment_count = 0;
    q.segments = nullptr;
    DeviceTimeseriesMetadataMap one{};
    ASSERT_EQ(RET_OK, tsfile_reader_get_timeseries_metadata_for_devices(
                          reader, &q, 1, &one));
    ASSERT_EQ(1u, one.device_count);
    tsfile_free_device_timeseries_metadata_map(&one);

    ASSERT_EQ(RET_OK, tsfile_reader_close(reader));
    free(m_int);
    remove(filename);
}

TEST_F(CWrapperMetadataTest, GetTimeseriesMetadataBooleanStatistic) {
    ERRNO code = RET_OK;
    const char* filename = "cwrapper_metadata_bool.tsfile";
    remove(filename);

    const char* device = "root.sg.bool";
    char* m_b = strdup("s_bool");
    timeseries_schema sch{};
    sch.timeseries_name = m_b;
    sch.data_type = TS_DATATYPE_BOOLEAN;
    sch.encoding = TS_ENCODING_PLAIN;
    sch.compression = TS_COMPRESSION_UNCOMPRESSED;

    auto* writer = static_cast<void*>(
        _tsfile_writer_new(filename, 128 * 1024 * 1024, &code));
    ASSERT_EQ(RET_OK, code);
    ASSERT_EQ(RET_OK, _tsfile_writer_register_timeseries(writer, device, &sch));

    const bool vals[] = {true, false, true};
    for (int row = 0; row < 3; row++) {
        auto* record = static_cast<TsRecord>(
            _ts_record_new(device, static_cast<int64_t>(row + 1), 1));
        ASSERT_EQ(RET_OK, _insert_data_into_ts_record_by_name_bool(record, m_b,
                                                                   vals[row]));
        ASSERT_EQ(RET_OK, _tsfile_writer_write_ts_record(writer, record));
        _free_tsfile_ts_record(reinterpret_cast<TsRecord*>(&record));
    }
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(filename, &code);
    ASSERT_EQ(RET_OK, code);

    DeviceTimeseriesMetadataMap map{};
    ASSERT_EQ(RET_OK, tsfile_reader_get_timeseries_metadata_all(reader, &map));
    TimeseriesMetadata& tm = map.entries[0].timeseries[0];
    ASSERT_STREQ(m_b, tm.measurement_name);
    ASSERT_EQ(TS_DATATYPE_BOOLEAN, tm.data_type);
    TsFileStatisticBase* sb = tsfile_statistic_base(&tm.statistic);
    ASSERT_TRUE(sb->has_statistic);
    EXPECT_DOUBLE_EQ(2.0, tm.statistic.u.bool_s.sum);
    ASSERT_EQ(TS_DATATYPE_BOOLEAN, sb->type);
    EXPECT_TRUE(tm.statistic.u.bool_s.first_bool);
    EXPECT_TRUE(tm.statistic.u.bool_s.last_bool);

    tsfile_free_device_timeseries_metadata_map(&map);
    ASSERT_EQ(RET_OK, tsfile_reader_close(reader));
    free(m_b);
    remove(filename);
}

TEST_F(CWrapperMetadataTest, GetTimeseriesMetadataStringStatistic) {
    ERRNO code = RET_OK;
    const char* filename = "cwrapper_metadata_str.tsfile";
    remove(filename);

    const char* device = "root.sg.str";
    char* m_str = strdup("s_str");
    timeseries_schema sch{};
    sch.timeseries_name = m_str;
    sch.data_type = TS_DATATYPE_STRING;
    sch.encoding = TS_ENCODING_PLAIN;
    sch.compression = TS_COMPRESSION_UNCOMPRESSED;

    auto* writer = static_cast<void*>(
        _tsfile_writer_new(filename, 128 * 1024 * 1024, &code));
    ASSERT_EQ(RET_OK, code);
    ASSERT_EQ(RET_OK, _tsfile_writer_register_timeseries(writer, device, &sch));

    const char* vals[] = {"aa", "cc", "bb"};
    for (int row = 0; row < 3; row++) {
        auto* record = static_cast<TsRecord>(
            _ts_record_new(device, static_cast<int64_t>(row + 1), 1));
        ASSERT_EQ(RET_OK, _insert_data_into_ts_record_by_name_string_with_len(
                              record, m_str, vals[row],
                              static_cast<int>(std::strlen(vals[row]))));
        ASSERT_EQ(RET_OK, _tsfile_writer_write_ts_record(writer, record));
        _free_tsfile_ts_record(reinterpret_cast<TsRecord*>(&record));
    }
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(filename, &code);
    ASSERT_EQ(RET_OK, code);

    DeviceTimeseriesMetadataMap map{};
    ASSERT_EQ(RET_OK, tsfile_reader_get_timeseries_metadata_all(reader, &map));
    ASSERT_EQ(1u, map.device_count);
    TimeseriesMetadata& tm = map.entries[0].timeseries[0];
    ASSERT_STREQ(m_str, tm.measurement_name);
    ASSERT_EQ(TS_DATATYPE_STRING, tm.data_type);
    TsFileStatisticBase* sb = tsfile_statistic_base(&tm.statistic);
    ASSERT_TRUE(sb->has_statistic);
    ASSERT_EQ(TS_DATATYPE_STRING, sb->type);
    ASSERT_NE(nullptr, tm.statistic.u.string_s.str_min);
    ASSERT_NE(nullptr, tm.statistic.u.string_s.str_max);
    ASSERT_NE(nullptr, tm.statistic.u.string_s.str_first);
    ASSERT_NE(nullptr, tm.statistic.u.string_s.str_last);
    EXPECT_STREQ("aa", tm.statistic.u.string_s.str_min);
    EXPECT_STREQ("cc", tm.statistic.u.string_s.str_max);
    EXPECT_STREQ("aa", tm.statistic.u.string_s.str_first);
    EXPECT_STREQ("bb", tm.statistic.u.string_s.str_last);

    tsfile_free_device_timeseries_metadata_map(&map);
    ASSERT_EQ(RET_OK, tsfile_reader_close(reader));
    free(m_str);
    remove(filename);
}

TEST_F(CWrapperMetadataTest, GetTimeseriesMetadataNullDevicePath) {
    ERRNO code = RET_OK;
    const char* filename = "cwrapper_metadata_null_path.tsfile";
    remove(filename);

    auto* writer = static_cast<void*>(
        _tsfile_writer_new(filename, 128 * 1024 * 1024, &code));
    ASSERT_EQ(RET_OK, code);
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(filename, &code);
    ASSERT_EQ(RET_OK, code);

    DeviceID bad{};
    bad.path = nullptr;
    bad.table_name = nullptr;
    bad.segment_count = 0;
    bad.segments = nullptr;
    DeviceTimeseriesMetadataMap map{};
    EXPECT_EQ(RET_INVALID_ARG,
              tsfile_reader_get_timeseries_metadata_for_devices(reader, &bad, 1,
                                                                &map));

    ASSERT_EQ(RET_OK, tsfile_reader_close(reader));
    remove(filename);
}

TEST_F(CWrapperMetadataTest, GetTimeseriesMetadataInvalidArgs) {
    ERRNO code = RET_OK;
    const char* filename = "cwrapper_metadata_empty.tsfile";
    remove(filename);

    auto* writer = static_cast<void*>(
        _tsfile_writer_new(filename, 128 * 1024 * 1024, &code));
    ASSERT_EQ(RET_OK, code);
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(filename, &code);
    ASSERT_EQ(RET_OK, code);

    DeviceTimeseriesMetadataMap map{};
    EXPECT_NE(RET_OK, tsfile_reader_get_timeseries_metadata_all(nullptr, &map));
    EXPECT_NE(RET_OK,
              tsfile_reader_get_timeseries_metadata_all(reader, nullptr));

    ASSERT_EQ(RET_OK, tsfile_reader_close(reader));
    remove(filename);
}

}  // namespace cwrapper_metadata
