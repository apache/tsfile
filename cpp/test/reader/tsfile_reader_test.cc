/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include "reader/tsfile_reader.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <map>
#include <random>
#include <unordered_map>
#include <vector>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "file/tsfile_io_reader.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "reader/block/single_device_tsblock_reader.h"
#include "reader/filter/time_operator.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_series_scan_iterator.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;

class TsFileReaderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tsfile_writer_ = new TsFileWriter();
        libtsfile_init();
        file_name_ = std::string("tsfile_writer_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        EXPECT_EQ(tsfile_writer_->open(file_name_, flags, mode), common::E_OK);
    }

    void TearDown() override {
        delete tsfile_writer_;
        // remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
    TsFileWriter* tsfile_writer_ = nullptr;

   public:
    static std::string generate_random_string(int length) {
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

        return random_string;
    }

    static std::string field_to_string(storage::Field* value) {
        if (value->type_ == common::TEXT) {
            return std::string(value->value_.sval_);
        } else {
            std::stringstream ss;
            switch (value->type_) {
                case common::BOOLEAN:
                    ss << (value->value_.bval_ ? "true" : "false");
                    break;
                case common::INT32:
                    ss << value->value_.ival_;
                    break;
                case common::INT64:
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
    }
};

TEST_F(TsFileReaderTest, ResultSetMetadata) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    tsfile_writer_->register_timeseries(
        device_path, storage::MeasurementSchema(measurement_name, data_type,
                                                encoding, compression_type));

    for (int i = 0; i < 50000; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_path);
        record.add_point(measurement_name, (int32_t)i);
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<std::string> select_list = {"device1.temperature"};

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(select_list, 1622505600000, 1622505600000 + 50000 * 1000,
                       tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    std::shared_ptr<ResultSetMetadata> result_set_metadata =
        qds->get_metadata();
    ASSERT_EQ(result_set_metadata->get_column_type(1), INT64);
    ASSERT_EQ(result_set_metadata->get_column_name(1), "time");
    ASSERT_EQ(result_set_metadata->get_column_type(2), data_type);
    ASSERT_EQ(result_set_metadata->get_column_name(2),
              device_path + "." + measurement_name);
    reader.destroy_query_data_set(qds);
    reader.close();
}

TEST_F(TsFileReaderTest, GetAllDevice) {
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    for (size_t i = 0; i < 1024; i++) {
        tsfile_writer_->register_timeseries(
            "device.ln" + std::to_string(i),
            storage::MeasurementSchema(measurement_name, data_type, encoding,
                                       compression_type));
    }

    for (size_t i = 0; i < 1024; i++) {
        TsRecord record(1622505600000, "device.ln" + std::to_string(i));
        record.add_point(measurement_name, (int32_t)0);
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    auto devices = reader.get_all_devices("device");
    ASSERT_EQ(devices.size(), 1024);
    std::vector<std::shared_ptr<IDeviceID>> devices_name_expected;
    for (size_t i = 0; i < 1024; i++) {
        devices_name_expected.push_back(std::make_shared<StringArrayDeviceID>(
            "device.ln" + std::to_string(i)));
    }
    std::sort(devices_name_expected.begin(), devices_name_expected.end(),
              [](const std::shared_ptr<IDeviceID>& left_str,
                 const std::shared_ptr<IDeviceID>& right_str) {
                  return left_str->operator<(*right_str);
              });

    for (size_t i = 0; i < devices.size(); i++) {
        ASSERT_TRUE(devices[i]->operator==(*devices_name_expected[i]));
    }
}

TEST_F(TsFileReaderTest, GetTimeseriesSchema) {
    std::vector<std::string> device_path = {"device.ln1", "device.ln2 "};
    std::vector<std::string> measurement_name = {"temperature", "humidity"};
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    tsfile_writer_->register_timeseries(
        device_path[0],
        storage::MeasurementSchema(measurement_name[0], data_type, encoding,
                                   compression_type));
    tsfile_writer_->register_timeseries(
        device_path[1],
        storage::MeasurementSchema(measurement_name[1], data_type, encoding,
                                   compression_type));
    TsRecord record_0(1622505600000, device_path[0]);
    record_0.add_point(measurement_name[0], (int32_t)0);
    TsRecord record_1(1622505600000, device_path[1]);
    record_1.add_point(measurement_name[1], (int32_t)1);
    ASSERT_EQ(tsfile_writer_->write_record(record_0), E_OK);
    ASSERT_EQ(tsfile_writer_->write_record(record_1), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    std::vector<MeasurementSchema> measurement_schemas;
    reader.get_timeseries_schema(
        std::make_shared<StringArrayDeviceID>(device_path[0]),
        measurement_schemas);
    ASSERT_EQ(measurement_schemas[0].measurement_name_, measurement_name[0]);
    ASSERT_EQ(measurement_schemas[0].data_type_, TSDataType::INT32);

    reader.get_timeseries_schema(
        std::make_shared<StringArrayDeviceID>(device_path[1]),
        measurement_schemas);
    ASSERT_EQ(measurement_schemas[1].measurement_name_, measurement_name[1]);
    ASSERT_EQ(measurement_schemas[1].data_type_, TSDataType::INT32);

    std::vector<std::shared_ptr<IDeviceID>> one_device = {
        std::make_shared<StringArrayDeviceID>(device_path[0])};
    auto one_meta = reader.get_timeseries_metadata(one_device);
    ASSERT_EQ(one_meta.size(), 1u);
    auto timeseries_list = one_meta.begin()->second;
    ASSERT_EQ(timeseries_list.size(), 1u);
    ASSERT_EQ(timeseries_list[0]->get_measurement_name().to_std_string(),
              measurement_name[0]);
    ASSERT_EQ(timeseries_list[0]->get_statistic()->start_time_, 1622505600000);
    ASSERT_EQ(timeseries_list[0]->get_statistic()->end_time_, 1622505600000);
    ASSERT_EQ(timeseries_list[0]->get_statistic()->count_, 1);

    auto device_timeseries_map = reader.get_timeseries_metadata();
    ASSERT_EQ(device_timeseries_map.size(), 2u);
    auto device_timeseries_1 = device_timeseries_map.at(
        std::make_shared<StringArrayDeviceID>(device_path[1]));
    ASSERT_EQ(device_timeseries_1.size(), 1u);
    ASSERT_EQ(device_timeseries_1[0]->get_measurement_name().to_std_string(),
              measurement_name[1]);
    ASSERT_EQ(device_timeseries_1[0]->get_statistic()->start_time_,
              1622505600000);
    ASSERT_EQ(device_timeseries_1[0]->get_statistic()->end_time_,
              1622505600000);
    ASSERT_EQ(device_timeseries_1[0]->get_statistic()->count_, 1);
    reader.close();
}

TEST_F(TsFileReaderTest, GetTimeseriesMetadataTableModelTypeAndDeviceFilter) {
    std::vector<MeasurementSchema*> measurement_schemas = {
        new MeasurementSchema("deviceid1", TSDataType::STRING),
        new MeasurementSchema("deviceid2", TSDataType::STRING),
        new MeasurementSchema("temperature", TSDataType::FLOAT),
        new MeasurementSchema("pressure", TSDataType::DOUBLE),
        new MeasurementSchema("humidity", TSDataType::INT32)};
    std::vector<ColumnCategory> column_categories = {
        ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD,
        ColumnCategory::FIELD, ColumnCategory::FIELD};
    auto table_schema = std::make_shared<TableSchema>(
        "testtable", measurement_schemas, column_categories);

    ASSERT_EQ(tsfile_writer_->register_table(table_schema), E_OK);

    Tablet tablet(table_schema->get_table_name(),
                  table_schema->get_measurement_names(),
                  table_schema->get_data_types(),
                  table_schema->get_column_categories(), 10);
    for (int row = 0; row < 5; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, row), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid1", "device_a"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid2", "device_b"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "temperature", static_cast<float>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "pressure", static_cast<double>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "humidity", static_cast<int32_t>(row)),
                  E_OK);
    }
    for (int row = 5; row < 10; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, row), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid1", "device_b"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid2", "device_a"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "temperature", static_cast<float>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "pressure", static_cast<double>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "humidity", static_cast<int32_t>(row)),
                  E_OK);
    }

    // Append one row whose middle TAG segment is null.
    Tablet null_tag_tablet(table_schema->get_table_name(),
                           table_schema->get_measurement_names(),
                           table_schema->get_data_types(),
                           table_schema->get_column_categories(), 1);
    int64_t null_tag_ts[1] = {10};
    int32_t null_tag_humidity[1] = {10};
    float null_tag_temperature[1] = {10.0F};
    double null_tag_pressure[1] = {10.0};
    // deviceid1 = null
    int32_t id1_offsets[2] = {0, 0};
    uint8_t id1_bitmap[1] = {0x01};  // row0 is null
    // deviceid2 = "device_b"
    int32_t id2_offsets[2] = {0, 8};
    const char id2_data[] = "device_b";
    ASSERT_EQ(null_tag_tablet.set_timestamps(null_tag_ts, 1), E_OK);
    ASSERT_EQ(null_tag_tablet.set_column_string_values(0, id1_offsets, "",
                                                       id1_bitmap, 1),
              E_OK);
    ASSERT_EQ(null_tag_tablet.set_column_string_values(1, id2_offsets, id2_data,
                                                       nullptr, 1),
              E_OK);
    ASSERT_EQ(
        null_tag_tablet.set_column_values(2, null_tag_temperature, nullptr, 1),
        E_OK);
    ASSERT_EQ(
        null_tag_tablet.set_column_values(3, null_tag_pressure, nullptr, 1),
        E_OK);
    ASSERT_EQ(
        null_tag_tablet.set_column_values(4, null_tag_humidity, nullptr, 1),
        E_OK);

    ASSERT_EQ(tsfile_writer_->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->write_table(null_tag_tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), common::E_OK);

    auto all_meta = reader.get_timeseries_metadata();
    ASSERT_EQ(all_meta.size(), 3u);

    std::vector<std::string> selected_device_segments = {
        "testtable", "device_a", "device_b"};
    std::vector<std::shared_ptr<IDeviceID>> selected_devices = {
        std::make_shared<StringArrayDeviceID>(selected_device_segments)};
    auto selected_meta = reader.get_timeseries_metadata(selected_devices);
    ASSERT_EQ(selected_meta.size(), 1u);

    auto selected_list = selected_meta.begin()->second;
    std::unordered_map<std::string, TSDataType> type_by_measurement;
    for (const auto& index : selected_list) {
        type_by_measurement[index->get_measurement_name().to_std_string()] =
            index->get_data_type();
    }
    ASSERT_EQ(type_by_measurement.at("temperature"), TSDataType::FLOAT);
    ASSERT_EQ(type_by_measurement.at("pressure"), TSDataType::DOUBLE);
    ASSERT_EQ(type_by_measurement.at("humidity"), TSDataType::INT32);

    // Query metadata for the device with null middle TAG segment.
    std::vector<std::string*> null_seg_device = {
        new std::string("testtable"), nullptr, new std::string("device_b")};
    std::vector<std::shared_ptr<IDeviceID>> null_seg_devices = {
        std::make_shared<StringArrayDeviceID>(null_seg_device)};
    for (auto* seg : null_seg_device) {
        if (seg != nullptr) {
            delete seg;
        }
    }
    auto null_seg_meta = reader.get_timeseries_metadata(null_seg_devices);
    ASSERT_EQ(null_seg_meta.size(), 1u);
    auto null_seg_list = null_seg_meta.begin()->second;
    ASSERT_EQ(null_seg_list.size(), 3u);
    std::unordered_map<std::string, TSDataType> null_seg_type_by_measurement;
    for (const auto& index : null_seg_list) {
        null_seg_type_by_measurement[index->get_measurement_name()
                                         .to_std_string()] =
            index->get_data_type();
    }
    ASSERT_EQ(null_seg_type_by_measurement.at("temperature"),
              TSDataType::FLOAT);
    ASSERT_EQ(null_seg_type_by_measurement.at("pressure"), TSDataType::DOUBLE);
    ASSERT_EQ(null_seg_type_by_measurement.at("humidity"), TSDataType::INT32);

    reader.close();
}

static const int64_t kLargeFileNumRecords = 300000000;
static const int64_t kLargeFileFlushBatch = 100000;

TEST_F(TsFileReaderTest,
       DISABLED_LargeFileNoEncodingNoCompression_WriteAndRead) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT64;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    tsfile_writer_->register_timeseries(
        device_path, storage::MeasurementSchema(measurement_name, data_type,
                                                encoding, compression_type));

    const int64_t start_time = 1622505600000LL;
    for (int64_t i = 0; i < kLargeFileNumRecords; ++i) {
        TsRecord record(start_time + i * 1000, device_path);
        record.add_point(measurement_name, static_cast<int64_t>(i));
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
        if ((i + 1) % kLargeFileFlushBatch == 0) {
            ASSERT_EQ(tsfile_writer_->flush(), E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<std::string> select_list = {"device1.temperature"};
    const int64_t end_time = start_time + (kLargeFileNumRecords - 1) * 1000 + 1;

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    storage::ResultSet* tmp_qds = nullptr;
    ret = reader.query(select_list, start_time, end_time, tmp_qds);
    ASSERT_EQ(ret, common::E_OK);
    ASSERT_NE(tmp_qds, nullptr);

    auto* qds = static_cast<QDSWithoutTimeGenerator*>(tmp_qds);
    std::shared_ptr<ResultSetMetadata> meta = qds->get_metadata();
    ASSERT_NE(meta, nullptr);
    ASSERT_EQ(meta->get_column_type(1), INT64);
    ASSERT_EQ(meta->get_column_type(2), INT64);

    int64_t row_count = 0;
    bool has_next = false;

    while (true) {
        ret = qds->next(has_next);
        ASSERT_EQ(ret, common::E_OK);
        if (!has_next) break;
        row_count++;
    }

    ASSERT_EQ(row_count, kLargeFileNumRecords);

    reader.destroy_query_data_set(qds);
    reader.close();
}

// Multi-value aligned chunk reader doesn't honour row_offset / row_limit /
// min_time_hint pushdown — silently dropping those args would hand the caller
// full-chunk data when it asked for a sub-range.  The guard at the top of
// AlignedChunkReader::get_next_page must turn the unsupported combination
// into an explicit E_NOT_SUPPORT.
TEST_F(TsFileReaderTest, MultiValueAlignedRowOffsetReturnsNotSupport) {
    const std::string device = "root.dev_multi_offset";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v1", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg),
                  E_OK);
    }
    const int N = 32;
    Tablet tablet(device,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  N);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(tablet.add_timestamp(i, 1000 + i), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<int64_t>(i * 2)), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileIOReader io_reader;
    ASSERT_EQ(io_reader.init(file_name_), E_OK);

    auto device_id = std::make_shared<StringArrayDeviceID>(device);
    std::vector<std::string> measurements = {"v0", "v1"};
    storage::TsFileSeriesScanIterator* ssi = nullptr;
    common::PageArena pa;
    pa.init(512, common::MOD_DEFAULT);
    ASSERT_EQ(io_reader.alloc_multi_ssi(device_id, measurements, ssi, pa,
                                        /*time_filter=*/nullptr),
              E_OK);
    ASSERT_NE(ssi, nullptr);

    // row_offset > 0 hits the multi-value guard at the top of
    // AlignedChunkReader::get_next_page; the SSI propagates the error code.
    ssi->set_row_range(/*offset=*/5, /*limit=*/-1);
    common::TsBlock* block = nullptr;
    EXPECT_EQ(ssi->get_next(block, /*alloc_tsblock=*/true),
              common::E_NOT_SUPPORT);

    if (block != nullptr) {
        ssi->revert_tsblock();
    }
    io_reader.revert_ssi(ssi);
    // RAII handles io_reader teardown — explicit reset() would destroy the
    // tsfile_meta page arena while tsfile_meta_ still holds shared_ptrs into
    // it, then ~TsFileMeta would call self_deleter on freed memory.
}

namespace storage {
// Subclass that lets the test (a) inject an error from the next-tsblock load
// and (b) wire a manually constructed TsBlock into the inherited iterator
// fields, so we can exercise the end-of-block branch of skip_rows()
// deterministically.  The base destructor calls revert_ssi(nullptr), which
// short-circuits safely; we hand it a default-constructed (never-init'd)
// TsFileIOReader purely to satisfy the constructor.
class FaultySingleMeasurementColumnContext
    : public SingleMeasurementColumnContext {
   public:
    using SingleMeasurementColumnContext::SingleMeasurementColumnContext;
    int get_next_tsblock_ret_ = common::E_OK;
    int get_next_tsblock_calls_ = 0;
    int get_next_tsblock(bool /*alloc_mem*/) override {
        ++get_next_tsblock_calls_;
        return get_next_tsblock_ret_;
    }
    void prime_iters_for_block(common::TsBlock* tsb) {
        tsblock_ = tsb;
        time_iter_ = new common::ColIterator(0, tsb);
        value_iter_ = new common::ColIterator(1, tsb);
    }
};
}  // namespace storage

// Regression: skip_rows() used to be a void method that called
// get_next_tsblock(false) for its side effects when the current block ran
// out.  An IO/decode error from that call was silently swallowed and the
// outer reader treated the source as exhausted, returning fewer rows than
// requested with no error indication.  skip_rows() now returns int and must
// surface hard errors (E_NO_MORE_DATA is the legitimate EOF and stays
// suppressed).
TEST_F(TsFileReaderTest,
       SingleMeasurementSkipRowsPropagatesGetNextTsBlockError) {
    common::TupleDesc desc;
    desc.push_back(common::ColumnSchema("time", common::INT64,
                                        common::UNCOMPRESSED, common::PLAIN));
    desc.push_back(common::ColumnSchema("v0", common::INT64,
                                        common::UNCOMPRESSED, common::PLAIN));
    common::TsBlock tsb(&desc, 4);
    ASSERT_EQ(tsb.init(), common::E_OK);
    common::RowAppender ra(&tsb);
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(ra.add_row());
        int64_t t = 1000 + i;
        int64_t v = i;
        ra.append(0, reinterpret_cast<const char*>(&t), sizeof(int64_t));
        ra.append(1, reinterpret_cast<const char*>(&v), sizeof(int64_t));
    }

    storage::TsFileIOReader io_reader_stub;
    storage::FaultySingleMeasurementColumnContext ctx(&io_reader_stub);
    ctx.prime_iters_for_block(&tsb);

    // Hard error: skip_rows must propagate.
    ctx.get_next_tsblock_ret_ = common::E_INVALID_ARG;
    EXPECT_EQ(ctx.skip_rows(2), common::E_INVALID_ARG);
    EXPECT_EQ(ctx.get_next_tsblock_calls_, 1);
}

TEST_F(TsFileReaderTest, SingleMeasurementSkipRowsSwallowsEndOfStream) {
    common::TupleDesc desc;
    desc.push_back(common::ColumnSchema("time", common::INT64,
                                        common::UNCOMPRESSED, common::PLAIN));
    desc.push_back(common::ColumnSchema("v0", common::INT64,
                                        common::UNCOMPRESSED, common::PLAIN));
    common::TsBlock tsb(&desc, 4);
    ASSERT_EQ(tsb.init(), common::E_OK);
    common::RowAppender ra(&tsb);
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(ra.add_row());
        int64_t t = 1000 + i;
        int64_t v = i;
        ra.append(0, reinterpret_cast<const char*>(&t), sizeof(int64_t));
        ra.append(1, reinterpret_cast<const char*>(&v), sizeof(int64_t));
    }

    storage::TsFileIOReader io_reader_stub;
    storage::FaultySingleMeasurementColumnContext ctx(&io_reader_stub);
    ctx.prime_iters_for_block(&tsb);

    // EOF: skip_rows must squash to E_OK so the outer loop notices via
    // available_rows() instead of bubbling the EOF up as a query failure.
    ctx.get_next_tsblock_ret_ = common::E_NO_MORE_DATA;
    EXPECT_EQ(ctx.skip_rows(2), common::E_OK);
    EXPECT_EQ(ctx.get_next_tsblock_calls_, 1);
}

// Regression: the multi-value aligned batch loop required the destination
// TsBlock to have >= BATCH (=129) rows of free capacity, otherwise it
// returned E_OVERFLOW immediately and the SSI surfaced that error to the
// caller.  When tsblock_max_memory_ is small enough to land max_row_count_
// below 129 (e.g. very small per-block memory in low-RAM configs) no rows
// could ever be decoded.  The fix caps the batch by remaining capacity,
// matching ChunkReader's per-type batch loops.
TEST_F(TsFileReaderTest, MultiValueAlignedProgressesWithSmallTsBlock) {
    const std::string device = "root.dev_multi_small_block";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v1", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg),
                  E_OK);
    }
    const int N = 200;  // > BATCH (129) so the batch loop iterates twice
    Tablet tablet(device,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  N);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(tablet.add_timestamp(i, 1000 + i), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<int64_t>(i * 2)), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    // Force max_row_count_ below BATCH: ~2 KB / 24 B per row → ~85 rows.
    // Also force the multi_DECODE_TV_BATCH path by disabling parallel reads:
    // with a thread pool the chunk-level pre-decode shortcut would otherwise
    // run for any multi-column query (no upper column-count cutoff anymore).
    uint32_t prev_capacity = common::g_config_value_.tsblock_max_memory_;
    bool prev_parallel = common::g_config_value_.parallel_read_enabled_;
    struct Guard {
        uint32_t cap;
        bool par;
        ~Guard() {
            common::g_config_value_.tsblock_max_memory_ = cap;
            common::g_config_value_.parallel_read_enabled_ = par;
        }
    } guard{prev_capacity, prev_parallel};
    common::g_config_value_.tsblock_max_memory_ = 2048;
    common::g_config_value_.parallel_read_enabled_ = false;

    storage::TsFileIOReader io_reader;
    ASSERT_EQ(io_reader.init(file_name_), E_OK);

    auto device_id = std::make_shared<StringArrayDeviceID>(device);
    std::vector<std::string> measurements = {"v0", "v1"};
    storage::TsFileSeriesScanIterator* ssi = nullptr;
    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    ASSERT_EQ(io_reader.alloc_multi_ssi(device_id, measurements, ssi, pa,
                                        /*time_filter=*/nullptr),
              E_OK);
    ASSERT_NE(ssi, nullptr);

    int collected = 0;
    while (true) {
        common::TsBlock* block = nullptr;
        int ret = ssi->get_next(block, /*alloc_tsblock=*/true);
        if (ret == common::E_NO_MORE_DATA) break;
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_NE(block, nullptr);
        ASSERT_GT(block->get_max_row_count(), 0u);
        ASSERT_LT(block->get_max_row_count(), 129u);
        collected += static_cast<int>(block->get_row_count());
        ssi->revert_tsblock();
    }
    EXPECT_EQ(collected, N);

    io_reader.revert_ssi(ssi);
}

// Regression: when a whole batch is filtered out, multi_DECODE_TV_BATCH skips
// the non-null value bytes for each column.  The old code ignored the skip
// return code and the `skipped` count, so a short/truncated page could leave
// the decoder mid-value; subsequent batches would then read garbage bytes as
// values.  This test exercises an intact page: the filter rejects rows
// 0..127 (one full batch worth), then the rows after must come back with
// their *correct* values — proving the decoder advanced exactly nonnull_count
// values, not some smaller number that would shift the value alignment.
TEST_F(TsFileReaderTest, MultiValueAlignedSkipsBatchPreservesValueAlignment) {
    const std::string device = "root.dev_multi_skip_align";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v1", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg),
                  E_OK);
    }
    // Two batches' worth of rows so the filter skips the first batch entirely
    // and decodes the second.
    const int N = 200;
    Tablet tablet(device,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  N);
    for (int i = 0; i < N; ++i) {
        // Distinctive value pattern: i and 1000000 + i.  If skip
        // mis-advances the decoder by even one value, the v0/v1 read after
        // the skip will land on the wrong row's bytes.
        ASSERT_EQ(tablet.add_timestamp(i, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<int64_t>(1000000 + i)),
                  E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    bool prev_parallel = common::g_config_value_.parallel_read_enabled_;
    struct Guard {
        bool par;
        ~Guard() { common::g_config_value_.parallel_read_enabled_ = par; }
    } guard{prev_parallel};
    // Force the multi_DECODE_TV_BATCH path (the chunk-level shortcut would
    // bypass the skip branch we want to exercise).
    common::g_config_value_.parallel_read_enabled_ = false;

    storage::TsFileIOReader io_reader;
    ASSERT_EQ(io_reader.init(file_name_), E_OK);

    auto device_id = std::make_shared<StringArrayDeviceID>(device);
    std::vector<std::string> measurements = {"v0", "v1"};
    storage::TsFileSeriesScanIterator* ssi = nullptr;
    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);

    // TimeIn filter selecting only rows 130..139 — entirely past the first
    // 129-row batch, so the first batch hits the pass_count==0 skip branch
    // for both value columns.
    std::vector<int64_t> want;
    for (int i = 130; i < 140; ++i) want.push_back(i);
    storage::TimeIn time_filter(want, /*not_in=*/false);

    ASSERT_EQ(io_reader.alloc_multi_ssi(device_id, measurements, ssi, pa,
                                        &time_filter),
              E_OK);
    ASSERT_NE(ssi, nullptr);

    std::vector<std::pair<int64_t, int64_t>> got;
    while (true) {
        common::TsBlock* block = nullptr;
        int ret = ssi->get_next(block, /*alloc_tsblock=*/true, &time_filter);
        if (ret == common::E_NO_MORE_DATA) break;
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_NE(block, nullptr);
        // Columns: time, v0, v1.
        common::ColIterator t_iter(0, block);
        common::ColIterator v0_iter(1, block);
        common::ColIterator v1_iter(2, block);
        const uint32_t rows = block->get_row_count();
        for (uint32_t r = 0; r < rows; ++r) {
            uint32_t len = 0;
            int64_t t = *reinterpret_cast<int64_t*>(t_iter.read(&len));
            int64_t v0 = *reinterpret_cast<int64_t*>(v0_iter.read(&len));
            int64_t v1 = *reinterpret_cast<int64_t*>(v1_iter.read(&len));
            got.push_back({t, v0});
            // The decoder must have advanced exactly nonnull_count values
            // when it skipped batch #1.  If it under-advanced (the latent
            // bug), v1 would land on the wrong row's bytes here.
            EXPECT_EQ(v1, 1000000 + t);
            EXPECT_EQ(v0, t);
            t_iter.next();
            v0_iter.next();
            v1_iter.next();
        }
        ssi->revert_tsblock();
    }

    ASSERT_EQ(got.size(), want.size());
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].first, want[i]);
        EXPECT_EQ(got[i].second, want[i]);
    }

    io_reader.revert_ssi(ssi);
}

// Coverage: an aligned read with > 6 value columns now takes the chunk-level
// parallel decode path (decode_all_planned_pages) exactly like the 2..6 column
// case — the old "<= 6 columns" dispatch cutoff that sent wide chunks down the
// per-page serial path is gone.  With libtsfile_init() having built the global
// pool and parallel_read_enabled_ on by default, an 8-column query exercises
// that path end-to-end; each column carries a disjoint value range so any
// cross-column misalignment in the wide chunk-level decode would be caught.
TEST_F(TsFileReaderTest, MultiValueAlignedWideChunkParallelDecode) {
    const std::string device = "root.dev_multi_wide";
    const uint32_t kCols = 8;  // > 6: previously bypassed the chunk-level path
    std::vector<MeasurementSchema> schema_vec;
    for (uint32_t c = 0; c < kCols; ++c) {
        schema_vec.emplace_back("v" + std::to_string(c), INT64, PLAIN,
                                UNCOMPRESSED);
    }
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg),
                  E_OK);
    }
    const int N = 200;  // > BATCH (129) so the decode loop iterates more once
    Tablet tablet(device,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  N);
    // Row i, column c carries c * 1000000 + i so each column's values occupy a
    // disjoint range; a wide-chunk decode that crossed column boundaries would
    // surface as a value landing in the wrong column's range.
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(tablet.add_timestamp(i, 1000 + i), E_OK);
        for (uint32_t c = 0; c < kCols; ++c) {
            ASSERT_EQ(
                tablet.add_value(i, c, static_cast<int64_t>(c * 1000000 + i)),
                E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    // parallel_read_enabled_ defaults to true and SetUp() ran libtsfile_init(),
    // so the SSI hands the AlignedChunkReader the global pool; with 8 value
    // columns (> 1) the reader takes the chunk-level decode path.
    ASSERT_TRUE(common::g_config_value_.parallel_read_enabled_);

    storage::TsFileIOReader io_reader;
    ASSERT_EQ(io_reader.init(file_name_), E_OK);

    auto device_id = std::make_shared<StringArrayDeviceID>(device);
    std::vector<std::string> measurements;
    for (uint32_t c = 0; c < kCols; ++c)
        measurements.push_back("v" + std::to_string(c));
    storage::TsFileSeriesScanIterator* ssi = nullptr;
    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    ASSERT_EQ(io_reader.alloc_multi_ssi(device_id, measurements, ssi, pa,
                                        /*time_filter=*/nullptr),
              E_OK);
    ASSERT_NE(ssi, nullptr);

    int collected = 0;
    while (true) {
        common::TsBlock* block = nullptr;
        int ret = ssi->get_next(block, /*alloc_tsblock=*/true);
        if (ret == common::E_NO_MORE_DATA) break;
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_NE(block, nullptr);
        const uint32_t rows = block->get_row_count();

        common::ColIterator t_iter(0, block);
        std::vector<int64_t> times;
        times.reserve(rows);
        for (uint32_t r = 0; r < rows; ++r) {
            uint32_t len = 0;
            times.push_back(*reinterpret_cast<int64_t*>(t_iter.read(&len)));
            t_iter.next();
        }
        // One independent iterator per value column so we never rely on
        // vector<ColIterator> being movable.
        for (uint32_t c = 0; c < kCols; ++c) {
            common::ColIterator it(c + 1, block);
            for (uint32_t r = 0; r < rows; ++r) {
                uint32_t len = 0;
                int64_t v = *reinterpret_cast<int64_t*>(it.read(&len));
                int64_t i = times[r] - 1000;  // timestamp == 1000 + i
                EXPECT_EQ(v, static_cast<int64_t>(c) * 1000000 + i);
                it.next();
            }
        }
        collected += static_cast<int>(rows);
        ssi->revert_tsblock();
    }
    EXPECT_EQ(collected, N);

    io_reader.revert_ssi(ssi);
}

// Regression: AlignedTimeseriesIndex::get_data_type() returns the time column
// type (VECTOR), which the schema accessor used to surface verbatim — every
// aligned column came back as VECTOR instead of its real INT32/FLOAT/etc.
// type.  get_timeseries_schema() now unwraps AlignedTimeseriesIndex to read
// value_ts_idx_->get_data_type() like the develop branch did.
TEST_F(TsFileReaderTest, AlignedSchemaReportsValueDataType) {
    const std::string device = "root.dev_aligned_schema";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v_i32", INT32, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v_dbl", DOUBLE, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg),
                  E_OK);
    }
    const int N = 8;
    Tablet tablet(device,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  N);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(tablet.add_timestamp(i, 1000 + i), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int32_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<double>(i) * 0.5), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);

    auto device_id = std::make_shared<StringArrayDeviceID>(device);
    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(reader.get_timeseries_schema(device_id, schemas), E_OK);
    ASSERT_EQ(schemas.size(), 2u);

    // Match by name — IO reader iteration order isn't part of the contract.
    common::TSDataType i32_type = common::INVALID_DATATYPE;
    common::TSDataType dbl_type = common::INVALID_DATATYPE;
    for (const auto& s : schemas) {
        if (s.measurement_name_ == "v_i32") i32_type = s.data_type_;
        if (s.measurement_name_ == "v_dbl") dbl_type = s.data_type_;
    }
    EXPECT_EQ(i32_type, INT32);
    EXPECT_EQ(dbl_type, DOUBLE);
    reader.close();
}

namespace storage {
class TsFileReaderMetaArenaTest {
   public:
    static int64_t arena_used(const storage::TsFileReader& r) {
        return r.tsfile_reader_meta_pa_.get_total_used_bytes();
    }
};
}  // namespace storage

// Regression: tsfile_reader_meta_pa_ used to be re-initialised at the start
// of each get_timeseries_metadata() call.  When that reset was removed,
// every call accumulated another copy of the per-device meta into the same
// arena, so a long-lived reader that polled metadata kept growing memory
// without bound.  Re-init now happens at the top of both overloads; verify
// arena usage stays flat across repeated calls instead of growing linearly.
TEST_F(TsFileReaderTest, RepeatedGetTimeseriesMetadataDoesNotLeakArena) {
    const std::string device = "root.dev_arena_growth";
    {
        std::vector<MeasurementSchema*> reg;
        reg.push_back(new MeasurementSchema("v0", INT64, PLAIN, UNCOMPRESSED));
        ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg),
                  E_OK);
    }
    TsRecord r(1000, device);
    r.points_.emplace_back("v0", static_cast<int64_t>(0));
    ASSERT_EQ(tsfile_writer_->write_record_aligned(r), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    std::vector<std::shared_ptr<IDeviceID>> ids = {
        std::make_shared<StringArrayDeviceID>(device)};

    // Prime the arena and capture the steady-state size.
    (void)reader.get_timeseries_metadata(ids);
    const int64_t after_one =
        storage::TsFileReaderMetaArenaTest::arena_used(reader);
    ASSERT_GT(after_one, 0);

    for (int i = 0; i < 10; ++i) {
        (void)reader.get_timeseries_metadata(ids);
    }
    const int64_t after_eleven =
        storage::TsFileReaderMetaArenaTest::arena_used(reader);
    // Without the fix, after_eleven ≈ 11 × after_one.  With the fix it
    // should equal after_one (arena reset before each call).  Allow a small
    // slack for arena page rounding, but reject anything close to 2× growth.
    EXPECT_LT(after_eleven, after_one * 2)
        << "arena grew from " << after_one << " to " << after_eleven
        << " across 11 calls — reset on entry is missing";
    reader.close();
}
