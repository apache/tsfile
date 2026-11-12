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

#include <fcntl.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/device_id.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

namespace storage {

/**
 * The reader keeps a bounded cache of deserialized index nodes, so repeated
 * device lookups reuse the node instead of re-reading and re-deserializing it.
 */
class IndexNodeCacheTest : public ::testing::Test {
   protected:
    static constexpr int kDevices = 8;
    static constexpr int kRowsPerDevice = 2;

    void SetUp() override {
        storage::libtsfile_init();
        saved_degree_ = common::g_config_value_.max_degree_of_index_node_;
        // A small degree forces several index-node levels for kDevices devices,
        // so the walk in search_from_internal_node() actually reads nodes.
        ASSERT_EQ(storage::set_max_degree_of_index_node(2), common::E_OK);
        file_name_ =
            "index_node_cache_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
        ASSERT_NO_FATAL_FAILURE(WriteFile_());
    }

    void TearDown() override {
        storage::set_max_degree_of_index_node(saved_degree_);
        remove(file_name_.c_str());
    }

    void WriteFile_() {
        storage::WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(file_name_, flags, 0666), common::E_OK);
        std::vector<common::ColumnSchema> columns;
        columns.emplace_back("id0", common::STRING, common::UNCOMPRESSED,
                             common::PLAIN, common::ColumnCategory::TAG);
        columns.emplace_back("value", common::INT64, common::UNCOMPRESSED,
                             common::PLAIN, common::ColumnCategory::FIELD);
        storage::TableSchema schema("test", columns);
        storage::TsFileTableWriter writer(&file, &schema);
        storage::Tablet tablet(
            "test", schema.get_measurement_names(), schema.get_data_types(),
            schema.get_column_categories(), kDevices * kRowsPerDevice);
        for (int device = 0; device < kDevices; ++device) {
            for (int row = 0; row < kRowsPerDevice; ++row) {
                const int index = device * kRowsPerDevice + row;
                ASSERT_EQ(tablet.add_timestamp(index, row), common::E_OK);
                ASSERT_EQ(
                    tablet.add_value(index, "id0",
                                     ("d" + std::to_string(device)).c_str()),
                    common::E_OK);
                ASSERT_EQ(tablet.add_value(index, "value",
                                           static_cast<int64_t>(index)),
                          common::E_OK);
            }
        }
        ASSERT_EQ(writer.write_table(tablet), common::E_OK);
        ASSERT_EQ(writer.close(), common::E_OK);
    }

    int64_t saved_degree_ = 0;
    std::string file_name_;
};

TEST_F(IndexNodeCacheTest, ReusesNodesAcrossDeviceLookups) {
    storage::TsFileIOReader reader;
    ASSERT_EQ(reader.init(file_name_), common::E_OK);
    TsFileMeta* meta = nullptr;
    ASSERT_EQ(reader.get_tsfile_meta(meta), common::E_OK);
    ASSERT_NE(meta, nullptr);

    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    const std::unordered_set<std::string> measurement_names{"value"};

    // Two rounds over every device: the second round must be served from the
    // cache (no new node reads) and still produce the same indexes.
    size_t misses_after_first_round = 0;
    for (int round = 0; round < 2; ++round) {
        for (int device = 0; device < kDevices; ++device) {
            auto device_id = std::make_shared<StringArrayDeviceID>(
                "test.d" + std::to_string(device));
            std::vector<ITimeseriesIndex*> indexes(1, nullptr);
            ASSERT_EQ(reader.get_timeseries_indexes(
                          device_id, measurement_names, indexes, pa),
                      common::E_OK)
                << "device " << device << " round " << round;
            ASSERT_NE(indexes[0], nullptr)
                << "device " << device << " round " << round;
            pa.reset();
        }
        if (round == 0) {
            misses_after_first_round = reader.TEST_index_node_cache_misses();
            EXPECT_GT(misses_after_first_round, 0u);
        }
    }

    // A second pass over the same devices is served entirely from the cache.
    EXPECT_EQ(reader.TEST_index_node_cache_misses(), misses_after_first_round);
    EXPECT_GT(reader.TEST_index_node_cache_size(), 0u);
    EXPECT_LE(reader.TEST_index_node_cache_size(),
              reader.TEST_index_node_cache_capacity());
}

TEST_F(IndexNodeCacheTest, ResetClearsCache) {
    storage::TsFileIOReader reader;
    ASSERT_EQ(reader.init(file_name_), common::E_OK);
    TsFileMeta* meta = nullptr;
    ASSERT_EQ(reader.get_tsfile_meta(meta), common::E_OK);

    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    const std::unordered_set<std::string> measurement_names{"value"};
    auto device_id =
        std::make_shared<StringArrayDeviceID>(std::string("test.d0"));
    std::vector<ITimeseriesIndex*> indexes(1, nullptr);
    ASSERT_EQ(reader.get_timeseries_indexes(device_id, measurement_names,
                                            indexes, pa),
              common::E_OK);
    EXPECT_GT(reader.TEST_index_node_cache_size(), 0u);

    reader.reset();
    EXPECT_EQ(reader.TEST_index_node_cache_size(), 0u);
    pa.destroy();
}

}  // namespace storage
