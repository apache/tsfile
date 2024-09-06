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
#include "file/open_file.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace storage {

class OpenFileTest : public ::testing::Test {
   protected:
    void SetUp() override {
        open_file_ = OpenFileFactory::alloc();
        EXPECT_EQ(open_file_->init(), common::E_OK);
    }

    void TearDown() override {
        open_file_->reset();
        OpenFileFactory::free(open_file_);
    }

    OpenFile* open_file_;
};

TEST_F(OpenFileTest, SetFileIdAndPath) {
    common::FileID file_id;
    file_id.seq_ = 1;
    std::string file_path = "/path/to/file";
    open_file_->set_file_id_and_path(file_id, file_path);

    EXPECT_EQ(open_file_->get_file_id(), file_id);
    EXPECT_EQ(open_file_->get_file_path(), file_path);
}

TEST_F(OpenFileTest, BuildFrom) {
    std::vector<TimeseriesTimeIndexEntry> time_index_vec = {
        {common::TsID(1, 1, 1), TimeRange{10, 20}},
        {common::TsID(2, 2, 2), TimeRange{30, 40}}};

    EXPECT_EQ(open_file_->build_from(time_index_vec), common::E_OK);

    TimeRange time_range;
    EXPECT_EQ(open_file_->get_time_range(common::TsID(1, 1, 1), time_range),
              common::E_OK);
    EXPECT_EQ(time_range.start_time_, 10);
    EXPECT_EQ(time_range.end_time_, 20);

    EXPECT_EQ(open_file_->get_time_range(common::TsID(2, 2, 2), time_range),
              common::E_OK);
    EXPECT_EQ(time_range.start_time_, 30);
    EXPECT_EQ(time_range.end_time_, 40);
}

TEST_F(OpenFileTest, Add) {
    TimeRange time_range{50, 60};
    common::TsID ts_id(1, 1, 1);

    EXPECT_EQ(open_file_->add(ts_id, time_range), common::E_OK);

    TimeRange ret_time_range;
    EXPECT_EQ(open_file_->get_time_range(ts_id, ret_time_range), common::E_OK);
    EXPECT_EQ(ret_time_range.start_time_, 50);
    EXPECT_EQ(ret_time_range.end_time_, 60);
}

TEST_F(OpenFileTest, ContainTimeseries) {
    TimeRange time_range{90, 100};
    common::TsID ts_id(1, 1, 1);

    open_file_->add(ts_id, time_range);
    EXPECT_TRUE(open_file_->contain_timeseries(ts_id));
    EXPECT_FALSE(open_file_->contain_timeseries(common::TsID(2, 2, 2)));
}

TEST_F(OpenFileTest, GetTimeRange) {
    TimeRange time_range{110, 120};
    common::TsID ts_id(1, 1, 1);

    open_file_->add(ts_id, time_range);

    TimeRange ret_time_range;
    EXPECT_EQ(open_file_->get_time_range(ts_id, ret_time_range), common::E_OK);
    EXPECT_EQ(ret_time_range.start_time_, 110);
    EXPECT_EQ(ret_time_range.end_time_, 120);

    EXPECT_EQ(open_file_->get_time_range(common::TsID(2, 2, 2), ret_time_range),
              common::E_NOT_EXIST);
}

}  // namespace storage